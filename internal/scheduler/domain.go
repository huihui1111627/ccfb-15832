package scheduler

import (
	"sort"

	"toposcheduler/internal/model"
)

const DefaultTopologyKey = "topology.kubernetes.io/zone"

// SkewNotCalculated is used for domains excluded by selector or availability.
const SkewNotCalculated = -1

type DomainInfo struct {
	Key                 string
	EligibleDomains     []string
	Eligible            map[string]bool
	Counts              map[string]int
	Stats               []model.DomainStat
	Baseline            int
	BaselineForcedZero  bool
	EligibleDomainCount int
}

func topologyKey(w *model.Workload) string {
	if w != nil && w.Spread != nil && w.Spread.TopologyKey != "" {
		return w.Spread.TopologyKey
	}
	return DefaultTopologyKey
}

func NodeDomain(n *model.Node, key string) string {
	// Topology calculations intentionally use the declared topology label, not
	// the display Hangar field; a missing label makes the node ineligible.
	return n.Labels[key]
}

func MatchesSelector(labels, selector map[string]string) bool {
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// SelectionFailures contains node-selection/availability predicates that
// define whether a topology domain participates in skew calculation. Resource
// fit is intentionally separate: a currently resource-full but Ready node still
// makes its selector-matched topology domain eligible.
func SelectionFailures(n *model.Node, w *model.Workload) []string {
	var failures []string
	if !n.Ready {
		failures = append(failures, "node_not_ready")
	}
	if !n.Schedulable {
		failures = append(failures, "node_cordoned")
	}
	if !MatchesSelector(n.Labels, w.Selector) {
		failures = append(failures, "selector_mismatch")
	}
	key := topologyKey(w)
	if w.Spread != nil && n.Labels[key] == "" {
		failures = append(failures, "missing_topology_label")
	}
	return failures
}

func ResourceFailures(n *model.Node, w *model.Workload) []string {
	var failures []string
	free := n.Free()
	if free.CPUMillis < w.Request.CPUMillis {
		failures = append(failures, "insufficient_cpu")
	}
	if free.MemoryMB < w.Request.MemoryMB {
		failures = append(failures, "insufficient_memory")
	}
	return failures
}

// FilterNode runs selector, availability and resource Filter preconditions.
// The hard topology skew filter is added by PlanOne after domain statistics
// have been computed.
func FilterNode(n *model.Node, w *model.Workload) []string {
	failures := SelectionFailures(n, w)
	failures = append(failures, ResourceFailures(n, w)...)
	return failures
}

// InspectDomains computes eligible topology domains and the global minimum.
//
// A domain is eligible only when at least one node passes selector, readiness,
// schedulability and resource-fit filters. Domains excluded by the selector or
// containing no currently usable node do not participate in the minimum.
func InspectDomains(st *model.State, w *model.Workload, version string) DomainInfo {
	key := topologyKey(w)
	nodes := st.SortedNodes()

	domainNodes := map[string][]*model.Node{}
	domainOrder := []string{}
	for _, n := range nodes {
		d := NodeDomain(n, key)
		if _, ok := domainNodes[d]; !ok {
			domainOrder = append(domainOrder, d)
		}
		domainNodes[d] = append(domainNodes[d], n)
	}
	sort.Strings(domainOrder)

	info := DomainInfo{
		Key:             key,
		Eligible:        map[string]bool{},
		Counts:          map[string]int{},
		EligibleDomains: []string{},
	}

	for _, d := range domainOrder {
		ns := domainNodes[d]
		usable := 0
		selectorMatches := 0
		for _, n := range ns {
			if MatchesSelector(n.Labels, w.Selector) {
				selectorMatches++
			}
			if len(SelectionFailures(n, w)) == 0 {
				usable++
			}
		}
		if usable > 0 {
			info.Eligible[d] = true
			info.EligibleDomains = append(info.EligibleDomains, d)
		}

		reason := ""
		if usable == 0 {
			switch {
			case d == "":
				reason = "missing_topology_label"
			case selectorMatches == 0:
				reason = "selector_mismatch"
			default:
				reason = "no_available_node"
			}
		}

		info.Stats = append(info.Stats, model.DomainStat{
			Domain:    displayDomain(d),
			NodeCount: len(ns),
			Eligible:  info.Eligible[d],
			Reason:    reason,
			Skew:      SkewNotCalculated,
		})
	}

	// Count only the requested version. Old and new ReplicaSets during a
	// rolling update therefore do not influence each other.
	for _, r := range st.Replicas {
		if r.WorkloadID != w.ID || r.Version != version {
			continue
		}
		d := r.Domain
		if d == "" {
			if n := st.Node(r.NodeID); n != nil {
				d = NodeDomain(n, key)
			}
		}
		if _, ok := domainNodes[d]; !ok {
			// Replica may survive in a domain whose nodes were removed. Keep
			// the count visible, but it only participates if the domain is
			// present and eligible.
			domainNodes[d] = nil
		}
		info.Counts[d]++
	}

	info.EligibleDomainCount = len(info.EligibleDomains)
	if info.EligibleDomainCount > 0 {
		min := int(^uint(0) >> 1)
		for _, d := range info.EligibleDomains {
			c := info.Counts[d]
			if c < min {
				min = c
			}
		}
		info.Baseline = min
	}

	if w.Spread != nil && w.Spread.MinDomains > 0 &&
		info.EligibleDomainCount < w.Spread.MinDomains {
		// This also covers zero eligible domains; baseline is already zero,
		// but the explicit reason is useful in the UI and audit details.
		info.Baseline = 0
		info.BaselineForcedZero = true
	}

	for i := range info.Stats {
		rawDomain := domainOrder[i]
		info.Stats[i].ReplicaCount = info.Counts[rawDomain]
		if info.Stats[i].Eligible {
			info.Stats[i].Skew = info.Counts[rawDomain] - info.Baseline
		}
	}

	return info
}

func displayDomain(d string) string {
	if d == "" {
		return "(missing-topology-label)"
	}
	return d
}

// WorstSkew returns the largest current skew among eligible domains.
func (i DomainInfo) WorstSkew() int {
	worst := 0
	for _, s := range i.Stats {
		if s.Eligible && s.Skew > worst {
			worst = s.Skew
		}
	}
	return worst
}

// MaxSkewExceeded reports whether any eligible domain violates a hard maxSkew.
func (i DomainInfo) MaxSkewExceeded(maxSkew int) bool {
	for _, s := range i.Stats {
		if s.Eligible && s.Skew > maxSkew {
			return true
		}
	}
	return false
}

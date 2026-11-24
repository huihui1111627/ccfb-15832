package scheduler

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"toposcheduler/internal/model"
)

var (
	ErrWorkloadNotFound = errors.New("workload not found")
	ErrNoFeasibleNode   = errors.New("no feasible node")
	ErrNoReplica        = errors.New("no replica")
	ErrNoViolation      = errors.New("no hard topology violation")
	ErrMinAvailable     = errors.New("minAvailable prevents eviction")
)

const DefaultWeight = 1

func resourceWeight(w *model.Workload) int {
	if w.ResourceWeight > 0 {
		return w.ResourceWeight
	}
	return DefaultWeight
}

func spreadWeight(c *model.SpreadConstraint) int {
	if c != nil && c.Weight > 0 {
		return c.Weight
	}
	return DefaultWeight
}

// ResourceScore is the average percentage of free CPU and memory. More free
// resources produce a higher score.
func ResourceScore(n *model.Node) float64 {
	_, _, avg := n.FreeRatio()
	return math.Round(avg*10000) / 100 // two decimal places
}

// SpreadScore maps post-placement skew to [0,100], with strictly smaller skew
// receiving a strictly higher score for ordinary cluster sizes.
func SpreadScore(maxSkew, postSkew int) float64 {
	if maxSkew < 1 {
		maxSkew = 1
	}
	if postSkew < 0 {
		postSkew = 0
	}
	return 100 * float64(maxSkew) / (float64(maxSkew) + float64(postSkew))
}

// PlanOne computes Filter and Score details for every node and returns the
// chosen node without mutating state.
func PlanOne(st *model.State, w *model.Workload, version string) *model.SchedulingDecision {
	if version == "" {
		version = w.Version
	}
	info := InspectDomains(st, w, version)

	dec := &model.SchedulingDecision{
		WorkloadID:          w.ID,
		WorkloadName:        w.Name,
		Version:             version,
		TopologyKey:         info.Key,
		EligibleDomainCount: info.EligibleDomainCount,
		Baseline:            info.Baseline,
		BaselineForcedZero:  info.BaselineForcedZero,
		Domains:             append([]model.DomainStat(nil), info.Stats...),
	}
	if w.Spread != nil {
		dec.MaxSkew = w.Spread.MaxSkew
		dec.MinDomains = w.Spread.MinDomains
		dec.Hard = w.Spread.Hard
	}

	rw := resourceWeight(w)
	if w.Spread != nil && !w.Spread.Hard {
		dec.Hard = false
	}

	for _, n := range st.SortedNodes() {
		nd := &model.NodeDecision{
			NodeID:         n.ID,
			Hangar:         n.Hangar,
			ResourceWeight: rw,
		}

		selectionFailures := SelectionFailures(n, w)
		failures := FilterNode(n, w)
		domain := NodeDomain(n, info.Key)
		nd.Hangar = domain
		if n.Hangar != "" {
			nd.Hangar = n.Hangar
		}

		if w.Spread != nil {
			nd.EligibleDomainCount = info.EligibleDomainCount
			nd.Baseline = info.Baseline
			nd.BaselineForcedZero = info.BaselineForcedZero
			nd.MinDomains = w.Spread.MinDomains
			nd.MaxSkew = w.Spread.MaxSkew
			nd.CurrentDomainCount = info.Counts[domain]
			nd.PostSkew = nd.CurrentDomainCount + 1 - info.Baseline
			if info.BaselineForcedZero {
				nd.Baseline = 0
			}

			// Hard topology Filter. Soft constraints intentionally do not
			// append a failure and fall through to scoring. Evaluate topology
			// even when resources fail so the UI can show all filter reasons.
			if len(selectionFailures) == 0 && w.Spread.Hard &&
				nd.PostSkew > w.Spread.MaxSkew {
				failures = append(failures, fmt.Sprintf(
					"topology_skew_%d_gt_max_%d", nd.PostSkew, w.Spread.MaxSkew))
			}
		}

		nd.FilterFailures = failures
		nd.Eligible = len(failures) == 0
		nd.ResourceScore = ResourceScore(n)

		if nd.Eligible {
			nd.WeightedScore = nd.ResourceScore * float64(rw)
			if w.Spread != nil && !w.Spread.Hard {
				sw := spreadWeight(w.Spread)
				nd.SpreadScored = true
				nd.SpreadWeight = sw
				nd.SpreadScore = SpreadScore(w.Spread.MaxSkew, nd.PostSkew)
				nd.WeightedScore = nd.SpreadScore*float64(sw) +
					nd.ResourceScore*float64(rw)
			}
		}

		dec.Nodes = append(dec.Nodes, nd)
		if nd.Eligible {
			dec.FeasibleNodeIDs = append(dec.FeasibleNodeIDs, nd.NodeID)
		}
	}

	var selected *model.NodeDecision
	for _, nd := range dec.Nodes {
		if !nd.Eligible {
			continue
		}
		if selected == nil ||
			nd.WeightedScore > selected.WeightedScore ||
			(nd.WeightedScore == selected.WeightedScore && nd.NodeID < selected.NodeID) {
			selected = nd
		}
	}
	if selected != nil {
		selected.Selected = true
		dec.SelectedNodeID = selected.NodeID
	} else {
		dec.Err = ErrNoFeasibleNode.Error()
	}

	return dec
}

// ScheduleOne plans and atomically applies one replica placement.
func ScheduleOne(st *model.State, workloadID, version, batchID string, batchIndex int) (*model.SchedulingDecision, *model.Replica, error) {
	w := st.Workload(workloadID)
	if w == nil {
		return nil, nil, fmt.Errorf("%w: %s", ErrWorkloadNotFound, workloadID)
	}
	if version == "" {
		version = w.Version
	}

	dec := PlanOne(st, w, version)
	seq := st.NextID()
	dec.Seq = seq
	dec.ID = fmt.Sprintf("schedule-%05d", seq)
	dec.BatchID = batchID
	dec.BatchIndex = batchIndex
	dec.RequestedAtSeq = st.Seq
	st.Decisions = append(st.Decisions, dec)

	if dec.SelectedNodeID == "" {
		return dec, nil, ErrNoFeasibleNode
	}

	n := st.Node(dec.SelectedNodeID)
	repSeq := st.NextID()
	r := &model.Replica{
		ID:         fmt.Sprintf("rep-%05d", repSeq),
		WorkloadID: w.ID,
		Version:    version,
		NodeID:     n.ID,
		Domain:     NodeDomain(n, topologyKey(w)),
		Request:    w.Request,
		Available:  true,
		Phase:      model.PhaseRunning,
		CreatedSeq: repSeq,
	}
	n.Allocated = n.Allocated.Add(w.Request)
	st.Replicas = append(st.Replicas, r)
	dec.SelectedReplicaID = r.ID
	return dec, r, nil
}

// ScheduleBatch places count replicas strictly one after another. State counts
// and resource allocations are visible to every subsequent PlanOne, matching
// repeated independent ScheduleOne calls.
func ScheduleBatch(st *model.State, workloadID, version, batchID string, count int) ([]*model.SchedulingDecision, []*model.Replica, error) {
	var decs []*model.SchedulingDecision
	var reps []*model.Replica
	for i := 0; i < count; i++ {
		dec, rep, err := ScheduleOne(st, workloadID, version, batchID, i+1)
		decs = append(decs, dec)
		if rep != nil {
			reps = append(reps, rep)
		}
		if err != nil {
			return decs, reps, err
		}
	}
	return decs, reps, nil
}

func sortedVersions(st *model.State, workloadID string) []string {
	seen := map[string]bool{}
	for _, r := range st.Replicas {
		if r.WorkloadID == workloadID {
			seen[r.Version] = true
		}
	}
	out := make([]string, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func sortedWorkloads(st *model.State) []*model.Workload {
	out := append([]*model.Workload(nil), st.Workloads...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

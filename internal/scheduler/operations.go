package scheduler

import (
	"errors"
	"fmt"
	"sort"

	"toposcheduler/internal/model"
)

type rankedReplica struct {
	rep  *model.Replica
	node *model.Node
	free float64
}

// chooseReplicaByFree breaks topology ties using node free-resource degree.
// Higher free ratio is preferred: this tends to delete from an already lightly
// loaded node and makes it possible to empty a node/hangar. Replica ID is the
// final deterministic tie-break.
func chooseReplicaByFree(st *model.State, candidates []*model.Replica) *rankedReplica {
	ranked := make([]*rankedReplica, 0, len(candidates))
	for _, r := range candidates {
		rr := &rankedReplica{rep: r, free: -1}
		if n := st.Node(r.NodeID); n != nil {
			_, _, avg := n.FreeRatio()
			rr.node = n
			rr.free = avg
		}
		ranked = append(ranked, rr)
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].free != ranked[j].free {
			return ranked[i].free > ranked[j].free
		}
		return ranked[i].rep.ID < ranked[j].rep.ID
	})
	if len(ranked) == 0 {
		return nil
	}
	return ranked[0]
}

func removeReplica(st *model.State, target *model.Replica) {
	if n := st.Node(target.NodeID); n != nil {
		n.Allocated = n.Allocated.Sub(target.Request)
		if n.Allocated.CPUMillis < 0 {
			n.Allocated.CPUMillis = 0
		}
		if n.Allocated.MemoryMB < 0 {
			n.Allocated.MemoryMB = 0
		}
	}
	out := st.Replicas[:0]
	for _, r := range st.Replicas {
		if r.ID != target.ID {
			out = append(out, r)
		}
	}
	st.Replicas = out
}

// ScaleDownOne deletes one replica of one workload version. It first chooses a
// domain with the largest same-version replica count, then breaks ties by node
// free-resource degree.
func ScaleDownOne(st *model.State, workloadID, version string) (*model.DisruptionRecord, error) {
	w := st.Workload(workloadID)
	if w == nil {
		return nil, fmt.Errorf("%w: %s", ErrWorkloadNotFound, workloadID)
	}
	if version == "" {
		version = w.Version
	}

	var candidates []*model.Replica
	for _, r := range st.Replicas {
		if r.WorkloadID == w.ID && r.Version == version {
			candidates = append(candidates, r)
		}
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("%w: %s/%s", ErrNoReplica, workloadID, version)
	}

	key := topologyKey(w)
	counts := map[string]int{}
	for _, r := range candidates {
		d := r.Domain
		if d == "" {
			if n := st.Node(r.NodeID); n != nil {
				d = NodeDomain(n, key)
			}
		}
		counts[d]++
	}
	maxCount := -1
	for _, c := range counts {
		if c > maxCount {
			maxCount = c
		}
	}
	var maxCandidates []*model.Replica
	for _, r := range candidates {
		d := r.Domain
		if d == "" {
			if n := st.Node(r.NodeID); n != nil {
				d = NodeDomain(n, key)
			}
		}
		if counts[d] == maxCount {
			maxCandidates = append(maxCandidates, r)
		}
	}

	pick := chooseReplicaByFree(st, maxCandidates)
	if pick == nil {
		return nil, ErrNoReplica
	}
	r := pick.rep

	before := InspectDomains(st, w, version)
	availableBefore := st.AvailableCount(w.ID)
	removeReplica(st, r)
	after := InspectDomains(st, w, version)
	availableAfter := st.AvailableCount(w.ID)

	seq := st.NextID()
	rec := &model.DisruptionRecord{
		ID:                 fmt.Sprintf("scale-%05d", seq),
		Seq:                seq,
		Kind:               "scale_in",
		Status:             "deleted",
		Allowed:            true,
		Reason:             "domain_with_most_replicas;resource_free_tie_break",
		WorkloadID:         w.ID,
		WorkloadName:       w.Name,
		Version:            version,
		ReplicaID:          r.ID,
		NodeID:             r.NodeID,
		Domain:             r.Domain,
		DomainReplicaCount: maxCount,
		ResourceFreeScore:  pick.free * 100,
		Baseline:           before.Baseline,
		SkewBefore:         before.WorstSkew(),
		SkewAfter:          after.WorstSkew(),
		MaxSkew:            maxSkewOf(w),
		MinAvailable:       w.MinAvailable,
		AvailableBefore:    availableBefore,
		AvailableAfter:     availableAfter,
	}
	st.Disruptions = append(st.Disruptions, rec)
	return rec, nil
}

func maxSkewOf(w *model.Workload) int {
	if w.Spread != nil {
		return w.Spread.MaxSkew
	}
	return 0
}

type Violation struct {
	Workload *model.Workload
	Version  string
	Info     DomainInfo
	Domain   string
	Count    int
	Skew     int
}

// FindViolations returns every hard maxSkew violation for every workload
// version, in deterministic worst-first order.
func FindViolations(st *model.State) []Violation {
	var violations []Violation
	for _, w := range sortedWorkloads(st) {
		if w.Spread == nil || !w.Spread.Hard {
			continue
		}
		for _, version := range sortedVersions(st, w.ID) {
			info := InspectDomains(st, w, version)
			for _, d := range info.EligibleDomains {
				count := info.Counts[d]
				skew := count - info.Baseline
				if skew > w.Spread.MaxSkew {
					violations = append(violations, Violation{
						Workload: w,
						Version:  version,
						Info:     info,
						Domain:   d,
						Count:    count,
						Skew:     skew,
					})
				}
			}
		}
	}

	sort.Slice(violations, func(i, j int) bool {
		a, b := violations[i], violations[j]
		if a.Skew != b.Skew {
			return a.Skew > b.Skew
		}
		if a.Count != b.Count {
			return a.Count > b.Count
		}
		if a.Workload.ID != b.Workload.ID {
			return a.Workload.ID < b.Workload.ID
		}
		if a.Version != b.Version {
			return a.Version < b.Version
		}
		return a.Domain < b.Domain
	})
	return violations
}

// EvictOne selects one replica from the worst violating domain and evicts it
// only when available-after-eviction remains at least minAvailable.
func EvictOne(st *model.State, trigger string) (*model.DisruptionRecord, error) {
	violations := FindViolations(st)
	if len(violations) == 0 {
		return nil, ErrNoViolation
	}
	v := violations[0]
	w := v.Workload

	var candidates []*model.Replica
	for _, r := range st.Replicas {
		if r.WorkloadID == w.ID && r.Version == v.Version && r.Domain == v.Domain {
			candidates = append(candidates, r)
		}
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("%w: violation %s/%s domain %s has no replica",
			ErrNoReplica, w.ID, v.Version, v.Domain)
	}
	pick := chooseReplicaByFree(st, candidates)
	r := pick.rep
	availableBefore := st.AvailableCount(w.ID)

	seq := st.NextID()
	rec := &model.DisruptionRecord{
		ID:                 fmt.Sprintf("evict-%05d", seq),
		Seq:                seq,
		Kind:               "rebalance",
		Trigger:            trigger,
		WorkloadID:         w.ID,
		WorkloadName:       w.Name,
		Version:            v.Version,
		ReplicaID:          r.ID,
		NodeID:             r.NodeID,
		Domain:             v.Domain,
		DomainReplicaCount: v.Count,
		ResourceFreeScore:  pick.free * 100,
		Baseline:           v.Info.Baseline,
		SkewBefore:         v.Info.WorstSkew(),
		MaxSkew:            w.Spread.MaxSkew,
		MinAvailable:       w.MinAvailable,
		AvailableBefore:    availableBefore,
	}

	// Require availableBefore - 1 >= MinAvailable. This is the hard eviction
	// budget checked immediately before mutating state.
	if availableBefore <= w.MinAvailable {
		rec.Status = "blocked_min_available"
		rec.Allowed = false
		rec.Reason = fmt.Sprintf("available %d would fall below minAvailable %d",
			availableBefore-1, w.MinAvailable)
		rec.AvailableAfter = availableBefore
		st.Disruptions = append(st.Disruptions, rec)
		return rec, fmt.Errorf("%w: workload %s available=%d minAvailable=%d",
			ErrMinAvailable, w.ID, availableBefore, w.MinAvailable)
	}

	rec.Allowed = true
	rec.Status = "evicted"
	rec.Reason = fmt.Sprintf("hard skew %d > maxSkew %d in domain %s",
		v.Skew, w.Spread.MaxSkew, v.Domain)

	removeReplica(st, r)
	rec.AvailableAfter = st.AvailableCount(w.ID)
	rec.SkewAfter = InspectDomains(st, w, v.Version).WorstSkew()
	st.Disruptions = append(st.Disruptions, rec)
	return rec, nil
}

// RebalanceAndReplaceOne evicts one safe replica and immediately asks the
// scheduler to place its replacement. The eviction budget is observable after
// EvictOne returns and before replacement.
func RebalanceAndReplaceOne(st *model.State, trigger, batchID string) (*model.DisruptionRecord, error) {
	rec, err := EvictOne(st, trigger)
	if err != nil {
		return rec, err
	}
	dec, _, schedErr := ScheduleOne(st, rec.WorkloadID, rec.Version, batchID, 1)
	if dec != nil {
		rec.ReplacementNodeID = dec.SelectedNodeID
		if schedErr != nil {
			rec.ReplacementErr = schedErr.Error()
		}
	} else if schedErr != nil {
		rec.ReplacementErr = schedErr.Error()
	}
	return rec, schedErr
}

// RebalanceAll converges hard constraints using evict-plus-replace moves.
func RebalanceAll(st *model.State, trigger string) ([]*model.DisruptionRecord, error) {
	var records []*model.DisruptionRecord
	limit := 4*len(st.Replicas) + 20
	for i := 0; i < limit; i++ {
		rec, err := RebalanceAndReplaceOne(st, trigger, fmt.Sprintf("rebalance-%d", i+1))
		if errors.Is(err, ErrNoViolation) {
			return records, nil
		}
		if rec != nil {
			records = append(records, rec)
		}
		if err != nil {
			return records, err
		}
	}
	return records, errors.New("rebalance did not converge within iteration limit")
}

// ValidateHardConstraints checks every hard workload/version.
func ValidateHardConstraints(st *model.State) error {
	for _, w := range sortedWorkloads(st) {
		if w.Spread == nil || !w.Spread.Hard {
			continue
		}
		for _, version := range sortedVersions(st, w.ID) {
			info := InspectDomains(st, w, version)
			for _, d := range info.EligibleDomains {
				skew := info.Counts[d] - info.Baseline
				if skew > w.Spread.MaxSkew {
					return fmt.Errorf(
						"hard constraint violated: workload=%s version=%s domain=%s count=%d baseline=%d skew=%d maxSkew=%d forcedZero=%v eligibleDomains=%d minDomains=%d",
						w.ID, version, d, info.Counts[d], info.Baseline, skew,
						w.Spread.MaxSkew, info.BaselineForcedZero,
						info.EligibleDomainCount, w.Spread.MinDomains)
				}
			}
		}
	}
	return nil
}

// ValidateNodeResources checks that simulated allocations never exceed capacity.
func ValidateNodeResources(st *model.State) error {
	for _, n := range st.Nodes {
		if n.Allocated.CPUMillis > n.Capacity.CPUMillis {
			return fmt.Errorf("node %s over CPU capacity: %d > %d",
				n.ID, n.Allocated.CPUMillis, n.Capacity.CPUMillis)
		}
		if n.Allocated.MemoryMB > n.Capacity.MemoryMB {
			return fmt.Errorf("node %s over memory capacity: %d > %d",
				n.ID, n.Allocated.MemoryMB, n.Capacity.MemoryMB)
		}
	}
	return nil
}

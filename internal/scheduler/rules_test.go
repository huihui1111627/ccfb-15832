package scheduler_test

import (
	"errors"
	"fmt"
	"testing"

	"toposcheduler/internal/model"
	"toposcheduler/internal/scheduler"
)

func testNode(id, hangar string, labels map[string]string) *model.Node {
	if labels == nil {
		labels = map[string]string{}
	}
	labels[scheduler.DefaultTopologyKey] = hangar
	labels["hangar"] = hangar
	return &model.Node{
		ID:          id,
		Hangar:      hangar,
		Labels:      labels,
		Capacity:    model.Resources{CPUMillis: 16000, MemoryMB: 32768},
		Allocated:   model.Resources{},
		Ready:       true,
		Schedulable: true,
	}
}

func hardWorkload() *model.Workload {
	return &model.Workload{
		ID:           "wl",
		Name:         "WL",
		Version:      "v1",
		Selector:     map[string]string{},
		Request:      model.Resources{CPUMillis: 500, MemoryMB: 512},
		MinAvailable: 1,
		Spread: &model.SpreadConstraint{
			TopologyKey: scheduler.DefaultTopologyKey,
			MaxSkew:     1,
			Hard:        true,
			Weight:      1,
		},
	}
}

func TestSelectorExcludedHangarsDoNotCount(t *testing.T) {
	w := hardWorkload()
	w.Selector = map[string]string{"region": "selected"}
	a := testNode("a-n1", "h-a", map[string]string{"region": "selected"})
	b := testNode("b-n1", "h-b", map[string]string{"region": "other"})
	st := &model.State{Nodes: []*model.Node{a, b}, Workloads: []*model.Workload{w}}

	for i := 0; i < 4; i++ {
		dec, _, err := scheduler.ScheduleOne(st, w.ID, "", "t", i+1)
		if err != nil {
			t.Fatalf("schedule %d: %v", i, err)
		}
		if dec.SelectedNodeID != "a-n1" {
			t.Fatalf("expected selector-only node a-n1, got %s", dec.SelectedNodeID)
		}
	}
	info := scheduler.InspectDomains(st, w, "v1")
	if info.EligibleDomainCount != 1 || info.Baseline != 4 {
		t.Fatalf("excluded domain changed skew: domains=%d baseline=%d",
			info.EligibleDomainCount, info.Baseline)
	}
}

func TestDomainWithNoAvailableNodeExcluded(t *testing.T) {
	w := hardWorkload()
	a := testNode("a-n1", "h-a", nil)
	b := testNode("b-n1", "h-b", nil)
	b.Ready = false
	st := &model.State{Nodes: []*model.Node{a, b}, Workloads: []*model.Workload{w}}

	for i := 0; i < 3; i++ {
		if _, _, err := scheduler.ScheduleOne(st, w.ID, "", "t", i+1); err != nil {
			t.Fatalf("schedule %d: %v", i, err)
		}
	}
	info := scheduler.InspectDomains(st, w, "v1")
	if info.EligibleDomainCount != 1 {
		t.Fatalf("not-ready domain should be excluded, eligible=%d", info.EligibleDomainCount)
	}
	if info.Baseline != 3 {
		t.Fatalf("only available domain should be its own minimum, baseline=%d", info.Baseline)
	}
}

func TestMinDomainsUnmetUsesZeroBaseline(t *testing.T) {
	w := hardWorkload()
	w.Spread.MinDomains = 5
	st := &model.State{Workloads: []*model.Workload{w}}
	for _, h := range []string{"h-1", "h-2", "h-3"} {
		st.Nodes = append(st.Nodes, testNode(h+"-n1", h, nil))
	}

	for i := 0; i < 3; i++ {
		if _, _, err := scheduler.ScheduleOne(st, w.ID, "", "t", i+1); err != nil {
			t.Fatalf("schedule %d: %v", i, err)
		}
	}
	plan := scheduler.PlanOne(st, w, "v1")
	if !plan.BaselineForcedZero || plan.Baseline != 0 {
		t.Fatalf("expected forced zero baseline, got %+v", plan)
	}
	if plan.SelectedNodeID != "" || len(plan.FeasibleNodeIDs) != 0 {
		t.Fatalf("maxSkew=1 with unmet minDomains should reject second replica in every domain: selected=%q feasible=%v",
			plan.SelectedNodeID, plan.FeasibleNodeIDs)
	}
}

func TestRollingVersionsAreCountedIndependently(t *testing.T) {
	w := hardWorkload()
	st := &model.State{Workloads: []*model.Workload{w}}
	for _, h := range []string{"h-1", "h-2", "h-3"} {
		st.Nodes = append(st.Nodes, testNode(h+"-n1", h, nil))
	}
	// Five old-version replicas are concentrated in h-1.
	for i := 0; i < 5; i++ {
		st.Replicas = append(st.Replicas, &model.Replica{
			ID: fmt.Sprintf("old-%d", i), WorkloadID: w.ID, Version: "v1-old",
			NodeID: "h-1-n1", Domain: "h-1", Available: true, Phase: model.PhaseRunning,
		})
	}

	plan := scheduler.PlanOne(st, w, "v2-new")
	if plan.Baseline != 0 || plan.EligibleDomainCount != 3 {
		t.Fatalf("new version should start from zero counts: baseline=%d domains=%d",
			plan.Baseline, plan.EligibleDomainCount)
	}
	var h1 *model.NodeDecision
	for _, nd := range plan.Nodes {
		if nd.NodeID == "h-1-n1" {
			h1 = nd
		}
	}
	if h1 == nil || h1.CurrentDomainCount != 0 || h1.PostSkew != 1 {
		t.Fatalf("old replicas leaked into rolling-new calculation: %+v", h1)
	}
}

func TestSoftConstraintDoesNotFilterButScores(t *testing.T) {
	w := hardWorkload()
	w.Spread.Hard = false
	st := &model.State{Workloads: []*model.Workload{w}}
	a := testNode("a-n1", "h-a", nil)
	b := testNode("b-n1", "h-b", nil)
	st.Nodes = []*model.Node{a, b}
	for i := 0; i < 5; i++ {
		st.Replicas = append(st.Replicas, &model.Replica{
			ID: fmt.Sprintf("r-a-%d", i), WorkloadID: w.ID, Version: "v1",
			NodeID: a.ID, Domain: "h-a", Available: true, Phase: model.PhaseRunning,
		})
	}

	plan := scheduler.PlanOne(st, w, "v1")
	var ndA *model.NodeDecision
	for _, nd := range plan.Nodes {
		if nd.NodeID == a.ID {
			ndA = nd
		}
	}
	if ndA == nil || !ndA.Eligible {
		t.Fatalf("soft constraint must not filter h-a: %+v", ndA)
	}
	if !ndA.SpreadScored || ndA.SpreadScore >= 50 {
		t.Fatalf("high skew should receive low spread score: %+v", ndA)
	}
	if plan.SelectedNodeID != b.ID {
		t.Fatalf("lower-skew node should win by score, got %s", plan.SelectedNodeID)
	}

	w.Spread.Hard = true
	hardPlan := scheduler.PlanOne(st, w, "v1")
	for _, nd := range hardPlan.Nodes {
		if nd.NodeID == a.ID && nd.Eligible {
			t.Fatal("hard constraint must filter high-skew node")
		}
	}
}

func TestScaleDownUsesMaxDomainThenFreeDegree(t *testing.T) {
	w := hardWorkload()
	st := &model.State{Workloads: []*model.Workload{w}}
	n1 := testNode("n1", "h-1", nil)
	n2 := testNode("n2", "h-2", nil)
	n3 := testNode("n3", "h-3", nil)
	setFree := func(n *model.Node, usedPct int64) {
		n.Allocated.CPUMillis = n.Capacity.CPUMillis * usedPct / 100
		n.Allocated.MemoryMB = n.Capacity.MemoryMB * usedPct / 100
	}
	setFree(n1, 10) // 90% free
	setFree(n2, 80) // 20% free
	setFree(n3, 50)
	st.Nodes = []*model.Node{n1, n2, n3}

	add := func(id, node, domain string) {
		st.Replicas = append(st.Replicas, &model.Replica{
			ID: id, WorkloadID: w.ID, Version: "v1", NodeID: node, Domain: domain,
			Request: w.Request, Available: true, Phase: model.PhaseRunning,
		})
	}
	add("r1", "n1", "h-1")
	add("r2", "n1", "h-1")
	add("r3", "n2", "h-2")
	add("r4", "n2", "h-2")
	add("r5", "n3", "h-3")

	rec, err := scheduler.ScaleDownOne(st, w.ID, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Domain != "h-1" {
		t.Fatalf("tied max domains should choose freest node h-1, got %s (%+v)", rec.Domain, rec)
	}
	rec, err = scheduler.ScaleDownOne(st, w.ID, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Domain != "h-2" {
		t.Fatalf("now unique max domain should be h-2, got %s", rec.Domain)
	}
	if rec.SkewAfter > rec.SkewBefore && rec.SkewAfter > w.Spread.MaxSkew {
		t.Fatalf("scale-down made unacceptable skew: before=%d after=%d",
			rec.SkewBefore, rec.SkewAfter)
	}
}

func TestRebalanceEvictionRespectsMinAvailableAndMovesReplica(t *testing.T) {
	w := hardWorkload()
	w.MinAvailable = 4
	st := &model.State{Workloads: []*model.Workload{w}}
	for _, h := range []string{"h-1", "h-2", "h-3"} {
		st.Nodes = append(st.Nodes, testNode(h+"-n1", h, nil))
	}
	if _, reps, err := scheduler.ScheduleBatch(st, w.ID, "v1", "init", 4); err != nil {
		t.Fatal(err)
	} else if len(reps) != 4 {
		t.Fatalf("expected 4 reps, got %d", len(reps))
	}

	h4 := testNode("h-4-n1", "h-4", nil)
	st.Nodes = append(st.Nodes, h4)

	blocked, err := scheduler.EvictOne(st, "test-blocked")
	if !errors.Is(err, scheduler.ErrMinAvailable) {
		t.Fatalf("expected minAvailable error, got %v", err)
	}
	if blocked == nil || blocked.Allowed || blocked.Status != "blocked_min_available" {
		t.Fatalf("invalid blocked record: %+v", blocked)
	}
	if len(st.Replicas) != 4 || st.AvailableCount(w.ID) != 4 {
		t.Fatal("blocked eviction must not mutate replicas")
	}

	w.MinAvailable = 3
	rec, err := scheduler.EvictOne(st, "test-allowed")
	if err != nil {
		t.Fatalf("allowed eviction: %v", err)
	}
	if !rec.Allowed || rec.AvailableAfter != 3 {
		t.Fatalf("expected one safe eviction leaving 3 available, got %+v", rec)
	}
	if st.AvailableCount(w.ID) < w.MinAvailable {
		t.Fatal("available fell below declared minimum")
	}
	dec, rep, err := scheduler.ScheduleOne(st, w.ID, "v1", "replace", 1)
	if err != nil {
		t.Fatalf("replacement: %v", err)
	}
	if dec.SelectedNodeID != "h-4-n1" || rep.Domain != "h-4" {
		t.Fatalf("replacement should spread into new empty hangar, got %s", dec.SelectedNodeID)
	}
	if err := scheduler.ValidateHardConstraints(st); err != nil {
		t.Fatal(err)
	}
}

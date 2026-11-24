package sim_test

import (
	"errors"
	"testing"

	"toposcheduler/internal/model"
	"toposcheduler/internal/scheduler"
	"toposcheduler/internal/sim"
)

func topologyNode(id, hangar string) *model.Node {
	return &model.Node{
		ID:     id,
		Hangar: hangar,
		Labels: map[string]string{
			scheduler.DefaultTopologyKey: hangar,
			"hangar":                     hangar,
		},
		Capacity:    model.Resources{CPUMillis: 16000, MemoryMB: 32768},
		Ready:       true,
		Schedulable: true,
	}
}

func TestHangarPowerLossAndEvictionBudget(t *testing.T) {
	w := &model.Workload{
		ID: "critical", Name: "critical", Version: "v1",
		Selector:     map[string]string{},
		Request:      model.Resources{CPUMillis: 500, MemoryMB: 512},
		MinAvailable: 3,
		Spread: &model.SpreadConstraint{
			TopologyKey: scheduler.DefaultTopologyKey,
			MaxSkew:     1,
			MinDomains:  3,
			Hard:        true,
		},
	}
	st := &model.State{Workloads: []*model.Workload{w}}
	for _, h := range []string{"h-1", "h-2", "h-3"} {
		st.Nodes = append(st.Nodes, topologyNode(h+"-n1", h))
	}
	if _, reps, err := scheduler.ScheduleBatch(st, w.ID, "v1", "init", 4); err != nil {
		t.Fatal(err)
	} else if len(reps) != 4 {
		t.Fatalf("expected 4 replicas, got %d", len(reps))
	}

	_, lost, err := sim.SetHangarPower(st, "h-3", false)
	if err != nil {
		t.Fatal(err)
	}
	if lost != 1 {
		t.Fatalf("expected one lost h-3 replica, got %d", lost)
	}
	info := scheduler.InspectDomains(st, w, "v1")
	if info.EligibleDomainCount != 2 || !info.BaselineForcedZero {
		t.Fatalf("power-loss domain should be excluded and minDomains unmet: %+v", info)
	}
	if !info.MaxSkewExceeded(1) {
		t.Fatal("expected remaining double-copy domain to violate after minDomains zero fallback")
	}

	blocked, err := scheduler.EvictOne(st, "power-test")
	if !errors.Is(err, scheduler.ErrMinAvailable) {
		t.Fatalf("expected PDB block at available=minAvailable, got %v", err)
	}
	if blocked == nil || blocked.Allowed {
		t.Fatalf("expected blocked record, got %+v", blocked)
	}
	if st.AvailableCount(w.ID) != 3 {
		t.Fatalf("blocked eviction changed availability: %d", st.AvailableCount(w.ID))
	}

	w.MinAvailable = 2
	rec, err := scheduler.EvictOne(st, "power-test")
	if err != nil {
		t.Fatalf("eviction after lowering budget: %v", err)
	}
	if rec.AvailableAfter != 2 || st.AvailableCount(w.ID) < w.MinAvailable {
		t.Fatalf("budget violated: record=%+v actual=%d", rec, st.AvailableCount(w.ID))
	}
	if err := scheduler.ValidateHardConstraints(st); err != nil {
		t.Fatal(err)
	}
}

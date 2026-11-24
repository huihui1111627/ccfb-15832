package sim_test

import (
	"testing"

	"toposcheduler/internal/scheduler"
	"toposcheduler/internal/sim"
)

func TestDemoScriptIsValid(t *testing.T) {
	s, err := sim.NewDemo(20261125)
	if err != nil {
		t.Fatalf("NewDemo: %v", err)
	}
	d := s.Dashboard()
	if err := scheduler.ValidateHardConstraints(d.State); err != nil {
		t.Fatalf("demo hard constraints: %v", err)
	}
	if err := scheduler.ValidateNodeResources(d.State); err != nil {
		t.Fatalf("demo resources: %v", err)
	}
	if d.Summary.Hangars < 12 {
		t.Fatalf("expected at least a dozen hangars, got %d", d.Summary.Hangars)
	}
	if d.Summary.Evictions == 0 {
		t.Fatal("expected demo rebalance evictions")
	}
	if d.Summary.ScaleIns < 3 {
		t.Fatalf("expected scale-in records, got %d", d.Summary.ScaleIns)
	}

	foundRolling := false
	for _, row := range d.Distribution {
		if row.WorkloadID == "mission-api" {
			foundRolling = true
			if row.Total == 0 {
				t.Fatalf("rolling row %s has no replicas", row.Version)
			}
		}
	}
	if !foundRolling {
		t.Fatal("missing rolling update distribution rows")
	}
}

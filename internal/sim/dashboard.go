package sim

import (
	"sort"

	"toposcheduler/internal/model"
	"toposcheduler/internal/scheduler"
)

type DomainCell struct {
	Domain   string `json:"domain"`
	Count    int    `json:"count"`
	Skew     int    `json:"skew"`
	Eligible bool   `json:"eligible"`
	Reason   string `json:"reason,omitempty"`
}

type DistributionRow struct {
	WorkloadID      string       `json:"workload_id"`
	WorkloadName    string       `json:"workload_name"`
	Version         string       `json:"version"`
	Mode            string       `json:"mode"` // hard | soft | none
	TopologyKey     string       `json:"topology_key"`
	MaxSkew         int          `json:"max_skew"`
	MinDomains      int          `json:"min_domains,omitempty"`
	Baseline        int          `json:"baseline"`
	BaselineKind    string       `json:"baseline_kind"`
	EligibleDomains int          `json:"eligible_domains"`
	WorstSkew       int          `json:"worst_skew"`
	Satisfied       bool         `json:"satisfied"`
	Total           int          `json:"total"`
	Cells           []DomainCell `json:"cells"`
}

type Summary struct {
	Hangars          int `json:"hangars"`
	Nodes            int `json:"nodes"`
	Workloads        int `json:"workloads"`
	Replicas         int `json:"replicas"`
	SchedulingCalls  int `json:"scheduling_calls"`
	FeasibleCalls    int `json:"feasible_calls"`
	FailedCalls      int `json:"failed_calls"`
	Evictions        int `json:"evictions"`
	ScaleIns         int `json:"scale_ins"`
	BlockedEvictions int `json:"blocked_evictions"`
}

type Dashboard struct {
	State        *model.State      `json:"state"`
	Hangars      []string          `json:"hangars"`
	Distribution []DistributionRow `json:"distribution"`
	Summary      Summary           `json:"summary"`
}

func BuildDashboard(st *model.State) Dashboard {
	hangars := st.Hangars()
	d := Dashboard{State: st, Hangars: hangars}

	for _, w := range st.Workloads {
		versions := dashboardVersions(st, w.ID, w.Version)
		for _, v := range versions {
			info := scheduler.InspectDomains(st, w, v)
			row := DistributionRow{
				WorkloadID:      w.ID,
				WorkloadName:    w.Name,
				Version:         v,
				TopologyKey:     info.Key,
				Baseline:        info.Baseline,
				EligibleDomains: info.EligibleDomainCount,
				WorstSkew:       info.WorstSkew(),
				Satisfied:       true,
			}
			if w.Spread == nil {
				row.Mode = "none"
				row.BaselineKind = "no_topology_constraint"
			} else {
				row.MaxSkew = w.Spread.MaxSkew
				row.MinDomains = w.Spread.MinDomains
				if w.Spread.Hard {
					row.Mode = "hard"
					row.Satisfied = !info.MaxSkewExceeded(w.Spread.MaxSkew)
				} else {
					row.Mode = "soft"
					// Soft constraints never block; this flag only reports
					// whether the preferred skew happens to be met.
					row.Satisfied = !info.MaxSkewExceeded(w.Spread.MaxSkew)
				}
			}
			switch {
			case info.EligibleDomainCount == 0:
				row.BaselineKind = "no_eligible_domain"
			case info.BaselineForcedZero:
				row.BaselineKind = "min_domains_unmet_zero"
			default:
				row.BaselineKind = "global_minimum"
			}

			reasonByDomain := map[string]string{}
			for _, ds := range info.Stats {
				if ds.Reason != "" {
					reasonByDomain[ds.Domain] = ds.Reason
				}
			}

			for _, h := range hangars {
				count := info.Counts[h]
				cell := DomainCell{
					Domain:   h,
					Count:    count,
					Eligible: info.Eligible[h],
					Reason:   reasonByDomain[h],
					Skew:     scheduler.SkewNotCalculated,
				}
				if cell.Eligible {
					cell.Skew = count - info.Baseline
				}
				row.Cells = append(row.Cells, cell)
				row.Total += count
			}
			d.Distribution = append(d.Distribution, row)
		}
	}

	d.Summary = Summary{
		Hangars:         len(hangars),
		Nodes:           len(st.Nodes),
		Workloads:       len(st.Workloads),
		Replicas:        len(st.Replicas),
		SchedulingCalls: len(st.Decisions),
	}
	for _, dec := range st.Decisions {
		if dec.SelectedNodeID != "" {
			d.Summary.FeasibleCalls++
		} else {
			d.Summary.FailedCalls++
		}
	}
	for _, rec := range st.Disruptions {
		switch rec.Kind {
		case "rebalance":
			if rec.Status == "blocked_min_available" {
				d.Summary.BlockedEvictions++
			} else {
				d.Summary.Evictions++
			}
		case "scale_in":
			d.Summary.ScaleIns++
		}
	}
	return d
}

func dashboardVersions(st *model.State, workloadID, preferred string) []string {
	seen := map[string]bool{}
	var out []string
	for _, r := range st.Replicas {
		if r.WorkloadID == workloadID && !seen[r.Version] {
			seen[r.Version] = true
			out = append(out, r.Version)
		}
	}
	if preferred != "" && !seen[preferred] {
		out = append(out, preferred)
	}
	sort.Strings(out)
	return out
}

func (s *Simulation) Dashboard() Dashboard {
	s.Lock()
	defer s.Unlock()
	return BuildDashboard(s.State)
}

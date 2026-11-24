package scheduler_test

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"toposcheduler/internal/model"
	"toposcheduler/internal/scheduler"
)

type bruteResult struct {
	selected        string
	feasible        []string
	baseline        int
	eligibleDomains int
	forcedZero      bool
	weighted        map[string]float64
	postSkew        map[string]int
}

func selectorMatches(labels, selector map[string]string) bool {
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}

func bruteSelectionFailures(n *model.Node, w *model.Workload) []string {
	var reasons []string
	if !n.Ready {
		reasons = append(reasons, "node_not_ready")
	}
	if !n.Schedulable {
		reasons = append(reasons, "node_cordoned")
	}
	if !selectorMatches(n.Labels, w.Selector) {
		reasons = append(reasons, "selector_mismatch")
	}
	key := scheduler.DefaultTopologyKey
	if w.Spread != nil && w.Spread.TopologyKey != "" {
		key = w.Spread.TopologyKey
	}
	if w.Spread != nil && n.Labels[key] == "" {
		reasons = append(reasons, "missing_topology_label")
	}
	return reasons
}

func bruteBaseFailures(n *model.Node, w *model.Workload) []string {
	reasons := bruteSelectionFailures(n, w)
	freeCPU := n.Capacity.CPUMillis - n.Allocated.CPUMillis
	freeMem := n.Capacity.MemoryMB - n.Allocated.MemoryMB
	if freeCPU < w.Request.CPUMillis {
		reasons = append(reasons, "insufficient_cpu")
	}
	if freeMem < w.Request.MemoryMB {
		reasons = append(reasons, "insufficient_memory")
	}
	return reasons
}

// bruteChoose is the independent enumerate-all-nodes reference described in
// the specification. It deliberately does not call PlanOne or InspectDomains.
func bruteChoose(st *model.State, w *model.Workload, version string) bruteResult {
	key := scheduler.DefaultTopologyKey
	maxSkew := 1
	minDomains := 0
	hard := false
	if w.Spread != nil {
		key = w.Spread.TopologyKey
		maxSkew = w.Spread.MaxSkew
		minDomains = w.Spread.MinDomains
		hard = w.Spread.Hard
	}

	nodes := append([]*model.Node(nil), st.Nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })

	domainHasUsable := map[string]bool{}
	baseOK := map[string]bool{}
	for _, n := range nodes {
		d := n.Labels[key]
		if len(bruteSelectionFailures(n, w)) == 0 {
			domainHasUsable[d] = true
		}
		if len(bruteBaseFailures(n, w)) == 0 {
			baseOK[n.ID] = true
		}
	}
	var eligible []string
	for d, ok := range domainHasUsable {
		if ok && d != "" {
			eligible = append(eligible, d)
		}
	}
	sort.Strings(eligible)

	counts := map[string]int{}
	for _, r := range st.Replicas {
		if r.WorkloadID != w.ID || r.Version != version {
			continue
		}
		d := r.Domain
		if d == "" {
			if n := st.Node(r.NodeID); n != nil {
				d = n.Labels[key]
			}
		}
		if domainHasUsable[d] {
			counts[d]++
		}
	}

	baseline := math.MaxInt
	for _, d := range eligible {
		if counts[d] < baseline {
			baseline = counts[d]
		}
	}
	if len(eligible) == 0 {
		baseline = 0
	}
	forcedZero := minDomains > 0 && len(eligible) < minDomains
	if forcedZero {
		baseline = 0
	}

	res := bruteResult{
		baseline:        baseline,
		eligibleDomains: len(eligible),
		forcedZero:      forcedZero,
		weighted:        map[string]float64{},
		postSkew:        map[string]int{},
	}
	rw := w.ResourceWeight
	if rw <= 0 {
		rw = 1
	}
	sw := 1
	if w.Spread != nil && w.Spread.Weight > 0 {
		sw = w.Spread.Weight
	}

	for _, n := range nodes {
		if !baseOK[n.ID] {
			continue
		}
		d := n.Labels[key]
		current := counts[d]
		post := current + 1 - baseline
		res.postSkew[n.ID] = post
		if hard && post > maxSkew {
			continue
		}

		cpuRatio := float64(n.Capacity.CPUMillis-n.Allocated.CPUMillis) /
			float64(n.Capacity.CPUMillis)
		memRatio := float64(n.Capacity.MemoryMB-n.Allocated.MemoryMB) /
			float64(n.Capacity.MemoryMB)
		resourceScore := math.Round(((cpuRatio+memRatio)/2)*10000) / 100
		weighted := resourceScore * float64(rw)
		if w.Spread != nil && !w.Spread.Hard {
			spreadScore := 100 * float64(maxSkew) /
				(float64(maxSkew) + float64(post))
			weighted = spreadScore*float64(sw) + resourceScore*float64(rw)
		}
		res.weighted[n.ID] = weighted
		res.feasible = append(res.feasible, n.ID)

		if res.selected == "" ||
			weighted > res.weighted[res.selected] ||
			(weighted == res.weighted[res.selected] && n.ID < res.selected) {
			res.selected = n.ID
		}
	}
	return res
}

func assertMatchesBrute(t *testing.T, st *model.State, w *model.Workload, version string, context string) {
	t.Helper()
	plan := scheduler.PlanOne(st, w, version)
	brute := bruteChoose(st, w, version)

	if plan.Baseline != brute.baseline ||
		plan.EligibleDomainCount != brute.eligibleDomains ||
		plan.BaselineForcedZero != brute.forcedZero {
		t.Fatalf("%s: domain baseline mismatch: plan=(base=%d domains=%d forced=%v) brute=(base=%d domains=%d forced=%v)",
			context, plan.Baseline, plan.EligibleDomainCount, plan.BaselineForcedZero,
			brute.baseline, brute.eligibleDomains, brute.forcedZero)
	}
	if strings.Join(plan.FeasibleNodeIDs, ",") != strings.Join(brute.feasible, ",") {
		t.Fatalf("%s: feasible nodes mismatch\nplan:  %v\nbrute: %v",
			context, plan.FeasibleNodeIDs, brute.feasible)
	}
	if plan.SelectedNodeID != brute.selected {
		var details []string
		for _, nd := range plan.Nodes {
			if nd.Eligible {
				details = append(details, fmt.Sprintf("%s=%.4f/post%d", nd.NodeID, nd.WeightedScore, nd.PostSkew))
			}
		}
		t.Fatalf("%s: selected %q, brute selected %q; scores [%s]",
			context, plan.SelectedNodeID, brute.selected, strings.Join(details, " "))
	}
	for id, want := range brute.weighted {
		var got *model.NodeDecision
		for _, nd := range plan.Nodes {
			if nd.NodeID == id {
				got = nd
				break
			}
		}
		if got == nil {
			t.Fatalf("%s: plan missing score for %s", context, id)
		}
		if math.Abs(got.WeightedScore-want) > 1e-9 {
			t.Fatalf("%s: score for %s = %.12f, brute %.12f",
				context, id, got.WeightedScore, want)
		}
	}
}

func checkedSchedule(t *testing.T, st *model.State, w *model.Workload, version string, i int) bool {
	t.Helper()
	ctx := fmt.Sprintf("wl=%s version=%s i=%d replicas=%d", w.ID, version, i, len(st.Replicas))
	expected := bruteChoose(st, w, version)
	assertMatchesBrute(t, st, w, version, ctx)

	dec, _, err := scheduler.ScheduleOne(st, w.ID, version, "test-batch", i)
	if expected.selected == "" {
		if !errors.Is(err, scheduler.ErrNoFeasibleNode) {
			t.Fatalf("%s: expected no-feasible error, got %v", ctx, err)
		}
		return false
	}
	if err != nil {
		t.Fatalf("%s: unexpected schedule error: %v", ctx, err)
	}
	if dec.SelectedNodeID != expected.selected {
		t.Fatalf("%s: applied decision selected %s after brute expected %s",
			ctx, dec.SelectedNodeID, expected.selected)
	}
	if err := scheduler.ValidateHardConstraints(st); err != nil {
		t.Fatalf("%s: hard constraint invalid after schedule: %v", ctx, err)
	}
	if err := scheduler.ValidateNodeResources(st); err != nil {
		t.Fatalf("%s: resource invalid after schedule: %v", ctx, err)
	}
	return true
}

func checkedBatch(t *testing.T, st *model.State, w *model.Workload, version string, count int) int {
	t.Helper()
	placed := 0
	for i := 1; i <= count; i++ {
		if !checkedSchedule(t, st, w, version, i) {
			break
		}
		placed++
	}
	return placed
}

func generateRandomState(seed int64, idx int) *model.State {
	r := rand.New(rand.NewSource(seed + int64(idx)*1000003))
	hangarCount := 3 + r.Intn(12)
	st := &model.State{Seed: seed + int64(idx)}
	regions := []string{"north", "south", "east"}

	for hi := 1; hi <= hangarCount; hi++ {
		h := fmt.Sprintf("h-%02d", hi)
		region := regions[r.Intn(len(regions))]
		nodeCount := 1 + r.Intn(3)
		for ni := 1; ni <= nodeCount; ni++ {
			capIdx := r.Intn(3)
			cpuCap := []int64{8000, 16000, 32000}[capIdx]
			memCap := []int64{16384, 32768, 65536}[capIdx]
			loadPct := r.Intn(51)
			labels := map[string]string{
				scheduler.DefaultTopologyKey: h,
				"hangar":                     h,
				"region":                     region,
				"pool":                       map[bool]string{true: "stable", false: "canary"}[r.Intn(100) < 70],
				"disk":                       map[bool]string{true: "nvme", false: "ssd"}[r.Intn(100) < 40],
			}
			if r.Intn(100) < 35 {
				labels["special"] = "true"
			}
			n := &model.Node{
				ID:          fmt.Sprintf("%s-n%d", h, ni),
				Hangar:      h,
				Labels:      labels,
				Capacity:    model.Resources{CPUMillis: cpuCap, MemoryMB: memCap},
				Ready:       r.Intn(100) < 92,
				Schedulable: r.Intn(100) < 90,
			}
			n.Allocated = model.Resources{
				CPUMillis: cpuCap * int64(loadPct) / 100,
				MemoryMB:  memCap * int64(loadPct) / 100,
			}
			st.Nodes = append(st.Nodes, n)
		}
	}

	workloadCount := 2 + r.Intn(3)
	for wi := 1; wi <= workloadCount; wi++ {
		var selector map[string]string
		choice := r.Intn(5)
		if idx%4 == 0 && wi == 1 {
			choice = 1 // guarantee selector-based hangar exclusion
		}
		switch choice {
		case 0:
			selector = map[string]string{}
		case 1:
			selector = map[string]string{"region": regions[r.Intn(len(regions))]}
		case 2:
			selector = map[string]string{"special": "true"}
		case 3:
			selector = map[string]string{"disk": "nvme"}
		default:
			selector = map[string]string{"pool": "stable"}
		}

		maxSkew := 1 + r.Intn(3)
		var minDomains int
		if idx%3 == 0 && wi == 1 {
			minDomains = hangarCount + 1 + r.Intn(3) // guarantee unmet minDomains
		} else if r.Intn(100) < 35 {
			minDomains = 1 + r.Intn(7)
		}
		hard := r.Intn(100) < 72
		w := &model.Workload{
			ID:             fmt.Sprintf("wl-%d", wi),
			Name:           fmt.Sprintf("模拟工作负载 %d-%d", idx, wi),
			Version:        "v1",
			Selector:       selector,
			Request:        model.Resources{CPUMillis: int64(500 + r.Intn(2500)), MemoryMB: int64(512 + r.Intn(4096))},
			MinAvailable:   1 + r.Intn(4),
			ResourceWeight: 1 + r.Intn(3),
			Spread: &model.SpreadConstraint{
				TopologyKey: scheduler.DefaultTopologyKey,
				MaxSkew:     maxSkew,
				MinDomains:  minDomains,
				Hard:        hard,
				Weight:      1 + r.Intn(4),
			},
			DesiredReplicas: 2 + r.Intn(23),
		}
		st.Workloads = append(st.Workloads, w)
	}
	return st
}

func bruteScaleChoice(st *model.State, w *model.Workload, version string) string {
	counts := map[string]int{}
	var candidates []*model.Replica
	for _, r := range st.Replicas {
		if r.WorkloadID != w.ID || r.Version != version {
			continue
		}
		counts[r.Domain]++
		candidates = append(candidates, r)
	}
	max := -1
	for _, c := range counts {
		if c > max {
			max = c
		}
	}
	var top []*model.Replica
	for _, r := range candidates {
		if counts[r.Domain] == max {
			top = append(top, r)
		}
	}
	sort.Slice(top, func(i, j int) bool {
		fi := -1.0
		fj := -1.0
		if n := st.Node(top[i].NodeID); n != nil {
			_, _, fi = n.FreeRatio()
		}
		if n := st.Node(top[j].NodeID); n != nil {
			_, _, fj = n.FreeRatio()
		}
		if fi != fj {
			return fi > fj
		}
		return top[i].ID < top[j].ID
	})
	if len(top) == 0 {
		return ""
	}
	return top[0].ID
}

func addTailoredEmptyDomain(st *model.State, w *model.Workload, suffix int) string {
	d := fmt.Sprintf("extra-%02d", suffix)
	labels := map[string]string{scheduler.DefaultTopologyKey: d, "hangar": d}
	for k, v := range w.Selector {
		labels[k] = v
	}
	st.Nodes = append(st.Nodes, &model.Node{
		ID:          d + "-n1",
		Hangar:      d,
		Labels:      labels,
		Capacity:    model.Resources{CPUMillis: 128000, MemoryMB: 262144},
		Allocated:   model.Resources{},
		Ready:       true,
		Schedulable: true,
	})
	return d
}

func TestRandomScenariosMatchBruteForce(t *testing.T) {
	const seed = 0x5eed1234
	const scenarios = 72
	totalEvictions := 0

	for idx := 1; idx <= scenarios; idx++ {
		st := generateRandomState(seed, idx)

		for _, w := range st.Workloads {
			checkedBatch(t, st, w, w.Version, w.DesiredReplicas)
		}

		// Rolling updates: old and new ReplicaSets are scored independently.
		if idx%2 == 0 {
			w := st.Workloads[0]
			oldN := 2 + (idx % 9)
			newN := 2 + ((idx * 3) % 11)
			checkedBatch(t, st, w, "roll-v1", oldN)
			checkedBatch(t, st, w, "roll-v2", newN)
		}

		// Every scale-down must choose the max-count domain and resource tie.
		for _, w := range st.Workloads {
			versions := map[string]bool{}
			for _, r := range st.Replicas {
				if r.WorkloadID == w.ID {
					versions[r.Version] = true
				}
			}
			for v := range versions {
				want := bruteScaleChoice(st, w, v)
				if want == "" {
					continue
				}
				rec, err := scheduler.ScaleDownOne(st, w.ID, v)
				if err != nil {
					t.Fatalf("scenario %d scale %s/%s: %v", idx, w.ID, v, err)
				}
				if rec.ReplicaID != want {
					t.Fatalf("scenario %d scale %s/%s chose %s, brute wanted %s",
						idx, w.ID, v, rec.ReplicaID, want)
				}
				if err := scheduler.ValidateHardConstraints(st); err != nil {
					t.Fatalf("scenario %d after scale: %v", idx, err)
				}
			}
		}

		// Create fresh empty eligible domains, then verify eviction budget and
		// hard-constraint convergence under randomly declared budgets.
		domainSuffix := 0
		r := rand.New(rand.NewSource(seed + int64(idx)*424242))
		for _, w := range st.Workloads {
			if w.Spread != nil && w.Spread.Hard {
				addTailoredEmptyDomain(st, w, domainSuffix)
				domainSuffix++
			}
		}
		for _, w := range st.Workloads {
			avail := st.AvailableCount(w.ID)
			if avail == 0 {
				w.MinAvailable = 0
			} else {
				w.MinAvailable = r.Intn(avail + 1)
			}
		}

		initialReplicas := len(st.Replicas)
		for guard := 0; guard < initialReplicas+len(st.Workloads)+10; guard++ {
			rec, err := scheduler.EvictOne(st, "random-test")
			if errors.Is(err, scheduler.ErrNoViolation) {
				break
			}
			if errors.Is(err, scheduler.ErrMinAvailable) {
				if rec == nil || rec.Allowed {
					t.Fatalf("scenario %d: blocked eviction record invalid: %+v", idx, rec)
				}
				avail := st.AvailableCount(rec.WorkloadID)
				if avail < rec.MinAvailable {
					t.Fatalf("scenario %d: budget already below minAvailable: %d < %d",
						idx, avail, rec.MinAvailable)
				}
				// A new declaration may lower the budget; subsequent evictions
				// must continue to obey the new value.
				st.Workload(rec.WorkloadID).MinAvailable = 0
				continue
			}
			if err != nil {
				t.Fatalf("scenario %d evict: %v", idx, err)
			}
			if !rec.Allowed {
				t.Fatalf("scenario %d: returned non-blocking error but record not allowed", idx)
			}
			if got := st.AvailableCount(rec.WorkloadID); got < rec.MinAvailable {
				t.Fatalf("scenario %d: available %d below minAvailable %d after eviction %s",
					idx, got, rec.MinAvailable, rec.ID)
			}
			if rec.AvailableAfter < rec.MinAvailable {
				t.Fatalf("scenario %d record %s reports %d < min %d",
					idx, rec.ID, rec.AvailableAfter, rec.MinAvailable)
			}
			totalEvictions++
			if guard == initialReplicas+len(st.Workloads)+9 {
				t.Fatalf("scenario %d: eviction loop did not converge", idx)
			}
		}
		if err := scheduler.ValidateHardConstraints(st); err != nil {
			t.Fatalf("scenario %d final: %v", idx, err)
		}
	}

	t.Logf("verified %d fixed-seed scenarios; performed %d budget-checked evictions",
		scenarios, totalEvictions)
	if totalEvictions == 0 {
		t.Fatal("expected random scenarios to exercise at least one rebalance eviction")
	}
}

func TestBatchMatchesSequentialCalls(t *testing.T) {
	stA := generateRandomState(777, 9)
	stB := stA.Clone()
	w := stA.Workloads[0]
	const n = 12

	decsA, _, errA := scheduler.ScheduleBatch(stA, w.ID, w.Version, "batch", n)
	for i := 1; i <= n; i++ {
		_, _, errB := scheduler.ScheduleOne(stB, w.ID, w.Version, "single", i)
		if (errA != nil) != (errB != nil) {
			t.Fatalf("step %d error mismatch: batch=%v single=%v", i, errA, errB)
		}
		if errB != nil {
			break
		}
	}

	var nodesA, nodesB []string
	for _, r := range stA.Replicas {
		if r.WorkloadID == w.ID && r.Version == w.Version {
			nodesA = append(nodesA, r.NodeID)
		}
	}
	for _, r := range stB.Replicas {
		if r.WorkloadID == w.ID && r.Version == w.Version {
			nodesB = append(nodesB, r.NodeID)
		}
	}
	if strings.Join(nodesA, ",") != strings.Join(nodesB, ",") {
		t.Fatalf("batch placements %v do not match sequential %v", nodesA, nodesB)
	}
	if len(decsA) == 0 {
		t.Fatal("expected batch decisions")
	}
}

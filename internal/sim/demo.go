package sim

import (
	"fmt"
	"math/rand"

	"toposcheduler/internal/model"
	"toposcheduler/internal/scheduler"
)

type Simulation struct {
	mu    chan struct{} // semaphore-style lock, convenient for HTTP handlers
	State *model.State
	Seed  int64
}

func NewSimulation(st *model.State, seed int64) *Simulation {
	return &Simulation{mu: make(chan struct{}, 1), State: st, Seed: seed}
}

func (s *Simulation) Lock()   { s.mu <- struct{}{} }
func (s *Simulation) Unlock() { <-s.mu }

// NewDemo builds a deterministic drone-operations cluster and plays a useful
// script: initial placement, rolling update, scale-in, hangar expansion and
// rebalance evictions.
func NewDemo(seed int64) (*Simulation, error) {
	if seed == 0 {
		seed = 20261125
	}
	rng := rand.New(rand.NewSource(seed))
	st := &model.State{Seed: seed}

	regions := []string{"north", "south", "east"}
	for i := 1; i <= 12; i++ {
		h := fmt.Sprintf("h-%02d", i)
		region := regions[(i-1)/4]
		edge := i >= 9
		critical := i <= 5
		gpu := i <= 8

		for j := 1; j <= 2; j++ {
			id := fmt.Sprintf("%s-n%d", h, j)
			labels := map[string]string{
				"topology.kubernetes.io/zone": h,
				"hangar":                      h,
				"region":                      region,
				"node-pool":                   "flight",
			}
			if edge {
				labels["edge"] = "true"
			}
			if critical {
				labels["critical-control"] = "true"
			}
			if gpu && j == 1 {
				labels["accelerator"] = "inference"
			}

			cpu := int64(16000)
			mem := int64(32768)
			if i%4 == 0 && j == 2 {
				cpu = 8000
				mem = 16384
			}
			n := &model.Node{
				ID:          id,
				Hangar:      h,
				Labels:      labels,
				Capacity:    model.Resources{CPUMillis: cpu, MemoryMB: mem},
				Ready:       true,
				Schedulable: true,
			}
			// Deterministic background load from other services.
			loadPct := 15 + rng.Intn(35)
			n.Allocated = model.Resources{
				CPUMillis: cpu * int64(loadPct) / 100,
				MemoryMB:  mem * int64(loadPct) / 100,
			}
			st.Nodes = append(st.Nodes, n)
		}
	}

	weights := func(hard bool, skew, minDomains, sw, rw int) *model.SpreadConstraint {
		return &model.SpreadConstraint{
			TopologyKey: scheduler.DefaultTopologyKey,
			MaxSkew:     skew,
			MinDomains:  minDomains,
			Hard:        hard,
			Weight:      sw,
		}
	}

	workloads := []*model.Workload{
		{
			ID: "route-planner", Name: "航线规划服务", Version: "v2.4",
			Selector:       map[string]string{},
			Spread:         weights(true, 1, 0, 1, 1),
			Request:        model.Resources{CPUMillis: 1000, MemoryMB: 2048},
			MinAvailable:   3,
			ResourceWeight: 1, DesiredReplicas: 18,
		},
		{
			ID: "image-recognition", Name: "图像识别服务", Version: "v1.9",
			Selector:       map[string]string{"accelerator": "inference"},
			Spread:         weights(true, 1, 0, 2, 1),
			Request:        model.Resources{CPUMillis: 2000, MemoryMB: 4096},
			MinAvailable:   2,
			ResourceWeight: 1, DesiredReplicas: 12,
		},
		{
			ID: "spray-controller", Name: "喷洒控制服务", Version: "v3.1",
			Selector:       map[string]string{"critical-control": "true"},
			Spread:         weights(true, 1, 8, 1, 1),
			Request:        model.Resources{CPUMillis: 750, MemoryMB: 1024},
			MinAvailable:   2,
			ResourceWeight: 1, DesiredReplicas: 5,
		},
		{
			ID: "telemetry-gateway", Name: "遥测接入网关", Version: "v4.0",
			Selector:       map[string]string{"edge": "true"},
			Spread:         weights(true, 1, 0, 1, 2),
			Request:        model.Resources{CPUMillis: 800, MemoryMB: 1024},
			MinAvailable:   2,
			ResourceWeight: 2, DesiredReplicas: 8,
		},
		{
			ID: "map-tile-cache", Name: "地图瓦片缓存", Version: "v2.0",
			Selector:       map[string]string{},
			Spread:         weights(false, 1, 0, 3, 1),
			Request:        model.Resources{CPUMillis: 600, MemoryMB: 3072},
			MinAvailable:   2,
			ResourceWeight: 1, DesiredReplicas: 18,
		},
		{
			ID: "mission-api", Name: "任务编排 API", Version: "v2.5",
			Selector:       map[string]string{},
			Spread:         weights(true, 1, 3, 2, 1),
			Request:        model.Resources{CPUMillis: 900, MemoryMB: 1536},
			MinAvailable:   3,
			ResourceWeight: 1, DesiredReplicas: 8,
		},
		{
			ID: "report-worker", Name: "作业报告 Worker", Version: "v1.4",
			Selector:       map[string]string{},
			Spread:         weights(true, 1, 0, 1, 2),
			Request:        model.Resources{CPUMillis: 700, MemoryMB: 1024},
			MinAvailable:   2,
			ResourceWeight: 2, DesiredReplicas: 9,
		},
		{
			ID: "flight-history-db", Name: "飞行历史数据库", Version: "v8",
			Selector:       map[string]string{},
			Spread:         weights(true, 2, 3, 1, 1),
			Request:        model.Resources{CPUMillis: 1200, MemoryMB: 4096},
			MinAvailable:   2,
			ResourceWeight: 1, DesiredReplicas: 18,
		},
	}
	st.Workloads = workloads

	mustBatch := func(id, version, batch string, n int) {
		_, _, err := scheduler.ScheduleBatch(st, id, version, batch, n)
		if err != nil {
			panic(fmt.Sprintf("demo setup schedule %s/%s: %v", id, version, err))
		}
	}

	mustBatch("route-planner", "", "initial", 18)
	mustBatch("image-recognition", "", "initial", 12)
	mustBatch("spray-controller", "", "initial", 5)
	mustBatch("telemetry-gateway", "", "initial", 8)
	mustBatch("map-tile-cache", "", "initial", 18)

	// Rolling update: six old replicas remain while eight new-version replicas
	// are scheduled. Counts are version-scoped.
	_, _, err := scheduler.ScheduleBatch(st, "mission-api", "v2.4", "rolling-old", 6)
	if err != nil {
		return nil, err
	}
	_, _, err = scheduler.ScheduleBatch(st, "mission-api", "v2.5", "rolling-new", 8)
	if err != nil {
		return nil, err
	}

	mustBatch("report-worker", "", "initial", 9)
	for i := 0; i < 3; i++ {
		if _, err := scheduler.ScaleDownOne(st, "report-worker", "v1.4"); err != nil {
			return nil, err
		}
	}

	mustBatch("flight-history-db", "", "initial", 18)

	// Hangar expansion creates a new empty eligible domain and lowers the
	// global minimum to zero, exposing previously allowed skew-1 double domains
	// as skew-2 hard violations.
	if err := AddDemoHangar(st, "h-13", true); err != nil {
		return nil, err
	}
	evs, err := scheduler.RebalanceAll(st, "新增机库 h-13 后拓扑重平衡")
	if err != nil {
		return nil, fmt.Errorf("demo rebalance: %w (evictions=%d)", err, len(evs))
	}
	st.Events = append(st.Events, &model.TopologyEvent{
		ID:     fmt.Sprintf("event-%d", st.NextID()),
		Seq:    st.Seq,
		Kind:   "rebalance_completed",
		Detail: fmt.Sprintf("h-13 上线后完成 %d 次驱逐并补回副本", len(evs)),
	})

	if err := scheduler.ValidateHardConstraints(st); err != nil {
		return nil, err
	}
	return NewSimulation(st, seed), nil
}

// AddDemoHangar adds two nodes to a new hangar. gpu controls whether the demo
// inference selector can use it.
func AddDemoHangar(st *model.State, h string, gpu bool) error {
	for _, existing := range st.Hangars() {
		if existing == h {
			return fmt.Errorf("hangar %s already exists", h)
		}
	}
	seq := st.NextID()
	st.Events = append(st.Events, &model.TopologyEvent{
		ID:     fmt.Sprintf("event-%05d", seq),
		Seq:    seq,
		Kind:   "hangar_added",
		Detail: fmt.Sprintf("新增机库/拓扑域 %s，含 2 个空节点", h),
	})
	for j := 1; j <= 2; j++ {
		labels := map[string]string{
			"topology.kubernetes.io/zone": h,
			"hangar":                      h,
			"region":                      "west",
			"node-pool":                   "flight",
		}
		if gpu {
			labels["accelerator"] = "inference"
		}
		st.Nodes = append(st.Nodes, &model.Node{
			ID:          fmt.Sprintf("%s-n%d", h, j),
			Hangar:      h,
			Labels:      labels,
			Capacity:    model.Resources{CPUMillis: 16000, MemoryMB: 32768},
			Allocated:   model.Resources{},
			Ready:       true,
			Schedulable: true,
		})
	}
	return nil
}

// AddNextHangar is the HTTP action used after the initial demo.
func (s *Simulation) AddNextHangar() (*model.TopologyEvent, error) {
	s.Lock()
	defer s.Unlock()
	next := len(s.State.Hangars()) + 1
	h := fmt.Sprintf("h-%02d", next)
	for s.State.Node(h+"-n1") != nil {
		next++
		h = fmt.Sprintf("h-%02d", next)
	}
	if err := AddDemoHangar(s.State, h, next%2 == 1); err != nil {
		return nil, err
	}
	return s.State.Events[len(s.State.Events)-1], nil
}

func (s *Simulation) Rebalance() ([]*model.DisruptionRecord, error) {
	s.Lock()
	defer s.Unlock()
	return scheduler.RebalanceAll(s.State, "页面手动触发后台重平衡")
}

func (s *Simulation) ScaleDown(workloadID, version string) (*model.DisruptionRecord, error) {
	s.Lock()
	defer s.Unlock()
	return scheduler.ScaleDownOne(s.State, workloadID, version)
}

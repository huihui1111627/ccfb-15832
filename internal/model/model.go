package model

import (
	"encoding/json"
	"fmt"
	"sort"
)

// Resources uses integer milli-CPU and MiB memory so simulations and tests
// have deterministic resource arithmetic.
type Resources struct {
	CPUMillis int64 `json:"cpu_millis"`
	MemoryMB  int64 `json:"memory_mb"`
}

func (r Resources) Add(o Resources) Resources {
	return Resources{CPUMillis: r.CPUMillis + o.CPUMillis, MemoryMB: r.MemoryMB + o.MemoryMB}
}

func (r Resources) Sub(o Resources) Resources {
	return Resources{CPUMillis: r.CPUMillis - o.CPUMillis, MemoryMB: r.MemoryMB - o.MemoryMB}
}

func (r Resources) Fits(request Resources) bool {
	return r.CPUMillis >= request.CPUMillis && r.MemoryMB >= request.MemoryMB
}

type Node struct {
	ID          string            `json:"id"`
	Hangar      string            `json:"hangar"`
	Labels      map[string]string `json:"labels"`
	Capacity    Resources         `json:"capacity"`
	Allocated   Resources         `json:"allocated"`
	Ready       bool              `json:"ready"`
	Schedulable bool              `json:"schedulable"`
}

func (n *Node) Free() Resources {
	free := n.Capacity.Sub(n.Allocated)
	if free.CPUMillis < 0 {
		free.CPUMillis = 0
	}
	if free.MemoryMB < 0 {
		free.MemoryMB = 0
	}
	return free
}

// FreeRatio returns average-free resource score inputs in [0,1].
func (n *Node) FreeRatio() (cpu float64, memory float64, average float64) {
	free := n.Free()
	cpu = float64(free.CPUMillis) / float64(n.Capacity.CPUMillis)
	memory = float64(free.MemoryMB) / float64(n.Capacity.MemoryMB)
	if cpu < 0 {
		cpu = 0
	}
	if cpu > 1 {
		cpu = 1
	}
	if memory < 0 {
		memory = 0
	}
	if memory > 1 {
		memory = 1
	}
	return cpu, memory, (cpu + memory) / 2
}

type ReplicaPhase string

const (
	PhaseRunning ReplicaPhase = "Running"
)

type Replica struct {
	ID         string       `json:"id"`
	WorkloadID string       `json:"workload_id"`
	Version    string       `json:"version"`
	NodeID     string       `json:"node_id"`
	Domain     string       `json:"domain"`
	Request    Resources    `json:"request"`
	Available  bool         `json:"available"`
	Phase      ReplicaPhase `json:"phase"`
	CreatedSeq int64        `json:"created_seq"`
}

// SpreadConstraint is the single topology distribution constraint declared by
// a workload. Hard constraints filter nodes; soft constraints only score.
type SpreadConstraint struct {
	TopologyKey string `json:"topology_key"`
	MaxSkew     int    `json:"max_skew"`
	// MinDomains <= 0 means unset. When the number of eligible domains is
	// smaller than MinDomains, the global minimum replica count is treated as 0.
	MinDomains int  `json:"min_domains,omitempty"`
	Hard       bool `json:"hard"`
	// Weight is the soft topology score weight. It is ignored for hard mode.
	Weight int `json:"weight,omitempty"`
}

type Workload struct {
	ID              string            `json:"id"`
	Name            string            `json:"name"`
	Version         string            `json:"version"`
	Selector        map[string]string `json:"selector,omitempty"`
	Spread          *SpreadConstraint `json:"spread,omitempty"`
	Request         Resources         `json:"request"`
	MinAvailable    int               `json:"min_available"`
	ResourceWeight  int               `json:"resource_weight,omitempty"`
	DesiredReplicas int               `json:"desired_replicas"`
}

// NodeDecision contains both Filter and Score output for one node in one
// scheduling attempt.
type NodeDecision struct {
	NodeID   string `json:"node_id"`
	Hangar   string `json:"hangar"`
	Eligible bool   `json:"eligible"`
	Selected bool   `json:"selected"`

	// FilterFailures is empty when all Filter plugins accept the node.
	FilterFailures []string `json:"filter_failures,omitempty"`

	EligibleDomainCount int  `json:"eligible_domain_count,omitempty"`
	Baseline            int  `json:"baseline,omitempty"`
	BaselineForcedZero  bool `json:"baseline_forced_zero,omitempty"`
	CurrentDomainCount  int  `json:"current_domain_count,omitempty"`
	PostSkew            int  `json:"post_skew,omitempty"`
	MaxSkew             int  `json:"max_skew,omitempty"`
	MinDomains          int  `json:"min_domains,omitempty"`

	ResourceScore  float64 `json:"resource_score"`
	SpreadScore    float64 `json:"spread_score,omitempty"`
	SpreadScored   bool    `json:"spread_scored"`
	SpreadWeight   int     `json:"spread_weight,omitempty"`
	ResourceWeight int     `json:"resource_weight"`
	WeightedScore  float64 `json:"weighted_score"`
}

type DomainStat struct {
	Domain       string `json:"domain"`
	NodeCount    int    `json:"node_count"`
	ReplicaCount int    `json:"replica_count"`
	Skew         int    `json:"skew"`
	Eligible     bool   `json:"eligible"`
	Reason       string `json:"reason,omitempty"`
}

type SchedulingDecision struct {
	ID             string `json:"id"`
	Seq            int64  `json:"seq"`
	BatchID        string `json:"batch_id,omitempty"`
	BatchIndex     int    `json:"batch_index,omitempty"`
	WorkloadID     string `json:"workload_id"`
	WorkloadName   string `json:"workload_name"`
	Version        string `json:"version"`
	RequestedAtSeq int64  `json:"requested_at_seq"`

	TopologyKey string `json:"topology_key,omitempty"`
	MaxSkew     int    `json:"max_skew,omitempty"`
	MinDomains  int    `json:"min_domains,omitempty"`
	Hard        bool   `json:"hard"`

	EligibleDomainCount int             `json:"eligible_domain_count"`
	Baseline            int             `json:"baseline"`
	BaselineForcedZero  bool            `json:"baseline_forced_zero"`
	Domains             []DomainStat    `json:"domains,omitempty"`
	Nodes               []*NodeDecision `json:"nodes"`
	FeasibleNodeIDs     []string        `json:"feasible_node_ids"`
	SelectedNodeID      string          `json:"selected_node_id,omitempty"`
	SelectedReplicaID   string          `json:"selected_replica_id,omitempty"`
	Err                 string          `json:"error,omitempty"`
}

// DisruptionRecord is used for both explicit scale-in deletions and
// background rebalance evictions.
type DisruptionRecord struct {
	ID      string `json:"id"`
	Seq     int64  `json:"seq"`
	Kind    string `json:"kind"`   // scale_in | rebalance
	Status  string `json:"status"` // deleted | evicted | blocked_min_available
	Allowed bool   `json:"allowed"`
	Reason  string `json:"reason"`
	Trigger string `json:"trigger,omitempty"`

	WorkloadID   string `json:"workload_id"`
	WorkloadName string `json:"workload_name"`
	Version      string `json:"version"`
	ReplicaID    string `json:"replica_id,omitempty"`
	NodeID       string `json:"node_id,omitempty"`
	Domain       string `json:"domain,omitempty"`

	DomainReplicaCount int     `json:"domain_replica_count"`
	ResourceFreeScore  float64 `json:"resource_free_score,omitempty"`
	Baseline           int     `json:"baseline"`
	SkewBefore         int     `json:"skew_before"`
	SkewAfter          int     `json:"skew_after"`
	MaxSkew            int     `json:"max_skew"`

	MinAvailable      int    `json:"min_available"`
	AvailableBefore   int    `json:"available_before"`
	AvailableAfter    int    `json:"available_after"`
	ReplacementNodeID string `json:"replacement_node_id,omitempty"`
	ReplacementErr    string `json:"replacement_error,omitempty"`
}

type TopologyEvent struct {
	ID     string `json:"id"`
	Seq    int64  `json:"seq"`
	Kind   string `json:"kind"`
	Detail string `json:"detail"`
}

type State struct {
	Seq         int64                 `json:"seq"`
	Seed        int64                 `json:"seed"`
	Nodes       []*Node               `json:"nodes"`
	Workloads   []*Workload           `json:"workloads"`
	Replicas    []*Replica            `json:"replicas"`
	Decisions   []*SchedulingDecision `json:"decisions"`
	Disruptions []*DisruptionRecord   `json:"disruptions"`
	Events      []*TopologyEvent      `json:"events"`
}

func (s *State) NextID() int64 {
	s.Seq++
	return s.Seq
}

func (s *State) Node(id string) *Node {
	for _, n := range s.Nodes {
		if n.ID == id {
			return n
		}
	}
	return nil
}

func (s *State) Workload(id string) *Workload {
	for _, w := range s.Workloads {
		if w.ID == id {
			return w
		}
	}
	return nil
}

func (s *State) SortedNodes() []*Node {
	out := append([]*Node(nil), s.Nodes...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (s *State) Hangars() []string {
	seen := map[string]bool{}
	var out []string
	for _, n := range s.Nodes {
		h := n.Hangar
		if h == "" {
			h = n.Labels["topology.kubernetes.io/zone"]
		}
		if h != "" && !seen[h] {
			seen[h] = true
			out = append(out, h)
		}
	}
	sort.Strings(out)
	return out
}

func (s *State) AvailableCount(workloadID string) int {
	n := 0
	for _, r := range s.Replicas {
		if r.WorkloadID == workloadID && r.Available && r.Phase == PhaseRunning {
			n++
		}
	}
	return n
}

func (s *State) Clone() *State {
	b, err := json.Marshal(s)
	if err != nil {
		panic(fmt.Sprintf("clone state: %v", err))
	}
	var out State
	if err := json.Unmarshal(b, &out); err != nil {
		panic(fmt.Sprintf("clone state: %v", err))
	}
	return &out
}

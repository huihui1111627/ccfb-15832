package sim

import (
	"fmt"

	"toposcheduler/internal/model"
)

// SetHangarPower simulates a whole-hangar power event. When power is lost, its
// nodes become unavailable and replicas running there are treated as lost.
// The topology domain then has no available node and is excluded from skew
// calculation by scheduler.InspectDomains.
func (s *Simulation) SetHangarPower(hangar string, on bool) (*model.TopologyEvent, int, error) {
	s.Lock()
	defer s.Unlock()
	ev, lost, err := SetHangarPower(s.State, hangar, on)
	return ev, lost, err
}

// SetHangarPower is the state-level operation used by tests and the web layer.
func SetHangarPower(st *model.State, hangar string, on bool) (*model.TopologyEvent, int, error) {
	var nodes []*model.Node
	nodeIDs := map[string]bool{}
	for _, n := range st.Nodes {
		if n.Hangar == hangar {
			nodes = append(nodes, n)
			nodeIDs[n.ID] = true
		}
	}
	if len(nodes) == 0 {
		return nil, 0, fmt.Errorf("hangar %s not found", hangar)
	}

	lost := 0
	if !on {
		for _, n := range nodes {
			n.Ready = false
			n.Schedulable = false
			n.Allocated = model.Resources{}
		}
		kept := st.Replicas[:0]
		for _, r := range st.Replicas {
			if nodeIDs[r.NodeID] {
				lost++
				continue
			}
			kept = append(kept, r)
		}
		st.Replicas = kept
	} else {
		for _, n := range nodes {
			n.Ready = true
			n.Schedulable = true
		}
	}

	kind := "hangar_power_on"
	detail := fmt.Sprintf("机库 %s 恢复供电，%d 个节点恢复可调度", hangar, len(nodes))
	if !on {
		kind = "hangar_power_off"
		detail = fmt.Sprintf("机库 %s 停电：%d 个节点不可用，%d 个副本丢失；该拓扑域不再参与偏斜基准",
			hangar, len(nodes), lost)
	}
	seq := st.NextID()
	ev := &model.TopologyEvent{ID: fmt.Sprintf("event-%05d", seq), Seq: seq, Kind: kind, Detail: detail}
	st.Events = append(st.Events, ev)
	return ev, lost, nil
}

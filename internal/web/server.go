package web

import (
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"log"
	"net/http"

	"toposcheduler/internal/scheduler"
	"toposcheduler/internal/sim"
)

//go:embed static
var staticFS embed.FS

type Server struct {
	Sim *sim.Simulation
	Mux *http.ServeMux
}

func New(s *sim.Simulation) *Server {
	srv := &Server{Sim: s, Mux: http.NewServeMux()}
	srv.routes()
	return srv
}

func (s *Server) Handler() http.Handler {
	return s.Mux
}

type actionResponse struct {
	OK        bool          `json:"ok"`
	Message   string        `json:"message,omitempty"`
	Error     string        `json:"error,omitempty"`
	Dashboard sim.Dashboard `json:"dashboard"`
}

func (s *Server) routes() {
	s.Mux.HandleFunc("GET /api/dashboard", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, s.Sim.Dashboard())
	})

	s.Mux.HandleFunc("POST /api/reset", s.handleReset)
	s.Mux.HandleFunc("POST /api/add-hangar", s.handleAddHangar)
	s.Mux.HandleFunc("POST /api/power-hangar", s.handlePowerHangar)
	s.Mux.HandleFunc("POST /api/rebalance", s.handleRebalance)
	s.Mux.HandleFunc("POST /api/scale-down", s.handleScaleDown)

	staticSub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err)
	}
	s.Mux.Handle("GET /", http.FileServer(http.FS(staticSub)))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("write json: %v", err)
	}
}

func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Seed int64 `json:"seed"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Seed == 0 {
		req.Seed = 20261125
	}
	next, err := sim.NewDemo(req.Seed)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, actionResponse{
			OK: false, Error: err.Error(), Dashboard: s.Sim.Dashboard(),
		})
		return
	}
	s.Sim.Lock()
	s.Sim.State = next.State
	s.Sim.Seed = next.Seed
	s.Sim.Unlock()

	writeJSON(w, http.StatusOK, actionResponse{
		OK: true, Message: "已按固定种子重新生成集群并播放调度/滚动/缩容/重平衡剧本",
		Dashboard: s.Sim.Dashboard(),
	})
}

func (s *Server) handleAddHangar(w http.ResponseWriter, r *http.Request) {
	ev, err := s.Sim.AddNextHangar()
	resp := actionResponse{Dashboard: s.Sim.Dashboard()}
	if err != nil {
		resp.OK = false
		resp.Error = err.Error()
		writeJSON(w, http.StatusBadRequest, resp)
		return
	}
	resp.OK = true
	resp.Message = "已新增机库；旧副本可能立即出现拓扑偏斜，可执行重平衡"
	if ev != nil {
		resp.Message = ev.Detail
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handlePowerHangar(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Hangar string `json:"hangar"`
		On     bool   `json:"on"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, actionResponse{OK: false, Error: err.Error()})
		return
	}
	ev, lost, err := s.Sim.SetHangarPower(req.Hangar, req.On)
	resp := actionResponse{Dashboard: s.Sim.Dashboard()}
	if err != nil {
		resp.OK = false
		resp.Error = err.Error()
		writeJSON(w, http.StatusBadRequest, resp)
		return
	}
	resp.OK = true
	if ev != nil {
		resp.Message = ev.Detail
	}
	_ = lost
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleRebalance(w http.ResponseWriter, r *http.Request) {
	records, err := s.Sim.Rebalance()
	resp := actionResponse{Dashboard: s.Sim.Dashboard()}
	switch {
	case err == nil:
		resp.OK = true
		resp.Message = "重平衡完成"
		if len(records) > 0 {
			resp.Message = "重平衡完成，本次处理 " + itoa(len(records)) + " 次驱逐/补位"
		} else {
			resp.Message = "没有发现需要驱逐的硬约束违反"
		}
	case errors.Is(err, scheduler.ErrMinAvailable):
		resp.OK = true
		resp.Error = err.Error()
		resp.Message = "已到达 minAvailable 保护下限，剩余违反暂不继续驱逐"
	default:
		resp.OK = false
		resp.Error = err.Error()
		resp.Message = "重平衡未完全收敛"
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleScaleDown(w http.ResponseWriter, r *http.Request) {
	var req struct {
		WorkloadID string `json:"workload_id"`
		Version    string `json:"version"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, actionResponse{OK: false, Error: err.Error()})
		return
	}
	rec, err := s.Sim.ScaleDown(req.WorkloadID, req.Version)
	resp := actionResponse{Dashboard: s.Sim.Dashboard()}
	if err != nil {
		resp.OK = false
		resp.Error = err.Error()
		writeJSON(w, http.StatusBadRequest, resp)
		return
	}
	resp.OK = true
	resp.Message = "已按最大域副本数和节点空闲度缩容：" + rec.ReplicaID
	writeJSON(w, http.StatusOK, resp)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

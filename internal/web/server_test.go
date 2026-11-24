package web_test

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"toposcheduler/internal/sim"
	"toposcheduler/internal/web"
)

func TestDashboardAndActionRoutes(t *testing.T) {
	simulation, err := sim.NewDemo(20261125)
	if err != nil {
		t.Fatal(err)
	}
	server := web.New(simulation)

	get := httptest.NewRequest("GET", "/api/dashboard", nil)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, get)
	if rec.Code != 200 {
		t.Fatalf("dashboard status=%d body=%s", rec.Code, rec.Body.String())
	}
	var dash map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &dash); err != nil {
		t.Fatal(err)
	}
	if dash["hangars"] == nil || dash["distribution"] == nil {
		t.Fatalf("dashboard missing expected fields: %v", dash)
	}

	postJSON := func(path, body string) map[string]any {
		req := httptest.NewRequest("POST", path, bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		server.Handler().ServeHTTP(rr, req)
		if rr.Code != 200 {
			t.Fatalf("%s status=%d body=%s", path, rr.Code, rr.Body.String())
		}
		var out map[string]any
		if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	scale := postJSON("/api/scale-down", `{"workload_id":"route-planner","version":"v2.4"}`)
	if scale["ok"] != true {
		t.Fatalf("scale-down failed: %v", scale)
	}
	power := postJSON("/api/power-hangar", `{"hangar":"h-02","on":false}`)
	if power["ok"] != true {
		t.Fatalf("power action failed: %v", power)
	}
	rebalance := postJSON("/api/rebalance", `{}`)
	if _, ok := rebalance["dashboard"]; !ok {
		t.Fatalf("rebalance response missing dashboard: %v", rebalance)
	}

	indexReq := httptest.NewRequest("GET", "/", nil)
	indexRec := httptest.NewRecorder()
	server.Handler().ServeHTTP(indexRec, indexReq)
	if indexRec.Code != 200 || !strings.Contains(indexRec.Body.String(), "拓扑分布约束") {
		t.Fatalf("index not served: code=%d", indexRec.Code)
	}
}

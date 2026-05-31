package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestHealthChecker_Standalone(t *testing.T) {
	cfg := &types.Config{
		NodeID:         "node-1",
		ClusterEnabled: false,
		PartitionCount: 8,
	}

	// standalone health check without partition manager or cluster manager
	hc := NewHealthChecker(cfg, nil, nil)
	mux := http.NewServeMux()
	hc.Register(mux)

	// Test /health
	req := httptest.NewRequest("GET", "/health", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rr.Code)
	}
	expected := "OK - Standalone Mode\n"
	if rr.Body.String() != expected {
		t.Errorf("Expected body %q, got %q", expected, rr.Body.String())
	}
}

func TestHealthChecker_Ready_Unhealthy(t *testing.T) {
	cfg := &types.Config{
		NodeID:         "node-1",
		ClusterEnabled: false,
		PartitionCount: 8,
	}

	pm := partition.NewPartitionManager("node-1", cfg)
	hc := NewHealthChecker(cfg, pm, nil)
	mux := http.NewServeMux()
	hc.Register(mux)

	// Test /health/ready - should be unhealthy because 0 partitions are loaded/active
	req := httptest.NewRequest("GET", "/health/ready", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503, got %d", rr.Code)
	}

	var resp DeepHealthResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if resp.Healthy {
		t.Error("Expected ready check to be unhealthy")
	}
	if resp.Status != "not_ready" {
		t.Errorf("Expected status 'not_ready', got %s", resp.Status)
	}
	chk, exists := resp.Checks["partitions"]
	if !exists {
		t.Fatal("Expected 'partitions' check in output")
	}
	if chk.Healthy {
		t.Error("Expected partitions check to be unhealthy")
	}
}

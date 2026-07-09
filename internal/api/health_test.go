package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

func TestHealthChecker_Ready_ReadOnlyWALCheck(t *testing.T) {
	skipWindows(t)

	cfg := &types.Config{
		NodeID:         "node-1",
		ClusterEnabled: false,
		PartitionCount: 1,
		TickMS:         10,
		WheelSize:      100,
	}

	pm := partition.NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	if err := pm.CreatePartition(0, "topic-test"); err != nil {
		t.Fatalf("CreatePartition failed: %v", err)
	}
	if err := pm.StartPartition(0); err != nil {
		t.Fatalf("StartPartition failed: %v", err)
	}

	// Append an event so the WAL has a readable high watermark.
	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatalf("GetInternalPartition failed: %v", err)
	}
	event := &types.Event{
		MessageId:  "health-readonly-test",
		Topic:      "topic-test",
		Payload:    []byte("payload"),
		ScheduleTs: time.Now().UnixMilli() + 1000,
	}
	if err := p.Wal.AppendEvent(event); err != nil {
		t.Fatalf("AppendEvent failed: %v", err)
	}

	hc := NewHealthChecker(cfg, pm, nil)
	mux := http.NewServeMux()
	hc.Register(mux)

	req := httptest.NewRequest("GET", "/health/ready", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp DeepHealthResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if !resp.Healthy {
		t.Errorf("Expected ready check to be healthy, got status %s", resp.Status)
	}
	chk, exists := resp.Checks["wal_readable"]
	if !exists {
		t.Fatal("Expected 'wal_readable' check in output")
	}
	if !chk.Healthy {
		t.Errorf("Expected wal_readable check to be healthy, got detail: %s", chk.Detail)
	}
	if !strings.Contains(chk.Detail, "HWM=") {
		t.Errorf("Expected wal_readable detail to contain HWM, got: %s", chk.Detail)
	}
}

func TestHealthChecker_Deep_ReadOnlyWALCheck(t *testing.T) {
	skipWindows(t)

	cfg := &types.Config{
		NodeID:         "node-1",
		ClusterEnabled: false,
		PartitionCount: 1,
		TickMS:         10,
		WheelSize:      100,
	}

	pm := partition.NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	if err := pm.CreatePartition(0, "topic-test"); err != nil {
		t.Fatalf("CreatePartition failed: %v", err)
	}
	if err := pm.StartPartition(0); err != nil {
		t.Fatalf("StartPartition failed: %v", err)
	}

	// Append an event so the WAL has a readable high watermark.
	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatalf("GetInternalPartition failed: %v", err)
	}
	event := &types.Event{
		MessageId:  "health-deep-readonly-test",
		Topic:      "topic-test",
		Payload:    []byte("payload"),
		ScheduleTs: time.Now().UnixMilli() + 1000,
	}
	if err := p.Wal.AppendEvent(event); err != nil {
		t.Fatalf("AppendEvent failed: %v", err)
	}
	expectedHWM := p.Wal.GetHighWatermark()

	hc := NewHealthChecker(cfg, pm, nil)
	mux := http.NewServeMux()
	hc.Register(mux)

	req := httptest.NewRequest("GET", "/health/deep", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp DeepHealthResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if !resp.Healthy {
		t.Errorf("Expected deep health check to be healthy, got status %s", resp.Status)
	}
	chk, exists := resp.Checks["partition_0_wal"]
	if !exists {
		t.Fatalf("Expected 'partition_0_wal' check in output, got %+v", resp.Checks)
	}
	if !chk.Healthy {
		t.Errorf("Expected partition_0_wal check to be healthy, got detail: %s", chk.Detail)
	}
	if !strings.Contains(chk.Detail, fmt.Sprintf("HWM=%d", expectedHWM)) {
		t.Errorf("Expected partition_0_wal detail to contain HWM=%d, got: %s", expectedHWM, chk.Detail)
	}
}

func TestHealthChecker_Ready_NoWALWrites(t *testing.T) {
	skipWindows(t)

	cfg := &types.Config{
		NodeID:         "node-1",
		ClusterEnabled: false,
		PartitionCount: 1,
		TickMS:         10,
		WheelSize:      100,
	}

	pm := partition.NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	if err := pm.CreatePartition(0, "topic-test"); err != nil {
		t.Fatalf("CreatePartition failed: %v", err)
	}
	if err := pm.StartPartition(0); err != nil {
		t.Fatalf("StartPartition failed: %v", err)
	}

	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatalf("GetInternalPartition failed: %v", err)
	}
	hwmBefore := p.Wal.GetHighWatermark()

	hc := NewHealthChecker(cfg, pm, nil)
	mux := http.NewServeMux()
	hc.Register(mux)

	// Call /health/ready and /health/deep multiple times.
	for _, path := range []string{"/health/ready", "/health/deep"} {
		for i := 0; i < 3; i++ {
			req := httptest.NewRequest("GET", path, nil)
			rr := httptest.NewRecorder()
			mux.ServeHTTP(rr, req)
			if rr.Code != http.StatusOK {
				t.Errorf("Expected status 200 for %s, got %d: %s", path, rr.Code, rr.Body.String())
			}
		}
	}

	hwmAfter := p.Wal.GetHighWatermark()
	if hwmAfter != hwmBefore {
		t.Errorf("health checks mutated the WAL: HWM before=%d, after=%d", hwmBefore, hwmAfter)
	}
}

package api

import (
	"context"
	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAuditReadyWithoutReplicationLeader(t *testing.T) {
	cfg := &types.Config{DataDir: t.TempDir(), PartitionCount: 1, ReplicationFactor: 3, MinInSyncReplicas: 2, TickMS: 10, WheelSize: 100, SegmentSizeBytes: 1 << 20, IndexInterval: 1, FsyncMode: "batch", FlushIntervalMS: 100, DedupTTLHours: 24, BloomCapacity: 1000}
	pm := partition.NewPartitionManager("audit", cfg)
	t.Cleanup(func() { pm.Close() })
	if err := pm.CreatePartition(0, "audit"); err != nil {
		t.Fatal(err)
	}
	if err := pm.StartPartition(0); err != nil {
		t.Fatal(err)
	}
	p, _ := pm.GetInternalPartition(0)
	h := NewEventServiceHandler(pm, p.DedupStore, p.ConsumerGroup)
	resp, err := h.Publish(context.Background(), &types.PublishRequest{Event: &types.Event{MessageId: "audit", Topic: "audit", Payload: []byte("p"), ScheduleTs: time.Now().Add(time.Hour).UnixMilli()}})
	if err != nil || resp.GetSuccess() {
		t.Fatalf("unexpected publish outcome: %v %v", resp, err)
	}
	rr := httptest.NewRecorder()
	NewHealthChecker(cfg, pm, nil).handleReady(rr, httptest.NewRequest("GET", "/health/ready", nil))
	if rr.Code == 200 {
		t.Fatalf("readiness returned 200 while publish refused: %s", resp.GetError())
	}
}

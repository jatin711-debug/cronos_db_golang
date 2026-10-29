package api

import (
	"context"
	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/internal/replication"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"testing"
	"time"
)

func auditManager(t *testing.T) *partition.PartitionManager {
	t.Helper()
	cfg := &types.Config{DataDir: t.TempDir(), PartitionCount: 1, TickMS: 10, WheelSize: 100, SegmentSizeBytes: 1 << 20, IndexInterval: 1, FsyncMode: "batch", FlushIntervalMS: 100, DedupTTLHours: 24, BloomCapacity: 1000}
	pm := partition.NewPartitionManager("audit", cfg)
	t.Cleanup(func() { pm.Close() })
	if err := pm.CreatePartition(0, "audit"); err != nil {
		t.Fatal(err)
	}
	return pm
}
func TestAuditDelayedAppendMustNotTruncateAcknowledgedTail(t *testing.T) {
	pm := auditManager(t)
	h := NewReplicationServiceHandler(pm)
	events := []*types.Event{{MessageId: "zero", Topic: "audit", Offset: 0, Payload: []byte("a"), ScheduleTs: 1}, {MessageId: "one", Topic: "audit", Offset: 1, Payload: []byte("b"), ScheduleTs: 1}}
	r, err := h.Append(context.Background(), &types.ReplicationAppendRequest{PartitionId: 0, Term: 2, Events: events})
	if err != nil || !r.GetSuccess() {
		t.Fatalf("setup append: %v %v", r, err)
	}
	r, err = h.Append(context.Background(), &types.ReplicationAppendRequest{PartitionId: 0, Term: 2, Events: events[:1]})
	if err != nil {
		t.Fatal(err)
	}
	p, _ := pm.GetInternalPartition(0)
	if n := p.Wal.GetNextOffset(); n != 2 {
		t.Fatalf("same-term retry erased acknowledged tail: next offset=%d, want 2 (response=%v)", n, r)
	}
}
func TestAuditZeroTermCannotBypassFence(t *testing.T) {
	pm := auditManager(t)
	p, _ := pm.GetInternalPartition(0)
	p.Epoch = 10
	r, err := NewReplicationServiceHandler(pm).Append(context.Background(), &types.ReplicationAppendRequest{PartitionId: 0, Term: 0, Events: []*types.Event{{MessageId: "stale", Offset: 0, Payload: []byte("x"), ScheduleTs: 1}}})
	if err == nil && r.GetSuccess() {
		t.Fatal("term 0 append accepted despite current epoch 10")
	}
}
func TestAuditQuorumFailureRetryMustNotReportSuccess(t *testing.T) {
	pm := auditManager(t)
	p, _ := pm.GetInternalPartition(0)
	p.ReplLeader = replication.NewLeader(0, 100, time.Second, p.Wal, 2, "audit", nil)
	defer p.ReplLeader.Stop()
	h := NewEventServiceHandler(pm, p.DedupStore, p.ConsumerGroup)
	req := &types.PublishBatchRequest{Events: []*types.Event{{MessageId: "quorum-failure", Topic: "audit", Payload: []byte("payload"), ScheduleTs: time.Now().Add(time.Minute).UnixMilli()}}}
	r, err := h.PublishBatch(context.Background(), req)
	if err != nil || r.GetSuccess() {
		t.Fatalf("expected initial quorum failure, got %v %v", r, err)
	}
	r, err = h.PublishBatch(context.Background(), req)
	if err == nil && r.GetSuccess() {
		t.Fatalf("retry reports success without quorum or scheduling: published=%d duplicates=%d timers=%d", r.GetPublishedCount(), r.GetDuplicateCount(), p.Scheduler.GetTimingWheelDepth())
	}
}

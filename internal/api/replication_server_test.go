package api

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc/metadata"
)

type mockReplicationSyncStream struct {
	ctx       context.Context
	responses []*types.ReplicationSyncResponse
}

func (m *mockReplicationSyncStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockReplicationSyncStream) SendHeader(metadata.MD) error { return nil }
func (m *mockReplicationSyncStream) SetTrailer(metadata.MD)       {}
func (m *mockReplicationSyncStream) Context() context.Context     { return m.ctx }
func (m *mockReplicationSyncStream) SendMsg(interface{}) error    { return nil }
func (m *mockReplicationSyncStream) RecvMsg(interface{}) error    { return nil }
func (m *mockReplicationSyncStream) Send(resp *types.ReplicationSyncResponse) error {
	m.responses = append(m.responses, resp)
	return nil
}

func TestReplicationServiceHandler_AppendAndSync(t *testing.T) {
	skipWindows(t)

	cfg := &types.Config{
		DataDir:          t.TempDir(),
		PartitionCount:   1,
		TickMS:           10,
		WheelSize:        100,
		SegmentSizeBytes: 1024,
		IndexInterval:    1,
		FsyncMode:        "periodic",
		FlushIntervalMS:  100,
		DedupTTLHours:    24,
		BloomCapacity:    1000,
	}

	pm := partition.NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	if err := pm.CreatePartition(0, "topic-test"); err != nil {
		t.Fatalf("CreatePartition failed: %v", err)
	}

	h := NewReplicationServiceHandler(pm)
	ctx := context.Background()

	appendResp, err := h.Append(ctx, &types.ReplicationAppendRequest{
		PartitionId: 0,
		Events: []*types.Event{
			{MessageId: "rep-1", Topic: "topic-test", ScheduleTs: time.Now().UnixMilli(), Payload: []byte("a")},
			{MessageId: "rep-2", Topic: "topic-test", ScheduleTs: time.Now().UnixMilli(), Payload: []byte("b")},
		},
	})
	if err != nil {
		t.Fatalf("Append returned gRPC error: %v", err)
	}
	if !appendResp.GetSuccess() {
		t.Fatalf("Append failed: %s", appendResp.GetError())
	}
	if appendResp.GetNextOffset() < 2 {
		t.Fatalf("expected next offset >= 2, got %d", appendResp.GetNextOffset())
	}

	stream := &mockReplicationSyncStream{ctx: ctx}
	if err := h.Sync(&types.ReplicationSyncRequest{PartitionId: 0, StartOffset: 0, MaxBytes: 1 << 20}, stream); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	if len(stream.responses) == 0 {
		t.Fatal("expected at least one sync response")
	}
	if len(stream.responses[0].GetEvents()) == 0 {
		t.Fatal("expected sync response to include events")
	}
}

func TestReplicationServiceHandler_AppendOffsetMismatch(t *testing.T) {
	skipWindows(t)

	cfg := &types.Config{
		DataDir:          t.TempDir(),
		PartitionCount:   1,
		TickMS:           10,
		WheelSize:        100,
		SegmentSizeBytes: 1024,
		IndexInterval:    1,
		FsyncMode:        "periodic",
		FlushIntervalMS:  100,
		DedupTTLHours:    24,
		BloomCapacity:    1000,
	}

	pm := partition.NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	if err := pm.CreatePartition(0, "topic-test"); err != nil {
		t.Fatalf("CreatePartition failed: %v", err)
	}

	h := NewReplicationServiceHandler(pm)
	ctx := context.Background()

	_, _ = h.Append(ctx, &types.ReplicationAppendRequest{
		PartitionId: 0,
		Events: []*types.Event{
			{MessageId: "rep-seed", Topic: "topic-test", ScheduleTs: time.Now().UnixMilli(), Payload: []byte("seed")},
		},
	})

	resp, err := h.Append(ctx, &types.ReplicationAppendRequest{
		PartitionId:        0,
		ExpectedNextOffset: 999,
		Events: []*types.Event{
			{MessageId: "rep-mismatch", Topic: "topic-test", ScheduleTs: time.Now().UnixMilli(), Payload: []byte("x")},
		},
	})
	if err != nil {
		t.Fatalf("Append returned gRPC error: %v", err)
	}
	if resp.GetSuccess() {
		t.Fatal("expected append to fail on offset mismatch")
	}
	if !strings.Contains(resp.GetError(), "offset mismatch") {
		t.Fatalf("expected offset mismatch error, got: %s", resp.GetError())
	}
}

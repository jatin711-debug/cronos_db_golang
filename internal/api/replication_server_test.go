package api

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
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

type mockReplicationSnapshotStream struct {
	ctx    context.Context
	chunks []*types.ReplicationSnapshotChunk
}

func (m *mockReplicationSnapshotStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockReplicationSnapshotStream) SendHeader(metadata.MD) error { return nil }
func (m *mockReplicationSnapshotStream) SetTrailer(metadata.MD)       {}
func (m *mockReplicationSnapshotStream) Context() context.Context     { return m.ctx }
func (m *mockReplicationSnapshotStream) SendMsg(interface{}) error    { return nil }
func (m *mockReplicationSnapshotStream) RecvMsg(interface{}) error    { return nil }
func (m *mockReplicationSnapshotStream) Send(chunk *types.ReplicationSnapshotChunk) error {
	m.chunks = append(m.chunks, chunk)
	return nil
}

func TestReplicationServiceHandler_AppendAndSync(t *testing.T) {
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

	now := time.Now().UnixMilli()
	appendResp, err := h.Append(ctx, &types.ReplicationAppendRequest{
		PartitionId: 0,
		Events: []*types.Event{
			{MessageId: "rep-1", Topic: "topic-test", Offset: 0, ScheduleTs: now, Payload: []byte("a")},
			{MessageId: "rep-2", Topic: "topic-test", Offset: 1, ScheduleTs: now, Payload: []byte("b")},
		},
	})
	if err != nil {
		t.Fatalf("Append returned gRPC error: %v", err)
	}
	if !appendResp.GetSuccess() {
		t.Fatalf("Append failed: %s", appendResp.GetError())
	}
	if appendResp.GetNextOffset() != 2 {
		t.Fatalf("expected next offset 2, got %d", appendResp.GetNextOffset())
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
	now := time.Now().UnixMilli()

	_, _ = h.Append(ctx, &types.ReplicationAppendRequest{
		PartitionId: 0,
		Events: []*types.Event{
			{MessageId: "rep-seed", Topic: "topic-test", Offset: 0, ScheduleTs: now, Payload: []byte("seed")},
		},
	})

	resp, err := h.Append(ctx, &types.ReplicationAppendRequest{
		PartitionId: 0,
		Events: []*types.Event{
			{MessageId: "rep-mismatch", Topic: "topic-test", Offset: 999, ScheduleTs: now, Payload: []byte("x")},
		},
	})
	if err != nil {
		t.Fatalf("Append returned gRPC error: %v", err)
	}
	if resp.GetSuccess() {
		t.Fatal("expected append to fail on offset mismatch")
	}
	if !strings.Contains(resp.GetError(), "gap") && !strings.Contains(resp.GetError(), "offset mismatch") {
		t.Fatalf("expected offset mismatch error, got: %s", resp.GetError())
	}
}

func TestReplicationServiceHandler_Snapshot(t *testing.T) {
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
	now := time.Now().UnixMilli()

	appendResp, err := h.Append(ctx, &types.ReplicationAppendRequest{
		PartitionId: 0,
		Events: []*types.Event{
			{MessageId: "snap-1", Topic: "topic-test", Offset: 0, ScheduleTs: now, Payload: []byte("a")},
			{MessageId: "snap-2", Topic: "topic-test", Offset: 1, ScheduleTs: now, Payload: []byte("b")},
		},
	})
	if err != nil {
		t.Fatalf("Append returned gRPC error: %v", err)
	}
	if !appendResp.GetSuccess() {
		t.Fatalf("Append failed: %s", appendResp.GetError())
	}

	stream := &mockReplicationSnapshotStream{ctx: ctx}
	if err := h.Snapshot(&types.ReplicationSnapshotRequest{PartitionId: 0, StartOffset: 0, MaxBytes: 1 << 20}, stream); err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	var headerCount, dataBytes, trailerCount int
	var trailer *types.ReplicationSnapshotTrailer
	for _, c := range stream.chunks {
		if c.GetHeader() != nil {
			headerCount++
		}
		dataBytes += len(c.GetData())
		if c.GetTrailer() != nil {
			trailerCount++
			trailer = c.GetTrailer()
		}
	}

	if headerCount == 0 {
		t.Fatal("expected at least one snapshot header")
	}
	if dataBytes == 0 {
		t.Fatal("expected non-zero data bytes")
	}
	if trailerCount != 1 {
		t.Fatalf("expected exactly one trailer, got %d", trailerCount)
	}
	if !trailer.GetSuccess() {
		t.Fatalf("snapshot trailer reported failure: %s", trailer.GetError())
	}
	if trailer.GetLastOffset() != 1 {
		t.Fatalf("expected last offset 1, got %d", trailer.GetLastOffset())
	}
}

// TestPartitionManager_SyncPartitionFromLeader_InstallSnapshot verifies that a
// follower partition can install a bulk segment snapshot streamed from a leader
// over the internal replication gRPC channel and end up with the same high
// watermark.
func TestPartitionManager_SyncPartitionFromLeader_InstallSnapshot(t *testing.T) {
	now := time.Now().UnixMilli()

	// --- Leader partition with a few events ---
	leaderCfg := &types.Config{
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
	leaderPM := partition.NewPartitionManager("leader-node", leaderCfg)
	defer leaderPM.StopAllPartitions()

	if err := leaderPM.CreatePartition(0, "topic-test"); err != nil {
		t.Fatalf("create leader partition: %v", err)
	}
	leaderP, err := leaderPM.GetInternalPartition(0)
	if err != nil {
		t.Fatalf("get leader partition: %v", err)
	}

	events := []*types.Event{
		{MessageId: "snap-1", Topic: "topic-test", Offset: 0, ScheduleTs: now, Payload: []byte("hello")},
		{MessageId: "snap-2", Topic: "topic-test", Offset: 1, ScheduleTs: now, Payload: []byte("world")},
		{MessageId: "snap-3", Topic: "topic-test", Offset: 2, ScheduleTs: now, Payload: []byte("!")},
	}
	if err := leaderP.Wal.AppendBatch(events); err != nil {
		t.Fatalf("append leader events: %v", err)
	}
	if err := leaderP.Wal.Flush(); err != nil {
		t.Fatalf("flush leader WAL: %v", err)
	}
	expectedHWM := leaderP.Wal.GetHighWatermark()

	// --- Internal gRPC server serving ReplicationService ---
	handler := NewReplicationServiceHandler(leaderPM)
	grpcServer := grpc.NewServer()
	types.RegisterReplicationServiceServer(grpcServer, handler)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	// --- Follower partition (empty) ---
	followerCfg := &types.Config{
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
	followerPM := partition.NewPartitionManager("follower-node", followerCfg)
	defer followerPM.StopAllPartitions()

	if err := followerPM.CreatePartition(0, "topic-test"); err != nil {
		t.Fatalf("create follower partition: %v", err)
	}

	// --- Sync from leader ---
	if err := followerPM.SyncPartitionFromLeader(0, lis.Addr().String()); err != nil {
		t.Fatalf("SyncPartitionFromLeader failed: %v", err)
	}

	followerP, err := followerPM.GetInternalPartition(0)
	if err != nil {
		t.Fatalf("get follower partition: %v", err)
	}
	if followerP.Wal.GetHighWatermark() != expectedHWM {
		t.Fatalf("follower HWM %d != leader HWM %d", followerP.Wal.GetHighWatermark(), expectedHWM)
	}

	// Verify events are readable on the follower.
	readEvents, err := followerP.Wal.ReadEvents(0, expectedHWM)
	if err != nil {
		t.Fatalf("read follower events: %v", err)
	}
	if len(readEvents) != len(events) {
		t.Fatalf("expected %d events on follower, got %d", len(events), len(readEvents))
	}
	for i, e := range events {
		if string(readEvents[i].Payload) != string(e.Payload) {
			t.Fatalf("event %d payload mismatch: got %q want %q", i, readEvents[i].Payload, e.Payload)
		}
	}
}

package api

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/dedup"
	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc/metadata"
)

type mockRegionFetchStream struct {
	ctx       context.Context
	responses []*types.RegionFetchResponse
}

func (m *mockRegionFetchStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockRegionFetchStream) SendHeader(metadata.MD) error { return nil }
func (m *mockRegionFetchStream) SetTrailer(metadata.MD)       {}
func (m *mockRegionFetchStream) Context() context.Context     { return m.ctx }
func (m *mockRegionFetchStream) SendMsg(interface{}) error    { return nil }
func (m *mockRegionFetchStream) RecvMsg(interface{}) error    { return nil }
func (m *mockRegionFetchStream) Send(resp *types.RegionFetchResponse) error {
	m.responses = append(m.responses, resp)
	return nil
}

// TestCrossRegionServer_FetchEvents_SendsTheLogInByteBoundedResponses fetches a
// log of one-MiB events and checks that it arrives whole and in order, in
// responses that each hold a part of it, with HasMore set on all but the last.
func TestCrossRegionServer_FetchEvents_SendsTheLogInByteBoundedResponses(t *testing.T) {
	cfg := &types.Config{
		DataDir:          t.TempDir(),
		PartitionCount:   1,
		TickMS:           100,
		WheelSize:        60,
		DedupTTLHours:    24,
		BloomCapacity:    1000,
		SegmentSizeBytes: 32 << 20,
	}

	pm := partition.NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	if err := pm.CreatePartition(0, "test-topic"); err != nil {
		t.Fatalf("Failed to create partition: %v", err)
	}
	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatalf("Failed to get internal partition: %v", err)
	}

	const total = 12
	payload := make([]byte, 1<<20)
	for i := 0; i < total; i++ {
		event := &types.Event{MessageId: fmt.Sprintf("big-%02d", i), Topic: "test-topic", Payload: payload}
		if err := p.Wal.AppendEvent(event); err != nil {
			t.Fatalf("AppendEvent failed: %v", err)
		}
	}
	p.Wal.Flush()

	stream := &mockRegionFetchStream{ctx: context.Background()}
	req := &types.RegionFetchRequest{RegionId: "region-east", PartitionId: 0, StartOffset: 0, MaxBytes: 64 << 20}
	if err := NewCrossRegionServer(pm).FetchEvents(req, stream); err != nil {
		t.Fatalf("FetchEvents failed: %v", err)
	}

	if len(stream.responses) < 2 {
		t.Fatalf("12 MiB of events arrived in %d responses; want several", len(stream.responses))
	}
	next := int64(0)
	for i, resp := range stream.responses {
		// A response holds 4 MiB of the log and at most the event that crosses that bound.
		if n := len(resp.Events); n == 0 || n > 5 {
			t.Fatalf("response %d holds %d events of 1 MiB; want 1 to 5", i, n)
		}
		for _, event := range resp.Events {
			if event.Offset != next {
				t.Fatalf("response %d holds offset %d; want %d", i, event.Offset, next)
			}
			next++
		}
		if last := i == len(stream.responses)-1; resp.HasMore == last {
			t.Fatalf("response %d of %d has HasMore=%v", i+1, len(stream.responses), resp.HasMore)
		}
	}
	if next != total {
		t.Fatalf("fetched %d events; want %d", next, total)
	}
}

func TestCrossRegionServer_ReplicateEvents_LWW(t *testing.T) {

	// Create temporary directory for testing
	dataDir := t.TempDir()

	cfg := &types.Config{
		DataDir:          dataDir,
		PartitionCount:   8,
		TickMS:           100,
		WheelSize:        60,
		DedupTTLHours:    24,
		BloomCapacity:    1000,
		SegmentSizeBytes: 10 * 1024 * 1024, // 10MB
	}

	pm := partition.NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	// Initialize partition 0
	err := pm.CreatePartition(0, "test-topic")
	if err != nil {
		t.Fatalf("Failed to create partition: %v", err)
	}

	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatalf("Failed to get internal partition: %v", err)
	}

	server := NewCrossRegionServer(pm)

	ctx := context.Background()

	// 1. Replicate a new event
	req1 := &types.RegionReplicateRequest{
		RegionId:    "region-east",
		PartitionId: 0,
		FirstOffset: 10,
		Events: []*types.Event{
			{
				MessageId: "event-123",
				CreatedTs: 1000, // Unix ms
				Payload:   []byte("hello version 1"),
				Topic:     "test-topic",
			},
		},
	}

	resp1, err := server.ReplicateEvents(ctx, req1)
	if err != nil {
		t.Fatalf("ReplicateEvents failed: %v", err)
	}
	if !resp1.Success {
		t.Fatalf("ReplicateEvents returned success=false: %s", resp1.Error)
	}

	// Verify it exists in dedup store and has correct timestamp
	ts1, exists, err := p.DedupStore.GetTimestamp(dedup.Key("test-topic", "event-123"))
	if err != nil {
		t.Fatalf("GetTimestamp failed: %v", err)
	}
	if !exists {
		t.Fatal("Expected message-id to be present in dedup store")
	}
	expectedTS1 := time.Unix(0, 1000*1_000_000)
	if !ts1.Equal(expectedTS1) {
		t.Errorf("Expected timestamp %v, got %v", expectedTS1, ts1)
	}

	// 2. Replicate duplicate with OLDER timestamp (should be ignored)
	req2 := &types.RegionReplicateRequest{
		RegionId:    "region-west",
		PartitionId: 0,
		FirstOffset: 20,
		Events: []*types.Event{
			{
				MessageId: "event-123",
				CreatedTs: 500, // Older than 1000
				Payload:   []byte("hello version older"),
				Topic:     "test-topic",
			},
		},
	}

	resp2, err := server.ReplicateEvents(ctx, req2)
	if err != nil {
		t.Fatalf("ReplicateEvents failed: %v", err)
	}
	if !resp2.Success {
		t.Fatalf("ReplicateEvents returned success=false: %s", resp2.Error)
	}

	// Timestamp should still be the older version's 1000 Unix ms
	ts2, _, _ := p.DedupStore.GetTimestamp(dedup.Key("test-topic", "event-123"))
	if !ts2.Equal(expectedTS1) {
		t.Errorf("Expected timestamp %v, got %v (LWW failed, older overwrite occurred)", expectedTS1, ts2)
	}

	// 3. Replicate duplicate with NEWER timestamp (should overwrite)
	req3 := &types.RegionReplicateRequest{
		RegionId:    "region-west",
		PartitionId: 0,
		FirstOffset: 30,
		Events: []*types.Event{
			{
				MessageId: "event-123",
				CreatedTs: 2000, // Newer than 1000
				Payload:   []byte("hello version 2 newer"),
				Topic:     "test-topic",
			},
		},
	}

	resp3, err := server.ReplicateEvents(ctx, req3)
	if err != nil {
		t.Fatalf("ReplicateEvents failed: %v", err)
	}
	if !resp3.Success {
		t.Fatalf("ReplicateEvents returned success=false: %s", resp3.Error)
	}

	// Timestamp in dedup store should now be updated to 2000 ms (2_000_000_000 ns)
	ts3, _, _ := p.DedupStore.GetTimestamp(dedup.Key("test-topic", "event-123"))
	expectedTS3 := time.Unix(0, 2000*1_000_000)
	if !ts3.Equal(expectedTS3) {
		t.Errorf("Expected timestamp %v, got %v (LWW failed to update timestamp)", expectedTS3, ts3)
	}

	// Verify we can read the newer event from WAL
	events, err := p.Wal.ReadEvents(0, 100)
	if err != nil {
		t.Fatalf("ReadEvents failed: %v", err)
	}

	// We expect 2 events in WAL: the original version 1, and the newer version 2.
	// The older version (500 ms) was ignored, so it was never written to WAL.
	if len(events) != 2 {
		t.Fatalf("Expected 2 events in WAL, got %d", len(events))
	}

	if string(events[0].Payload) != "hello version 1" {
		t.Errorf("Expected payload 'hello version 1', got '%s'", string(events[0].Payload))
	}
	if string(events[1].Payload) != "hello version 2 newer" {
		t.Errorf("Expected payload 'hello version 2 newer', got '%s'", string(events[1].Payload))
	}
}

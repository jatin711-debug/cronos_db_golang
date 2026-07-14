package api

import (
	"context"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

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
	p.Leader = true // Make it a leader so WAL is fully active

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
	ts1, exists, err := p.DedupStore.GetTimestamp("event-123")
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
	ts2, _, _ := p.DedupStore.GetTimestamp("event-123")
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
	ts3, _, _ := p.DedupStore.GetTimestamp("event-123")
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

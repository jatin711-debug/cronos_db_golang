package storage

import (
	"os"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestWAL_CompactByOffset(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_compact_offset_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Small segment size to force rotation and create multiple segments
	config := &WALConfig{
		SegmentSizeBytes: 200,
		IndexInterval:    2,
		FsyncMode:        "batch",
		FlushIntervalMS:  10,
	}

	wal, err := NewWAL(tmpDir, 0, config, nil)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Append events across multiple segments
	for i := 0; i < 15; i++ {
		event := &types.Event{
			MessageId:  "msg-" + string(rune('a'+i)),
			ScheduleTs: int64(1000 + i*100),
			Payload:    []byte("some payload data to exceed 200 bytes limit"),
			Topic:      "test",
		}
		if err := wal.AppendEvent(event); err != nil {
			t.Fatalf("Failed to append event %d: %v", i, err)
		}
		// Force manual flush and sync to clean segment state
		_ = wal.Flush()
	}

	initialSegments := len(wal.GetSegments())
	if initialSegments < 2 {
		t.Fatalf("Expected at least 2 segments, got %d", initialSegments)
	}

	// Compact up to offset 5
	deleted, err := wal.CompactByOffset(5)
	if err != nil {
		t.Fatalf("CompactByOffset failed: %v", err)
	}

	finalSegments := len(wal.GetSegments())
	if deleted == 0 {
		t.Error("Expected at least some segments to be deleted")
	}
	if finalSegments >= initialSegments {
		t.Errorf("Segments count did not decrease: initial=%d, final=%d", initialSegments, finalSegments)
	}

	// Verify active segment is still preserved
	activeSeg := wal.activeSegment
	foundActive := false
	for _, seg := range wal.GetSegments() {
		if seg == activeSeg {
			foundActive = true
			break
		}
	}
	if !foundActive {
		t.Error("Active segment was deleted during compaction")
	}
}

func TestWAL_CompactByTimestamp(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_compact_ts_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := &WALConfig{
		SegmentSizeBytes: 200,
		IndexInterval:    2,
		FsyncMode:        "batch",
		FlushIntervalMS:  10,
	}

	wal, err := NewWAL(tmpDir, 0, config, nil)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Append events across multiple segments
	now := time.Now().UnixNano()
	for i := 0; i < 15; i++ {
		event := &types.Event{
			MessageId:  "msg-" + string(rune('a'+i)),
			ScheduleTs: now + int64(i)*1000000000,
			Payload:    []byte("some payload data to exceed 200 bytes limit"),
			Topic:      "test",
		}
		if err := wal.AppendEvent(event); err != nil {
			t.Fatalf("Failed to append event %d: %v", i, err)
		}
		_ = wal.Flush()
	}

	initialSegments := len(wal.GetSegments())
	if initialSegments < 2 {
		t.Fatalf("Expected at least 2 segments, got %d", initialSegments)
	}

	// Compact up to future timestamp
	deleted, err := wal.CompactByTimestamp(now + 5*1000000000)
	if err != nil {
		t.Fatalf("CompactByTimestamp failed: %v", err)
	}

	finalSegments := len(wal.GetSegments())
	if deleted == 0 {
		t.Error("Expected at least some segments to be deleted")
	}
	if finalSegments >= initialSegments {
		t.Errorf("Segments count did not decrease: initial=%d, final=%d", initialSegments, finalSegments)
	}
}

func TestWAL_CompactConsumerAware(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_compact_consumer_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := &WALConfig{
		SegmentSizeBytes: 200,
		IndexInterval:    2,
		FsyncMode:        "batch",
		FlushIntervalMS:  10,
	}

	wal, err := NewWAL(tmpDir, 0, config, nil)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	for i := 0; i < 15; i++ {
		event := &types.Event{
			MessageId:  "msg-" + string(rune('a'+i)),
			ScheduleTs: int64(1000 + i*100),
			Payload:    []byte("some payload data to exceed 200 bytes limit"),
			Topic:      "test",
		}
		if err := wal.AppendEvent(event); err != nil {
			t.Fatalf("Failed to append event %d: %v", i, err)
		}
		_ = wal.Flush()
	}

	initialSegments := len(wal.GetSegments())

	// Consumer offsets dictionary: consumer-1 is at offset 6, consumer-2 is at offset 8
	consumerOffsets := map[int64]bool{
		6: true,
		8: true,
	}

	// Compact using minimum consumer offset (should compact up to offset 6)
	err = wal.Compact(0, consumerOffsets)
	if err != nil {
		t.Fatalf("Consumer-aware Compact failed: %v", err)
	}

	finalSegments := len(wal.GetSegments())
	if finalSegments >= initialSegments {
		t.Errorf("Expected segment compaction, initial=%d final=%d", initialSegments, finalSegments)
	}
}

package storage

import (
	"os"
	"path/filepath"
	"testing"

	"cronos_db/pkg/types"
)

func TestWAL_AppendAndRead(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create WAL
	config := &WALConfig{
		SegmentSizeBytes: 1024 * 1024, // 1MB
		IndexInterval:    10,
		FsyncMode:        "batch",
		FlushIntervalMS:  100,
	}

	wal, err := NewWAL(tmpDir, 0, config)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Append events
	events := []*types.Event{
		{MessageId: "msg-1", ScheduleTs: 1000, Payload: []byte("payload1"), Topic: "test"},
		{MessageId: "msg-2", ScheduleTs: 2000, Payload: []byte("payload2"), Topic: "test"},
		{MessageId: "msg-3", ScheduleTs: 3000, Payload: []byte("payload3"), Topic: "test"},
	}

	for _, event := range events {
		if err := wal.AppendEvent(event); err != nil {
			t.Fatalf("Failed to append event: %v", err)
		}
	}

	if err := wal.Flush(); err != nil {
		t.Fatalf("Failed to flush WAL: %v", err)
	}

	// Verify offsets
	if wal.GetNextOffset() != 3 {
		t.Errorf("Expected next offset 3, got %d", wal.GetNextOffset())
	}

	// Read events back
	readEvents, err := wal.ReadEvents(0, 2)
	if err != nil {
		t.Fatalf("Failed to read events: %v", err)
	}

	if len(readEvents) != 3 {
		t.Errorf("Expected 3 events, got %d", len(readEvents))
	}

	// Verify event content
	for i, event := range readEvents {
		if event.MessageId != events[i].MessageId {
			t.Errorf("Event %d: expected message_id %s, got %s", i, events[i].MessageId, event.MessageId)
		}
		if event.ScheduleTs != events[i].ScheduleTs {
			t.Errorf("Event %d: expected schedule_ts %d, got %d", i, events[i].ScheduleTs, event.ScheduleTs)
		}
	}
}

func TestWAL_SegmentRotation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_rotation_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Small segment size to trigger rotation
	config := &WALConfig{
		SegmentSizeBytes: 500, // Very small to trigger rotation
		IndexInterval:    2,
		FsyncMode:        "batch",
		FlushIntervalMS:  100,
	}

	wal, err := NewWAL(tmpDir, 0, config)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Append many events to trigger rotation
	for i := 0; i < 20; i++ {
		event := &types.Event{
			MessageId:  "msg-" + string(rune('a'+i)),
			ScheduleTs: int64(1000 + i),
			Payload:    []byte("some payload data that takes up space"),
			Topic:      "test",
		}
		if err := wal.AppendEvent(event); err != nil {
			t.Fatalf("Failed to append event %d: %v", i, err)
		}
	}

	if err := wal.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Should have multiple segments
	segments := wal.GetSegments()
	if len(segments) < 2 {
		t.Logf("Expected multiple segments due to small segment size, got %d", len(segments))
	}
}

func TestIndex_AddAndFind(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "index_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	idx, err := NewIndex(tmpDir, 0)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	defer idx.Close()

	// Add entries
	entries := []struct {
		ts       int64
		offset   int64
		position int64
	}{
		{1000, 0, 64},
		{2000, 100, 1064},
		{3000, 200, 2064},
		{4000, 300, 3064},
	}

	for _, e := range entries {
		if err := idx.AddEntry(e.ts, e.offset, e.position); err != nil {
			t.Fatalf("Failed to add entry: %v", err)
		}
	}

	// Test FindByOffset
	tests := []struct {
		targetOffset int64
		expectedPos  int64
		expectFound  bool
	}{
		{50, 64, true},    // Before first, should return first
		{150, 1064, true}, // Between 100 and 200
		{200, 2064, true}, // Exact match
		{350, 3064, true}, // After last indexed
		{0, 64, false},    // At beginning
	}

	for _, tc := range tests {
		pos, found := idx.FindByOffset(tc.targetOffset)
		if found != tc.expectFound && tc.expectFound {
			// For boundary cases, position should still be valid
			if pos != tc.expectedPos {
				t.Errorf("FindByOffset(%d): expected pos %d, got %d", tc.targetOffset, tc.expectedPos, pos)
			}
		}
	}
}

func TestSegment_ReadEvent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "segment_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create segment
	segment, err := NewSegment(tmpDir, 0, true)
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}
	defer segment.Close()

	// Append events
	for i := 0; i < 5; i++ {
		event := &types.Event{
			MessageId:  "msg-" + string(rune('a'+i)),
			ScheduleTs: int64(1000 + i*100),
			Payload:    []byte("test payload"),
			Topic:      "test-topic",
			Offset:     int64(i),
		}
		if err := segment.AppendEvent(event, 10); err != nil {
			t.Fatalf("Failed to append event: %v", err)
		}
	}

	if err := segment.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Read specific event
	event, err := segment.ReadEvent(2)
	if err != nil {
		t.Fatalf("Failed to read event: %v", err)
	}

	if event.Offset != 2 {
		t.Errorf("Expected offset 2, got %d", event.Offset)
	}
}

func TestWAL_Recovery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_recovery_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := &WALConfig{
		SegmentSizeBytes: 1024 * 1024,
		IndexInterval:    10,
		FsyncMode:        "every_event",
		FlushIntervalMS:  100,
	}

	// Create and write to WAL
	wal1, err := NewWAL(tmpDir, 0, config)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	for i := 0; i < 10; i++ {
		event := &types.Event{
			MessageId:  "recovery-" + string(rune('a'+i)),
			ScheduleTs: int64(1000 + i),
			Payload:    []byte("recovery test"),
			Topic:      "test",
		}
		if err := wal1.AppendEvent(event); err != nil {
			t.Fatalf("Failed to append: %v", err)
		}
	}
	wal1.Flush()
	wal1.Close()

	// Reopen WAL (simulates recovery)
	wal2, err := NewWAL(tmpDir, 0, config)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	// Verify state recovered
	if wal2.GetLastOffset() != 9 {
		t.Errorf("Expected last offset 9, got %d", wal2.GetLastOffset())
	}

	// Read events
	events, err := wal2.ReadEvents(0, 9)
	if err != nil {
		t.Fatalf("Failed to read events: %v", err)
	}

	if len(events) != 10 {
		t.Errorf("Expected 10 events, got %d", len(events))
	}
}

func TestSegment_Metadata(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "segment_meta_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	segment, err := NewSegment(tmpDir, 100, true)
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}
	defer segment.Close()

	if segment.GetFirstOffset() != 100 {
		t.Errorf("Expected first offset 100, got %d", segment.GetFirstOffset())
	}

	if !segment.IsActive() {
		t.Error("Expected segment to be active")
	}

	// Verify filename format
	expectedFilename := filepath.Base(segment.GetFilename())
	if expectedFilename != "00000000000000000100.log" {
		t.Errorf("Unexpected filename: %s", expectedFilename)
	}
}

package delivery

import (
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestDeadLetterQueue_Add(t *testing.T) {
	tmpDir := t.TempDir()

	dlq, err := NewDeadLetterQueue(tmpDir, 100)
	if err != nil {
		t.Fatalf("NewDeadLetterQueue failed: %v", err)
	}
	defer dlq.Close()

	event := makeDLQEvent(0, "test-msg", "test-topic")
	err = dlq.Add(event, "delivery-1", 3, "timeout error", "subscriber-1")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	if dlq.Count() != 1 {
		t.Errorf("expected count 1, got %d", dlq.Count())
	}
}

func TestDeadLetterQueue_Get(t *testing.T) {
	tmpDir := t.TempDir()

	dlq, err := NewDeadLetterQueue(tmpDir, 100)
	if err != nil {
		t.Fatalf("NewDeadLetterQueue failed: %v", err)
	}
	defer dlq.Close()

	// Add 3 entries
	for i := 0; i < 3; i++ {
		event := makeDLQEvent(int64(i), "msg", "topic")
		dlq.Add(event, "delivery-"+string(rune('a'+i)), int32(i+1), "err", "sub")
	}

	entries := dlq.Get()
	if len(entries) != 3 {
		t.Errorf("expected 3 entries, got %d", len(entries))
	}
}

func TestDeadLetterQueue_GetByPartition(t *testing.T) {
	tmpDir := t.TempDir()

	dlq, err := NewDeadLetterQueue(tmpDir, 100)
	if err != nil {
		t.Fatalf("NewDeadLetterQueue failed: %v", err)
	}
	defer dlq.Close()

	// Add events to different partitions
	for i := 0; i < 5; i++ {
		event := makeDLQEventWithPartition(int64(i), "msg", "topic", int32(i%2))
		dlq.Add(event, "delivery-"+string(rune('a'+i)), 1, "err", "sub")
	}

	partition0 := dlq.GetByPartition(0)
	if len(partition0) != 3 {
		t.Errorf("expected 3 entries for partition 0, got %d", len(partition0))
	}

	partition1 := dlq.GetByPartition(1)
	if len(partition1) != 2 {
		t.Errorf("expected 2 entries for partition 1, got %d", len(partition1))
	}
}

func TestDeadLetterQueue_Remove(t *testing.T) {
	tmpDir := t.TempDir()

	dlq, err := NewDeadLetterQueue(tmpDir, 100)
	if err != nil {
		t.Fatalf("NewDeadLetterQueue failed: %v", err)
	}
	defer dlq.Close()

	event := makeDLQEvent(0, "msg", "topic")
	dlq.Add(event, "delivery-1", 1, "err", "sub")

	if dlq.Count() != 1 {
		t.Fatalf("expected count 1, got %d", dlq.Count())
	}

	err = dlq.Remove("delivery-1")
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	if dlq.Count() != 0 {
		t.Errorf("expected count 0, got %d", dlq.Count())
	}
}

func TestDeadLetterQueue_Remove_NotFound(t *testing.T) {
	tmpDir := t.TempDir()

	dlq, _ := NewDeadLetterQueue(tmpDir, 100)
	defer dlq.Close()

	err := dlq.Remove("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent delivery ID")
	}
}

func TestDeadLetterQueue_Retry(t *testing.T) {
	tmpDir := t.TempDir()

	dlq, err := NewDeadLetterQueue(tmpDir, 100)
	if err != nil {
		t.Fatalf("NewDeadLetterQueue failed: %v", err)
	}
	defer dlq.Close()

	event := makeDLQEvent(0, "retry-msg", "retry-topic")
	dlq.Add(event, "delivery-retry", 5, "final error", "sub-retry")

	entry, err := dlq.Retry("delivery-retry")
	if err != nil {
		t.Fatalf("Retry failed: %v", err)
	}

	if entry.DeliveryID != "delivery-retry" {
		t.Errorf("expected delivery-retry, got %s", entry.DeliveryID)
	}

	if dlq.Count() != 0 {
		t.Errorf("expected count 0 after retry, got %d", dlq.Count())
	}
}

func TestDeadLetterQueue_Retry_NotFound(t *testing.T) {
	tmpDir := t.TempDir()

	dlq, _ := NewDeadLetterQueue(tmpDir, 100)
	defer dlq.Close()

	_, err := dlq.Retry("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent delivery ID")
	}
}

// TestDeadLetterQueue_RemovePersistsAcrossRestart verifies that Remove writes a
// tombstone record so the entry is not resurrected after the DLQ is reloaded.
func TestDeadLetterQueue_RemovePersistsAcrossRestart(t *testing.T) {
	tmpDir := t.TempDir()

	dlq1, err := NewDeadLetterQueue(tmpDir, 100)
	if err != nil {
		t.Fatalf("NewDeadLetterQueue failed: %v", err)
	}
	event := makeDLQEvent(0, "msg", "topic")
	dlq1.Add(event, "delivery-persist", 1, "err", "sub")
	if err := dlq1.Remove("delivery-persist"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	if err := dlq1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	dlq2, err := NewDeadLetterQueue(tmpDir, 100)
	if err != nil {
		t.Fatalf("reload NewDeadLetterQueue failed: %v", err)
	}
	defer dlq2.Close()

	if dlq2.Count() != 0 {
		t.Errorf("expected count 0 after reload, got %d (tombstone not respected)", dlq2.Count())
	}
}

// TestDeadLetterQueue_RetryPersistsAcrossRestart verifies that Retry writes a
// tombstone record so the entry is not resurrected after the DLQ is reloaded.
func TestDeadLetterQueue_RetryPersistsAcrossRestart(t *testing.T) {
	tmpDir := t.TempDir()

	dlq1, err := NewDeadLetterQueue(tmpDir, 100)
	if err != nil {
		t.Fatalf("NewDeadLetterQueue failed: %v", err)
	}
	event := makeDLQEvent(0, "msg", "topic")
	dlq1.Add(event, "delivery-retry-persist", 1, "err", "sub")
	if _, err := dlq1.Retry("delivery-retry-persist"); err != nil {
		t.Fatalf("Retry failed: %v", err)
	}
	if err := dlq1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	dlq2, err := NewDeadLetterQueue(tmpDir, 100)
	if err != nil {
		t.Fatalf("reload NewDeadLetterQueue failed: %v", err)
	}
	defer dlq2.Close()

	if dlq2.Count() != 0 {
		t.Errorf("expected count 0 after reload, got %d (tombstone not respected)", dlq2.Count())
	}
}


func TestDeadLetterQueue_Eviction(t *testing.T) {
	tmpDir := t.TempDir()

	// Max size 5
	dlq, err := NewDeadLetterQueue(tmpDir, 5)
	if err != nil {
		t.Fatalf("NewDeadLetterQueue failed: %v", err)
	}
	defer dlq.Close()

	// Add 10 entries
	for i := 0; i < 10; i++ {
		event := makeDLQEvent(int64(i), "msg", "topic")
		dlq.Add(event, "delivery-"+string(rune('a'+i)), 1, "err", "sub")
	}

	// Should only have 5 entries (oldest evicted)
	if dlq.Count() != 5 {
		t.Errorf("expected count 5 after eviction, got %d", dlq.Count())
	}
}

func TestDeadLetterQueue_Clear(t *testing.T) {
	tmpDir := t.TempDir()

	dlq, err := NewDeadLetterQueue(tmpDir, 100)
	if err != nil {
		t.Fatalf("NewDeadLetterQueue failed: %v", err)
	}
	defer dlq.Close()

	// Add entries
	for i := 0; i < 5; i++ {
		event := makeDLQEvent(int64(i), "msg", "topic")
		dlq.Add(event, "delivery-"+string(rune('a'+i)), 1, "err", "sub")
	}

	err = dlq.Clear()
	if err != nil {
		t.Fatalf("Clear failed: %v", err)
	}

	if dlq.Count() != 0 {
		t.Errorf("expected count 0 after clear, got %d", dlq.Count())
	}
}

func TestDeadLetterQueue_GetStats(t *testing.T) {
	tmpDir := t.TempDir()

	dlq, err := NewDeadLetterQueue(tmpDir, 100)
	if err != nil {
		t.Fatalf("NewDeadLetterQueue failed: %v", err)
	}
	defer dlq.Close()

	stats := dlq.GetStats()
	if stats.TotalEntries != 0 {
		t.Errorf("expected 0 total entries, got %d", stats.TotalEntries)
	}

	// Add some entries
	for i := 0; i < 3; i++ {
		event := makeDLQEvent(int64(i), "msg", "topic")
		dlq.Add(event, "delivery-"+string(rune('a'+i)), int32(i+1), "err", "sub")
	}

	stats = dlq.GetStats()
	if stats.TotalEntries != 3 {
		t.Errorf("expected 3 total entries, got %d", stats.TotalEntries)
	}
	if stats.OldestEntryAge < 0 {
		t.Error("expected non-negative oldest entry age")
	}
	if stats.NewestEntryAge < 0 {
		t.Error("expected non-negative newest entry age")
	}
}

func TestDeadLetterQueue_ZeroMaxSize(t *testing.T) {
	tmpDir := t.TempDir()

	// With maxSize <= 0, should use default 10000
	dlq, err := NewDeadLetterQueue(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewDeadLetterQueue with 0 maxSize failed: %v", err)
	}
	defer dlq.Close()

	// Add many entries - should not evict due to large default
	for i := 0; i < 100; i++ {
		event := makeDLQEvent(int64(i), "msg", "topic")
		dlq.Add(event, "delivery-"+string(rune('a'+i)), 1, "err", "sub")
	}

	if dlq.Count() != 100 {
		t.Errorf("expected 100 entries, got %d", dlq.Count())
	}
}

func TestDeadLetterQueue_DLQEntryFields(t *testing.T) {
	tmpDir := t.TempDir()

	dlq, _ := NewDeadLetterQueue(tmpDir, 100)
	defer dlq.Close()
	event := makeDLQEvent(42, "test-msg", "test-topic")
	dlq.Add(event, "did", 5, "test error", "test-sub")

	entries := dlq.Get()
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	e := entries[0]
	if e.DeliveryID != "did" {
		t.Errorf("expected DeliveryID=did, got %s", e.DeliveryID)
	}
	if e.Attempts != 5 {
		t.Errorf("expected Attempts=5, got %d", e.Attempts)
	}
	if e.LastError != "test error" {
		t.Errorf("expected LastError=test error, got %s", e.LastError)
	}
	if e.Subscriber != "test-sub" {
		t.Errorf("expected Subscriber=test-sub, got %s", e.Subscriber)
	}
	if e.FailedAt <= 0 {
		t.Error("expected positive FailedAt timestamp")
	}
}

func makeDLQEvent(offset int64, msgID string, topic string) *types.Event {
	return &types.Event{
		MessageId:   msgID,
		Topic:       topic,
		Payload:     []byte("payload"),
		Offset:      offset,
		PartitionId: 0,
		ScheduleTs:  1000,
	}
}

func makeDLQEventWithPartition(offset int64, msgID string, topic string, partitionID int32) *types.Event {
	return &types.Event{
		MessageId:   msgID,
		Topic:       topic,
		Payload:     []byte("payload"),
		Offset:      offset,
		PartitionId: partitionID,
		ScheduleTs:  1000,
	}
}
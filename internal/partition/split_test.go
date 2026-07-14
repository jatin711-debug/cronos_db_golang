package partition

import (
	"fmt"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestSplitPartition_Success(t *testing.T) {
	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
		TickMS:         10,
		WheelSize:      100,
	}

	pm := NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	sourceID := int32(1)
	newID := int32(2)

	err := pm.CreatePartition(sourceID, "topic-test")
	if err != nil {
		t.Fatalf("Failed to create partition: %v", err)
	}

	err = pm.StartPartition(sourceID)
	if err != nil {
		t.Fatalf("Failed to start partition: %v", err)
	}

	sourcePart, err := pm.GetInternalPartition(sourceID)
	if err != nil {
		t.Fatalf("Failed to get internal partition: %v", err)
	}

	// 1. Add some initial events
	events := []*types.Event{
		{MessageId: "msg-1", Topic: "topic-test", Payload: []byte("payload-1"), ScheduleTs: time.Now().UnixMilli()},
		{MessageId: "msg-2", Topic: "topic-test", Payload: []byte("payload-2"), ScheduleTs: time.Now().UnixMilli()},
		{MessageId: "msg-3", Topic: "topic-test", Payload: []byte("payload-3"), ScheduleTs: time.Now().UnixMilli()},
	}
	events[0].Meta = map[string]string{"partition_key": "apple"}
	events[1].Meta = map[string]string{"partition_key": "banana"}
	events[2].Meta = map[string]string{"partition_key": "cherry"}

	for _, ev := range events {
		if err := sourcePart.Wal.AppendEvent(ev); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// 2. Perform the split
	sm := NewSplitManager(pm)
	var splitCompletedCalled bool
	sm.OnSplitComplete = func(src, nw int32, srcEpoch, nwEpoch int64) error {
		splitCompletedCalled = true
		if srcEpoch != 1 || nwEpoch != 1 {
			t.Errorf("Expected epochs to be 1, got src=%d nw=%d", srcEpoch, nwEpoch)
		}
		return nil
	}

	// Split partition at key "banana" (keys >= "banana" move to partition 2)
	err = sm.SplitPartition(sourceID, newID, 0, "banana")
	if err != nil {
		t.Fatalf("SplitPartition failed: %v", err)
	}

	if !splitCompletedCalled {
		t.Error("OnSplitComplete callback was not called")
	}

	// 3. Verify partition boundaries
	srcMin, srcMax, _ := pm.GetPartitionBounds(sourceID)
	nwMin, nwMax, _ := pm.GetPartitionBounds(newID)

	if srcMin != "" || srcMax != "banana" {
		t.Errorf("Expected source boundaries [\"\", \"banana\"), got [\"%s\", \"%s\")", srcMin, srcMax)
	}
	if nwMin != "banana" || nwMax != "" {
		t.Errorf("Expected new boundaries [\"banana\", \"\"), got [\"%s\", \"%s\")", nwMin, nwMax)
	}

	// 4. Verify migrated events
	newPart, err := pm.GetInternalPartition(newID)
	if err != nil {
		t.Fatalf("Failed to get new partition: %v", err)
	}

	newHW := newPart.Wal.GetHighWatermark()
	if newHW < 0 {
		t.Errorf("Expected events in new partition, got high watermark: %d", newHW)
	}

	migratedEvents, err := newPart.Wal.ReadEvents(0, newHW+1)
	if err != nil {
		t.Fatalf("Failed to read migrated events: %v", err)
	}

	// Should have "banana" and "cherry"
	if len(migratedEvents) != 2 {
		t.Errorf("Expected 2 migrated events, got %d", len(migratedEvents))
	}
	for _, ev := range migratedEvents {
		pk := ev.Meta["partition_key"]
		if pk != "banana" && pk != "cherry" {
			t.Errorf("Unexpected migrated event key: %s", pk)
		}
	}
}

func TestSplitPartition_EpochFencing(t *testing.T) {
	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
		TickMS:         10,
		WheelSize:      100,
	}

	pm := NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	sourceID := int32(1)
	pm.CreatePartition(sourceID, "topic-test")
	pm.StartPartition(sourceID)

	_ = NewSplitManager(pm)

	// Mark splitting as active manually to simulate active fence
	pm.SetSplitting(sourceID, true)

	if !pm.IsSplitting(sourceID) {
		t.Error("Expected IsSplitting to be true")
	}

	// Release fence
	pm.SetSplitting(sourceID, false)
	if pm.IsSplitting(sourceID) {
		t.Error("Expected IsSplitting to be false")
	}
}

func TestSplitPartition_BoundsValidation(t *testing.T) {
	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
		TickMS:         10,
		WheelSize:      100,
	}

	pm := NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	sourceID := int32(1)
	pm.CreatePartition(sourceID, "topic-test")
	pm.StartPartition(sourceID)

	part, _ := pm.GetInternalPartition(sourceID)

	// Set boundaries: ["apple", "cherry")
	pm.SetPartitionBounds(sourceID, "apple", "cherry")

	if !part.IsKeyInBounds("banana") {
		t.Error("banana should be in bounds [apple, cherry)")
	}
	if part.IsKeyInBounds("appl") {
		t.Error("appl should not be in bounds [apple, cherry)")
	}
	if part.IsKeyInBounds("cherry") {
		t.Error("cherry should not be in bounds [apple, cherry)")
	}
	if part.IsKeyInBounds("dog") {
		t.Error("dog should not be in bounds [apple, cherry)")
	}
}

func TestSplitPartition_AtomicRollback(t *testing.T) {
	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
		TickMS:         10,
		WheelSize:      100,
	}

	pm := NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	sourceID := int32(1)
	newID := int32(2)

	pm.CreatePartition(sourceID, "topic-test")
	pm.StartPartition(sourceID)

	sm := NewSplitManager(pm)

	// Force split failure by registering split callback that always returns error
	sm.OnSplitComplete = func(src, nw int32, srcEpoch, nwEpoch int64) error {
		return fmt.Errorf("simulated Raft sync error")
	}

	err := sm.SplitPartition(sourceID, newID, 0, "banana")
	if err == nil {
		t.Fatal("Expected SplitPartition to fail due to Raft callback error")
	}

	// Verify rollback restored boundaries and cleaned up new partition
	_, _, err = pm.GetPartitionBounds(newID)
	if err == nil {
		t.Error("Expected new partition to be cleaned up and deleted from pm")
	}

	srcMin, srcMax, _ := pm.GetPartitionBounds(sourceID)
	if srcMin != "" || srcMax != "" {
		t.Errorf("Expected source boundaries to be reverted to empty, got [%s, %s)", srcMin, srcMax)
	}

	if pm.IsSplitting(sourceID) {
		t.Error("Expected splitting lock to be released on failure")
	}
}

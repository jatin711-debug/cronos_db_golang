package partition

import (
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// A dead-lettered event has reached its final disposition for the group. The
// partition must record that, or the WAL redrive keeps delivering the poison
// message and retention never sees the event as finished.
func TestDeadLetteredEventsAreRecordedComplete(t *testing.T) {
	cfg := &types.Config{DataDir: t.TempDir(), PartitionCount: 1, FsyncMode: "periodic", FlushIntervalMS: 10, TickMS: 10, WheelSize: 64, DedupTTLHours: 1, BloomCapacity: 1000}
	pm := NewPartitionManager("node", cfg)
	defer pm.Close()
	if err := pm.CreatePartition(0, "orders"); err != nil {
		t.Fatal(err)
	}
	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ConsumerGroup.CreateGroup("workers", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	if p.Dispatcher.OnDeadLettered == nil {
		t.Fatal("dispatcher has no dead-letter disposition hook")
	}

	poison := &types.Event{MessageId: "poison", Topic: "orders", PartitionId: 0, Offset: 3}
	if p.Dispatcher.IsCompleted("workers", poison.Offset) {
		t.Fatal("test setup: event already complete")
	}
	p.Dispatcher.OnDeadLettered("workers", []*types.Event{poison})
	if !p.Dispatcher.IsCompleted("workers", poison.Offset) {
		t.Fatal("dead-lettered event is still eligible for redelivery")
	}
}

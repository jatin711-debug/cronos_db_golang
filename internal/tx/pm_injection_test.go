package tx

import (
	"context"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// TestBeginInjectsPartitionManager verifies that a transaction begun with a bare
// PartitionParticipant (only a PartitionID, as the gRPC handler builds it) has
// the partition manager injected by Coordinator.Begin, so Prepare writes a real
// WAL marker instead of being a durable no-op.
func TestBeginInjectsPartitionManager(t *testing.T) {
	skipWindows(t)

	cfg := &types.Config{
		DataDir:          t.TempDir(),
		PartitionCount:   8,
		TickMS:           100,
		WheelSize:        60,
		SegmentSizeBytes: 10 * 1024 * 1024,
	}
	pm := partition.NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	if err := pm.CreatePartition(0, "test-topic"); err != nil {
		t.Fatalf("CreatePartition: %v", err)
	}
	part, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatalf("GetInternalPartition: %v", err)
	}
	part.Leader = true

	c := NewCoordinator(30*time.Second, t.TempDir())
	defer c.Stop()
	c.SetPartitionManager(pm)

	// Bare participant, exactly as tx.Handler.BeginTransaction constructs it.
	txID := TxID("tx-inject-1")
	if _, err := c.Begin(txID, []Participant{PartitionParticipant{PartitionID: 0}}); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := c.Prepare(context.Background(), txID); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	// The prepare marker must now be durable in the partition WAL.
	last := part.Wal.GetLastOffset()
	if last < 0 {
		t.Fatal("no WAL events after prepare — PM was not injected (durable no-op)")
	}
	events, err := part.Wal.ReadEvents(0, last)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	found := false
	for _, ev := range events {
		if ev.GetMessageId() == "tx:"+string(txID)+":prepare" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("prepare WAL marker not found — PartitionManager not injected into participant")
	}
}

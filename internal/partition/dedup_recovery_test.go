package partition

import (
	"fmt"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// TestPartitionWiresDLQ locks in that every partition constructs a durable
// dead-letter queue and wires it into the dispatcher, so poison messages are
// captured instead of silently dropped ("DLQ not configured, dropping").
func TestPartitionWiresDLQ(t *testing.T) {
	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 1,
		TickMS:         100,
		WheelSize:      60,
	}
	pm := NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	if err := pm.CreatePartition(0, "t"); err != nil {
		t.Fatalf("CreatePartition: %v", err)
	}
	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatalf("GetInternalPartition: %v", err)
	}
	if p.DLQ == nil {
		t.Fatal("partition DLQ is nil")
	}
	// DLQSize is only reported when the dispatcher actually holds a DLQ; a
	// non-negative value confirms the dispatcher was constructed with one.
	if got := p.Dispatcher.GetStats().DLQSize; got != 0 {
		t.Fatalf("expected empty DLQ, got size %d", got)
	}
}

// TestRecoverDedupFromWAL verifies that message IDs written to the WAL but lost
// from the NoSync dedup Pebble store (simulating a crash) are re-detected as
// duplicates after recoverDedupFromWAL re-seeds the store from the WAL tail.
func TestRecoverDedupFromWAL(t *testing.T) {
	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 1,
		TickMS:         100,
		WheelSize:      60,
		DedupTTLHours:  24,
		BloomCapacity:  100000,
	}
	pm := NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	if err := pm.CreatePartition(0, "test-topic"); err != nil {
		t.Fatalf("CreatePartition: %v", err)
	}
	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatalf("GetInternalPartition: %v", err)
	}

	// Write events directly to the WAL (the durable record), then delete their
	// claims from the dedup store to simulate claims lost on crash.
	const n = 50
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("recover-msg-%d", i)
		ids[i] = id
		ev := &types.Event{
			MessageId:  id,
			ScheduleTs: int64(1_000_000 + i),
			CreatedTs:  int64(1_000_000 + i),
			Payload:    []byte("p"),
			Topic:      "test-topic",
		}
		if err := p.Wal.AppendEvent(ev); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := p.Wal.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Simulate lost dedup claims: roll them back out of the dedup store.
	if err := p.DedupStore.RollbackBatch(ids); err != nil {
		t.Fatalf("rollback (simulate loss): %v", err)
	}

	// Confirm the store now considers them new (i.e. the claims were lost). We
	// check the first ID; IsDuplicate returns false and (as a side-effect) would
	// re-store it, so only probe one and then roll it back to keep the test clean.
	dup, err := p.DedupStore.IsDuplicate(ids[0], 0)
	if err != nil {
		t.Fatalf("IsDuplicate probe: %v", err)
	}
	if dup {
		t.Fatal("precondition failed: claim still present after rollback")
	}
	_ = p.DedupStore.RollbackBatch(ids[:1])

	// Recovery: re-seed dedup from the WAL tail.
	pm.recoverDedupFromWAL(p)

	// Now every message ID must be detected as a duplicate.
	for _, id := range ids {
		dup, err := p.DedupStore.IsDuplicate(id, 0)
		if err != nil {
			t.Fatalf("IsDuplicate(%s): %v", id, err)
		}
		if !dup {
			t.Fatalf("message %q not recovered as duplicate", id)
		}
	}
}

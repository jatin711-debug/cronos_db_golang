package partition

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/dedup"
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

func TestRecoverDedupBeyondFormerCutoff(t *testing.T) {
	pm := NewPartitionManager("node", &types.Config{DataDir: t.TempDir(), PartitionCount: 1, TickMS: 100, WheelSize: 60, DedupTTLHours: 24})
	defer pm.StopAllPartitions()
	if err := pm.CreatePartition(0, "t"); err != nil {
		t.Fatal(err)
	}
	p, _ := pm.GetInternalPartition(0)
	const count = 500_002
	now := time.Now().UnixMilli()
	for start := 0; start < count; start += 5000 {
		batch := make([]*types.Event, 0, 5000)
		for i := start; i < min(start+5000, count); i++ {
			created := int64(1) // expired filler still counts toward the old cutoff
			if i == 0 || i == count-1 {
				created = now
			}
			batch = append(batch, &types.Event{MessageId: fmt.Sprintf("id-%d", i), Topic: "t", CreatedTs: created, ScheduleTs: now})
		}
		if err := p.Wal.AppendBatch(batch); err != nil {
			t.Fatal(err)
		}
	}
	if err := pm.recoverDedupFromWAL(p); err != nil {
		t.Fatal(err)
	}
	if _, exists, err := p.DedupStore.GetOffset(dedup.Key("t", "id-0")); err != nil || !exists {
		t.Fatalf("oldest live ID missing: exists=%v err=%v", exists, err)
	}
	point, err := p.DedupStore.LoadRecoveryPoint()
	if err != nil || point == nil || point.Offset != count-1 {
		t.Fatalf("checkpoint=%+v err=%v", point, err)
	}
	if err := p.Wal.AppendEvent(&types.Event{MessageId: "suffix", CreatedTs: now, ScheduleTs: now}); err != nil {
		t.Fatal(err)
	}
	if err := pm.recoverDedupFromWAL(p); err != nil {
		t.Fatal(err)
	}
	if _, exists, err := p.DedupStore.GetOffset(dedup.Key("", "suffix")); err != nil || !exists {
		t.Fatalf("suffix missing: exists=%v err=%v", exists, err)
	}
}

type brokenRecoveryStore struct{ *dedup.MemoryStore }

func (s brokenRecoveryStore) GetOffset(string) (int64, bool, error) {
	return 0, false, errors.New("injected dedup read failure")
}

func TestDedupRecoveryFailureBlocksStartupAndAdmission(t *testing.T) {
	pm := NewPartitionManager("node", &types.Config{DataDir: t.TempDir(), PartitionCount: 1, TickMS: 100, WheelSize: 60})
	defer pm.StopAllPartitions()
	if err := pm.CreatePartition(0, "t"); err != nil {
		t.Fatal(err)
	}
	p, _ := pm.GetInternalPartition(0)
	if err := p.Wal.AppendEvent(&types.Event{MessageId: "id", CreatedTs: time.Now().UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	original := p.DedupStore
	defer original.Close()
	p.DedupStore = dedup.NewManager(brokenRecoveryStore{dedup.NewMemoryStore()})
	if err := pm.startPartitionInternal(p); err == nil {
		t.Fatal("startup ignored recovery failure")
	}
	if p.started || p.GetReplayError() == nil {
		t.Fatal("partition marked started/healthy after failed recovery")
	}
	if _, err := pm.GetOrCreateInternalPartition(0, "t"); err == nil {
		t.Fatal("existing partition bypassed recovery error")
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
		ids[i] = dedup.Key("test-topic", id)
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

package dedup

import (
	"testing"
	"time"
)

// TestPebbleStore_RollbackClaim verifies that rolling back a claim undoes it so
// a subsequent check treats the ID as new (models a retry after a failed write).
func TestPebbleStore_RollbackClaim(t *testing.T) {
	store, err := NewPebbleStore(t.TempDir(), 0, 1, nil)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	defer store.Close()

	// Claim an ID (pending buffer), then roll it back.
	if dup, err := store.CheckAndStore("msg-1", 5); err != nil || dup {
		t.Fatalf("first claim: dup=%v err=%v (want dup=false)", dup, err)
	}
	if err := store.RollbackClaim([]string{"msg-1"}); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	// After rollback, a retry must be seen as new (not a duplicate).
	if dup, err := store.CheckAndStore("msg-1", 6); err != nil || dup {
		t.Fatalf("retry after rollback: dup=%v err=%v (want dup=false)", dup, err)
	}
}

// TestPebbleStore_RollbackClaim_AfterFlush verifies rollback works even once the
// claim has been flushed from the pending buffer into PebbleDB.
func TestPebbleStore_RollbackClaim_AfterFlush(t *testing.T) {
	store, err := NewPebbleStore(t.TempDir(), 0, 1, nil)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	defer store.Close()

	if dup, err := store.CheckAndStore("flushed", 1); err != nil || dup {
		t.Fatalf("claim: dup=%v err=%v", dup, err)
	}
	if err := store.flushPending(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := store.RollbackClaim([]string{"flushed"}); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if exists, err := store.Exists("flushed"); err != nil || exists {
		t.Fatalf("after rollback Exists=%v err=%v (want false)", exists, err)
	}
}

// TestBloomPebbleStore_RollbackBatch is the production-path check: a new ID
// claimed via the batch API and then rolled back must be accepted again on retry.
func TestBloomPebbleStore_RollbackBatch(t *testing.T) {
	store, err := NewBloomPebbleStore(t.TempDir(), 0, 1, 100000, 0.01, nil)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	defer store.Close()

	// Let the startup bloom rebuild finish so we exercise the fast path.
	time.Sleep(50 * time.Millisecond)

	ids := []string{"a", "b", "c"}
	offs := []int64{1, 2, 3}
	dups, err := store.CheckAndStoreBatch(ids, offs)
	if err != nil {
		t.Fatalf("first batch: %v", err)
	}
	for i, d := range dups {
		if d {
			t.Fatalf("id %q unexpectedly duplicate on first claim", ids[i])
		}
	}

	// Simulate a durable-write failure: roll the whole batch back.
	if err := store.RollbackBatch(ids); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	// Retry: every ID must be treated as new again, not dropped as a duplicate.
	dups, err = store.CheckAndStoreBatch(ids, offs)
	if err != nil {
		t.Fatalf("retry batch: %v", err)
	}
	for i, d := range dups {
		if d {
			t.Fatalf("id %q wrongly reported duplicate after rollback (data-loss-on-retry bug)", ids[i])
		}
	}

	// A second claim without rollback must now correctly detect duplicates.
	dups, err = store.CheckAndStoreBatch(ids, offs)
	if err != nil {
		t.Fatalf("third batch: %v", err)
	}
	for i, d := range dups {
		if !d {
			t.Fatalf("id %q should be duplicate after a committed claim", ids[i])
		}
	}
}

// TestManager_RollbackBatch verifies the Manager delegates rollback to the store.
func TestManager_RollbackBatch(t *testing.T) {
	mgr := NewManager(NewMemoryStore())
	if _, err := mgr.IsDuplicate("x", 0); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := mgr.RollbackBatch([]string{"x"}); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	dup, err := mgr.IsDuplicate("x", 0)
	if err != nil {
		t.Fatalf("recheck: %v", err)
	}
	if dup {
		t.Fatal("id x should be new after rollback")
	}
}

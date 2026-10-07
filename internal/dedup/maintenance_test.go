package dedup

import (
	"testing"
	"time"
)

func TestMaintenanceStatsCountDistinctKeysAcrossFlush(t *testing.T) {
	store, err := NewPebbleStore(t.TempDir(), 0, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Put("same", 0, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		store.claimMu.Lock()
		err := store.bufferWrite("same", int64(i))
		store.claimMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		stats, err := store.GetStats()
		if err != nil || stats.ApproximateCount != 1 {
			t.Fatalf("double-counted pending/persisted key: %+v %v", stats, err)
		}
	}
}

func TestMaintenanceBatchCompletionSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	store, err := NewBloomPebbleStore(dir, 0, 1, 1000, 0.01, nil)
	if err != nil {
		t.Fatal(err)
	}
	manager := NewManager(store)
	ids := []string{"first", "second", "third"}
	claims, err := manager.IsDuplicateBatch(ids, []int64{-1, -1, -1})
	if err != nil {
		t.Fatal(err)
	}
	for i, duplicate := range claims {
		if duplicate {
			t.Fatalf("new ID %q was a duplicate", ids[i])
		}
	}
	created := time.Now().UnixNano()
	if err := manager.PutBatch(ids, []int64{4, 5, 6}, []int64{created, created, created}); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewBloomPebbleStore(dir, 0, 1, 1000, 0.01, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	manager = NewManager(reopened)
	for i, id := range ids {
		offset, found, err := manager.GetOffset(id)
		if err != nil || !found || offset != int64(i+4) {
			t.Fatalf("completed claim %q: offset=%d found=%t err=%v", id, offset, found, err)
		}
	}
}

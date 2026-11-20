package dedup

import (
	"fmt"
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

// Counters, which metrics and health checks read for every partition, report
// what GetStats does except the ID count, which needs a read of every key.
func TestCountersMatchStatsWithoutCounting(t *testing.T) {
	store, err := NewBloomPebbleStore(t.TempDir(), 0, 1, 1000, 0.01, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// The filter is filled from Pebble in the background once the store is
	// open. Done after the claims below, it added them a second time, and
	// the two reads compared here found different counts.
	store.rebuildWG.Wait()
	m := NewManager(store)
	for i := 0; i < 50; i++ {
		id := fmt.Sprintf("m-%d", i)
		if _, err := store.CheckAndStore(id, int64(i)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.CheckAndStore(id, int64(i)); err != nil { // a duplicate
			t.Fatal(err)
		}
	}
	stats, err := m.GetStats()
	if err != nil {
		t.Fatal(err)
	}
	counters, err := m.Counters()
	if err != nil {
		t.Fatal(err)
	}
	if stats.ApproximateCount != 50 {
		t.Fatalf("GetStats counted %d IDs, want 50", stats.ApproximateCount)
	}
	if counters.ApproximateCount != 0 {
		t.Fatalf("Counters counted %d IDs; it should not read the store", counters.ApproximateCount)
	}
	if counters.BloomHits != stats.BloomHits || counters.BloomFalsePositives != stats.BloomFalsePositives ||
		counters.PebbleHits != stats.PebbleHits || counters.BloomMemoryBytes != stats.BloomMemoryBytes ||
		counters.BloomCount != stats.BloomCount || counters.TTLHours != stats.TTLHours {
		t.Fatalf("Counters %+v differ from GetStats %+v", counters, stats)
	}
	if counters.BloomCount == 0 {
		t.Fatal("bloom counted no IDs")
	}

	// A store without counters of its own gives its stats.
	mem := NewManager(NewMemoryStore())
	if _, err := mem.Counters(); err != nil {
		t.Fatal(err)
	}
}

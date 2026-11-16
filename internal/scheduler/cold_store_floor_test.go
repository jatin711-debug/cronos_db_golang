package scheduler

import (
	"math/rand"
	"sync"
	"testing"
)

func openColdStore(tb testing.TB) *ColdStore {
	tb.Helper()
	cs, err := NewColdStore(tb.TempDir(), nil)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = cs.Close() })
	return cs
}

// takeDue scans from the start, as the hydrator does, and deletes what it
// finds, returning it.
func takeDue(t *testing.T, cs *ColdStore, byOffset map[int64]int64, mu *sync.Mutex, endTS int64, limit int) []int64 {
	t.Helper()
	offsets, err := cs.ScanRangeLimit(0, endTS, limit)
	if err != nil {
		t.Fatal(err)
	}
	if len(offsets) == 0 {
		return nil
	}
	batch := make([]struct{ Offset, ScheduleTS int64 }, 0, len(offsets))
	mu.Lock()
	for _, offset := range offsets {
		batch = append(batch, struct{ Offset, ScheduleTS int64 }{offset, byOffset[offset]})
	}
	mu.Unlock()
	if err := cs.DeleteBatch(batch); err != nil {
		t.Fatal(err)
	}
	return offsets
}

// Scans start at a floor below which the store holds nothing, so they do not
// step over the tombstones of what earlier passes took. An entry stored below
// the floor is still found.
func TestColdStore_ScansFindWhatIsStoredBelowTheFloor(t *testing.T) {
	cs := openColdStore(t)
	byOffset := map[int64]int64{}
	var mu sync.Mutex
	for i := int64(0); i < 100; i++ {
		byOffset[i] = 1000 + i
		if err := cs.Store(i, 1000+i); err != nil {
			t.Fatal(err)
		}
	}
	if got := takeDue(t, cs, byOffset, &mu, 1049, 0); len(got) != 50 {
		t.Fatalf("took %d entries due by 1049, want 50", len(got))
	}
	// The floor follows a pass behind: a scan sets it to the first entry it
	// finds, which is deleted afterwards. So each scan steps over the last
	// pass's tombstones at most.
	if _, err := cs.ScanRangeLimit(0, 1049, 0); err != nil {
		t.Fatal(err)
	}
	if !cs.floorSet || cs.floor != 1050 {
		t.Fatalf("floor = %d (set %v), want 1050", cs.floor, cs.floorSet)
	}
	// Stored below the floor, as an evicted timer can be.
	byOffset[500] = 10
	if err := cs.Store(500, 10); err != nil {
		t.Fatal(err)
	}
	got := takeDue(t, cs, byOffset, &mu, 1049, 0)
	if len(got) != 1 || got[0] != 500 {
		t.Fatalf("took %v, want the entry stored below the floor", got)
	}
	// After a Clear the store is empty and scans start over.
	if err := cs.Clear(); err != nil {
		t.Fatal(err)
	}
	if err := cs.Store(600, 5); err != nil {
		t.Fatal(err)
	}
	if offsets, _ := cs.ScanRangeLimit(0, 2000, 0); len(offsets) != 1 || offsets[0] != 600 {
		t.Fatalf("after Clear and a store: %v, want [600]", offsets)
	}
}

// While entries are stored, at times below the floor too, and a scanner
// takes what is due, every entry is taken exactly once: a scan must not raise
// the floor over an entry stored while it ran.
func TestColdStore_FloorNeverSkipsConcurrentStores(t *testing.T) {
	cs := openColdStore(t)
	byOffset := map[int64]int64{}
	var mu sync.Mutex
	const writers, perWriter = 4, 2000

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			for i := 0; i < perWriter; i++ {
				offset := int64(w*perWriter + i)
				ts := int64(rng.Intn(10000))
				mu.Lock()
				byOffset[offset] = ts
				mu.Unlock()
				if err := cs.Store(offset, ts); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	taken := map[int64]int{}
	for finished := false; ; {
		select {
		case <-done:
			finished = true
		default:
		}
		got := takeDue(t, cs, byOffset, &mu, 10000, 64)
		for _, offset := range got {
			taken[offset]++
		}
		if finished && len(got) == 0 {
			break
		}
	}
	if len(taken) != writers*perWriter {
		t.Fatalf("took %d distinct entries of %d stored", len(taken), writers*perWriter)
	}
	for offset, n := range taken {
		if n != 1 {
			t.Fatalf("entry %d was taken %d times", offset, n)
		}
	}
	if left, _, _, _ := cs.scan(0, 10000, 0); len(left) != 0 {
		t.Fatalf("%d entries left in the store that no scan found", len(left))
	}
}

// The hydrator's pattern: take the first few due entries and delete them,
// again and again, with a long backlog of deletions behind.
func BenchmarkColdStore_TakeDueAfterManyDeletes(b *testing.B) {
	cs := openColdStore(b)
	const total = 100_000
	entries := make([]struct{ Offset, ScheduleTS int64 }, 0, total)
	for i := int64(0); i < total; i++ {
		entries = append(entries, struct{ Offset, ScheduleTS int64 }{i, i})
	}
	if err := cs.StoreBatch(entries); err != nil {
		b.Fatal(err)
	}
	next := int64(0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offsets, err := cs.ScanRangeLimit(0, total, 8)
		if err != nil || len(offsets) == 0 {
			b.Fatalf("scan: %v (%d entries)", err, len(offsets))
		}
		batch := make([]struct{ Offset, ScheduleTS int64 }, 0, len(offsets))
		for _, offset := range offsets {
			batch = append(batch, struct{ Offset, ScheduleTS int64 }{offset, offset})
		}
		if err := cs.DeleteBatch(batch); err != nil {
			b.Fatal(err)
		}
		next += int64(len(offsets))
		if next >= total-8 {
			b.StopTimer()
			if err := cs.StoreBatch(entries); err != nil {
				b.Fatal(err)
			}
			next = 0
			b.StartTimer()
		}
	}
}

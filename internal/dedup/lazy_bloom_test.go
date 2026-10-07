package dedup

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// The filter behind a dedup store is not built until a key is added, and it
// answers as an empty filter until then.
func TestLazyBloomAllocatesOnFirstAdd(t *testing.T) {
	var built atomic.Int32
	b := newLazyBloom(func() BloomFilter {
		built.Add(1)
		return NewGoBloomFilter(1000, 0.01)
	})

	if b.MayContain("a") || b.Count() != 0 || b.MemoryUsageBytes() != 0 {
		t.Fatal("an unused filter does not look empty")
	}
	if got := b.MayContainBatch([]string{"a", "b"}); len(got) != 2 || got[0] || got[1] {
		t.Fatalf("batch lookup on an unused filter = %v, want two misses", got)
	}
	b.Reset()
	if built.Load() != 0 {
		t.Fatal("reading or resetting an unused filter built it")
	}

	// Concurrent first adds build exactly one filter and lose no key.
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b.Add(fmt.Sprintf("key-%d", i))
		}(i)
	}
	wg.Wait()
	if built.Load() != 1 {
		t.Fatalf("%d filters built, want 1", built.Load())
	}
	for i := 0; i < 16; i++ {
		if !b.MayContain(fmt.Sprintf("key-%d", i)) {
			t.Fatalf("key-%d was lost by a concurrent first add", i)
		}
	}
	if b.MemoryUsageBytes() == 0 {
		t.Fatal("a filter in use reports no memory")
	}
	b.Reset()
	if b.MayContain("key-0") || b.Count() != 0 {
		t.Fatal("reset did not clear the filter")
	}
}

// A store that has never claimed an ID holds no filter, whatever capacity it
// was configured for; the first claim builds it and dedup then works as usual.
func TestBloomPebbleStore_FilterIsBuiltOnFirstClaim(t *testing.T) {
	store, err := NewBloomPebbleStore(t.TempDir(), 0, 1, 100_000_000, 0.01, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.rebuildWG.Wait()

	if stats, err := store.GetStats(); err != nil || stats.BloomMemoryBytes != 0 {
		t.Fatalf("an unused store holds a %d byte filter (err=%v)", stats.BloomMemoryBytes, err)
	}
	if _, found, err := store.GetOffset("never-seen"); err != nil || found {
		t.Fatalf("lookup in an unused store: found=%v err=%v", found, err)
	}
	if dup, err := store.CheckAndStore("first", 7); err != nil || dup {
		t.Fatalf("first claim: dup=%v err=%v", dup, err)
	}
	if dup, err := store.CheckAndStore("first", 7); err != nil || !dup {
		t.Fatalf("second claim of the same ID: dup=%v err=%v", dup, err)
	}
	if stats, err := store.GetStats(); err != nil || stats.BloomMemoryBytes == 0 {
		t.Fatalf("a store in use reports no filter (err=%v)", err)
	}
}

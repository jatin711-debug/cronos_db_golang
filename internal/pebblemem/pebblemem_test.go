package pebblemem

import (
	"testing"

	"github.com/cockroachdb/pebble"
)

// held is about the most a node's Pebble databases hold once their memtables
// are full grown: two memtables per database, or the cache when that is
// larger, since memtables reserve their size from it.
func held(b *Budget, partitions int) uint64 {
	var memtables uint64
	for k := range kinds {
		memtables += 2 * b.MemTableSize(k)
	}
	return max(memtables*uint64(partitions), uint64(b.CacheBytes))
}

func TestPlanKeepsPebbleWellInsideTheLimit(t *testing.T) {
	for _, c := range []struct {
		limit      uint64
		partitions int
	}{
		{384 << 20, 4},
		{256 << 20, 4},
		{512 << 20, 8},
		{2 << 30, 16},
		{16 << 30, 16},
		{64 << 30, 4},
	} {
		b := Plan(c.limit, c.partitions)
		if got := held(b, c.partitions); got > c.limit/3 {
			t.Errorf("limit %d MiB, %d partitions: Pebble holds up to %d MiB, more than a third of the limit",
				c.limit>>20, c.partitions, got>>20)
		}
		if b.MemTableSize(Dedup) < b.MemTableSize(Offsets) {
			t.Errorf("limit %d MiB: dedup memtable %d is smaller than the offsets one %d",
				c.limit>>20, b.MemTableSize(Dedup), b.MemTableSize(Offsets))
		}
	}
}

func TestPlanForTheSoakNode(t *testing.T) {
	// The 24-hour soak runs three nodes of four partitions with a 384 MiB
	// limit. Fixed sizes held 225 MiB of dedup memtables and 43 MiB of cache.
	b := Plan(384<<20, 4)
	if b.CacheBytes != 48<<20 {
		t.Errorf("cache = %d MiB, want 48", b.CacheBytes>>20)
	}
	if got := b.MemTableSize(Dedup); got != 3<<20 {
		t.Errorf("dedup memtable = %d KiB, want 3072", got>>10)
	}
	if got := held(b, 4); got > 64<<20 {
		t.Errorf("Pebble holds up to %d MiB", got>>20)
	}
}

func TestPlanWithoutALimitKeepsTheDefaults(t *testing.T) {
	b := Plan(0, 4)
	if b.CacheBytes != DefaultCacheBytes {
		t.Errorf("cache = %d, want %d", b.CacheBytes, DefaultCacheBytes)
	}
	for k := range kinds {
		if b.MemTableSize(k) != defaults[k] {
			t.Errorf("kind %d: memtable %d, want %d", k, b.MemTableSize(k), defaults[k])
		}
	}
	// A large machine gets them too: planned sizes never exceed them.
	big := Plan(1<<40, 1)
	for k := range kinds {
		if big.MemTableSize(k) != defaults[k] {
			t.Errorf("1 TiB, kind %d: memtable %d, want %d", k, big.MemTableSize(k), defaults[k])
		}
	}
	if big.CacheBytes != DefaultCacheBytes {
		t.Errorf("1 TiB: cache = %d, want %d", big.CacheBytes, DefaultCacheBytes)
	}
}

func TestPlanFloorsTinyMemtables(t *testing.T) {
	b := Plan(64<<20, 256)
	for k := range kinds {
		if b.MemTableSize(k) != minMemTable {
			t.Errorf("kind %d: memtable %d, want %d", k, b.MemTableSize(k), minMemTable)
		}
	}
	if b.CacheBytes != minCacheBytes {
		t.Errorf("cache = %d, want %d", b.CacheBytes, minCacheBytes)
	}
}

func TestApply(t *testing.T) {
	var opts pebble.Options
	var none *Budget
	none.Apply(Cold, &opts)
	if opts.MemTableSize != defaults[Cold] || opts.Cache != nil {
		t.Fatalf("nil budget: memtable %d, cache %v", opts.MemTableSize, opts.Cache)
	}

	b := Plan(384<<20, 4)
	b.Cache = pebble.NewCache(b.CacheBytes)
	defer b.Cache.Unref()
	opts = pebble.Options{}
	b.Apply(Dedup, &opts)
	if opts.MemTableSize != b.MemTableSize(Dedup) || opts.Cache != b.Cache {
		t.Fatalf("memtable %d, cache %p; want %d, %p", opts.MemTableSize, opts.Cache, b.MemTableSize(Dedup), b.Cache)
	}
}

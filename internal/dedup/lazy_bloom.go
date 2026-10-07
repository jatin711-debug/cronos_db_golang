package dedup

import (
	"sync"
	"sync/atomic"
)

// lazyBloom is a BloomFilter that is allocated when the first key is added.
//
// A filter sized for the default capacity takes over a hundred megabytes, and
// every partition has one. Most of them would otherwise go unused: a
// partition that only receives publishes with duplicates allowed never adds a
// key, and neither does one that a node loads but never writes to. Until a key
// is added the filter is simply empty.
//
// Like the filters it wraps, it leaves ordering Add against Reset to its
// owner.
type lazyBloom struct {
	newFilter func() BloomFilter
	// allocMu makes sure only one filter is ever built.
	allocMu sync.Mutex
	filter  atomic.Pointer[allocatedBloom]
}

type allocatedBloom struct{ BloomFilter }

func newLazyBloom(newFilter func() BloomFilter) *lazyBloom {
	return &lazyBloom{newFilter: newFilter}
}

func (b *lazyBloom) Add(key string) {
	f := b.filter.Load()
	if f == nil {
		b.allocMu.Lock()
		if f = b.filter.Load(); f == nil {
			f = &allocatedBloom{b.newFilter()}
			b.filter.Store(f)
		}
		b.allocMu.Unlock()
	}
	f.Add(key)
}

func (b *lazyBloom) MayContain(key string) bool {
	if f := b.filter.Load(); f != nil {
		return f.MayContain(key)
	}
	return false
}

func (b *lazyBloom) MayContainBatch(keys []string) []bool {
	if f := b.filter.Load(); f != nil {
		return f.MayContainBatch(keys)
	}
	return make([]bool, len(keys))
}

func (b *lazyBloom) Count() uint64 {
	if f := b.filter.Load(); f != nil {
		return f.Count()
	}
	return 0
}

func (b *lazyBloom) Reset() {
	if f := b.filter.Load(); f != nil {
		f.Reset()
	}
}

func (b *lazyBloom) MemoryUsageBytes() uint64 {
	if f := b.filter.Load(); f != nil {
		return f.MemoryUsageBytes()
	}
	return 0
}

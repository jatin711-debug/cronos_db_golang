// Package pebblemem sizes a node's Pebble databases to the memory the node
// may use.
//
// Every partition opens four Pebble databases: dedup, the scheduler's cold
// store, consumer offsets and the event lifecycle index. Pebble allocates a
// memtable at its full size when it makes one, and keeps one spent memtable to
// reuse, so a database that has filled a few holds about twice its memtable
// size even when idle. Built without cgo, as the release binaries are,
// memtables and the block cache are on the Go heap: they count toward the
// runtime's memory limit and toward the share of --memory-limit at which a
// node refuses publishes. With fixed sizes (a 256 MiB cache, 64 MiB dedup
// memtables) a four-partition node held more than 300 MiB of them a few hours
// in, against a 384 MiB limit, and refused most publishes.
package pebblemem

import "github.com/cockroachdb/pebble"

// Kind is one of the Pebble databases a partition opens.
type Kind int

const (
	Dedup     Kind = iota // message IDs: two writes per publish
	Cold                  // the scheduler's far-future events
	Offsets               // consumer group offsets and completions
	Lifecycle             // the event lifecycle index
	kinds
)

// defaults are the memtable sizes for a node whose memory is unknown, and the
// most a planned size can be: the sizes used before they followed the limit.
var defaults = [kinds]uint64{
	Dedup:     64 << 20,
	Cold:      32 << 20,
	Offsets:   4 << 20,
	Lifecycle: 4 << 20,
}

// weights are each kind's part, in eighths, of a partition's memtable share.
var weights = [kinds]uint64{Dedup: 4, Cold: 2, Offsets: 1, Lifecycle: 1}

const (
	// DefaultCacheBytes is the block cache of a node whose memory is
	// unknown, and the largest a planned cache is.
	DefaultCacheBytes = 256 << 20
	minCacheBytes     = 8 << 20
	// minMemTable keeps memtables from being so small that the databases
	// do little but flush them.
	minMemTable = 1 << 20
)

// Budget is what a node's Pebble databases share: one block cache, and a
// memtable size for each kind of database. A nil Budget, as tests use, leaves
// each database Pebble's own cache and the default sizes.
type Budget struct {
	// Cache is the block cache every database shares; nil gives each its own.
	Cache *pebble.Cache
	// CacheBytes is the size to make Cache.
	CacheBytes int64
	memTable   [kinds]uint64
}

// Plan sizes the Pebble databases of a node that may use limit bytes and holds
// up to partitions partitions. An eighth of the limit is the block cache, and
// another eighth is divided among the partitions' memtables, half of each
// partition's part to dedup. Memtables reserve their size from the cache, so
// while they fill it the cache keeps fewer blocks rather than adding to them.
// A limit of 0 means it is unknown, and gives the defaults.
func Plan(limit uint64, partitions int) *Budget {
	b := &Budget{CacheBytes: DefaultCacheBytes, memTable: defaults}
	if limit == 0 {
		return b
	}
	b.CacheBytes = int64(min(max(limit/8, minCacheBytes), DefaultCacheBytes))
	perPartition := limit / 8 / uint64(max(partitions, 1))
	for k := range kinds {
		// Each database holds about two memtables: the one it fills and
		// the spent one Pebble keeps.
		size := perPartition * weights[k] / 8 / 2
		b.memTable[k] = min(max(size, minMemTable), defaults[k])
	}
	return b
}

// MemTableSize returns the memtable size for a kind of database.
func (b *Budget) MemTableSize(k Kind) uint64 {
	if b == nil || b.memTable[k] == 0 {
		return defaults[k]
	}
	return b.memTable[k]
}

// Apply sets a kind of database's memtable size and the shared cache on opts.
func (b *Budget) Apply(k Kind, opts *pebble.Options) {
	opts.MemTableSize = b.MemTableSize(k)
	if b != nil && b.Cache != nil {
		opts.Cache = b.Cache
	}
}

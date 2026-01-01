package dedup

import (
	"sync/atomic"
)

// BloomFilter interface abstracting the backend
type BloomFilter interface {
	Add(key string)
	MayContain(key string) bool
	MayContainBatch(keys []string) []bool
	Count() uint64
	Reset()
	MemoryUsageBytes() uint64
}

// GoBloomFilter is a simple bloom filter implementation for fast dedup checks
type GoBloomFilter struct {
	bits    []uint64
	size    uint64 // Number of bits
	numHash uint64 // Number of hash functions
	count   uint64 // Approximate number of items (atomic)
}

// NewBloomFilter creates a bloom filter sized for expectedItems with targetFPR false positive rate
func NewBloomFilter(expectedItems uint64, targetFPR float64) BloomFilter {
	// Using Rust implementation for 5-10x performance gain
	return NewRustBloomFilter(expectedItems, targetFPR)
}

// NewGoBloomFilter creates a pure Go bloom filter
func NewGoBloomFilter(expectedItems uint64, targetFPR float64) *GoBloomFilter {
	var bitsPerItem uint64
	if targetFPR <= 0.001 {
		bitsPerItem = 15
	} else if targetFPR <= 0.01 {
		bitsPerItem = 10
	} else {
		bitsPerItem = 8
	}

	size := expectedItems * bitsPerItem
	if size < 1024 {
		size = 1024
	}

	numWords := (size + 63) / 64
	size = numWords * 64

	numHash := uint64(float64(bitsPerItem) * 0.7)
	if numHash < 3 {
		numHash = 3
	}
	if numHash > 7 {
		numHash = 7 // Cap at 7 for performance
	}

	return &GoBloomFilter{
		bits:    make([]uint64, numWords),
		size:    size,
		numHash: numHash,
		count:   0,
	}
}

// Fast inline FNV-1a hash - no allocations
func fnvHash(key string) (h1, h2 uint64) {
	// FNV-1a for h1
	h1 = 14695981039346656037 // FNV offset basis
	for i := 0; i < len(key); i++ {
		h1 ^= uint64(key[i])
		h1 *= 1099511628211 // FNV prime
	}

	// Different seed for h2
	h2 = 14695981039346656037
	h2 ^= 0xABCDEF // Salt
	h2 *= 1099511628211
	for i := 0; i < len(key); i++ {
		h2 ^= uint64(key[i])
		h2 *= 1099511628211
	}
	return h1, h2
}

// Add adds a key to the bloom filter (lock-free using atomic CAS)
func (bf *GoBloomFilter) Add(key string) {
	h1, h2 := fnvHash(key)

	for i := uint64(0); i < bf.numHash; i++ {
		idx := (h1 + i*h2) % bf.size
		wordIdx := idx / 64
		bitIdx := idx % 64
		mask := uint64(1) << bitIdx

		// Atomic OR - lock-free
		for {
			old := atomic.LoadUint64(&bf.bits[wordIdx])
			if old&mask != 0 {
				break // Already set
			}
			if atomic.CompareAndSwapUint64(&bf.bits[wordIdx], old, old|mask) {
				break
			}
		}
	}
	atomic.AddUint64(&bf.count, 1)
}

// MayContain returns true if key might be in the set (lock-free)
func (bf *GoBloomFilter) MayContain(key string) bool {
	h1, h2 := fnvHash(key)

	for i := uint64(0); i < bf.numHash; i++ {
		idx := (h1 + i*h2) % bf.size
		wordIdx := idx / 64
		bitIdx := idx % 64

		if atomic.LoadUint64(&bf.bits[wordIdx])&(1<<bitIdx) == 0 {
			return false
		}
	}
	return true
}

// MayContainBatch checks multiple keys efficiently
func (bf *GoBloomFilter) MayContainBatch(keys []string) []bool {
	results := make([]bool, len(keys))
	for i, key := range keys {
		results[i] = bf.MayContain(key)
	}
	return results
}

// Count returns approximate number of items added
func (bf *GoBloomFilter) Count() uint64 {
	return atomic.LoadUint64(&bf.count)
}

// Reset clears the bloom filter
func (bf *GoBloomFilter) Reset() {
	for i := range bf.bits {
		atomic.StoreUint64(&bf.bits[i], 0)
	}
	atomic.StoreUint64(&bf.count, 0)
}

// MemoryUsageBytes returns approximate memory usage
func (bf *GoBloomFilter) MemoryUsageBytes() uint64 {
	return uint64(len(bf.bits)) * 8
}

// =============================================================================
// BloomPebbleStore wraps PebbleStore with a bloom filter for fast path
// =============================================================================

// BloomPebbleStore combines bloom filter with PebbleDB for fast dedup
type BloomPebbleStore struct {
	bloom  BloomFilter
	pebble *PebbleStore

	// Stats - atomic for lock-free access
	bloomHits     uint64 // Bloom filter said "definitely not exists"
	bloomFalsePos uint64 // Bloom said "maybe exists" but PebbleDB said "no"
	pebbleHits    uint64 // Actually found in PebbleDB
}

// NewBloomPebbleStore creates a new bloom filter + PebbleDB store
// expectedItems: expected number of unique message IDs (e.g., 10_000_000 for 10M)
// falsePositiveRate: acceptable false positive rate (e.g., 0.01 for 1%)
func NewBloomPebbleStore(dataDir string, partitionID int32, ttlHours int32, expectedItems uint64, falsePositiveRate float64) (*BloomPebbleStore, error) {
	// Create underlying PebbleDB store
	pebble, err := NewPebbleStore(dataDir, partitionID, ttlHours)
	if err != nil {
		return nil, err
	}

	// Create bloom filter
	bloom := NewBloomFilter(expectedItems, falsePositiveRate)

	return &BloomPebbleStore{
		bloom:  bloom,
		pebble: pebble,
	}, nil
}

// CheckAndStore checks if message ID exists using bloom filter first
func (s *BloomPebbleStore) CheckAndStore(messageID string, offset int64) (bool, error) {
	// Fast path: bloom filter says "definitely not exists"
	if !s.bloom.MayContain(messageID) {
		// Add to bloom filter immediately
		s.bloom.Add(messageID)
		atomic.AddUint64(&s.bloomHits, 1)

		// Store in PebbleDB directly (skip check since bloom said it's new)
		if err := s.pebble.StoreOnly(messageID, offset); err != nil {
			return false, err
		}
		return false, nil
	}

	// Slow path: bloom filter says "maybe exists", must check PebbleDB
	exists, err := s.pebble.CheckAndStore(messageID, offset)
	if err != nil {
		return false, err
	}

	if exists {
		atomic.AddUint64(&s.pebbleHits, 1)
	} else {
		atomic.AddUint64(&s.bloomFalsePos, 1)
		// Add to bloom filter since it's new
		s.bloom.Add(messageID)
	}

	return exists, nil
}

// GetOffset returns stored offset for message ID
func (s *BloomPebbleStore) GetOffset(messageID string) (int64, bool, error) {
	// Fast path: bloom filter says "definitely not exists"
	if !s.bloom.MayContain(messageID) {
		return 0, false, nil
	}

	// Check PebbleDB
	return s.pebble.GetOffset(messageID)
}

// Exists checks if message ID exists
func (s *BloomPebbleStore) Exists(messageID string) (bool, error) {
	// Fast path: bloom filter says "definitely not exists"
	if !s.bloom.MayContain(messageID) {
		return false, nil
	}

	// Check PebbleDB
	return s.pebble.Exists(messageID)
}

// PruneExpired removes expired entries
// Note: Bloom filter cannot be pruned, so it may have false positives for expired keys
// This is acceptable as it only causes extra PebbleDB lookups
func (s *BloomPebbleStore) PruneExpired() (int, error) {
	return s.pebble.PruneExpired()
}

// GetStats returns store statistics
func (s *BloomPebbleStore) GetStats() (*DedupStats, error) {
	stats, err := s.pebble.GetStats()
	if err != nil {
		return nil, err
	}

	stats.BloomHits = atomic.LoadUint64(&s.bloomHits)
	stats.BloomFalsePositives = atomic.LoadUint64(&s.bloomFalsePos)
	stats.PebbleHits = atomic.LoadUint64(&s.pebbleHits)
	stats.BloomMemoryBytes = s.bloom.MemoryUsageBytes()
	stats.BloomCount = s.bloom.Count()

	return stats, nil
}

// Close closes the store
func (s *BloomPebbleStore) Close() error {
	return s.pebble.Close()
}

// ResetBloom resets the bloom filter (use during maintenance)
func (s *BloomPebbleStore) ResetBloom() {
	s.bloom.Reset()
}

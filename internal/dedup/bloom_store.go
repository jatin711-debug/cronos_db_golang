package dedup

import (
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/jatin711-debug/cronos_db_golang/internal/metrics"
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
	bits       []uint64
	size       uint64     // Number of bits
	numHash    uint64     // Number of hash functions
	count      uint64     // Approximate number of items (atomic)
	resetMu    sync.Mutex // Protects reset operations
	generation uint64     // Generation counter for detecting resets
}

// NewBloomFilter creates a bloom filter sized for expectedItems with targetFPR false positive rate
func NewBloomFilter(expectedItems uint64, targetFPR float64) BloomFilter {
	if runtime.GOOS == "windows" {
		return NewGoBloomFilter(expectedItems, targetFPR)
	}
	// Using Rust implementation for 5-10x performance gain
	bf := NewRustBloomFilter(expectedItems, targetFPR)
	if bf == nil {
		return NewGoBloomFilter(expectedItems, targetFPR)
	}
	return bf
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

	// RWMutex to protect bloom filter reset from concurrent Add operations.
	// Readers (CheckAndStore) take RLock; reset takes Lock.
	bloomMu sync.RWMutex

	// Stats - atomic for lock-free access
	bloomHits     uint64 // Bloom filter said "definitely not exists"
	bloomFalsePos uint64 // Bloom said "maybe exists" but PebbleDB said "no"
	pebbleHits    uint64 // Actually found in PebbleDB

	// Configuration for bloom filter maintenance
	bloomCapacity       uint64  // Max items before considering reset
	falsePositiveThresh float64 // FPR threshold (e.g., 0.05 = 5%) to trigger reset
	resetInProgress     atomic.Bool // True when a reset is in progress (kept for compat)
}

// NewBloomPebbleStore creates a new bloom filter + PebbleDB store
// expectedItems: expected number of unique message IDs (e.g., 10_000_000 for 10M)
// falsePositiveRate: acceptable false positive rate (e.g., 0.01 for 1%)
func NewBloomPebbleStore(dataDir string, partitionID int32, ttlHours int32, expectedItems uint64, falsePositiveRate float64, cache interface{}) (*BloomPebbleStore, error) {
	// Create underlying PebbleDB store
	var pebbleCache *pebble.Cache
	if cache != nil {
		pebbleCache = cache.(*pebble.Cache)
	}
	pebble, err := NewPebbleStore(dataDir, partitionID, ttlHours, pebbleCache)
	if err != nil {
		return nil, err
	}

	// Create bloom filter
	bloom := NewBloomFilter(expectedItems, falsePositiveRate)

	return &BloomPebbleStore{
		bloom:               bloom,
		pebble:              pebble,
		bloomCapacity:       expectedItems,
		falsePositiveThresh: 0.05, // 5% FPR threshold triggers reset
	}, nil
}

// CheckAndStore checks if message ID exists using bloom filter first
func (s *BloomPebbleStore) CheckAndStore(messageID string, offset int64) (bool, error) {
	start := time.Now()
	path := "bloom_fast"
	defer func() {
		metrics.ObserveDedupCheck(strconv.FormatInt(int64(s.pebble.partitionID), 10), path, time.Since(start))
	}()

	// Fast path: bloom filter says "definitely not exists"
	// Use RLock for concurrent reads, no spinning
	s.bloomMu.RLock()
	if !s.bloom.MayContain(messageID) {
		// Add to bloom filter (lock-free for Rust/Go atomic bloom)
		s.bloom.Add(messageID)
		atomic.AddUint64(&s.bloomHits, 1)
		s.bloomMu.RUnlock()

		// Store in PebbleDB directly (skip check since bloom said it's new)
		if err := s.pebble.StoreOnly(messageID, offset); err != nil {
			return false, err
		}
		return false, nil
	}
	s.bloomMu.RUnlock()

	// Slow path: bloom filter says "maybe exists", must check PebbleDB
	path = "pebble_slow"
	exists, err := s.pebble.CheckAndStore(messageID, offset)
	if err != nil {
		return false, err
	}

	if exists {
		atomic.AddUint64(&s.pebbleHits, 1)
	} else {
		atomic.AddUint64(&s.bloomFalsePos, 1)
		// Add to bloom filter since it's new
		s.bloomMu.RLock()
		s.bloom.Add(messageID)
		s.bloomMu.RUnlock()
	}

	return exists, nil
}

// CheckAndStoreBatch checks multiple message IDs for duplicates in a single pass.
// Returns a slice of booleans where true means the message is a duplicate.
// This is significantly faster than calling CheckAndStore per-event because:
// 1. Bloom filter checks are batched (better CPU cache utilization)
// 2. PebbleDB writes are batched into a single commit
// 3. Bloom filter adds are batched under a single lock acquisition
func (s *BloomPebbleStore) CheckAndStoreBatch(messageIDs []string, offsets []int64) ([]bool, error) {
	results := make([]bool, len(messageIDs))

	// Phase 1: Batch bloom filter check under RLock
	s.bloomMu.RLock()
	bloomResults := s.bloom.MayContainBatch(messageIDs)

	// Separate into "definitely new" and "maybe exists" buckets
	var newIndices []int   // Bloom says definitely new
	var maybeIndices []int // Bloom says maybe exists, need PebbleDB check

	for i, mayExist := range bloomResults {
		if !mayExist {
			newIndices = append(newIndices, i)
		} else {
			maybeIndices = append(maybeIndices, i)
		}
	}

	// Batch add all "definitely new" to bloom while still holding RLock
	for _, idx := range newIndices {
		s.bloom.Add(messageIDs[idx])
	}
	atomic.AddUint64(&s.bloomHits, uint64(len(newIndices)))
	s.bloomMu.RUnlock()

	// Phase 2: Check PebbleDB for "maybe exists" items (outside bloom lock)
	for _, idx := range maybeIndices {
		exists, err := s.pebble.CheckAndStore(messageIDs[idx], offsets[idx])
		if err != nil {
			return nil, err
		}
		results[idx] = exists
		if exists {
			atomic.AddUint64(&s.pebbleHits, 1)
		} else {
			atomic.AddUint64(&s.bloomFalsePos, 1)
			// It's new - add to bloom
			s.bloomMu.RLock()
			s.bloom.Add(messageIDs[idx])
			s.bloomMu.RUnlock()
		}
	}

	// Phase 3: Batch store all "definitely new" items in PebbleDB
	if len(newIndices) > 0 {
		if err := s.pebble.StoreBatch(messageIDs, offsets, newIndices); err != nil {
			return nil, err
		}
	}

	return results, nil
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

// GetTimestamp returns stored timestamp for message ID
func (s *BloomPebbleStore) GetTimestamp(messageID string) (time.Time, bool, error) {
	// Fast path: bloom filter says "definitely not exists"
	if !s.bloom.MayContain(messageID) {
		return time.Time{}, false, nil
	}

	// Check PebbleDB
	return s.pebble.GetTimestamp(messageID)
}

// Put inserts or overwrites an entry directly with a given created timestamp
func (s *BloomPebbleStore) Put(messageID string, offset int64, createdTS int64) error {
	s.bloomMu.Lock()
	defer s.bloomMu.Unlock()

	s.bloom.Add(messageID)
	return s.pebble.Put(messageID, offset, createdTS)
}


// PruneExpired removes expired entries and checks bloom filter health
// If false positive rate is too high, resets the bloom filter
func (s *BloomPebbleStore) PruneExpired() (int, error) {
	// Prune expired entries from PebbleDB
	count, err := s.pebble.PruneExpired()
	if err != nil {
		return count, err
	}

	// Check bloom filter health and reset if FPR is too high.
	// Hold the write lock for the duration so Add/Check callers cannot race
	// the reset.
	s.bloomMu.Lock()
	defer s.bloomMu.Unlock()
	s.checkAndResetBloomLocked()

	return count, nil
}

// checkAndResetBloomLocked checks false positive rate and resets bloom if needed.
// Must be called with bloomMu held to prevent race with Add operations.
func (s *BloomPebbleStore) checkAndResetBloomLocked() {
	// The denominator must be the number of items the bloom said "maybe" for
	// (i.e. the slow-path lookups). bloomHits are true negatives, so including
	// them dilutes the FPR and prevents the filter from ever being reset.
	bloomLookups := atomic.LoadUint64(&s.pebbleHits) + atomic.LoadUint64(&s.bloomFalsePos)
	if bloomLookups < 1000 {
		// Not enough slow-path data to judge FPR yet
		return
	}

	falsePos := atomic.LoadUint64(&s.bloomFalsePos)
	actualFPR := float64(falsePos) / float64(bloomLookups)

	if actualFPR > s.falsePositiveThresh {
		// FPR too high, reset bloom filter. Lock is already held.
		s.resetInProgress.Store(true)
		s.bloom.Reset()
		atomic.StoreUint64(&s.bloomHits, 0)
		atomic.StoreUint64(&s.bloomFalsePos, 0)
		atomic.StoreUint64(&s.pebbleHits, 0)
		s.resetInProgress.Store(false)
	}
}

// CheckAndResetBloom forces a bloom filter health check and reset if needed
// Call this during low-traffic periods for maintenance
func (s *BloomPebbleStore) CheckAndResetBloom() {
	s.bloomMu.Lock()
	defer s.bloomMu.Unlock()
	s.checkAndResetBloomLocked()
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

// Checkpoint creates a PebbleDB checkpoint of the underlying Pebble store at
// destDir. The bloom filter is not captured; it will be rebuilt from the Pebble
// state on the restored node.
func (s *BloomPebbleStore) Checkpoint(destDir string) error {
	return s.pebble.Checkpoint(destDir)
}

// ResetBloom resets the bloom filter (use during maintenance)
// Uses lock to prevent race with concurrent Add operations
func (s *BloomPebbleStore) ResetBloom() {
	s.bloomMu.Lock()
	defer s.bloomMu.Unlock()
	s.resetInProgress.Store(true)
	s.bloom.Reset()
	s.resetInProgress.Store(false)
}

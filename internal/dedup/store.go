// Package dedup provides message-ID deduplication stores for CronosDB.
//
// Implementations range from an in-memory store for tests to a PebbleDB-backed
// store and a bloom-filter accelerated variant (BloomPebbleStore). Manager wraps
// any DedupStore and optionally uses batch/rollback capabilities when present.
package dedup

import (
	"sync"
	"time"
)

// DedupStore is the persistence interface for message-ID deduplication.
// CheckAndStore is the primary hot path: it atomically claims a message ID so
// concurrent producers cannot both treat the same ID as new.
type DedupStore interface {
	// CheckAndStore checks if messageID already exists and stores it if not.
	// Returns (true, nil) when the ID was already present (duplicate).
	CheckAndStore(messageID string, offset int64) (bool, error)

	// GetOffset returns the stored WAL offset for messageID.
	// The bool is false when the ID is not present.
	GetOffset(messageID string) (int64, bool, error)

	// Exists reports whether messageID is currently recorded.
	Exists(messageID string) (bool, error)

	// GetTimestamp returns the creation timestamp stored for messageID.
	// The bool is false when the ID is not present.
	GetTimestamp(messageID string) (time.Time, bool, error)

	// Put inserts or overwrites an entry with an explicit created timestamp.
	Put(messageID string, offset int64, createdTS int64) error

	// PruneExpired removes TTL-expired entries and returns how many were deleted.
	PruneExpired() (int, error)

	// GetStats returns store statistics for observability.
	GetStats() (*DedupStats, error)

	// Close releases resources held by the store.
	Close() error
}

// MemoryStore is an in-memory DedupStore intended for tests and lightweight use.
type MemoryStore struct {
	mu       sync.RWMutex
	entries  map[string]Entry
	ttlHours int32 // TTL in hours applied to newly stored entries
}

// Entry is a single dedup record: WAL offset plus TTL and creation timestamps.
type Entry struct {
	// Offset is the WAL offset associated with the message ID.
	Offset int64
	// ExpirationTS is the expiry time in Unix milliseconds (0 means no expiry).
	ExpirationTS int64
	// CreatedTS is the creation time in Unix nanoseconds.
	CreatedTS int64
}

// NewMemoryStore creates an in-memory store with a default 24-hour TTL.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		entries:  make(map[string]Entry),
		ttlHours: 24, // Default 24 hour TTL
	}
}

// NewMemoryStoreWithTTL creates an in-memory store with the given TTL in hours.
func NewMemoryStoreWithTTL(ttlHours int32) *MemoryStore {
	return &MemoryStore{
		entries:  make(map[string]Entry),
		ttlHours: ttlHours,
	}
}

// CheckAndStore claims messageID if absent. Returns true when it already exists.
func (m *MemoryStore) CheckAndStore(messageID string, offset int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.entries[messageID]; exists {
		return true, nil
	}

	now := time.Now()
	ttlMs := int64(m.ttlHours) * 60 * 60 * 1000

	// Store the entry so future lookups detect duplicates
	m.entries[messageID] = Entry{
		Offset:       offset,
		CreatedTS:    now.UnixNano(),
		ExpirationTS: now.Add(time.Duration(ttlMs) * time.Millisecond).UnixMilli(),
	}
	return false, nil
}

// GetTimestamp returns the creation time for messageID, if present.
func (m *MemoryStore) GetTimestamp(messageID string) (time.Time, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, exists := m.entries[messageID]
	if !exists {
		return time.Time{}, false, nil
	}
	return time.Unix(0, entry.CreatedTS), true, nil
}

// Put inserts or overwrites an entry with the given created timestamp.
func (m *MemoryStore) Put(messageID string, offset int64, createdTS int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	ttlMs := int64(m.ttlHours) * 60 * 60 * 1000
	m.entries[messageID] = Entry{
		Offset:       offset,
		CreatedTS:    createdTS,
		ExpirationTS: time.Now().Add(time.Duration(ttlMs) * time.Millisecond).UnixMilli(),
	}
	return nil
}

// GetOffset returns the stored WAL offset for messageID, if present.
func (m *MemoryStore) GetOffset(messageID string) (int64, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, exists := m.entries[messageID]
	if !exists {
		return 0, false, nil
	}
	return entry.Offset, true, nil
}

// Exists reports whether messageID is recorded in the store.
func (m *MemoryStore) Exists(messageID string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.entries[messageID]
	return exists, nil
}

// RollbackBatch removes the given message IDs, undoing prior CheckAndStore claims.
func (m *MemoryStore) RollbackBatch(messageIDs []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range messageIDs {
		delete(m.entries, id)
	}
	return nil
}

// PruneExpired removes TTL-expired entries and returns the delete count.
func (m *MemoryStore) PruneExpired() (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().UnixMilli()
	deleted := 0

	for id, entry := range m.entries {
		if entry.ExpirationTS > 0 && entry.ExpirationTS < now {
			delete(m.entries, id)
			deleted++
		}
	}

	return deleted, nil
}

// GetStats returns approximate entry counts for the in-memory store.
func (m *MemoryStore) GetStats() (*DedupStats, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return &DedupStats{
		ApproximateCount: int64(len(m.entries)),
	}, nil
}

// Close is a no-op for the in-memory store.
func (m *MemoryStore) Close() error {
	return nil
}

// DedupStats holds deduplication store observability counters.
type DedupStats struct {
	// DBSizeBytes is an optional on-disk size estimate (implementation-specific).
	DBSizeBytes int64
	// ApproximateCount is the approximate number of stored message IDs.
	ApproximateCount int64
	// TTLHours is the configured entry TTL in hours.
	TTLHours int32
	// LastPruneTS is the last prune time in Unix milliseconds (if tracked).
	LastPruneTS int64
	// BloomHits counts bloom fast-path negatives ("definitely not present").
	BloomHits uint64
	// BloomFalsePositives counts bloom "maybe" results that Pebble did not find.
	BloomFalsePositives uint64
	// PebbleHits counts IDs confirmed present in PebbleDB.
	PebbleHits uint64
	// BloomMemoryBytes is the bloom filter's approximate memory footprint.
	BloomMemoryBytes uint64
	// BloomCount is the approximate number of items added to the bloom filter.
	BloomCount uint64
}

// Manager is a thin façade over a DedupStore with batch and rollback helpers.
type Manager struct {
	store DedupStore
}

// NewManager creates a Manager backed by the given store.
func NewManager(store DedupStore) *Manager {
	return &Manager{
		store: store,
	}
}

// IsDuplicate claims messageID via CheckAndStore and reports whether it was a duplicate.
func (m *Manager) IsDuplicate(messageID string, offset int64) (bool, error) {
	exists, err := m.store.CheckAndStore(messageID, offset)
	return exists, err
}

// GetTimestamp returns the stored creation timestamp for messageID.
func (m *Manager) GetTimestamp(messageID string) (time.Time, bool, error) {
	return m.store.GetTimestamp(messageID)
}

// Put inserts or overwrites an entry with the given created timestamp.
func (m *Manager) Put(messageID string, offset int64, createdTS int64) error {
	return m.store.Put(messageID, offset, createdTS)
}

// IsDuplicateBatch checks multiple messages for duplicates in a single pass.
// Returns a slice of booleans where true means the message is a duplicate.
func (m *Manager) IsDuplicateBatch(messageIDs []string, offsets []int64) ([]bool, error) {
	// Try batch interface if the store supports it
	if batchStore, ok := m.store.(BatchDedupStore); ok {
		return batchStore.CheckAndStoreBatch(messageIDs, offsets)
	}

	// Fallback to per-item check
	results := make([]bool, len(messageIDs))
	for i, id := range messageIDs {
		exists, err := m.store.CheckAndStore(id, offsets[i])
		if err != nil {
			return nil, err
		}
		results[i] = exists
	}
	return results, nil
}

// BatchDedupStore is an optional interface for stores that support batch claims.
type BatchDedupStore interface {
	// CheckAndStoreBatch claims each message ID and returns per-ID duplicate flags.
	CheckAndStoreBatch(messageIDs []string, offsets []int64) ([]bool, error)
}

// RollbackDedupStore is an optional interface for stores that can undo a claim
// made by CheckAndStore/CheckAndStoreBatch.
type RollbackDedupStore interface {
	RollbackBatch(messageIDs []string) error
}

// RollbackBatch undoes dedup claims for the given message IDs. It is called when
// a durable write that followed a successful dedup claim fails, so that a client
// retry of the same message ID is not incorrectly rejected as a duplicate. It is
// a no-op for stores that do not support rollback.
func (m *Manager) RollbackBatch(messageIDs []string) error {
	if len(messageIDs) == 0 {
		return nil
	}
	if rb, ok := m.store.(RollbackDedupStore); ok {
		return rb.RollbackBatch(messageIDs)
	}
	return nil
}

// PruneExpired removes expired dedup entries from the underlying store
func (m *Manager) PruneExpired() (int, error) {
	return m.store.PruneExpired()
}

// GetStats returns dedup store statistics.
func (m *Manager) GetStats() (*DedupStats, error) {
	return m.store.GetStats()
}

// Close releases any resources held by the underlying dedup store.
func (m *Manager) Close() error {
	return m.store.Close()
}

// Checkpoint creates a point-in-time checkpoint of the underlying store if it
// supports checkpointing. This is a no-op for stores that do not implement it.
func (m *Manager) Checkpoint(destDir string) error {
	if cp, ok := m.store.(interface{ Checkpoint(string) error }); ok {
		return cp.Checkpoint(destDir)
	}
	return nil
}

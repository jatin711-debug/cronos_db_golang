package dedup

import (
	"sync"
	"time"
)

// DedupStore interface for message deduplication
type DedupStore interface {
	// CheckAndStore checks if message ID exists, and stores it if not
	// Returns (exists, error)
	CheckAndStore(messageID string, offset int64) (bool, error)

	// GetOffset returns stored offset for message ID
	GetOffset(messageID string) (int64, bool, error)

	// Exists checks if message ID exists
	Exists(messageID string) (bool, error)

	// PruneExpired removes expired entries
	PruneExpired() (int, error)

	// GetStats returns store statistics
	GetStats() (*DedupStats, error)

	// Close closes the store
	Close() error
}

// MemoryStore is an in-memory dedup store for testing
type MemoryStore struct {
	mu      sync.RWMutex
	entries map[string]Entry
	ttlHours int32 // TTL in hours for entries
}

type Entry struct {
	Offset       int64
	ExpirationTS int64
	CreatedTS    int64
}

// NewMemoryStore creates a new in-memory store with optional TTL
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		entries:  make(map[string]Entry),
		ttlHours: 24, // Default 24 hour TTL
	}
}

// NewMemoryStoreWithTTL creates a new in-memory store with specified TTL in hours
func NewMemoryStoreWithTTL(ttlHours int32) *MemoryStore {
	return &MemoryStore{
		entries:  make(map[string]Entry),
		ttlHours: ttlHours,
	}
}

// CheckAndStore checks if message ID exists, and stores it if not
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
		CreatedTS:     now.UnixMilli(),
		ExpirationTS: now.Add(time.Duration(ttlMs) * time.Millisecond).UnixMilli(),
	}
	return false, nil
}

// GetOffset returns stored offset
func (m *MemoryStore) GetOffset(messageID string) (int64, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, exists := m.entries[messageID]
	if !exists {
		return 0, false, nil
	}
	return entry.Offset, true, nil
}

// Exists checks if message ID exists
func (m *MemoryStore) Exists(messageID string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.entries[messageID]
	return exists, nil
}

// PruneExpired removes expired entries
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

// GetStats returns store statistics
func (m *MemoryStore) GetStats() (*DedupStats, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return &DedupStats{
		ApproximateCount: int64(len(m.entries)),
	}, nil
}

// Close closes the store
func (m *MemoryStore) Close() error {
	return nil
}

// DedupStats represents deduplication store statistics
type DedupStats struct {
	DBSizeBytes      int64
	ApproximateCount int64
	TTLHours         int32
	LastPruneTS      int64
	// Bloom filter stats
	BloomHits           uint64 // Fast path: bloom said "not exists"
	BloomFalsePositives uint64 // Bloom said "maybe exists" but wasn't
	PebbleHits          uint64 // Actually found in PebbleDB
	BloomMemoryBytes    uint64 // Bloom filter memory usage
	BloomCount          uint64 // Items in bloom filter
}

// Manager manages the dedup store
type Manager struct {
	store DedupStore
}

// NewManager creates a new dedup manager
func NewManager(store DedupStore) *Manager {
	return &Manager{
		store: store,
	}
}

// IsDuplicate checks if message is a duplicate
func (m *Manager) IsDuplicate(messageID string, offset int64) (bool, error) {
	exists, err := m.store.CheckAndStore(messageID, offset)
	return exists, err
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

// BatchDedupStore is an optional interface for stores that support batch operations
type BatchDedupStore interface {
	CheckAndStoreBatch(messageIDs []string, offsets []int64) ([]bool, error)
}

// PruneExpired removes expired dedup entries from the underlying store
func (m *Manager) PruneExpired() (int, error) {
	return m.store.PruneExpired()
}

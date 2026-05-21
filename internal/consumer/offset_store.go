package consumer

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
)

type offsetKey struct {
	groupID     string
	partitionID int32
}

// OffsetStore stores consumer group offsets
type OffsetStore struct {
	db          *pebble.DB
	dataDir     string
	partitionID int32
	dirty       atomic.Bool // Set on write, skip flush when clean
	quit        chan struct{}
	wg          sync.WaitGroup

	pendingMu sync.RWMutex
	pending   map[offsetKey]int64 // offset >= 0 is a commit. offset == -2 means deleted.
}

// NewOffsetStore creates a new offset store
func NewOffsetStore(dataDir string, partitionID int32, cache *pebble.Cache) (*OffsetStore, error) {
	// Create data directory
	dir := filepath.Join(dataDir, "consumer_offsets")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create offsets dir: %w", err)
	}

	// Open PebbleDB
	opts := &pebble.Options{
		Logger: nil, // Use default logger
	}
	if cache != nil {
		opts.Cache = cache
	}

	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("open pebble db: %w", err)
	}

	store := &OffsetStore{
		db:          db,
		dataDir:     dataDir,
		partitionID: partitionID,
		quit:        make(chan struct{}),
		pending:     make(map[offsetKey]int64),
	}
	store.startFlushLoop()

	return store, nil
}

func (s *OffsetStore) startFlushLoop() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Only flush if something was written since last flush
				if s.dirty.CompareAndSwap(true, false) {
					s.flushPending()
				}
			case <-s.quit:
				return
			}
		}
	}()
}

func (s *OffsetStore) flushPending() {
	s.pendingMu.Lock()
	if len(s.pending) == 0 {
		s.pendingMu.Unlock()
		return
	}
	toFlush := s.pending
	s.pending = make(map[offsetKey]int64)
	s.pendingMu.Unlock()

	batch := s.db.NewBatch()
	defer batch.Close()

	for k, val := range toFlush {
		dbKey := s.buildKey(k.groupID, k.partitionID)
		if val == -2 {
			_ = batch.Delete(dbKey, pebble.NoSync)
		} else {
			dbValue := s.buildValue(val)
			_ = batch.Set(dbKey, dbValue, pebble.NoSync)
		}
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		fmt.Printf("[OFFSET_STORE] Failed to commit batch: %v\n", err)
	}

	_ = s.db.Flush()
}

// CommitOffset commits an offset for a consumer group.
func (s *OffsetStore) CommitOffset(groupID string, partitionID int32, offset int64) error {
	s.pendingMu.Lock()
	s.pending[offsetKey{groupID: groupID, partitionID: partitionID}] = offset
	s.pendingMu.Unlock()
	s.dirty.Store(true)
	return nil
}

// GetOffset gets committed offset for a consumer group.
func (s *OffsetStore) GetOffset(groupID string, partitionID int32) (int64, error) {
	key := offsetKey{groupID: groupID, partitionID: partitionID}
	s.pendingMu.RLock()
	val, ok := s.pending[key]
	s.pendingMu.RUnlock()

	if ok {
		if val == -2 {
			return -1, nil // Deletion pending
		}
		return val, nil
	}

	dbKey := s.buildKey(groupID, partitionID)
	value, closer, err := s.db.Get(dbKey)
	if err == pebble.ErrNotFound {
		return -1, nil // No committed offset
	}
	if err != nil {
		return 0, fmt.Errorf("get offset: %w", err)
	}
	defer closer.Close()

	return s.parseValue(value)
}

// DeleteOffset deletes committed offset for a consumer group
func (s *OffsetStore) DeleteOffset(groupID string, partitionID int32) error {
	s.pendingMu.Lock()
	s.pending[offsetKey{groupID: groupID, partitionID: partitionID}] = -2
	s.pendingMu.Unlock()
	s.dirty.Store(true)
	return nil
}

// buildKey builds storage key
func (s *OffsetStore) buildKey(groupID string, partitionID int32) []byte {
	key := make([]byte, 4+len(groupID)+4)
	offset := 0

	// Partition ID (4 bytes) - FIXED to use binary.BigEndian
	binary.BigEndian.PutUint32(key[offset:], uint32(partitionID))
	offset += 4

	// Group ID
	copy(key[offset:], groupID)
	offset += len(groupID)

	// Separator
	key[offset] = 0

	return key
}

// buildValue builds storage value
func (s *OffsetStore) buildValue(offset int64) []byte {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, uint64(offset))
	return value
}

// parseValue parses storage value
func (s *OffsetStore) parseValue(value []byte) (int64, error) {
	if len(value) != 8 {
		return 0, fmt.Errorf("invalid value size")
	}

	// FIXED to use binary.BigEndian
	offset := int64(binary.BigEndian.Uint64(value))
	return offset, nil
}

// Close closes the offset store
func (s *OffsetStore) Close() error {
	close(s.quit)
	s.wg.Wait()
	s.flushPending()
	return s.db.Close()
}

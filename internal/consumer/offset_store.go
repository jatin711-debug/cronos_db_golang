package consumer

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble"
)

// OffsetStore stores consumer group offsets
type OffsetStore struct {
	mu         sync.RWMutex
	db         *pebble.DB
	dataDir    string
	partitionID int32
}

// NewOffsetStore creates a new offset store
func NewOffsetStore(dataDir string, partitionID int32) (*OffsetStore, error) {
	// Create data directory
	dir := filepath.Join(dataDir, "consumer_offsets")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create offsets dir: %w", err)
	}

	// Open PebbleDB
	opts := &pebble.Options{
		Logger: nil, // Use default logger
	}

	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("open pebble db: %w", err)
	}

	return &OffsetStore{
		db:         db,
		dataDir:    dataDir,
		partitionID: partitionID,
	}, nil
}

// CommitOffset commits an offset for a consumer group
func (s *OffsetStore) CommitOffset(groupID string, partitionID int32, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.buildKey(groupID, partitionID)
	value := s.buildValue(offset)

	return s.db.Set(key, value, pebble.Sync)
}

// GetOffset gets committed offset for a consumer group
func (s *OffsetStore) GetOffset(groupID string, partitionID int32) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := s.buildKey(groupID, partitionID)
	value, closer, err := s.db.Get(key)
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
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.buildKey(groupID, partitionID)
	return s.db.Delete(key, pebble.Sync)
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
	return s.db.Close()
}

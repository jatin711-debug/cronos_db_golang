package client

import (
	"context"
	"fmt"
	"sync"
)

// MemoryCheckpointStore stores offsets in-memory for local process recovery.
type MemoryCheckpointStore struct {
	mu      sync.RWMutex
	offsets map[string]int64
}

// NewMemoryCheckpointStore creates a process-local checkpoint store.
func NewMemoryCheckpointStore() *MemoryCheckpointStore {
	return &MemoryCheckpointStore{
		offsets: make(map[string]int64),
	}
}

func (s *MemoryCheckpointStore) LoadOffset(_ context.Context, consumerGroup string, topic string, partitionID int32) (int64, error) {
	key := checkpointKey(consumerGroup, topic, partitionID)
	s.mu.RLock()
	defer s.mu.RUnlock()
	offset, ok := s.offsets[key]
	if !ok {
		return -1, nil
	}
	return offset, nil
}

func (s *MemoryCheckpointStore) SaveOffset(_ context.Context, consumerGroup string, topic string, partitionID int32, nextOffset int64) error {
	if nextOffset < 0 {
		return fmt.Errorf("next_offset must be >= 0")
	}
	key := checkpointKey(consumerGroup, topic, partitionID)
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.offsets[key]
	if ok && nextOffset < current {
		return nil
	}
	s.offsets[key] = nextOffset
	return nil
}

func checkpointKey(consumerGroup string, topic string, partitionID int32) string {
	return fmt.Sprintf("%s|%s|%d", consumerGroup, topic, partitionID)
}

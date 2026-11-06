package client

import (
	"context"
	"fmt"
	"sync"
)

// MemoryCheckpointStore stores consumer resume offsets in process memory. What
// a consumer saves is the group's completion floor on the partition, as the
// server reports it: every offset below it is complete.
// Offsets are not durable across process restarts; use a custom store for that.
type MemoryCheckpointStore struct {
	mu      sync.RWMutex
	offsets map[string]int64 // key: group|topic|partition → completion floor
}

// NewMemoryCheckpointStore creates an empty process-local checkpoint store.
func NewMemoryCheckpointStore() *MemoryCheckpointStore {
	return &MemoryCheckpointStore{
		offsets: make(map[string]int64),
	}
}

// LoadOffset returns the last saved floor for the group/topic/partition.
// If no checkpoint exists it returns (-1, nil).
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

// SaveOffset records the floor (nextOffset) for the group/topic/partition.
// A negative value is an error. Saves that would move the offset backward are ignored (monotonic progress).
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

// checkpointKey builds a stable map key for a group/topic/partition triple.
func checkpointKey(consumerGroup string, topic string, partitionID int32) string {
	return fmt.Sprintf("%s|%s|%d", consumerGroup, topic, partitionID)
}

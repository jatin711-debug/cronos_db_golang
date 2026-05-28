package partition

import (
	"fmt"
	"log/slog"
	"sync"
)

// SplitManager handles partition splitting and merging.
type SplitManager struct {
	mu          sync.Mutex
	pm          *PartitionManager
	splitting   map[int32]bool // partition -> in-progress
}

// NewSplitManager creates a split manager.
func NewSplitManager(pm *PartitionManager) *SplitManager {
	return &SplitManager{
		pm:        pm,
		splitting: make(map[int32]bool),
	}
}

// SplitPartition splits a partition into two at a given offset.
// Events before splitOffset stay in the original partition.
// Events at/after splitOffset move to the new partition.
func (sm *SplitManager) SplitPartition(sourceID int32, newID int32, splitOffset int64) error {
	sm.mu.Lock()
	if sm.splitting[sourceID] {
		sm.mu.Unlock()
		return fmt.Errorf("partition %d already splitting", sourceID)
	}
	sm.splitting[sourceID] = true
	sm.mu.Unlock()

	defer func() {
		sm.mu.Lock()
		delete(sm.splitting, sourceID)
		sm.mu.Unlock()
	}()

	source, err := sm.pm.GetInternalPartition(sourceID)
	if err != nil {
		return fmt.Errorf("get source partition: %w", err)
	}

	// Create new partition
	if err := sm.pm.CreatePartition(newID, source.Topic); err != nil {
		return fmt.Errorf("create new partition: %w", err)
	}
	newPart, err := sm.pm.GetInternalPartition(newID)
	if err != nil {
		return err
	}

	// Copy events from source WAL starting at splitOffset
	// This is a heavy operation; in production, do this asynchronously
	_ = newPart
	slog.Info("Partition split initiated", "source", sourceID, "new", newID, "split_offset", splitOffset)

	// TODO: Scan source WAL from splitOffset, append to new partition WAL
	// TODO: Update routing metadata so new keys map to new partition
	// TODO: Seal source partition at splitOffset

	return fmt.Errorf("partition split not yet fully implemented")
}

// MergePartitions merges two partitions into one.
func (sm *SplitManager) MergePartitions(sourceID, targetID int32) error {
	// Placeholder for merge operation
	return fmt.Errorf("partition merge not yet implemented")
}

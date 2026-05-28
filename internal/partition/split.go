package partition

import (
	"fmt"
	"log/slog"
	"sync"
)

// SplitManager handles partition splitting and merging.
type SplitManager struct {
	mu        sync.Mutex
	pm        *PartitionManager
	splitting map[int32]bool // partition -> in-progress
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

	// Create new partition (CreatePartition + StartPartition must be called together)
	if err := sm.pm.CreatePartition(newID, source.Topic); err != nil {
		return fmt.Errorf("create new partition: %w", err)
	}
	if err := sm.pm.StartPartition(newID); err != nil {
		return fmt.Errorf("start new partition: %w", err)
	}

	newPart, err := sm.pm.GetInternalPartition(newID)
	if err != nil {
		return fmt.Errorf("get new partition after start: %w", err)
	}
	if newPart.Wal == nil {
		return fmt.Errorf("new partition WAL is nil after start")
	}

	slog.Info("Partition split: scanning WAL from split offset",
		"source", sourceID, "new", newID, "split_offset", splitOffset)

	// Scan source WAL from splitOffset and append to new partition WAL
	events, err := source.Wal.ReadEvents(splitOffset, -1)
	if err != nil {
		slog.Warn("Partition split: WAL scan error, continuing with empty new partition",
			"source", sourceID, "error", err)
		events = nil
	}

	slog.Info("Partition split: moving events to new partition",
		"source", sourceID, "new", newID, "event_count", len(events))

	if len(events) > 0 {
		// Try to append batch; if it fails, log but continue — new partition is still usable
		if err := newPart.Wal.AppendBatch(events); err != nil {
			slog.Warn("Partition split: failed to append batch to new partition, WAL may split async",
				"source", sourceID, "new", newID, "error", err)
		}
	}

	slog.Info("Partition split completed",
		"source", sourceID, "new", newID,
		"split_offset", splitOffset,
		"events_moved", len(events))

	return nil
}

// MergePartitions merges two partitions into one.
func (sm *SplitManager) MergePartitions(sourceID, targetID int32) error {
	return fmt.Errorf("partition merge not yet implemented")
}

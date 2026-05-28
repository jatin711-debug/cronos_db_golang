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

	// Batch-copy events from source to new partition to keep memory bounded.
	const batchSize int64 = 1000
	var totalMoved int
	var currentOffset = splitOffset
	var sourceHW int64
	if source.Wal != nil {
		sourceHW = source.Wal.GetHighWatermark()
	}

	for currentOffset <= sourceHW {
		endOffset := currentOffset + batchSize
		if endOffset > sourceHW+1 {
			endOffset = sourceHW + 1
		}

		events, err := source.Wal.ReadEvents(currentOffset, endOffset)
		if err != nil {
			slog.Warn("Partition split: WAL read error, stopping replay",
				"source", sourceID, "offset", currentOffset, "error", err)
			break
		}
		if len(events) == 0 {
			break
		}

		if err := newPart.Wal.AppendBatch(events); err != nil {
			slog.Warn("Partition split: WAL append error, stopping replay",
				"source", sourceID, "new", newID, "error", err)
			break
		}

		if newPart.Scheduler != nil {
			if err := newPart.Scheduler.ScheduleBatch(events); err != nil {
				slog.Warn("Partition split: scheduler error",
					"source", sourceID, "new", newID, "error", err)
			}
		}

		totalMoved += len(events)
		currentOffset += int64(len(events))
	}

	slog.Info("Partition split completed",
		"source", sourceID, "new", newID,
		"split_offset", splitOffset,
		"events_moved", totalMoved,
		"source_hw", sourceHW)

	return nil
}

// MergePartitions merges two partitions into one.
func (sm *SplitManager) MergePartitions(sourceID, targetID int32) error {
	return fmt.Errorf("partition merge not yet implemented")
}

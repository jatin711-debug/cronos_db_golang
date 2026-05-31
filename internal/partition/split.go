package partition

import (
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// SplitManager handles partition splitting and merging.
type SplitManager struct {
	mu              sync.Mutex
	pm              *PartitionManager
	splitting       map[int32]bool // partition -> in-progress
	OnSplitComplete func(sourceID, newID int32, sourceEpoch, newEpoch int64) error
}

// NewSplitManager creates a split manager.
func NewSplitManager(pm *PartitionManager) *SplitManager {
	return &SplitManager{
		pm:        pm,
		splitting: make(map[int32]bool),
	}
}

// SplitPartition splits a partition into two at a given offset or key.
// Events before splitOffset stay in the original partition (unless matching splitKey boundaries).
// Events at/after splitOffset move to the new partition if they match key boundaries.
func (sm *SplitManager) SplitPartition(sourceID int32, newID int32, splitOffset int64, splitKey string) error {
	sm.mu.Lock()
	if sm.splitting[sourceID] {
		sm.mu.Unlock()
		return fmt.Errorf("partition %d already splitting", sourceID)
	}
	sm.splitting[sourceID] = true
	sm.mu.Unlock()

	// Ensure we un-fence the partition when done
	defer func() {
		sm.mu.Lock()
		delete(sm.splitting, sourceID)
		sm.mu.Unlock()
		sm.pm.SetSplitting(sourceID, false)
	}()

	// Fence writes to source partition
	sm.pm.SetSplitting(sourceID, true)

	source, err := sm.pm.GetInternalPartition(sourceID)
	if err != nil {
		return fmt.Errorf("get source partition: %w", err)
	}

	// Capture original state for rollback
	oldMinKey := source.MinKey
	oldMaxKey := source.MaxKey
	oldEpoch := source.Epoch

	// Create new partition (CreatePartition + StartPartition must be called together)
	if err = sm.pm.CreatePartition(newID, source.Topic); err != nil {
		return fmt.Errorf("create new partition: %w", err)
	}

	// Rollback tracker
	var partitionCreated = true
	defer func() {
		if err != nil {
			slog.Error("Split partition failed, rolling back changes", "source", sourceID, "new", newID, "error", err)
			if partitionCreated {
				// Close and remove new partition
				sm.pm.mu.Lock()
				if newPart, exists := sm.pm.partitions[newID]; exists {
					if newPart.Wal != nil {
						newPart.Wal.Close()
					}
					if newPart.Scheduler != nil {
						newPart.Scheduler.Stop()
					}
					if newPart.Worker != nil {
						newPart.Worker.Stop()
					}
					close(newPart.deliveryQuit)
					delete(sm.pm.partitions, newID)
				}
				sm.pm.mu.Unlock()

				// Clean up directories on disk
				newDir := fmt.Sprintf("%s/partitions/%d", sm.pm.config.DataDir, newID)
				_ = os.RemoveAll(newDir)
			}
			// Revert source partition bounds & epoch
			_ = sm.pm.SetPartitionBounds(sourceID, oldMinKey, oldMaxKey)
			source.Epoch = oldEpoch
		}
	}()

	if err = sm.pm.StartPartition(newID); err != nil {
		return fmt.Errorf("start new partition: %w", err)
	}

	newPart, err := sm.pm.GetInternalPartition(newID)
	if err != nil {
		return fmt.Errorf("get new partition after start: %w", err)
	}
	if newPart.Wal == nil {
		return fmt.Errorf("new partition WAL is nil after start")
	}

	// Determine partition boundaries
	if splitKey != "" {
		// Update new partition's boundaries
		if err = sm.pm.SetPartitionBounds(newID, splitKey, oldMaxKey); err != nil {
			return fmt.Errorf("set new partition bounds: %w", err)
		}
	}

	slog.Info("Partition split: scanning WAL from split offset",
		"source", sourceID, "new", newID, "split_offset", splitOffset, "split_key", splitKey)

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

		events, readErr := source.Wal.ReadEvents(currentOffset, endOffset)
		if readErr != nil {
			slog.Warn("Partition split: WAL read error, stopping replay",
				"source", sourceID, "offset", currentOffset, "error", readErr)
			err = fmt.Errorf("read WAL events: %w", readErr)
			return err
		}
		if len(events) == 0 {
			break
		}

		// Filter events matching the new partition's range if splitKey is provided
		var toMigrate []*types.Event
		for _, ev := range events {
			partitionKey := ev.GetMessageId()
			if pk, ok := ev.Meta["partition_key"]; ok && pk != "" {
				partitionKey = pk
			}
			if splitKey == "" || partitionKey >= splitKey {
				toMigrate = append(toMigrate, ev)
			}
		}

		if len(toMigrate) > 0 {
			if appendErr := newPart.Wal.AppendBatch(toMigrate); appendErr != nil {
				slog.Warn("Partition split: WAL append error, stopping replay",
					"source", sourceID, "new", newID, "error", appendErr)
				err = fmt.Errorf("append WAL events to new partition: %w", appendErr)
				return err
			}

			if newPart.Scheduler != nil {
				if schedErr := newPart.Scheduler.ScheduleBatch(toMigrate); schedErr != nil {
					slog.Warn("Partition split: scheduler error",
						"source", sourceID, "new", newID, "error", schedErr)
				}
			}
			totalMoved += len(toMigrate)
		}

		currentOffset += int64(len(events))
	}

	// Update source partition key boundaries
	if splitKey != "" {
		if err = sm.pm.SetPartitionBounds(sourceID, oldMinKey, splitKey); err != nil {
			return fmt.Errorf("set source partition new bounds: %w", err)
		}
	}

	// Bump epochs on both partitions
	source.Epoch = oldEpoch + 1
	newPart.Epoch = oldEpoch + 1

	// Propagate updates to Raft consensus via callback if registered
	if sm.OnSplitComplete != nil {
		if err = sm.OnSplitComplete(sourceID, newID, source.Epoch, newPart.Epoch); err != nil {
			return fmt.Errorf("propagate split to cluster: %w", err)
		}
	}

	slog.Info("Partition split completed successfully",
		"source", sourceID, "new", newID,
		"split_offset", splitOffset,
		"split_key", splitKey,
		"events_moved", totalMoved,
		"source_hw", sourceHW,
		"new_epoch", source.Epoch)

	return nil
}

// MergePartitions merges two partitions into one.
func (sm *SplitManager) MergePartitions(sourceID, targetID int32) error {
	return fmt.Errorf("partition merge not yet implemented")
}

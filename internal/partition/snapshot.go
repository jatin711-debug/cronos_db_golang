package partition

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Snapshot represents a point-in-time snapshot of partition state.
// This enables fast recovery without replaying the entire WAL.
type Snapshot struct {
	PartitionID         int32            `json:"partition_id"`
	HighWatermark       int64            `json:"high_watermark"`
	LastScheduledOffset int64            `json:"last_scheduled_offset"`
	ConsumerOffsets     map[string]int64 `json:"consumer_offsets"` // groupID -> offset
	Timestamp           int64            `json:"timestamp"`
	Version             int              `json:"version"`
}

const snapshotVersion = 1
const snapshotFilename = "snapshot.json"

// SnapshotManager handles creating and loading snapshots for fast recovery.
type SnapshotManager struct {
	mu               sync.RWMutex
	dataDir          string
	partitionID      int32
	lastSnapshot     *Snapshot
	lastSnapshotTime time.Time
}

// NewSnapshotManager creates a snapshot manager for a partition.
func NewSnapshotManager(dataDir string, partitionID int32) *SnapshotManager {
	return &SnapshotManager{
		dataDir:     dataDir,
		partitionID: partitionID,
	}
}

// CreateSnapshot creates a new snapshot of partition state.
func (sm *SnapshotManager) CreateSnapshot(partition *Partition) error {
	if partition == nil || partition.Wal == nil {
		return fmt.Errorf("partition or WAL is nil")
	}

	snapshot := &Snapshot{
		PartitionID:         partition.ID,
		HighWatermark:       partition.Wal.GetHighWatermark(),
		LastScheduledOffset: partition.Wal.GetLastOffset(),
		ConsumerOffsets:     make(map[string]int64),
		Timestamp:           time.Now().UnixMilli(),
		Version:             snapshotVersion,
	}

	// Capture consumer group offsets
	if partition.ConsumerGroup != nil {
		groups := partition.ConsumerGroup.ListGroups()
		for _, group := range groups {
			if offset, ok := group.CommittedOffsets[partition.ID]; ok {
				snapshot.ConsumerOffsets[group.GroupID] = offset
			}
		}
	}

	// Write atomically
	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}

	snapshotPath := filepath.Join(sm.dataDir, snapshotFilename)
	tmpPath := snapshotPath + ".tmp"

	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("write snapshot temp: %w", err)
	}

	if err := os.Rename(tmpPath, snapshotPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename snapshot: %w", err)
	}

	sm.mu.Lock()
	sm.lastSnapshot = snapshot
	sm.lastSnapshotTime = time.Now()
	sm.mu.Unlock()

	log.Printf("[Partition %d] Snapshot created: HWM=%d, offsets=%d, time=%s",
		partition.ID, snapshot.HighWatermark, len(snapshot.ConsumerOffsets),
		time.Now().Format(time.RFC3339))

	return nil
}

// LoadSnapshot loads the most recent snapshot if available.
func (sm *SnapshotManager) LoadSnapshot() (*Snapshot, error) {
	sm.mu.RLock()
	if sm.lastSnapshot != nil {
		sm.mu.RUnlock()
		return sm.lastSnapshot, nil
	}
	sm.mu.RUnlock()

	snapshotPath := filepath.Join(sm.dataDir, snapshotFilename)
	data, err := os.ReadFile(snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No snapshot exists
		}
		return nil, fmt.Errorf("read snapshot: %w", err)
	}

	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("parse snapshot: %w", err)
	}

	if snapshot.Version != snapshotVersion {
		return nil, fmt.Errorf("snapshot version mismatch: got %d, want %d", snapshot.Version, snapshotVersion)
	}

	sm.mu.Lock()
	sm.lastSnapshot = &snapshot
	sm.mu.Unlock()

	log.Printf("[Partition %d] Snapshot loaded: HWM=%d, offsets=%d, age=%s",
		snapshot.PartitionID, snapshot.HighWatermark, len(snapshot.ConsumerOffsets),
		time.Since(time.UnixMilli(snapshot.Timestamp)).String())

	return &snapshot, nil
}

// HasSnapshot returns true if a snapshot exists.
func (sm *SnapshotManager) HasSnapshot() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if sm.lastSnapshot != nil {
		return true
	}

	snapshotPath := filepath.Join(sm.dataDir, snapshotFilename)
	_, err := os.Stat(snapshotPath)
	return err == nil
}

// GetLastSnapshotTime returns when the last snapshot was created.
func (sm *SnapshotManager) GetLastSnapshotTime() time.Time {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.lastSnapshotTime
}

// SnapshotStats returns snapshot statistics for metrics.
func (sm *SnapshotManager) SnapshotStats() map[string]interface{} {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	stats := map[string]interface{}{
		"has_snapshot": sm.lastSnapshot != nil,
	}

	if sm.lastSnapshot != nil {
		stats["high_watermark"] = sm.lastSnapshot.HighWatermark
		stats["last_scheduled_offset"] = sm.lastSnapshot.LastScheduledOffset
		stats["consumer_offsets"] = len(sm.lastSnapshot.ConsumerOffsets)
		stats["snapshot_age_ms"] = time.Since(time.UnixMilli(sm.lastSnapshot.Timestamp)).Milliseconds()
	}

	return stats
}

// StartPeriodicSnapshots runs a loop that creates snapshots periodically. It
// should be invoked in a background goroutine (e.g. via utils.GoSafe).
func (sm *SnapshotManager) StartPeriodicSnapshots(partition *Partition, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := sm.CreateSnapshot(partition); err != nil {
				log.Printf("[Partition %d] Periodic snapshot failed: %v", partition.ID, err)
			}
		case <-partition.deliveryQuit:
			// Create final snapshot before shutdown
			if err := sm.CreateSnapshot(partition); err != nil {
				log.Printf("[Partition %d] Final snapshot failed: %v", partition.ID, err)
			}
			return
		}
	}
}

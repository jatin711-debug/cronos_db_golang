package partition

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
)

// Snapshot is a point-in-time capture of partition recovery state.
// Loading a snapshot lets startup skip full WAL timer replay up to
// LastScheduledOffset.
type Snapshot struct {
	// PartitionID identifies the partition this snapshot belongs to.
	PartitionID int32 `json:"partition_id"`
	// HighWatermark is the WAL high watermark at snapshot time.
	HighWatermark int64 `json:"high_watermark"`
	// LastScheduledOffset is the last WAL offset whose timer was known scheduled.
	LastScheduledOffset int64 `json:"last_scheduled_offset"`
	// ConsumerOffsets maps consumer group ID to committed offset for this partition.
	ConsumerOffsets map[string]int64 `json:"consumer_offsets"`
	// Timestamp is when the snapshot was taken (Unix milliseconds).
	Timestamp int64 `json:"timestamp"`
	// Version is the on-disk snapshot format version (must match snapshotVersion).
	Version int `json:"version"`
}

const snapshotVersion = 1
const snapshotFilename = "snapshot.json"

// SnapshotManager creates and loads partition recovery snapshots under dataDir.
type SnapshotManager struct {
	mu               sync.RWMutex
	dataDir          string    // partition data directory
	partitionID      int32     // owning partition
	lastSnapshot     *Snapshot // cached last loaded/created snapshot
	lastSnapshotTime time.Time // wall time of last successful CreateSnapshot
}

// NewSnapshotManager creates a snapshot manager for the given partition dataDir.
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

	// Write atomically with fsync for crash safety.
	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}

	snapshotPath := filepath.Join(sm.dataDir, snapshotFilename)

	if err := utils.AtomicWriteFile(snapshotPath, data, 0644); err != nil {
		return fmt.Errorf("write snapshot: %w", err)
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

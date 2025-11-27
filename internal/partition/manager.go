package partition

import (
	"fmt"
	"sync"
	"time"

	"cronos_db/pkg/types"
	"cronos_db/internal/scheduler"
	"cronos_db/internal/storage"
	"cronos_db/internal/consumer"
	"cronos_db/internal/dedup"
	"cronos_db/internal/delivery"
)

// Partition represents a data partition
type Partition struct {
	ID             int32
	Topic          string
	DataDir        string
	Wal            *storage.WAL
	Scheduler      *scheduler.Scheduler
	ConsumerGroup  *consumer.GroupManager
	DedupStore     *dedup.Manager
	Dispatcher     *delivery.Dispatcher
	Worker         *delivery.Worker
	Leader         bool
	CreatedTS      time.Time
	UpdatedTS      time.Time
}

// PartitionManager manages all partitions
type PartitionManager struct {
	mu         sync.RWMutex
	partitions map[int32]*Partition
	nodeID     string
	config     *types.Config
}

// NewPartitionManager creates a new partition manager
func NewPartitionManager(nodeID string, config *types.Config) *PartitionManager {
	return &PartitionManager{
		partitions: make(map[int32]*Partition),
		nodeID:     nodeID,
		config:     config,
	}
}

// CreatePartition creates a new partition
func (pm *PartitionManager) CreatePartition(partitionID int32, topic string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Check if partition already exists
	if _, exists := pm.partitions[partitionID]; exists {
		return fmt.Errorf("partition %d already exists", partitionID)
	}

	// Create data directory
	dataDir := fmt.Sprintf("%s/partitions/%d", pm.config.DataDir, partitionID)

	// Create WAL
	walConfig := &storage.WALConfig{
		SegmentSizeBytes: pm.config.SegmentSizeBytes,
		IndexInterval:    pm.config.IndexInterval,
		FsyncMode:        pm.config.FsyncMode,
		FlushIntervalMS:  pm.config.FlushIntervalMS,
	}
	wal, err := storage.NewWAL(dataDir, partitionID, walConfig)
	if err != nil {
		return fmt.Errorf("create WAL: %w", err)
	}

	// Create scheduler
	scheduler, err := scheduler.NewScheduler(dataDir, partitionID, int32(pm.config.TickMS), int32(pm.config.WheelSize))
	if err != nil {
		return fmt.Errorf("create scheduler: %w", err)
	}

	// Create consumer group manager
	consumerGroup := consumer.NewGroupManager()

	// Create dedup store
	dedupStore, err := dedup.NewPebbleStore(dataDir, partitionID, int32(pm.config.DedupTTLHours))
	if err != nil {
		return fmt.Errorf("create dedup store: %w", err)
	}
	dedupManager := dedup.NewManager(dedupStore)

	// Create dispatcher
	dispatcherConfig := delivery.DefaultConfig()
	dispatcher := delivery.NewDispatcher(dispatcherConfig)

	// Create worker
	worker := delivery.NewWorker(dispatcher, 100)

	// Create partition
	partition := &Partition{
		ID:            partitionID,
		Topic:         topic,
		DataDir:       dataDir,
		Wal:           wal,
		Scheduler:     scheduler,
		ConsumerGroup: consumerGroup,
		DedupStore:    dedupManager,
		Dispatcher:    dispatcher,
		Worker:        worker,
		Leader:        false,
		CreatedTS:     time.Now(),
		UpdatedTS:     time.Now(),
	}

	pm.partitions[partitionID] = partition
	return nil
}

// GetPartition gets a partition by ID
func (pm *PartitionManager) GetPartition(partitionID int32) (*types.Partition, error) {
	partition, err := pm.GetInternalPartition(partitionID)
	if err != nil {
		return nil, err
	}

	return &types.Partition{
		ID:            partition.ID,
		Topic:         partition.Topic,
		NextOffset:    0, // Would get from WAL
		HighWatermark: 0, // Would get from WAL
		Active:        true,
		CreatedTS:     partition.CreatedTS.UnixMilli(),
		UpdatedTS:     partition.UpdatedTS.UnixMilli(),
	}, nil
}

// GetInternalPartition gets the internal partition object
func (pm *PartitionManager) GetInternalPartition(partitionID int32) (*Partition, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	partition, exists := pm.partitions[partitionID]
	if !exists {
		return nil, types.ErrPartitionNotFound
	}

	return partition, nil
}

// GetPartitionForTopic gets partition for a topic using consistent hashing
func (pm *PartitionManager) GetPartitionForTopic(topic string) (*types.Partition, error) {
	// TODO: Implement consistent hashing
	// For now, return first partition
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if len(pm.partitions) == 0 {
		return nil, types.ErrPartitionNotFound
	}

	for _, partition := range pm.partitions {
		if partition.Topic == topic {
			return &types.Partition{
				ID:            partition.ID,
				Topic:         partition.Topic,
				NextOffset:    0,
				HighWatermark: 0,
				Active:        true,
				CreatedTS:     partition.CreatedTS.UnixMilli(),
				UpdatedTS:     partition.UpdatedTS.UnixMilli(),
			}, nil
		}
	}

	// Return any partition if topic not found
	for _, partition := range pm.partitions {
		return &types.Partition{
			ID:            partition.ID,
			Topic:         partition.Topic,
			NextOffset:    0,
			HighWatermark: 0,
			Active:        true,
			CreatedTS:     partition.CreatedTS.UnixMilli(),
			UpdatedTS:     partition.UpdatedTS.UnixMilli(),
		}, nil
	}

	return nil, types.ErrPartitionNotFound
}

// ListPartitions lists all partitions
func (pm *PartitionManager) ListPartitions() []*Partition {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	partitions := make([]*Partition, 0, len(pm.partitions))
	for _, partition := range pm.partitions {
		partitions = append(partitions, partition)
	}

	return partitions
}

// StartPartition starts a partition
func (pm *PartitionManager) StartPartition(partitionID int32) error {
	partition, err := pm.GetInternalPartition(partitionID)
	if err != nil {
		return err
	}

	// Start scheduler
	partition.Scheduler.Start()

	// Start worker
	partition.Worker.Start()

	// Start delivery loop (poll scheduler for ready events and dispatch them)
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond) // Poll every 50ms
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Get ready events from scheduler
				readyEvents := partition.Scheduler.GetReadyEvents()
				if readyEvents != nil && len(readyEvents) > 0 {
					// Dispatch each ready event
					for _, event := range readyEvents {
						partition.Dispatcher.Dispatch(event)
					}
				}
			}
		}
	}()

	return nil
}

// StopPartition stops a partition
func (pm *PartitionManager) StopPartition(partitionID int32) error {
	partition, err := pm.GetInternalPartition(partitionID)
	if err != nil {
		return err
	}

	// Stop scheduler
	partition.Scheduler.Stop()

	// Close WAL
	if err := partition.Wal.Close(); err != nil {
		return fmt.Errorf("close WAL: %w", err)
	}

	return nil
}

// GetStats returns partition manager statistics
func (pm *PartitionManager) GetStats() *PartitionManagerStats {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	return &PartitionManagerStats{
		TotalPartitions: int64(len(pm.partitions)),
		LeaderPartitions: pm.countLeaderPartitions(),
		ActivePartitions: int64(len(pm.partitions)), // Simplified
	}
}

// countLeaderPartitions counts leader partitions
func (pm *PartitionManager) countLeaderPartitions() int64 {
	var count int64
	for _, partition := range pm.partitions {
		if partition.Leader {
			count++
		}
	}
	return count
}

// PartitionManagerStats represents partition manager statistics
type PartitionManagerStats struct {
	TotalPartitions  int64
	LeaderPartitions int64
	ActivePartitions int64
}

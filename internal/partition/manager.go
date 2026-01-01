package partition

import (
	"fmt"
	"hash/fnv"
	"log"
	"sync"
	"time"

	"cronos_db/internal/consumer"
	"cronos_db/internal/dedup"
	"cronos_db/internal/delivery"
	"cronos_db/internal/scheduler"
	"cronos_db/internal/storage"
	"cronos_db/pkg/types"
)

// Partition represents a data partition
type Partition struct {
	ID            int32
	Topic         string
	DataDir       string
	Wal           *storage.WAL
	Scheduler     *scheduler.Scheduler
	ConsumerGroup *consumer.GroupManager
	DedupStore    *dedup.Manager
	Dispatcher    *delivery.Dispatcher
	Worker        *delivery.Worker
	Leader        bool
	CreatedTS     time.Time
	UpdatedTS     time.Time
	deliveryQuit  chan struct{} // Quit channel for delivery goroutine
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

// createPartitionLocked creates a new partition (assumes lock is held)
func (pm *PartitionManager) createPartitionLocked(partitionID int32, topic string) error {
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
	// Clean up WAL on any subsequent failure
	defer func() {
		if err != nil {
			wal.Close()
		}
	}()

	// Create scheduler
	scheduler, err := scheduler.NewScheduler(dataDir, partitionID, int32(pm.config.TickMS), int32(pm.config.WheelSize))
	if err != nil {
		return fmt.Errorf("create scheduler: %w", err)
	}

	// Create dedup store with bloom filter for high performance
	// Expected items per partition with 1% false positive rate
	// Uses ~10MB memory per partition for bloom filter (at 10M items)
	dedupStore, err := dedup.NewBloomPebbleStore(dataDir, partitionID, int32(pm.config.DedupTTLHours), pm.config.BloomCapacity, 0.01)
	if err != nil {
		return fmt.Errorf("create dedup store: %w", err)
	}
	dedupManager := dedup.NewManager(dedupStore)

	// Create offset store for persistent consumer offsets
	offsetStore, err := consumer.NewOffsetStore(dataDir, partitionID)
	if err != nil {
		return fmt.Errorf("create offset store: %w", err)
	}

	// Create consumer group manager with persistent offset store
	consumerGroup := consumer.NewGroupManagerWithStore(offsetStore)

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
		deliveryQuit:  make(chan struct{}),
	}

	pm.partitions[partitionID] = partition
	return nil
}

// CreatePartition creates a new partition (public method with lock management)
func (pm *PartitionManager) CreatePartition(partitionID int32, topic string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	return pm.createPartitionLocked(partitionID, topic)
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
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Check if partition already exists for this topic
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

	// Auto-create partition for unknown topic
	partitionID := int32(len(pm.partitions))
	if err := pm.createPartitionLocked(partitionID, topic); err != nil {
		return nil, fmt.Errorf("auto-create partition: %w", err)
	}

	// Start the newly created partition synchronously (before returning)
	partition := pm.partitions[partitionID]
	if err := pm.startPartitionInternal(partition); err != nil {
		log.Printf("Failed to start auto-created partition %d: %v", partitionID, err)
		// Continue anyway - partition is created but may not deliver
	}

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

// GetPartitionForKey gets partition for a key using hash-based distribution
// This provides Kafka-like key-based partitioning for even distribution
func (pm *PartitionManager) GetPartitionForKey(key string) (*types.Partition, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if len(pm.partitions) == 0 {
		return nil, fmt.Errorf("no partitions available")
	}

	// Hash the key to determine partition
	h := fnv.New32a()
	h.Write([]byte(key))
	hash := h.Sum32()

	// Select partition based on hash
	partitionID := int32(hash % uint32(len(pm.partitions)))

	partition, exists := pm.partitions[partitionID]
	if !exists {
		return nil, fmt.Errorf("partition %d not found", partitionID)
	}

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

// startPartitionLocked starts a partition (assumes lock is held)
func (pm *PartitionManager) startPartitionLocked(partitionID int32) error {
	partition, exists := pm.partitions[partitionID]
	if !exists {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	return pm.startPartitionInternal(partition)
}

// startPartitionInternal starts a partition's background workers
func (pm *PartitionManager) startPartitionInternal(partition *Partition) error {
	// Start scheduler
	partition.Scheduler.Start()

	// Start worker
	partition.Worker.Start()

	// Get poll interval from config (default 10ms for better responsiveness)
	pollInterval := time.Duration(pm.config.DeliveryPollMS) * time.Millisecond
	if pollInterval == 0 {
		pollInterval = 10 * time.Millisecond
	}

	// Create dispatch channel for parallel processing
	dispatchCh := make(chan *types.Event, 10000)

	// Start multiple dispatch workers (parallel delivery)
	numWorkers := 4
	for i := 0; i < numWorkers; i++ {
		go func() {
			for {
				select {
				case event, ok := <-dispatchCh:
					if !ok {
						return
					}
					if err := partition.Dispatcher.Dispatch(event); err != nil {
						log.Printf("Dispatch failed: %v", err)
					}
				case <-partition.deliveryQuit:
					return
				}
			}
		}()
	}

	// Start delivery loop (poll scheduler for ready events and dispatch them)
	go func() {
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Get ready events from scheduler
				readyEvents := partition.Scheduler.GetReadyEvents()
				if len(readyEvents) > 0 {
					// Send to dispatch workers
					for _, event := range readyEvents {
						select {
						case dispatchCh <- event:
						default:
							// Channel full - dispatch directly
							if err := partition.Dispatcher.Dispatch(event); err != nil {
								log.Printf("Dispatch failed: %v", err)
							}
						}
					}
				}
			case <-partition.deliveryQuit:
				close(dispatchCh)
				return
			}
		}
	}()

	return nil
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
	pm.mu.Lock()
	defer pm.mu.Unlock()

	return pm.startPartitionLocked(partitionID)
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
		TotalPartitions:  int64(len(pm.partitions)),
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

package types

// PartitionManager is a minimal interface for resolving partitions by topic or ID.
// Server handlers and tests may depend on this instead of the concrete manager.
type PartitionManager interface {
	// GetPartitionForTopic returns a partition responsible for the given topic.
	GetPartitionForTopic(topic string) (*Partition, error)
	// GetPartition returns the partition with the given ID.
	GetPartition(partitionID int32) (*Partition, error)
}

package types

// PartitionManager interface
type PartitionManager interface {
	GetPartitionForTopic(topic string) (*Partition, error)
	GetPartition(partitionID int32) (*Partition, error)
}

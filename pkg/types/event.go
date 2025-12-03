package types

import "time"

// Config represents system configuration
type Config struct {
	NodeID               string
	DataDir              string
	GPRCAddress          string
	HTTPAddress          string
	PartitionCount       int
	ReplicationFactor    int
	SegmentSizeBytes     int64
	IndexInterval        int64
	FsyncMode            string
	FlushIntervalMS      int32
	TickMS               int
	WheelSize            int
	DefaultAckTimeout    time.Duration
	MaxRetries           int
	RetryBackoff         time.Duration
	MaxDeliveryCredits   int
	DeliveryPollMS       int
	DedupTTLHours        int
	ReplicationBatchSize int
	ReplicationTimeout   time.Duration
	RaftDir              string
	RaftJoinAddr         string
	StatsPrintInterval   time.Duration
	CheckpointInterval   time.Duration
}

// Partition represents a data partition
type Partition struct {
	ID            int32
	Topic         string
	NextOffset    int64
	HighWatermark int64
	Active        bool
	CreatedTS     int64
	UpdatedTS     int64
}

// ConsumerGroup represents a consumer group
type ConsumerGroup struct {
	GroupID          string
	Topic            string
	Partitions       []int32
	CommittedOffsets map[int32]int64
	MemberOffsets    map[string]int64
	Members          map[string]*ConsumerMember
	CreatedTS        int64
	UpdatedTS        int64
}

// ConsumerMember represents a consumer group member
type ConsumerMember struct {
	MemberID          string
	Address           string
	AssignedPartition int32
	Active            bool
	LastSeenTS        int64
	ConnectedTS       int64
}

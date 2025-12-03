package config

import "time"

// Default configuration values
const (
	// Node configuration
	DefaultNodeID            = ""
	DefaultDataDir           = "./data"
	DefaultGRPCAddress       = ":9000"
	DefaultHTTPAddress       = ":8080"
	DefaultPartitionCount    = 1
	DefaultReplicationFactor = 1

	// WAL configuration
	DefaultSegmentSizeBytes = 536870912 // 512MB
	DefaultIndexInterval    = 1000
	DefaultFsyncMode        = "periodic"
	DefaultFlushIntervalMS  = 1000

	// Scheduler configuration
	DefaultTickMS    = 100
	DefaultWheelSize = 60

	// Delivery configuration
	DefaultMaxRetries         = 5
	DefaultMaxDeliveryCredits = 1000
	DefaultDeliveryPollMS     = 50

	// Dedup configuration
	DefaultDedupTTLHours = 168 // 7 days

	// Replication configuration
	DefaultReplicationBatchSize = 100

	// Raft configuration
	DefaultRaftDir      = "./raft"
	DefaultRaftJoinAddr = ""

	// Stats configuration
	DefaultStatsPrintInterval = 30 * time.Second

	// Checkpoint configuration
	DefaultCheckpointInterval = 10 * time.Second
)

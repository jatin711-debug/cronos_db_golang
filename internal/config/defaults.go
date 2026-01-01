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
	DefaultDedupTTLHours = 168         // 7 days
	DefaultBloomCapacity = 100_000_000 // 100M items (increased from 10M)

	// Replication configuration
	DefaultReplicationBatchSize = 100

	// Raft configuration
	DefaultRaftDir      = "./raft"
	DefaultRaftJoinAddr = ""

	// Stats configuration
	DefaultStatsPrintInterval = 30 * time.Second

	// Checkpoint configuration
	DefaultCheckpointInterval = 10 * time.Second

	// Cluster configuration
	DefaultClusterEnabled    = false
	DefaultClusterGossipAddr = ":7946"
	DefaultClusterGRPCAddr   = ":7947"
	DefaultClusterRaftAddr   = ":7948"
	DefaultVirtualNodes      = 150
	DefaultHeartbeatInterval = 1 * time.Second
	DefaultFailureTimeout    = 5 * time.Second
	DefaultSuspectTimeout    = 3 * time.Second
)

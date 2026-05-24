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

	// Scheduler cold store configuration
	DefaultHotWindowMinutes = 60 // 1 hour hot window

	// Admission control configuration (0 = disabled)
	DefaultMaxReadyQueueSize       = 1_000_000
	DefaultMaxTimingWheelSize      = 10_000_000
	DefaultMaxInFlightPerPartition = 500_000

	// Circuit breaker configuration
	DefaultCircuitBreakerFailureThreshold = 0.5  // 50% failure rate trips breaker
	DefaultCircuitBreakerOpenDurationMs   = 30000 // 30 seconds
	DefaultCircuitBreakerMinAttempts      = 10

	// Clock skew detection
	DefaultClockSkewThresholdMs = 5000 // 5 seconds

	// Cluster configuration
	DefaultClusterEnabled    = false
	DefaultClusterGossipAddr = ":7946"
	DefaultClusterGRPCAddr   = ":7947"
	DefaultClusterRaftAddr   = ":7948"
	DefaultVirtualNodes      = 150
	DefaultHeartbeatInterval = 1 * time.Second
	DefaultFailureTimeout    = 5 * time.Second
	DefaultSuspectTimeout    = 3 * time.Second

	// Gossip backend
	DefaultUseMemberlist = false // Default to custom gossip for backward compatibility
)

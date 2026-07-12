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
	DefaultFsyncMode        = "batch"
	DefaultFlushIntervalMS  = 1000

	// Retention configuration
	DefaultRetentionMaxAgeHours = 168 // 7 days
	DefaultRetentionMaxSizeGB   = 0   // Disabled by default

	// Scheduler configuration. 10ms tick + 600 slots = 6s wheel span, giving
	// finer granularity for scheduled events without increasing CPU overhead.
	DefaultTickMS    = 10
	DefaultWheelSize = 600

	// Delivery configuration
	DefaultMaxRetries         = 5
	DefaultMaxDeliveryCredits = 1000
	DefaultDeliveryPollMS     = 50

	// Dedup configuration
	DefaultDedupTTLHours = 168         // 7 days
	DefaultBloomCapacity = 100_000_000 // 100M items (increased from 10M)

	// Replication configuration
	DefaultReplicationBatchSize = 100
	// DefaultMinInSyncReplicas is the minimum ISR size (including leader) for a
	// write to be acknowledged as durable. 1 preserves the historical behavior
	// (leader-only is sufficient). Raise it (e.g. RF=3, minISR=2) to refuse
	// writes when the cluster degrades below a majority.
	DefaultMinInSyncReplicas = 1

	// Raft configuration
	DefaultRaftDir      = "./raft"
	DefaultRaftJoinAddr = ""

	// Stats configuration
	DefaultStatsPrintInterval = 30 * time.Second

	// Checkpoint configuration
	DefaultCheckpointInterval = 10 * time.Second

	// Tracing configuration (kept conservative to protect throughput)
	DefaultTracingEnabled      = false
	DefaultTracingExporter     = "none"
	DefaultTracingOTLPEndpoint = "127.0.0.1:4317"
	DefaultTracingSampleRatio  = 0.01
	DefaultTracingInsecure     = true

	// Scheduler cold store configuration
	DefaultHotWindowMinutes = 60 // 1 hour hot window

	// Adaptive hydrator configuration
	DefaultHydratorMinIntervalMs = 5000   // 5 seconds minimum
	DefaultHydratorMaxIntervalMs = 300000 // 5 minutes maximum

	// Admission control configuration (0 = disabled)
	DefaultMaxReadyQueueSize       = 1_000_000
	DefaultMaxTimingWheelSize      = 10_000_000
	DefaultMaxInFlightPerPartition = 500_000

	// Circuit breaker configuration
	DefaultCircuitBreakerFailureThreshold = 0.5   // 50% failure rate trips breaker
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

	// Encryption at rest
	DefaultEncryptionEnabled = false
	DefaultEncryptionKeyFile = ""

	// Topic rate limiting (0 = disabled)
	DefaultTopicRateLimitPerSecond = 0.0
	DefaultTopicRateLimitBurst     = 0.0

	// Memory-based backpressure (0 = disabled)
	DefaultMaxMemoryUsagePercent = 0.0  // Disabled by default
	DefaultMemoryCheckIntervalMs = 5000 // 5 seconds

	// Ingest rate limiting per partition (0 = disabled)
	DefaultMaxIngestRatePerPartition = 0 // Unlimited by default
	DefaultIngestRateBurstSize       = 0
)

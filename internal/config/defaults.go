package config

import "time"

// Default configuration values applied by LoadConfig when flags and env are unset.
const (
	// DefaultNodeID is empty so operators must set a unique node id (or env).
	DefaultNodeID = ""
	// DefaultDataDir is the local data root for WAL, schemas, audit, and raft.
	DefaultDataDir = "./data"
	// DefaultGRPCAddress is the public client-facing gRPC listen address.
	DefaultGRPCAddress = ":9000"
	// DefaultHTTPAddress is the health/metrics/dashboard HTTP listen address.
	DefaultHTTPAddress = ":8080"
	// DefaultPartitionCount is the number of partitions in standalone mode.
	DefaultPartitionCount = 1
	// DefaultReplicationFactor is the number of replicas per partition.
	DefaultReplicationFactor = 1

	// DefaultSegmentSizeBytes is the WAL segment size before rotation (512MB).
	DefaultSegmentSizeBytes = 536870912 // 512MB
	// DefaultIndexInterval is how many events between sparse index entries.
	DefaultIndexInterval = 1000
	// DefaultFsyncMode is the WAL durability mode: every_event, batch, or periodic.
	DefaultFsyncMode = "batch"
	// DefaultFlushIntervalMS is the periodic flush interval when using batch/periodic fsync.
	DefaultFlushIntervalMS = 1000

	// DefaultRetentionMaxAgeHours is max WAL segment age before deletion (7 days).
	DefaultRetentionMaxAgeHours = 168 // 7 days
	// DefaultRetentionMaxSizeGB is max total WAL size in GB; 0 disables size retention.
	DefaultRetentionMaxSizeGB = 0 // Disabled by default

	// DefaultTickMS is the scheduler timing-wheel tick in milliseconds.
	// 10ms tick + 600 slots = 6s wheel span, giving finer granularity for
	// scheduled events without increasing CPU overhead.
	DefaultTickMS = 10
	// DefaultWheelSize is the number of slots in the hot timing wheel.
	DefaultWheelSize = 600

	// DefaultMaxRetries is the maximum delivery retry attempts per event.
	DefaultMaxRetries = 5
	// DefaultMaxDeliveryCredits is the default per-subscription credit budget.
	DefaultMaxDeliveryCredits = 1000
	// DefaultDeliveryPollMS is the delivery worker poll interval in milliseconds.
	DefaultDeliveryPollMS = 50

	// DefaultDedupTTLHours is how long dedup keys are retained (7 days).
	DefaultDedupTTLHours = 168 // 7 days
	// DefaultBloomCapacity is bloom filter capacity per partition (100M items).
	DefaultBloomCapacity = 100_000_000 // 100M items (increased from 10M)

	// DefaultReplicationBatchSize is events per replication batch.
	DefaultReplicationBatchSize = 100
	// DefaultMinInSyncReplicas is the minimum ISR size (including leader) for a
	// write to be acknowledged as durable. 1 preserves the historical behavior
	// (leader-only is sufficient). Raise it (e.g. RF=3, minISR=2) to refuse
	// writes when the cluster degrades below a majority.
	DefaultMinInSyncReplicas = 1
	// DefaultSnapshotCatchupThreshold is the replication lag (in events) above
	// which a follower will request a full segment snapshot instead of
	// incremental event-by-event catch-up.
	DefaultSnapshotCatchupThreshold = 10_000

	// DefaultRaftDir is the default Raft log/state directory.
	DefaultRaftDir = "./raft"
	// DefaultRaftJoinAddr is empty when the node is not joining an existing Raft group.
	DefaultRaftJoinAddr = ""

	// DefaultStatsPrintInterval is how often the process logs runtime stats.
	DefaultStatsPrintInterval = 30 * time.Second

	// DefaultCheckpointInterval is how often consumer/offset checkpoints are written.
	DefaultCheckpointInterval = 10 * time.Second

	// DefaultTracingEnabled disables OpenTelemetry by default to protect throughput.
	DefaultTracingEnabled = false
	// DefaultTracingExporter is "none", "stdout", or "otlp".
	DefaultTracingExporter = "none"
	// DefaultTracingOTLPEndpoint is the default OTLP gRPC collector address.
	DefaultTracingOTLPEndpoint = "127.0.0.1:4317"
	// DefaultTracingSampleRatio samples 1% of traces when tracing is enabled.
	DefaultTracingSampleRatio = 0.01
	// DefaultTracingInsecure uses plaintext OTLP (no TLS) by default.
	DefaultTracingInsecure = true

	// DefaultHotWindowMinutes is the scheduler hot window before cold-store spill (1 hour).
	DefaultHotWindowMinutes = 60 // 1 hour hot window

	// DefaultHydratorMinIntervalMs is the minimum cold-store hydrator scan interval.
	DefaultHydratorMinIntervalMs = 5000 // 5 seconds minimum
	// DefaultHydratorMaxIntervalMs is the maximum cold-store hydrator scan interval.
	DefaultHydratorMaxIntervalMs = 300000 // 5 minutes maximum

	// DefaultMaxReadyQueueSize is max ready-queue depth per partition (0 = disabled).
	DefaultMaxReadyQueueSize = 1_000_000
	// DefaultMaxTimingWheelSize is max active timers in the hot timing wheel.
	DefaultMaxTimingWheelSize = 10_000_000
	// DefaultMaxInFlightPerPartition is max in-flight deliveries per partition.
	DefaultMaxInFlightPerPartition = 500_000

	// DefaultCircuitBreakerFailureThreshold trips the breaker at 50% failure rate.
	DefaultCircuitBreakerFailureThreshold = 0.5 // 50% failure rate trips breaker
	// DefaultCircuitBreakerOpenDurationMs is how long the breaker stays open (30s).
	DefaultCircuitBreakerOpenDurationMs = 30000 // 30 seconds
	// DefaultCircuitBreakerMinAttempts is the min samples before evaluating the breaker.
	DefaultCircuitBreakerMinAttempts = 10

	// DefaultClockSkewThresholdMs is max allowed clock skew from leader (0 = disabled).
	DefaultClockSkewThresholdMs = 5000 // 5 seconds

	// DefaultClusterEnabled leaves cluster mode off for single-node deployments.
	DefaultClusterEnabled = false
	// DefaultClusterGossipAddr is the membership gossip listen address.
	DefaultClusterGossipAddr = ":7946"
	// DefaultClusterGRPCAddr is the internal cluster gRPC (replication/raft) address.
	DefaultClusterGRPCAddr = ":7947"
	// DefaultClusterRaftAddr is the Raft transport listen address.
	DefaultClusterRaftAddr = ":7948"
	// DefaultVirtualNodes is virtual nodes per physical node on the hash ring.
	// 150 is too sparse for low partition counts and can produce severe ownership
	// skew in small clusters. Keep the binary default aligned with benchmarks.
	DefaultVirtualNodes = 2048
	// DefaultHeartbeatInterval is the cluster membership heartbeat period.
	DefaultHeartbeatInterval = 1 * time.Second
	// DefaultFailureTimeout is how long without heartbeats before marking a node failed.
	DefaultFailureTimeout = 5 * time.Second
	// DefaultSuspectTimeout is how long a node stays suspect before failure.
	DefaultSuspectTimeout = 3 * time.Second

	// DefaultUseMemberlist uses custom TCP gossip unless HashiCorp Memberlist is enabled.
	DefaultUseMemberlist = false // Default to custom gossip for backward compatibility

	// DefaultEncryptionEnabled leaves WAL encryption at rest disabled.
	DefaultEncryptionEnabled = false
	// DefaultEncryptionKeyFile is empty until an operator supplies a 32-byte key path.
	DefaultEncryptionKeyFile = ""

	// DefaultTopicRateLimitPerSecond is per-subject per-topic events/sec; 0 disables.
	DefaultTopicRateLimitPerSecond = 0.0
	// DefaultTopicRateLimitBurst is the token-bucket burst for topic rate limits; 0 disables.
	DefaultTopicRateLimitBurst = 0.0

	// DefaultMaxMemoryUsagePercent rejects publishes above this process RSS ratio; 0 disables.
	DefaultMaxMemoryUsagePercent = 0.0 // Disabled by default
	// DefaultMemoryCheckIntervalMs is how often memory usage is sampled.
	DefaultMemoryCheckIntervalMs = 5000 // 5 seconds

	// DefaultMaxIngestRatePerPartition is max events/sec per partition; 0 is unlimited.
	DefaultMaxIngestRatePerPartition = 0 // Unlimited by default
	// DefaultIngestRateBurstSize is the ingest token-bucket burst size; 0 disables.
	DefaultIngestRateBurstSize = 0
)

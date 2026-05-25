package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// LoadConfig loads configuration from flags and environment
func LoadConfig() (*types.Config, error) {
	var config types.Config

	// Set defaults
	config.DataDir = DefaultDataDir
	config.GPRCAddress = DefaultGRPCAddress
	config.HTTPAddress = DefaultHTTPAddress
	config.PartitionCount = DefaultPartitionCount
	config.ReplicationFactor = DefaultReplicationFactor
	config.SegmentSizeBytes = DefaultSegmentSizeBytes
	config.IndexInterval = DefaultIndexInterval
	config.FsyncMode = DefaultFsyncMode
	config.FlushIntervalMS = DefaultFlushIntervalMS
	config.TickMS = DefaultTickMS
	config.WheelSize = DefaultWheelSize
	config.DefaultAckTimeout = 30 * time.Second
	config.MaxRetries = DefaultMaxRetries
	config.RetryBackoff = 1 * time.Second
	config.MaxDeliveryCredits = DefaultMaxDeliveryCredits
	config.DeliveryPollMS = DefaultDeliveryPollMS
	config.DedupTTLHours = DefaultDedupTTLHours
	config.BloomCapacity = DefaultBloomCapacity
	config.ReplicationBatchSize = DefaultReplicationBatchSize
	config.ReplicationTimeout = 10 * time.Second
	config.RaftDir = DefaultRaftDir
	config.StatsPrintInterval = DefaultStatsPrintInterval
	config.CheckpointInterval = DefaultCheckpointInterval
	config.TracingEnabled = DefaultTracingEnabled
	config.TracingExporter = DefaultTracingExporter
	config.TracingOTLPEndpoint = DefaultTracingOTLPEndpoint
	config.TracingSampleRatio = DefaultTracingSampleRatio
	config.TracingInsecure = DefaultTracingInsecure

	// Scheduler cold store defaults
	config.HotWindowMinutes = DefaultHotWindowMinutes

	// Admission control defaults
	config.MaxReadyQueueSize = DefaultMaxReadyQueueSize
	config.MaxTimingWheelSize = DefaultMaxTimingWheelSize
	config.MaxInFlightPerPartition = DefaultMaxInFlightPerPartition

	// Circuit breaker defaults
	config.CircuitBreakerFailureThreshold = DefaultCircuitBreakerFailureThreshold
	config.CircuitBreakerOpenDurationMs = DefaultCircuitBreakerOpenDurationMs
	config.CircuitBreakerMinAttempts = DefaultCircuitBreakerMinAttempts

	// Clock skew defaults
	config.ClockSkewThresholdMs = DefaultClockSkewThresholdMs

	// Gossip backend defaults
	config.UseMemberlist = DefaultUseMemberlist

	// Cluster defaults
	config.ClusterEnabled = DefaultClusterEnabled
	config.ClusterGossipAddr = DefaultClusterGossipAddr
	config.ClusterGRPCAddr = DefaultClusterGRPCAddr
	config.ClusterRaftAddr = DefaultClusterRaftAddr
	config.VirtualNodes = DefaultVirtualNodes
	config.HeartbeatInterval = DefaultHeartbeatInterval
	config.FailureTimeout = DefaultFailureTimeout
	config.SuspectTimeout = DefaultSuspectTimeout

	// Node configuration
	flag.StringVar(&config.NodeID, "node-id", DefaultNodeID, "Unique node ID")
	flag.StringVar(&config.DataDir, "data-dir", DefaultDataDir, "Data directory")
	flag.StringVar(&config.GPRCAddress, "grpc-addr", DefaultGRPCAddress, "gRPC address")
	flag.StringVar(&config.HTTPAddress, "http-addr", DefaultHTTPAddress, "HTTP address for health checks")
	flag.IntVar(&config.PartitionCount, "partition-count", DefaultPartitionCount, "Number of partitions")
	flag.IntVar(&config.ReplicationFactor, "replication-factor", DefaultReplicationFactor, "Replication factor")

	// WAL configuration
	flag.Int64Var(&config.SegmentSizeBytes, "segment-size", DefaultSegmentSizeBytes, "Segment size in bytes (512MB)")
	flag.Int64Var(&config.IndexInterval, "index-interval", DefaultIndexInterval, "Index interval (events)")
	flag.StringVar(&config.FsyncMode, "fsync-mode", DefaultFsyncMode, "fsync mode: every_event, batch, periodic")
	var flushInterval int
	flag.IntVar(&flushInterval, "flush-interval", DefaultFlushIntervalMS, "Flush interval in milliseconds")

	// Scheduler configuration
	flag.IntVar(&config.TickMS, "tick-ms", DefaultTickMS, "Scheduler tick duration in milliseconds")
	flag.IntVar(&config.WheelSize, "wheel-size", DefaultWheelSize, "Timing wheel size")
	flag.IntVar(&config.HotWindowMinutes, "hot-window-minutes", DefaultHotWindowMinutes, "Hot window in minutes for cold store (0 = disable)")
	flag.IntVar(&config.HydratorMinIntervalMs, "hydrator-min-interval", DefaultHydratorMinIntervalMs, "Minimum hydrator scan interval in ms")
	flag.IntVar(&config.HydratorMaxIntervalMs, "hydrator-max-interval", DefaultHydratorMaxIntervalMs, "Maximum hydrator scan interval in ms")

	// Admission control configuration
	flag.Int64Var(&config.MaxReadyQueueSize, "max-ready-queue", DefaultMaxReadyQueueSize, "Max ready queue depth per partition")
	flag.Int64Var(&config.MaxTimingWheelSize, "max-timing-wheel-size", DefaultMaxTimingWheelSize, "Max active timers in hot timing wheel")
	flag.Int64Var(&config.MaxInFlightPerPartition, "max-in-flight", DefaultMaxInFlightPerPartition, "Max in-flight deliveries per partition")

	// Delivery configuration
	flag.DurationVar(&config.DefaultAckTimeout, "ack-timeout", 30*time.Second, "Default ack timeout")
	flag.IntVar(&config.MaxRetries, "max-retries", DefaultMaxRetries, "Maximum delivery retries")
	flag.DurationVar(&config.RetryBackoff, "retry-backoff", 1*time.Second, "Retry backoff")
	flag.IntVar(&config.MaxDeliveryCredits, "max-credits", DefaultMaxDeliveryCredits, "Maximum delivery credits")
	flag.Float64Var(&config.CircuitBreakerFailureThreshold, "cb-failure-threshold", DefaultCircuitBreakerFailureThreshold, "Circuit breaker failure rate to trip (0.0-1.0)")
	flag.Int64Var(&config.CircuitBreakerMinAttempts, "cb-min-attempts", DefaultCircuitBreakerMinAttempts, "Min attempts before circuit breaker evaluates")
	flag.Int64Var(&config.CircuitBreakerOpenDurationMs, "cb-open-duration-ms", DefaultCircuitBreakerOpenDurationMs, "Circuit breaker open duration in milliseconds")

	// Dedup configuration
	flag.IntVar(&config.DedupTTLHours, "dedup-ttl", DefaultDedupTTLHours, "Deduplication TTL in hours (7 days)")
	flag.Uint64Var(&config.BloomCapacity, "bloom-capacity", DefaultBloomCapacity, "Bloom filter capacity per partition")

	// Replication configuration
	flag.IntVar(&config.ReplicationBatchSize, "replication-batch", DefaultReplicationBatchSize, "Replication batch size")
	flag.DurationVar(&config.ReplicationTimeout, "replication-timeout", 10*time.Second, "Replication timeout")

	// Raft configuration
	flag.StringVar(&config.RaftDir, "raft-dir", DefaultRaftDir, "Raft data directory")
	flag.StringVar(&config.RaftJoinAddr, "raft-join", DefaultRaftJoinAddr, "Raft cluster join address")

	// Cluster configuration
	flag.BoolVar(&config.ClusterEnabled, "cluster", DefaultClusterEnabled, "Enable cluster mode")
	flag.StringVar(&config.ClusterGossipAddr, "cluster-gossip-addr", DefaultClusterGossipAddr, "Cluster gossip UDP address")
	flag.StringVar(&config.ClusterGRPCAddr, "cluster-grpc-addr", DefaultClusterGRPCAddr, "Cluster gRPC address")
	flag.StringVar(&config.ClusterRaftAddr, "cluster-raft-addr", DefaultClusterRaftAddr, "Cluster Raft address")
	flag.IntVar(&config.VirtualNodes, "virtual-nodes", DefaultVirtualNodes, "Virtual nodes per physical node")
	flag.DurationVar(&config.HeartbeatInterval, "heartbeat-interval", DefaultHeartbeatInterval, "Cluster heartbeat interval")
	flag.DurationVar(&config.FailureTimeout, "failure-timeout", DefaultFailureTimeout, "Node failure detection timeout")
	flag.DurationVar(&config.SuspectTimeout, "suspect-timeout", DefaultSuspectTimeout, "Node suspect timeout")
	flag.BoolVar(&config.UseMemberlist, "use-memberlist", DefaultUseMemberlist, "Use HashiCorp Memberlist (SWIM) instead of custom TCP gossip")
	flag.Int64Var(&config.ClockSkewThresholdMs, "clock-skew-threshold-ms", DefaultClockSkewThresholdMs, "Max allowed clock skew from leader in ms (0 = disabled)")

	var clusterSeeds string
	flag.StringVar(&clusterSeeds, "cluster-seeds", "", "Comma-separated list of seed node addresses")

	// Tracing configuration
	flag.BoolVar(&config.TracingEnabled, "tracing-enabled", DefaultTracingEnabled, "Enable OpenTelemetry tracing")
	flag.StringVar(&config.TracingExporter, "tracing-exporter", DefaultTracingExporter, "Tracing exporter: none, stdout, otlp")
	flag.StringVar(&config.TracingOTLPEndpoint, "tracing-otlp-endpoint", DefaultTracingOTLPEndpoint, "OTLP gRPC endpoint (host:port)")
	flag.Float64Var(&config.TracingSampleRatio, "tracing-sample-ratio", DefaultTracingSampleRatio, "Tracing sample ratio from 0.0 to 1.0")
	flag.BoolVar(&config.TracingInsecure, "tracing-insecure", DefaultTracingInsecure, "Use insecure OTLP connection (no TLS)")

	flag.Parse()
	config.FlushIntervalMS = int32(flushInterval)

	// Parse cluster seeds
	if clusterSeeds != "" {
		config.ClusterSeeds = strings.Split(clusterSeeds, ",")
		for i, seed := range config.ClusterSeeds {
			config.ClusterSeeds[i] = strings.TrimSpace(seed)
		}
	}

	// Environment variable overrides
	if nodeID := os.Getenv("CRONOS_NODE_ID"); nodeID != "" && config.NodeID == DefaultNodeID {
		config.NodeID = nodeID
	}
	if dataDir := os.Getenv("CRONOS_DATA_DIR"); dataDir != "" && config.DataDir == DefaultDataDir {
		config.DataDir = dataDir
	}
	if grpcAddr := os.Getenv("CRONOS_GRPC_ADDR"); grpcAddr != "" && config.GPRCAddress == DefaultGRPCAddress {
		config.GPRCAddress = grpcAddr
	}
	if httpAddr := os.Getenv("CRONOS_HTTP_ADDR"); httpAddr != "" && config.HTTPAddress == DefaultHTTPAddress {
		config.HTTPAddress = httpAddr
	}
	if clusterEnabled := os.Getenv("CRONOS_CLUSTER"); clusterEnabled == "true" {
		config.ClusterEnabled = true
	}
	if seeds := os.Getenv("CRONOS_CLUSTER_SEEDS"); seeds != "" && len(config.ClusterSeeds) == 0 {
		config.ClusterSeeds = strings.Split(seeds, ",")
		for i, seed := range config.ClusterSeeds {
			config.ClusterSeeds[i] = strings.TrimSpace(seed)
		}
	}
	if tracingEnabled := os.Getenv("CRONOS_TRACING_ENABLED"); tracingEnabled != "" {
		if parsed, err := strconv.ParseBool(tracingEnabled); err == nil {
			config.TracingEnabled = parsed
		}
	}
	if tracingExporter := os.Getenv("CRONOS_TRACING_EXPORTER"); tracingExporter != "" {
		config.TracingExporter = strings.TrimSpace(tracingExporter)
	}
	if tracingEndpoint := os.Getenv("CRONOS_TRACING_OTLP_ENDPOINT"); tracingEndpoint != "" {
		config.TracingOTLPEndpoint = strings.TrimSpace(tracingEndpoint)
	}
	if tracingRatio := os.Getenv("CRONOS_TRACING_SAMPLE_RATIO"); tracingRatio != "" {
		if parsed, err := strconv.ParseFloat(tracingRatio, 64); err == nil {
			config.TracingSampleRatio = parsed
		}
	}
	if tracingInsecure := os.Getenv("CRONOS_TRACING_INSECURE"); tracingInsecure != "" {
		if parsed, err := strconv.ParseBool(tracingInsecure); err == nil {
			config.TracingInsecure = parsed
		}
	}

	// Validate required configuration
	if err := ValidateConfig(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

// ValidateConfig validates the configuration
func ValidateConfig(c *types.Config) error {
	if c.NodeID == "" {
		return fmt.Errorf("node-id is required")
	}
	if c.PartitionCount <= 0 {
		return fmt.Errorf("partition-count must be > 0")
	}
	if c.ReplicationFactor <= 0 {
		return fmt.Errorf("replication-factor must be > 0")
	}
	if c.DataDir == "" {
		return fmt.Errorf("data-dir is required")
	}
	if c.GPRCAddress == "" {
		return fmt.Errorf("grpc-addr is required")
	}
	if c.TracingSampleRatio < 0 || c.TracingSampleRatio > 1 {
		return fmt.Errorf("tracing-sample-ratio must be between 0.0 and 1.0")
	}
	return nil
}

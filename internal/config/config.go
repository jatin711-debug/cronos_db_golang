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
	config.RetentionMaxAgeHours = DefaultRetentionMaxAgeHours
	config.RetentionMaxSizeGB = DefaultRetentionMaxSizeGB
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
	config.MinInSyncReplicas = DefaultMinInSyncReplicas
	config.SnapshotCatchupThreshold = DefaultSnapshotCatchupThreshold
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

	// TLS defaults (all disabled by default)
	config.TLSEnabled = false
	config.TLSClientAuth = false

	// Auth defaults (all disabled by default)
	config.AuthEnabled = false

	// Node topology defaults
	config.NodeRack = ""
	config.NodeZone = ""
	config.NodeRegion = ""

	// Exactly-once commits default
	config.ExactlyOnceCommits = false

	// Load shedding default (disabled)
	config.LoadSheddingThreshold = 0.0

	// Follower reads default
	config.FollowerReadsEnabled = false

	// Encryption defaults
	config.EncryptionEnabled = DefaultEncryptionEnabled
	config.EncryptionKeyFile = DefaultEncryptionKeyFile

	// Topic rate limit defaults
	config.TopicRateLimitPerSecond = DefaultTopicRateLimitPerSecond
	config.TopicRateLimitBurst = DefaultTopicRateLimitBurst

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
	flag.IntVar(&config.RetentionMaxAgeHours, "retention-max-age-hours", DefaultRetentionMaxAgeHours, "Delete WAL segments older than this many hours (0 = disable)")
	flag.Int64Var(&config.RetentionMaxSizeGB, "retention-max-size-gb", DefaultRetentionMaxSizeGB, "Keep WAL segments within this many GB by deleting oldest (0 = disable)")

	// Security / dev mode
	flag.BoolVar(&config.DevMode, "dev", false, "Developer mode: disables production security requirements")

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
	flag.IntVar(&config.MinInSyncReplicas, "min-insync-replicas", DefaultMinInSyncReplicas, "Minimum in-sync replicas (incl. leader) required to ack a write; 0 = 1")
	flag.Int64Var(&config.SnapshotCatchupThreshold, "snapshot-catchup-threshold", DefaultSnapshotCatchupThreshold, "Replication lag (events) above which a follower requests a full snapshot; 0 = disable")

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

	// TLS flags
	flag.BoolVar(&config.TLSEnabled, "tls-enabled", false, "Enable TLS for gRPC")
	flag.StringVar(&config.TLSCAFile, "tls-ca-file", "", "Path to CA certificate file")
	flag.StringVar(&config.TLSCertFile, "tls-cert-file", "", "Path to TLS certificate file")
	flag.StringVar(&config.TLSKeyFile, "tls-key-file", "", "Path to TLS private key file")
	flag.BoolVar(&config.TLSClientAuth, "tls-client-auth", false, "Require client certificates (mTLS)")

	// Internal replication mTLS flags
	flag.BoolVar(&config.ReplicationTLSEnabled, "replication-tls-enabled", false, "Enable mTLS for internal replication traffic")
	flag.StringVar(&config.ReplicationTLSCAFile, "replication-tls-ca-file", "", "Path to internal replication CA certificate file")
	flag.StringVar(&config.ReplicationTLSCertFile, "replication-tls-cert-file", "", "Path to internal replication certificate file")
	flag.StringVar(&config.ReplicationTLSKeyFile, "replication-tls-key-file", "", "Path to internal replication private key file")

	// Auth flags
	flag.BoolVar(&config.AuthEnabled, "auth-enabled", false, "Enable JWT authentication")
	flag.StringVar(&config.AuthJWTSecret, "auth-jwt-secret", "", "HMAC secret for JWT verification")
	flag.StringVar(&config.AuthJWTPublicKey, "auth-jwt-public-key", "", "Path to Ed25519/RSA public key file for JWT verification")
	flag.StringVar(&config.AuthPolicyFile, "auth-policy-file", "", "Path to RBAC policy JSON file")

	// Node topology flags
	flag.StringVar(&config.NodeRack, "node-rack", "", "Rack / AZ label for topology-aware placement")
	flag.StringVar(&config.NodeZone, "node-zone", "", "Zone label for topology-aware placement")
	flag.StringVar(&config.NodeRegion, "node-region", "", "Region label for topology-aware placement")

	// Exactly-once commits
	flag.BoolVar(&config.ExactlyOnceCommits, "exactly-once-commits", false, "Enable exactly-once consumer offset commits")

	// Load shedding
	flag.Float64Var(&config.LoadSheddingThreshold, "load-shedding-threshold", 0.0, "Load shedding threshold (0.0-1.0, 0 = disabled)")

	// Follower reads
	flag.BoolVar(&config.FollowerReadsEnabled, "follower-reads", false, "Allow follower nodes to serve replay reads")

	// Encryption flags
	flag.BoolVar(&config.EncryptionEnabled, "encryption-enabled", DefaultEncryptionEnabled, "Enable AES-256-GCM encryption at rest for WAL segments")
	flag.StringVar(&config.EncryptionKeyFile, "encryption-key-file", DefaultEncryptionKeyFile, "Path to 32-byte encryption key file")

	// Topic rate limit flags
	flag.Float64Var(&config.TopicRateLimitPerSecond, "topic-rate-limit", DefaultTopicRateLimitPerSecond, "Per-subject per-topic rate limit (events/sec, 0 = disabled)")
	flag.Float64Var(&config.TopicRateLimitBurst, "topic-rate-burst", DefaultTopicRateLimitBurst, "Per-subject per-topic rate limit burst (0 = disabled)")

	// Memory-based backpressure flags
	flag.Float64Var(&config.MaxMemoryUsagePercent, "max-memory-percent", DefaultMaxMemoryUsagePercent, "Max memory usage %% before rejecting publishes (0 = disabled)")
	flag.Int64Var(&config.MemoryCheckIntervalMs, "memory-check-interval", DefaultMemoryCheckIntervalMs, "Memory check interval in milliseconds")

	// Ingest rate limiting per partition
	flag.Int64Var(&config.MaxIngestRatePerPartition, "max-ingest-rate", DefaultMaxIngestRatePerPartition, "Max events/sec per partition (0 = unlimited)")
	flag.Int64Var(&config.IngestRateBurstSize, "ingest-burst-size", DefaultIngestRateBurstSize, "Token bucket burst size for ingest rate limit")

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
	if devMode := os.Getenv("CRONOS_DEV"); devMode != "" {
		if parsed, err := strconv.ParseBool(devMode); err == nil {
			config.DevMode = parsed
		}
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

	// TLS environment overrides
	if tlsEnabled := os.Getenv("CRONOS_TLS_ENABLED"); tlsEnabled != "" {
		if parsed, err := strconv.ParseBool(tlsEnabled); err == nil {
			config.TLSEnabled = parsed
		}
	}
	if caFile := os.Getenv("CRONOS_TLS_CA_FILE"); caFile != "" {
		config.TLSCAFile = caFile
	}
	if certFile := os.Getenv("CRONOS_TLS_CERT_FILE"); certFile != "" {
		config.TLSCertFile = certFile
	}
	if keyFile := os.Getenv("CRONOS_TLS_KEY_FILE"); keyFile != "" {
		config.TLSKeyFile = keyFile
	}

	// Internal replication mTLS environment overrides
	if replicationTLSEnabled := os.Getenv("CRONOS_REPLICATION_TLS_ENABLED"); replicationTLSEnabled != "" {
		if parsed, err := strconv.ParseBool(replicationTLSEnabled); err == nil {
			config.ReplicationTLSEnabled = parsed
		}
	}
	if caFile := os.Getenv("CRONOS_REPLICATION_TLS_CA_FILE"); caFile != "" {
		config.ReplicationTLSCAFile = caFile
	}
	if certFile := os.Getenv("CRONOS_REPLICATION_TLS_CERT_FILE"); certFile != "" {
		config.ReplicationTLSCertFile = certFile
	}
	if keyFile := os.Getenv("CRONOS_REPLICATION_TLS_KEY_FILE"); keyFile != "" {
		config.ReplicationTLSKeyFile = keyFile
	}

	// Auth environment overrides. We only honor CRONOS_AUTH_ENABLED when the
	// --auth-enabled flag was not explicitly provided on the command line, so
	// that flags always take precedence over environment variables.
	authEnabledExplicit := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "auth-enabled" {
			authEnabledExplicit = true
		}
	})
	if !authEnabledExplicit {
		if authEnabled := os.Getenv("CRONOS_AUTH_ENABLED"); authEnabled != "" {
			if parsed, err := strconv.ParseBool(authEnabled); err == nil {
				config.AuthEnabled = parsed
			}
		}
	}
	if jwtSecret := os.Getenv("CRONOS_AUTH_JWT_SECRET"); jwtSecret != "" {
		config.AuthJWTSecret = jwtSecret
	}

	// Replication environment overrides
	if minISR := os.Getenv("CRONOS_MIN_IN_SYNC_REPLICAS"); minISR != "" {
		if parsed, err := strconv.Atoi(minISR); err == nil {
			config.MinInSyncReplicas = parsed
		}
	}
	if snapshotThreshold := os.Getenv("CRONOS_SNAPSHOT_CATCHUP_THRESHOLD"); snapshotThreshold != "" {
		if parsed, err := strconv.ParseInt(snapshotThreshold, 10, 64); err == nil {
			config.SnapshotCatchupThreshold = parsed
		}
	}

	// Exactly-once commits
	if eo := os.Getenv("CRONOS_EXACTLY_ONCE_COMMITS"); eo != "" {
		if parsed, err := strconv.ParseBool(eo); err == nil {
			config.ExactlyOnceCommits = parsed
		}
	}

	// Encryption at rest environment overrides
	if encEnabled := os.Getenv("CRONOS_ENCRYPTION_ENABLED"); encEnabled != "" {
		if parsed, err := strconv.ParseBool(encEnabled); err == nil {
			config.EncryptionEnabled = parsed
		}
	}
	if encKeyFile := os.Getenv("CRONOS_ENCRYPTION_KEY_FILE"); encKeyFile != "" {
		config.EncryptionKeyFile = encKeyFile
	}

	// Topology environment overrides
	if rack := os.Getenv("CRONOS_NODE_RACK"); rack != "" {
		config.NodeRack = rack
	}
	if zone := os.Getenv("CRONOS_NODE_ZONE"); zone != "" {
		config.NodeZone = zone
	}
	if region := os.Getenv("CRONOS_NODE_REGION"); region != "" {
		config.NodeRegion = region
	}

	// Validate required configuration
	if err := ValidateConfig(&config); err != nil {
		return nil, err
	}

	// Validate TLS config if enabled
	if config.TLSEnabled {
		if config.TLSCertFile == "" || config.TLSKeyFile == "" {
			return nil, fmt.Errorf("tls-enabled requires tls-cert-file and tls-key-file")
		}
	}

	// Validate auth config if enabled
	if config.AuthEnabled {
		if config.AuthJWTSecret == "" && config.AuthJWTPublicKey == "" {
			return nil, fmt.Errorf("auth-enabled requires auth-jwt-secret or auth-jwt-public-key")
		}
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
	if c.MinInSyncReplicas < 0 {
		return fmt.Errorf("min-insync-replicas must be >= 0")
	}
	if c.MinInSyncReplicas > c.ReplicationFactor {
		return fmt.Errorf("min-insync-replicas (%d) cannot exceed replication-factor (%d)", c.MinInSyncReplicas, c.ReplicationFactor)
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
	if c.FlushIntervalMS <= 0 {
		return fmt.Errorf("flush-interval must be > 0")
	}

	// Production hardening: require TLS, auth, encryption, and replication safety
	// unless the operator explicitly opts into developer mode.
	if !c.DevMode {
		if c.ReplicationFactor < 3 {
			return fmt.Errorf("production mode requires replication-factor >= 3 (use --dev to bypass)")
		}
		if c.MinInSyncReplicas < 2 {
			return fmt.Errorf("production mode requires min-insync-replicas >= 2 (use --dev to bypass)")
		}
		if !c.TLSEnabled {
			return fmt.Errorf("production mode requires TLS to be enabled (use --dev to bypass)")
		}
		if c.TLSCertFile == "" || c.TLSKeyFile == "" {
			return fmt.Errorf("production mode requires tls-cert-file and tls-key-file")
		}
		if !c.AuthEnabled {
			return fmt.Errorf("production mode requires auth to be enabled (use --dev to bypass)")
		}
		if c.AuthJWTSecret == "" && c.AuthJWTPublicKey == "" {
			return fmt.Errorf("production mode requires auth-jwt-secret or auth-jwt-public-key")
		}
		if !c.EncryptionEnabled {
			return fmt.Errorf("production mode requires encryption at rest to be enabled (use --dev to bypass)")
		}
		if c.EncryptionKeyFile == "" {
			return fmt.Errorf("production mode requires encryption-key-file")
		}
		if !c.ReplicationTLSEnabled {
			return fmt.Errorf("production mode requires replication TLS to be enabled (use --dev to bypass)")
		}
	}

	return nil
}

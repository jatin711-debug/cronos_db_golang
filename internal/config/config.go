package config

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"cronos_db/pkg/types"
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
	config.ReplicationBatchSize = DefaultReplicationBatchSize
	config.ReplicationTimeout = 10 * time.Second
	config.RaftDir = DefaultRaftDir
	config.StatsPrintInterval = DefaultStatsPrintInterval
	config.CheckpointInterval = DefaultCheckpointInterval

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
	config.FlushIntervalMS = int32(flushInterval)

	// Scheduler configuration
	flag.IntVar(&config.TickMS, "tick-ms", DefaultTickMS, "Scheduler tick duration in milliseconds")
	flag.IntVar(&config.WheelSize, "wheel-size", DefaultWheelSize, "Timing wheel size")
	// Delivery configuration
	flag.DurationVar(&config.DefaultAckTimeout, "ack-timeout", 30*time.Second, "Default ack timeout")
	flag.IntVar(&config.MaxRetries, "max-retries", DefaultMaxRetries, "Maximum delivery retries")
	flag.DurationVar(&config.RetryBackoff, "retry-backoff", 1*time.Second, "Retry backoff")
	flag.IntVar(&config.MaxDeliveryCredits, "max-credits", DefaultMaxDeliveryCredits, "Maximum delivery credits")

	// Dedup configuration
	flag.IntVar(&config.DedupTTLHours, "dedup-ttl", DefaultDedupTTLHours, "Deduplication TTL in hours (7 days)")

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

	var clusterSeeds string
	flag.StringVar(&clusterSeeds, "cluster-seeds", "", "Comma-separated list of seed node addresses")

	flag.Parse()

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
	if clusterEnabled := os.Getenv("CRONOS_CLUSTER"); clusterEnabled == "true" {
		config.ClusterEnabled = true
	}
	if seeds := os.Getenv("CRONOS_CLUSTER_SEEDS"); seeds != "" && len(config.ClusterSeeds) == 0 {
		config.ClusterSeeds = strings.Split(seeds, ",")
		for i, seed := range config.ClusterSeeds {
			config.ClusterSeeds[i] = strings.TrimSpace(seed)
		}
	}

	// Validate required configuration
	if config.NodeID == "" {
		return nil, fmt.Errorf("node-id is required")
	}

	return &config, nil
}

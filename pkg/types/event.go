package types

import "time"

// Config represents system configuration
type Config struct {
	NodeID      string
	DataDir     string
	GPRCAddress string
	HTTPAddress string

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
	BloomCapacity        uint64
	ReplicationBatchSize int
	ReplicationTimeout   time.Duration
	RaftDir              string
	RaftJoinAddr         string
	StatsPrintInterval   time.Duration
	CheckpointInterval   time.Duration

	// Tracing/telemetry configuration
	TracingEnabled      bool
	TracingExporter     string  // "none", "stdout", "otlp"
	TracingOTLPEndpoint string  // host:port
	TracingSampleRatio  float64 // 0.0-1.0
	TracingInsecure     bool

	// Scheduler cold store configuration
	HotWindowMinutes int // Events scheduled beyond this window go to cold store (0 = disable)

	// Adaptive hydrator configuration (0 = use defaults)
	HydratorMinIntervalMs int // Minimum hydrator scan interval in ms
	HydratorMaxIntervalMs int // Maximum hydrator scan interval in ms

	// Admission control configuration (0 = disabled)
	MaxReadyQueueSize       int64 // Max events in ready queue before rejecting publishes
	MaxTimingWheelSize      int64 // Max active timers before rejecting publishes
	MaxInFlightPerPartition int64 // Max in-flight deliveries per partition

	// Circuit breaker configuration
	CircuitBreakerFailureThreshold float64 // Failure rate to trip breaker (0.0-1.0, 1.0 = disabled)
	CircuitBreakerOpenDurationMs   int64   // How long breaker stays open
	CircuitBreakerMinAttempts      int64   // Min attempts before breaker can trip

	// Clock skew detection
	ClockSkewThresholdMs int64 // Max allowed clock skew from leader in ms (0 = disabled)

	// Cluster configuration
	ClusterEnabled    bool
	ClusterGossipAddr string   // UDP address for gossip protocol
	ClusterGRPCAddr   string   // gRPC address for cluster communication
	ClusterSeeds      []string // Seed nodes for cluster discovery
	ClusterRaftAddr   string   // Raft bind address
	VirtualNodes      int      // Virtual nodes per physical node for hashing
	HeartbeatInterval time.Duration
	FailureTimeout    time.Duration
	SuspectTimeout    time.Duration

	// Gossip backend selection
	UseMemberlist bool // true = HashiCorp Memberlist, false = custom TCP gossip
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

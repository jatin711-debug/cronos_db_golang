// Package types holds shared domain models and server configuration used across
// CronosDB packages. Generated protobuf types live in sibling *.pb.go files.
package types

import "time"

// Config holds process-wide server configuration loaded from flags and environment.
// Zero values for optional limits generally mean "disabled" or "use defaults"
// (see field comments). Critical fields such as DataDir and PartitionCount
// typically require a restart to take effect after change.
type Config struct {
	// NodeID is the unique identifier for this process in a cluster.
	NodeID string
	// DataDir is the root directory for WAL segments, Pebble stores, Raft, and audit logs.
	DataDir string
	// GPRCAddress is the public client gRPC listen address (e.g. ":9000").
	// Note: the field name is historical; it is the gRPC address.
	GPRCAddress string
	// HTTPAddress is the HTTP listen address for health, metrics, and the admin UI.
	HTTPAddress string

	// PartitionCount is the configured number of partitions in the cluster/topic space.
	PartitionCount int
	// ReplicationFactor is how many replicas (including the leader) store each partition.
	ReplicationFactor int
	// SegmentSizeBytes is the target max size of a WAL segment file before rotation.
	SegmentSizeBytes int64
	// IndexInterval is how many events between sparse index entries in a segment.
	IndexInterval int64
	// FsyncMode controls WAL durability: "every_event", "batch", or "periodic".
	FsyncMode string
	// FlushIntervalMS is the background flush interval in milliseconds for periodic fsync.
	FlushIntervalMS int32
	// TickMS is the hierarchical timing wheel tick duration in milliseconds.
	TickMS int
	// WheelSize is the number of slots in each level of the timing wheel.
	WheelSize int
	// DefaultAckTimeout is how long the dispatcher waits for a consumer ack before retry.
	DefaultAckTimeout time.Duration
	// MaxRetries is the maximum delivery attempts before a message is sent to the DLQ.
	MaxRetries int
	// RetryBackoff is the base backoff between delivery retries.
	RetryBackoff time.Duration
	// MaxDeliveryCredits is the default credit window for a subscription (flow control).
	MaxDeliveryCredits int
	// DeliveryPollMS is how often the delivery worker polls the ready queue (milliseconds).
	DeliveryPollMS int
	// DedupTTLHours is how long message IDs are retained in the dedup store.
	DedupTTLHours int
	// BloomCapacity is the expected item capacity for the per-partition bloom filter.
	BloomCapacity uint64
	// ReplicationBatchSize is the max events per replication Append batch to followers.
	ReplicationBatchSize int
	// ReplicationTimeout is the RPC timeout for leader→follower replication calls.
	ReplicationTimeout time.Duration
	// MinInSyncReplicas is the minimum number of in-sync replicas (including the
	// leader) required to acknowledge a write before it is considered durable.
	// A write is rejected with a NotEnoughReplicas error if fewer than this
	// number of replicas are in the ISR. 0 means derive a default of 1 (i.e.
	// only the leader is required), preserving the historical single-replica
	// behavior. Equivalent to Kafka's min.insync.replicas.
	MinInSyncReplicas int
	// RaftDir is the on-disk path for HashiCorp Raft state (often under DataDir).
	RaftDir string
	// RaftJoinAddr is an optional address used when joining an existing Raft cluster.
	RaftJoinAddr string
	// StatsPrintInterval is how often operational stats are logged (if used).
	StatsPrintInterval time.Duration
	// CheckpointInterval is how often partition/scheduler checkpoints are written.
	CheckpointInterval time.Duration

	// TracingEnabled turns OpenTelemetry tracing on or off.
	TracingEnabled bool
	// TracingExporter selects the trace exporter: "none", "stdout", or "otlp".
	TracingExporter string
	// TracingOTLPEndpoint is the OTLP gRPC host:port when Exporter is "otlp".
	TracingOTLPEndpoint string
	// TracingSampleRatio is the parent-based sampling probability in [0.0, 1.0].
	TracingSampleRatio float64
	// TracingInsecure disables TLS when exporting OTLP traces (dev only).
	TracingInsecure bool

	// HotWindowMinutes is the scheduler hot window: events further in the future
	// than this many minutes go to the cold store. 0 disables the cold path.
	HotWindowMinutes int

	// HydratorMinIntervalMs is the minimum adaptive cold-store scan interval in ms (0 = default).
	HydratorMinIntervalMs int
	// HydratorMaxIntervalMs is the maximum adaptive cold-store scan interval in ms (0 = default).
	HydratorMaxIntervalMs int

	// MaxReadyQueueSize caps ready-queue depth per partition before publish rejection (0 = disabled).
	MaxReadyQueueSize int64
	// MaxTimingWheelSize caps active hot timers before publish rejection (0 = disabled).
	MaxTimingWheelSize int64
	// MaxInFlightPerPartition caps concurrent in-flight deliveries per partition (0 = disabled).
	MaxInFlightPerPartition int64

	// CircuitBreakerFailureThreshold is the failure rate [0.0, 1.0] that trips a
	// per-subscription breaker. 1.0 effectively disables tripping on rate.
	CircuitBreakerFailureThreshold float64
	// CircuitBreakerOpenDurationMs is how long a tripped breaker stays open (ms).
	CircuitBreakerOpenDurationMs int64
	// CircuitBreakerMinAttempts is the minimum samples before the breaker evaluates.
	CircuitBreakerMinAttempts int64

	// ClockSkewThresholdMs is the max allowed clock skew from the leader in ms (0 = disabled).
	ClockSkewThresholdMs int64

	// ClusterEnabled turns multi-node clustering on (membership, ring, Raft metadata).
	ClusterEnabled bool
	// ClusterGossipAddr is the bind address for gossip / membership heartbeats.
	ClusterGossipAddr string
	// ClusterGRPCAddr is the dedicated internal cluster gRPC address (replication/Raft).
	ClusterGRPCAddr string
	// ClusterSeeds is the list of seed nodes used to join an existing cluster.
	ClusterSeeds []string
	// ClusterRaftAddr is the Raft transport bind address.
	ClusterRaftAddr string
	// VirtualNodes is the number of virtual nodes per physical node on the hash ring.
	VirtualNodes int
	// HeartbeatInterval is how often membership heartbeats are sent.
	HeartbeatInterval time.Duration
	// FailureTimeout is how long without heartbeat before a node is considered failed.
	FailureTimeout time.Duration
	// SuspectTimeout is how long a node stays in suspect state before failure.
	SuspectTimeout time.Duration

	// UseMemberlist selects HashiCorp Memberlist (SWIM) when true; otherwise custom TCP gossip.
	UseMemberlist bool

	// TLSEnabled enables TLS for the public gRPC server.
	TLSEnabled bool
	// TLSCAFile is the path to the CA certificate used to verify clients/peers.
	TLSCAFile string
	// TLSCertFile is the path to the server TLS certificate.
	TLSCertFile string
	// TLSKeyFile is the path to the server TLS private key.
	TLSKeyFile string
	// TLSClientAuth requires client certificates (mTLS) on the public gRPC server.
	TLSClientAuth bool

	// ReplicationTLSEnabled enables mTLS for internal leader↔follower replication.
	ReplicationTLSEnabled bool
	// ReplicationTLSCAFile is the CA for verifying replication peers.
	ReplicationTLSCAFile string
	// ReplicationTLSCertFile is this node's replication certificate.
	ReplicationTLSCertFile string
	// ReplicationTLSKeyFile is this node's replication private key.
	ReplicationTLSKeyFile string

	// SnapshotCatchupThreshold is the replication lag (in events) above which a
	// follower will request a full segment snapshot instead of incremental Sync.
	// 0 disables snapshot-driven catch-up.
	SnapshotCatchupThreshold int64

	// AuthEnabled enables JWT authentication on gRPC/HTTP admin paths.
	AuthEnabled bool
	// AuthJWTSecret is the HMAC secret for JWT verification (empty if using public key).
	AuthJWTSecret string
	// AuthJWTPublicKey is the path to an Ed25519/RSA public key file for JWT verification.
	AuthJWTPublicKey string
	// AuthPolicyFile is the path to the RBAC policy JSON file.
	AuthPolicyFile string

	// NodeRack is the rack/AZ label for topology-aware replica placement.
	NodeRack string
	// NodeZone is the zone label for topology-aware placement.
	NodeZone string
	// NodeRegion is the region label for topology-aware and cross-region logic.
	NodeRegion string

	// DevMode disables production security requirements for local development and CI.
	DevMode bool

	// RetentionMaxAgeHours deletes WAL segments older than this many hours (0 = disabled).
	RetentionMaxAgeHours int
	// RetentionMaxSizeGB keeps WAL under this many gigabytes by deleting oldest segments (0 = disabled).
	RetentionMaxSizeGB int64

	// ExactlyOnceCommits enables durable exactly-once commit-ID tracking in the offset store.
	ExactlyOnceCommits bool

	// LoadSheddingThreshold is a load fraction [0.0, 1.0] above which work may be shed (0 = disabled).
	LoadSheddingThreshold float64

	// FollowerReadsEnabled allows serving some read/replay traffic from followers.
	FollowerReadsEnabled bool

	// EncryptionEnabled turns on AES-GCM encryption at rest for WAL segments.
	EncryptionEnabled bool
	// EncryptionKeyFile is the path to the master key material for at-rest encryption.
	EncryptionKeyFile string

	// TopicRateLimitPerSecond is the default per-topic publish rate limit (0 = disabled).
	TopicRateLimitPerSecond float64
	// TopicRateLimitBurst is the token-bucket burst for topic rate limiting (0 = disabled).
	TopicRateLimitBurst float64

	// MaxMemoryUsagePercent rejects publishes when process memory exceeds this percent (0 = disabled).
	MaxMemoryUsagePercent float64
	// MemoryCheckIntervalMs is how often memory usage is sampled in ms.
	MemoryCheckIntervalMs int64

	// MaxIngestRatePerPartition caps events/sec accepted per partition (0 = unlimited).
	MaxIngestRatePerPartition int64
	// IngestRateBurstSize is the token-bucket burst size for per-partition ingest limiting.
	IngestRateBurstSize int64
}

// Partition is the public API view of a data partition (metadata and log progress).
// Runtime engines (WAL, scheduler, etc.) live in internal/partition, not here.
type Partition struct {
	// ID is the numeric partition identifier.
	ID int32
	// Topic is the logical topic (or synthetic name) associated with this partition.
	Topic string
	// NextOffset is the next offset that will be assigned on append.
	NextOffset int64
	// HighWatermark is the highest offset considered durable/visible to consumers.
	HighWatermark int64
	// Active indicates whether the partition is accepting work.
	Active bool
	// CreatedTS is creation time as Unix milliseconds.
	CreatedTS int64
	// UpdatedTS is last update time as Unix milliseconds.
	UpdatedTS int64
}

// ConsumerGroup is the public API view of a consumer group and its offset state.
type ConsumerGroup struct {
	// GroupID is the unique consumer group identifier.
	GroupID string
	// Topic is the topic this group consumes.
	Topic string
	// Partitions lists partition IDs assigned to or tracked by this group.
	Partitions []int32
	// CommittedOffsets maps partition ID → last committed offset.
	CommittedOffsets map[int32]int64
	// MemberOffsets maps member ID → last reported offset (when tracked).
	MemberOffsets map[string]int64
	// Members maps member ID → membership metadata.
	Members map[string]*ConsumerMember
	// CreatedTS is group creation time as Unix milliseconds.
	CreatedTS int64
	// UpdatedTS is last group update time as Unix milliseconds.
	UpdatedTS int64
}

// ConsumerMember describes one member of a consumer group.
type ConsumerMember struct {
	// MemberID uniquely identifies the member within the group.
	MemberID string
	// Address is an optional network address advertised by the member.
	Address string
	// AssignedPartition is the partition currently assigned to this member, if any.
	AssignedPartition int32
	// Active indicates whether the member is considered live.
	Active bool
	// LastSeenTS is the last heartbeat/activity time as Unix milliseconds.
	LastSeenTS int64
	// ConnectedTS is when the member joined/connected as Unix milliseconds.
	ConnectedTS int64
}

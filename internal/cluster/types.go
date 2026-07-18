// Package cluster implements cluster membership, consistent hashing, Raft-backed
// metadata consensus, partition routing, and leader failover for CronosDB.
package cluster

import (
	"time"
)

// NodeState represents the liveness state of a cluster node in membership.
type NodeState int

const (
	// NodeStateUnknown indicates the node state has not been determined.
	NodeStateUnknown NodeState = iota
	// NodeStateAlive indicates the node is responding to heartbeats.
	NodeStateAlive
	// NodeStateSuspect indicates the node missed heartbeats and is under suspicion.
	NodeStateSuspect
	// NodeStateDead indicates the node is considered failed after the suspect timeout.
	NodeStateDead
	// NodeStateLeft indicates the node left the cluster gracefully.
	NodeStateLeft
)

// String returns a human-readable name for the node state.
func (s NodeState) String() string {
	switch s {
	case NodeStateAlive:
		return "alive"
	case NodeStateSuspect:
		return "suspect"
	case NodeStateDead:
		return "dead"
	case NodeStateLeft:
		return "left"
	default:
		return "unknown"
	}
}

// NodeRole represents the Raft/consensus role of a node in the cluster.
type NodeRole int

const (
	// NodeRoleFollower indicates the node is a Raft follower (or partition replica).
	NodeRoleFollower NodeRole = iota
	// NodeRoleCandidate indicates the node is campaigning for leadership.
	NodeRoleCandidate
	// NodeRoleLeader indicates the node is the cluster or partition leader.
	NodeRoleLeader
)

// String returns a human-readable name for the node role.
func (r NodeRole) String() string {
	switch r {
	case NodeRoleFollower:
		return "follower"
	case NodeRoleCandidate:
		return "candidate"
	case NodeRoleLeader:
		return "leader"
	default:
		return "unknown"
	}
}

// Node represents a peer in the CronosDB cluster, including network endpoints,
// liveness, role, and topology labels used for rack-aware placement.
type Node struct {
	// ID is the unique node identifier within the cluster.
	ID string `json:"id"`
	// Address is the public gRPC address used for client and inter-node RPCs.
	Address string `json:"address"`
	// GossipAddr is the membership/gossip bind address used for heartbeats.
	GossipAddr string `json:"gossip_addr"`
	// HTTPAddr is the HTTP address used for health checks and admin endpoints.
	HTTPAddr string `json:"http_addr"`
	// RaftAddr is the Raft transport address used for consensus RPCs.
	RaftAddr string `json:"raft_addr"`
	// State is the current membership liveness state of the node.
	State NodeState `json:"state"`
	// Role is the current cluster consensus role of the node.
	Role NodeRole `json:"role"`
	// Meta holds optional free-form key/value metadata advertised by the node.
	Meta map[string]string `json:"meta"`
	// JoinedAt is when the node first joined the cluster view.
	JoinedAt time.Time `json:"joined_at"`
	// UpdatedAt is the last time membership information for the node was refreshed.
	UpdatedAt time.Time `json:"updated_at"`
	// Partitions lists partition assignments currently associated with the node.
	Partitions []PartitionAssignment `json:"partitions"`
	// Rack is the rack topology label used for rack-aware replica placement.
	Rack string `json:"rack,omitempty"`
	// Zone is the availability-zone topology label.
	Zone string `json:"zone,omitempty"`
	// Region is the geographic region topology label.
	Region string `json:"region,omitempty"`
}

// PartitionAssignment represents a single partition assigned to a node with a
// leadership or replica role and leadership epoch.
type PartitionAssignment struct {
	// PartitionID is the numeric partition identifier.
	PartitionID int32 `json:"partition_id"`
	// Role is Leader or Follower for this partition on the node.
	Role NodeRole `json:"role"`
	// Epoch is the leadership epoch for the assignment.
	Epoch int64 `json:"epoch"`
}

// ClusterState represents the authoritative (or membership-view) snapshot of
// cluster-wide metadata: nodes, partitions, and current leader.
type ClusterState struct {
	// ClusterID is the logical cluster identifier.
	ClusterID string `json:"cluster_id"`
	// LeaderID is the node ID of the current cluster metadata leader.
	LeaderID string `json:"leader_id"`
	// Term is the current Raft term when Raft is enabled.
	Term int64 `json:"term"`
	// Nodes maps node ID to node metadata.
	Nodes map[string]*Node `json:"nodes"`
	// Partitions maps partition ID to partition assignment metadata.
	Partitions map[int32]*PartitionInfo `json:"partitions"`
	// UpdatedAt is when this cluster state snapshot was last modified.
	UpdatedAt time.Time `json:"updated_at"`
}

// PartitionInfo represents partition assignment and replication metadata used
// for routing, failover, and ISR tracking.
type PartitionInfo struct {
	// ID is the partition identifier.
	ID int32 `json:"id"`
	// Topic is the logical topic associated with the partition, if any.
	Topic string `json:"topic"`
	// LeaderID is the node ID of the current partition leader.
	LeaderID string `json:"leader_id"`
	// Replicas is the ordered list of replica node IDs (leader first when set by the ring).
	Replicas []string `json:"replicas"`
	// ISR is the set of in-sync replica node IDs.
	ISR []string `json:"isr"`
	// Epoch is the leadership epoch; increments on each leader change.
	Epoch int64 `json:"epoch"`
	// State is the operational state of the partition (online, rebalancing, offline).
	State PartitionState `json:"state"`
	// ReplicaOffsets maps replica node ID to its known high-watermark offset.
	ReplicaOffsets map[string]int64 `json:"replica_offsets"`
}

// PartitionState represents the operational state of a partition.
type PartitionState int

const (
	// PartitionStateOffline indicates the partition is not serving traffic.
	PartitionStateOffline PartitionState = iota
	// PartitionStateOnline indicates the partition is active and serving.
	PartitionStateOnline
	// PartitionStateRebalancing indicates replicas or leadership are being moved.
	PartitionStateRebalancing
)

// String returns a human-readable name for the partition state.
func (s PartitionState) String() string {
	switch s {
	case PartitionStateOnline:
		return "online"
	case PartitionStateRebalancing:
		return "rebalancing"
	default:
		return "offline"
	}
}

// Config is the simplified cluster configuration used by process startup (main).
// It is converted into ClusterConfig by NewManager.
type Config struct {
	// NodeID is this node's unique identifier.
	NodeID string
	// DataDir is the base data directory for local state.
	DataDir string
	// GossipAddr is the bind address for membership/gossip traffic.
	GossipAddr string
	// GRPCAddr is the public gRPC listen address.
	GRPCAddr string
	// RaftAddr is the Raft transport listen address.
	RaftAddr string
	// RaftDir is the on-disk directory for Raft logs and snapshots.
	RaftDir string
	// SeedNodes are seed addresses used to join an existing cluster.
	SeedNodes []string
	// VirtualNodes is the number of virtual nodes per physical node on the hash ring.
	VirtualNodes int
	// HeartbeatInterval is how often membership heartbeats are sent.
	HeartbeatInterval time.Duration
	// FailureTimeout is how long without heartbeats before failure detection escalates.
	FailureTimeout time.Duration
	// SuspectTimeout is how long a node remains suspect before being marked dead.
	SuspectTimeout time.Duration
	// PartitionCount is the total number of partitions in the cluster.
	PartitionCount int
	// ReplicationFactor is the desired number of replicas per partition.
	ReplicationFactor int
	// UseMemberlist enables HashiCorp Memberlist (SWIM) instead of custom TCP gossip.
	UseMemberlist bool
	// Rack is this node's rack topology label.
	Rack string
	// Zone is this node's availability-zone topology label.
	Zone string
	// Region is this node's geographic region topology label.
	Region string
}

// ClusterConfig is the full internal cluster configuration used by membership,
// Raft, and the router.
type ClusterConfig struct {
	// ClusterID is the logical cluster identifier.
	ClusterID string `json:"cluster_id"`
	// NodeID is this node's unique identifier.
	NodeID string `json:"node_id"`
	// BindAddr is the local address membership binds to for gossip.
	BindAddr string `json:"bind_addr"`
	// AdvertiseAddr is the address advertised to peers for membership.
	AdvertiseAddr string `json:"advertise_addr"`
	// GRPCAddr is the public gRPC address for this node.
	GRPCAddr string `json:"grpc_addr"`
	// HTTPAddr is the HTTP address for health and admin endpoints.
	HTTPAddr string `json:"http_addr"`
	// RaftAddr is the Raft transport address for this node.
	RaftAddr string `json:"raft_addr"`
	// RaftDataDir is where Raft stores durable log and snapshot data.
	RaftDataDir string `json:"raft_data_dir"`
	// SeedNodes are seed addresses used to join an existing cluster.
	SeedNodes []string `json:"seed_nodes"`
	// HeartbeatInterval is the membership heartbeat interval.
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`
	// ElectionTimeout is the Raft election timeout.
	ElectionTimeout time.Duration `json:"election_timeout"`
	// FailureTimeout is the membership failure-detection timeout.
	FailureTimeout time.Duration `json:"failure_timeout"`
	// SuspectTimeout is how long a node stays suspect before being marked dead.
	SuspectTimeout time.Duration `json:"suspect_timeout"`
	// ReplicationFactor is the desired replica count per partition.
	ReplicationFactor int `json:"replication_factor"`
	// NumPartitions is the total number of partitions.
	NumPartitions int `json:"num_partitions"`
	// VirtualNodes is the hash-ring virtual-node count per physical node.
	VirtualNodes int `json:"virtual_nodes"`
	// UseMemberlist enables HashiCorp Memberlist for membership.
	UseMemberlist bool `json:"use_memberlist"`
	// Rack is this node's rack topology label.
	Rack string `json:"rack,omitempty"`
	// Zone is this node's availability-zone topology label.
	Zone string `json:"zone,omitempty"`
	// Region is this node's geographic region topology label.
	Region string `json:"region,omitempty"`
}

// DefaultClusterConfig returns a ClusterConfig populated with production-oriented defaults.
func DefaultClusterConfig() *ClusterConfig {
	return &ClusterConfig{
		ClusterID:         "cronos-cluster",
		HeartbeatInterval: 1 * time.Second,
		ElectionTimeout:   5 * time.Second,
		FailureTimeout:    5 * time.Second,
		SuspectTimeout:    3 * time.Second,
		ReplicationFactor: 3,
		NumPartitions:     16,
		VirtualNodes:      150,
	}
}

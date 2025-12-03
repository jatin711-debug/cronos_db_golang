package cluster

import (
	"time"
)

// NodeState represents the state of a node
type NodeState int

const (
	NodeStateUnknown NodeState = iota
	NodeStateAlive
	NodeStateSuspect
	NodeStateDead
	NodeStateLeft
)

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

// NodeRole represents the role of a node in the cluster
type NodeRole int

const (
	NodeRoleFollower NodeRole = iota
	NodeRoleCandidate
	NodeRoleLeader
)

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

// Node represents a node in the cluster
type Node struct {
	ID         string            `json:"id"`
	Address    string            `json:"address"`     // gRPC address
	GossipAddr string            `json:"gossip_addr"` // Gossip/membership address
	HTTPAddr   string            `json:"http_addr"`   // HTTP address for health checks
	RaftAddr   string            `json:"raft_addr"`   // Raft consensus address
	State      NodeState         `json:"state"`
	Role       NodeRole          `json:"role"`
	Meta       map[string]string `json:"meta"`
	JoinedAt   time.Time         `json:"joined_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
	// Partition assignments
	Partitions []PartitionAssignment `json:"partitions"`
}

// PartitionAssignment represents a partition assigned to a node
type PartitionAssignment struct {
	PartitionID int32    `json:"partition_id"`
	Role        NodeRole `json:"role"` // Leader or Follower for this partition
	Epoch       int64    `json:"epoch"`
}

// ClusterState represents the entire cluster state
type ClusterState struct {
	ClusterID  string                   `json:"cluster_id"`
	LeaderID   string                   `json:"leader_id"`
	Term       int64                    `json:"term"`
	Nodes      map[string]*Node         `json:"nodes"`
	Partitions map[int32]*PartitionInfo `json:"partitions"`
	UpdatedAt  time.Time                `json:"updated_at"`
}

// PartitionInfo represents partition metadata
type PartitionInfo struct {
	ID       int32          `json:"id"`
	Topic    string         `json:"topic"`
	LeaderID string         `json:"leader_id"`
	Replicas []string       `json:"replicas"` // Node IDs
	ISR      []string       `json:"isr"`      // In-Sync Replicas
	Epoch    int64          `json:"epoch"`
	State    PartitionState `json:"state"`
}

// PartitionState represents the state of a partition
type PartitionState int

const (
	PartitionStateOffline PartitionState = iota
	PartitionStateOnline
	PartitionStateRebalancing
)

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

// Config is the simplified configuration used by main.go
type Config struct {
	NodeID            string
	DataDir           string
	GossipAddr        string
	GRPCAddr          string
	RaftAddr          string
	RaftDir           string
	SeedNodes         []string // Seed nodes to join
	VirtualNodes      int
	HeartbeatInterval time.Duration
	FailureTimeout    time.Duration
	SuspectTimeout    time.Duration
	PartitionCount    int
	ReplicationFactor int
}

// ClusterConfig represents cluster configuration (internal)
type ClusterConfig struct {
	ClusterID         string        `json:"cluster_id"`
	NodeID            string        `json:"node_id"`
	BindAddr          string        `json:"bind_addr"`
	AdvertiseAddr     string        `json:"advertise_addr"`
	GRPCAddr          string        `json:"grpc_addr"`
	HTTPAddr          string        `json:"http_addr"`
	RaftAddr          string        `json:"raft_addr"`
	RaftDataDir       string        `json:"raft_data_dir"`
	SeedNodes         []string      `json:"seed_nodes"`
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`
	ElectionTimeout   time.Duration `json:"election_timeout"`
	FailureTimeout    time.Duration `json:"failure_timeout"`
	SuspectTimeout    time.Duration `json:"suspect_timeout"`
	ReplicationFactor int           `json:"replication_factor"`
	NumPartitions     int           `json:"num_partitions"`
	VirtualNodes      int           `json:"virtual_nodes"`
}

// DefaultClusterConfig returns default cluster configuration
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

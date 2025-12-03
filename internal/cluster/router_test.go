package cluster

import (
	"testing"
	"time"
)

// TestHashRingRouting tests the routing via hash ring
func TestHashRingRouting(t *testing.T) {
	ring := NewHashRing(100, 3)

	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	// Route to partition
	partition := ring.GetKeyPartition("test-key", 4)
	if partition < 0 || partition >= 4 {
		t.Errorf("Invalid partition %d", partition)
	}

	// Same key should always route to same partition
	for i := 0; i < 100; i++ {
		got := ring.GetKeyPartition("test-key", 4)
		if got != partition {
			t.Errorf("Inconsistent routing: expected %d, got %d", partition, got)
		}
	}
}

func TestHashRing_GetPartitionNodes(t *testing.T) {
	ring := NewHashRing(100, 3)

	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	// Get nodes for partition 0
	nodes := ring.GetPartitionNodes(0)
	if len(nodes) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(nodes))
	}

	// All nodes should be unique
	seen := make(map[string]bool)
	for _, node := range nodes {
		if seen[node] {
			t.Errorf("Duplicate node: %s", node)
		}
		seen[node] = true
	}
}

func TestTypes_NodeState(t *testing.T) {
	tests := []struct {
		state    NodeState
		expected string
	}{
		{NodeStateUnknown, "unknown"},
		{NodeStateAlive, "alive"},
		{NodeStateSuspect, "suspect"},
		{NodeStateDead, "dead"},
		{NodeStateLeft, "left"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("NodeState.String() = %s, want %s", got, tt.expected)
		}
	}
}

func TestTypes_NodeRole(t *testing.T) {
	tests := []struct {
		role     NodeRole
		expected string
	}{
		{NodeRoleFollower, "follower"},
		{NodeRoleCandidate, "candidate"},
		{NodeRoleLeader, "leader"},
	}

	for _, tt := range tests {
		if got := tt.role.String(); got != tt.expected {
			t.Errorf("NodeRole.String() = %s, want %s", got, tt.expected)
		}
	}
}

func TestTypes_PartitionState(t *testing.T) {
	tests := []struct {
		state    PartitionState
		expected string
	}{
		{PartitionStateOffline, "offline"},
		{PartitionStateOnline, "online"},
		{PartitionStateRebalancing, "rebalancing"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("PartitionState.String() = %s, want %s", got, tt.expected)
		}
	}
}

func TestDefaultClusterConfig(t *testing.T) {
	config := DefaultClusterConfig()

	if config.ClusterID != "cronos-cluster" {
		t.Errorf("Expected cluster ID 'cronos-cluster', got %s", config.ClusterID)
	}
	if config.HeartbeatInterval != 1*time.Second {
		t.Errorf("Expected 1s heartbeat interval, got %v", config.HeartbeatInterval)
	}
	if config.ReplicationFactor != 3 {
		t.Errorf("Expected RF=3, got %d", config.ReplicationFactor)
	}
	if config.NumPartitions != 16 {
		t.Errorf("Expected 16 partitions, got %d", config.NumPartitions)
	}
}

func TestPartitionInfo(t *testing.T) {
	info := &PartitionInfo{
		ID:       0,
		Topic:    "test-topic",
		LeaderID: "node1",
		Replicas: []string{"node1", "node2", "node3"},
		ISR:      []string{"node1", "node2"},
		Epoch:    5,
		State:    PartitionStateOnline,
	}

	if info.ID != 0 {
		t.Errorf("Expected ID 0, got %d", info.ID)
	}
	if info.LeaderID != "node1" {
		t.Errorf("Expected leader node1, got %s", info.LeaderID)
	}
	if len(info.Replicas) != 3 {
		t.Errorf("Expected 3 replicas, got %d", len(info.Replicas))
	}
	if info.State.String() != "online" {
		t.Errorf("Expected online state, got %s", info.State.String())
	}
}

func TestNode(t *testing.T) {
	node := &Node{
		ID:       "node1",
		Address:  "localhost:9000",
		HTTPAddr: "localhost:8080",
		RaftAddr: "localhost:7948",
		State:    NodeStateAlive,
		Role:     NodeRoleLeader,
	}

	if node.ID != "node1" {
		t.Errorf("Expected ID node1, got %s", node.ID)
	}
	if node.State.String() != "alive" {
		t.Errorf("Expected alive state, got %s", node.State.String())
	}
	if node.Role.String() != "leader" {
		t.Errorf("Expected leader role, got %s", node.Role.String())
	}
}

func TestClusterState(t *testing.T) {
	state := &ClusterState{
		ClusterID:  "test-cluster",
		LeaderID:   "node1",
		Term:       5,
		Nodes:      make(map[string]*Node),
		Partitions: make(map[int32]*PartitionInfo),
	}

	state.Nodes["node1"] = &Node{ID: "node1", State: NodeStateAlive}
	state.Nodes["node2"] = &Node{ID: "node2", State: NodeStateAlive}

	state.Partitions[0] = &PartitionInfo{ID: 0, LeaderID: "node1"}
	state.Partitions[1] = &PartitionInfo{ID: 1, LeaderID: "node2"}

	if state.Term != 5 {
		t.Errorf("Expected term 5, got %d", state.Term)
	}
	if len(state.Nodes) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(state.Nodes))
	}
	if len(state.Partitions) != 2 {
		t.Errorf("Expected 2 partitions, got %d", len(state.Partitions))
	}
}

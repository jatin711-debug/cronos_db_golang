package cluster

import (
	"context"
	"fmt"
	"testing"
)

type MockPartitionAccessor struct {
	syncCalls     map[int32]string
	promoteCalls  map[int32]int64
	followerCalls map[int32]string
	demoteCalls   map[int32]bool
}

func (m *MockPartitionAccessor) SyncPartitionFromLeader(partitionID int32, leaderAddr string) error {
	m.syncCalls[partitionID] = leaderAddr
	return nil
}

func (m *MockPartitionAccessor) GetOrCreatePartition(partitionID int32) error {
	return nil
}

func (m *MockPartitionAccessor) PromoteToLeader(partitionID int32, epoch int64) error {
	m.promoteCalls[partitionID] = epoch
	return nil
}

func (m *MockPartitionAccessor) AddFollower(partitionID int32, followerID string, followerAddr string) error {
	m.followerCalls[partitionID] = followerID
	return nil
}

func (m *MockPartitionAccessor) DemoteFromLeader(partitionID int32) error {
	m.demoteCalls[partitionID] = true
	return nil
}

func (m *MockPartitionAccessor) GetPartitionReplicaOffsets(partitionID int32) map[string]int64 {
	return nil
}

func TestClusterManager_Initialization(t *testing.T) {
	cfg := &Config{
		NodeID:            "node-1",
		GossipAddr:        "127.0.0.1:8001",
		GRPCAddr:          "127.0.0.1:9001",
		RaftAddr:          "127.0.0.1:10001",
		RaftDir:           t.TempDir(),
		PartitionCount:    8,
		ReplicationFactor: 1,
	}

	mgr := NewManager(cfg)
	if mgr == nil {
		t.Fatal("Expected NewManager to return a valid instance")
	}

	if mgr.config.NodeID != "node-1" {
		t.Errorf("Expected node ID node-1, got %s", mgr.config.NodeID)
	}

	if mgr.config.NumPartitions != 8 {
		t.Errorf("Expected 8 partitions, got %d", mgr.config.NumPartitions)
	}
}

func TestClusterRouter_Assignments(t *testing.T) {
	localNode := &Node{
		ID:         "node-1",
		Address:    "127.0.0.1:9002",
		GossipAddr: "127.0.0.1:8002",
		State:      NodeStateAlive,
	}

	mockMembership := &mockMembershipService{
		local: localNode,
		nodes: map[string]*Node{
			"node-1": localNode,
		},
	}

	accessor := &MockPartitionAccessor{
		syncCalls:     make(map[int32]string),
		promoteCalls:  make(map[int32]int64),
		followerCalls: make(map[int32]string),
		demoteCalls:   make(map[int32]bool),
	}

	router := NewRouter(mockMembership, 4, 1, 150, accessor)
	if router == nil {
		t.Fatal("Failed to create router")
	}

	// Verify partitions are assigned to node-1 (since it is the only alive node in the ring)
	for i := int32(0); i < 4; i++ {
		leader, err := router.GetPartitionLeader(i)
		if err != nil {
			t.Fatalf("GetPartitionLeader failed for partition %d: %v", i, err)
		}
		if leader.ID != "node-1" {
			t.Errorf("Expected partition %d leader to be node-1, got %s", i, leader.ID)
		}
		if !router.IsLocalPartition(i) {
			t.Errorf("Expected partition %d to be local", i)
		}
		if !router.IsPartitionLeader(i) {
			t.Errorf("Expected this node to be leader of partition %d", i)
		}
	}
}

func TestStringSliceSetEqual(t *testing.T) {
	cases := []struct {
		a      []string
		b      []string
		equal  bool
		reason string
	}{
		{[]string{"a", "b"}, []string{"b", "a"}, true, "order ignored"},
		{[]string{"a", "b"}, []string{"a", "b", "c"}, false, "length differs"},
		{[]string{}, []string{}, true, "both empty"},
		{[]string{"a"}, []string{"a"}, true, "single element"},
		{[]string{"a", "a"}, []string{"a", "a"}, true, "duplicates match"},
		{[]string{"a", "a"}, []string{"a"}, false, "duplicate count differs"},
	}
	for _, tc := range cases {
		got := stringSliceSetEqual(tc.a, tc.b)
		if got != tc.equal {
			t.Errorf("stringSliceSetEqual(%v, %v): expected %v (%s), got %v", tc.a, tc.b, tc.equal, tc.reason, got)
		}
	}
}

func TestPartitionAssignmentChanged(t *testing.T) {
	base := &PartitionInfo{
		ID:       1,
		LeaderID: "node1",
		Replicas: []string{"node1", "node2"},
		ISR:      []string{"node1", "node2"},
		State:    PartitionStateOnline,
	}
	cases := []struct {
		name     string
		fsm      *PartitionInfo
		router   *PartitionInfo
		expected bool
	}{
		{"identical", base, base, false},
		{"leader changed", base, func() *PartitionInfo { p := *base; p.LeaderID = "node2"; return &p }(), true},
		{"replica order changed", base, func() *PartitionInfo { p := *base; p.Replicas = []string{"node2", "node1"}; return &p }(), false},
		{"replica set changed", base, func() *PartitionInfo { p := *base; p.Replicas = []string{"node1", "node3"}; return &p }(), true},
		{"isr set changed", base, func() *PartitionInfo { p := *base; p.ISR = []string{"node1"}; return &p }(), true},
		{"state changed", base, func() *PartitionInfo { p := *base; p.State = PartitionStateRebalancing; return &p }(), true},
		{"nil fsm", nil, base, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := partitionAssignmentChanged(tc.fsm, tc.router)
			if got != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, got)
			}
		})
	}
}

func TestRouterUpdatePartitionAssignment(t *testing.T) {
	localNode := &Node{
		ID:         "node-1",
		Address:    "127.0.0.1:9002",
		GossipAddr: "127.0.0.1:8002",
		State:      NodeStateAlive,
	}
	mockMembership := &mockMembershipService{
		local: localNode,
		nodes: map[string]*Node{"node-1": localNode},
	}
	accessor := &MockPartitionAccessor{
		syncCalls:     make(map[int32]string),
		promoteCalls:  make(map[int32]int64),
		followerCalls: make(map[int32]string),
		demoteCalls:   make(map[int32]bool),
	}
	router := NewRouter(mockMembership, 4, 1, 150, accessor)

	router.UpdatePartitionAssignment(2, "node-2", []string{"node-2", "node-3"}, []string{"node-2"})

	info, err := router.GetPartitionInfo(2)
	if err != nil {
		t.Fatalf("GetPartitionInfo failed: %v", err)
	}
	if info.LeaderID != "node-2" {
		t.Errorf("expected leader node-2, got %s", info.LeaderID)
	}
	if !stringSliceSetEqual(info.Replicas, []string{"node-2", "node-3"}) {
		t.Errorf("expected replicas [node-2 node-3], got %v", info.Replicas)
	}
	if !stringSliceSetEqual(info.ISR, []string{"node-2"}) {
		t.Errorf("expected ISR [node-2], got %v", info.ISR)
	}
	if info.State != PartitionStateOnline {
		t.Errorf("expected state online, got %v", info.State)
	}
}

type mockMembershipService struct {
	local *Node
	nodes map[string]*Node
}

func (m *mockMembershipService) Start(ctx context.Context) error { return nil }
func (m *mockMembershipService) Stop()                           {}
func (m *mockMembershipService) Join(node *Node) error           { return nil }
func (m *mockMembershipService) Leave(nodeID string) error       { return nil }
func (m *mockMembershipService) GetNodes() []*Node               { return m.GetAliveNodes() }
func (m *mockMembershipService) GetClusterState() *ClusterState  { return nil }
func (m *mockMembershipService) OnJoin(cb func(node *Node))      {}
func (m *mockMembershipService) OnLeave(cb func(node *Node))     {}

func (m *mockMembershipService) GetLocalNode() *Node {
	return m.local
}

func (m *mockMembershipService) GetNode(id string) (*Node, error) {
	node, exists := m.nodes[id]
	if !exists {
		return nil, fmt.Errorf("node not found")
	}
	return node, nil
}

func (m *mockMembershipService) GetAliveNodes() []*Node {
	var result []*Node
	for _, node := range m.nodes {
		result = append(result, node)
	}
	return result
}

func (m *mockMembershipService) Events() <-chan MemberEvent {
	return make(chan MemberEvent)
}

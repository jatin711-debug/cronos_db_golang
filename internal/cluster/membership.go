package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

// Membership manages cluster membership and node discovery
type Membership struct {
	mu        sync.RWMutex
	config    *ClusterConfig
	localNode *Node
	nodes     map[string]*Node
	state     *ClusterState
	eventCh   chan MemberEvent
	stopCh    chan struct{}
	listener  net.Listener

	// Callbacks
	onJoin   func(node *Node)
	onLeave  func(node *Node)
	onUpdate func(node *Node)
}

// MemberEvent represents a membership change event
type MemberEvent struct {
	Type EventType
	Node *Node
	Time time.Time
}

// EventType represents the type of membership event
type EventType int

const (
	EventTypeJoin EventType = iota
	EventTypeLeave
	EventTypeUpdate
	EventTypeFailed
)

func (e EventType) String() string {
	switch e {
	case EventTypeJoin:
		return "join"
	case EventTypeLeave:
		return "leave"
	case EventTypeUpdate:
		return "update"
	case EventTypeFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// NewMembership creates a new membership manager
func NewMembership(config *ClusterConfig) (*Membership, error) {
	localNode := &Node{
		ID:         config.NodeID,
		Address:    config.GRPCAddr,
		GossipAddr: config.BindAddr, // Gossip address for heartbeats
		HTTPAddr:   config.HTTPAddr,
		RaftAddr:   config.RaftAddr,
		State:      NodeStateAlive,
		Role:       NodeRoleFollower,
		Meta:       make(map[string]string),
		JoinedAt:   time.Now(),
		UpdatedAt:  time.Now(),
	}

	m := &Membership{
		config:    config,
		localNode: localNode,
		nodes:     make(map[string]*Node),
		state: &ClusterState{
			ClusterID:  config.ClusterID,
			Nodes:      make(map[string]*Node),
			Partitions: make(map[int32]*PartitionInfo),
			UpdatedAt:  time.Now(),
		},
		eventCh: make(chan MemberEvent, 100),
		stopCh:  make(chan struct{}),
	}

	// Add self to nodes
	m.nodes[localNode.ID] = localNode
	m.state.Nodes[localNode.ID] = localNode

	return m, nil
}

// Start starts the membership manager
func (m *Membership) Start(ctx context.Context) error {
	log.Printf("[MEMBERSHIP] Starting membership manager for node %s", m.localNode.ID)

	// Start gossip listener
	listener, err := net.Listen("tcp", m.config.BindAddr)
	if err != nil {
		return fmt.Errorf("start gossip listener on %s: %w", m.config.BindAddr, err)
	}
	m.listener = listener
	log.Printf("[MEMBERSHIP] Gossip listener started on %s", m.config.BindAddr)

	// Accept incoming connections
	go m.acceptLoop(ctx)

	// Start heartbeat sender
	go m.heartbeatLoop(ctx)

	// Start failure detector
	go m.failureDetectorLoop(ctx)

	// Join seed nodes
	if len(m.config.SeedNodes) > 0 {
		go m.joinSeedNodes(ctx)
	}

	return nil
}

// Stop stops the membership manager
func (m *Membership) Stop() {
	close(m.stopCh)
	if m.listener != nil {
		m.listener.Close()
	}
}

// acceptLoop accepts incoming gossip connections
func (m *Membership) acceptLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		default:
		}

		conn, err := m.listener.Accept()
		if err != nil {
			select {
			case <-m.stopCh:
				return
			default:
				log.Printf("[MEMBERSHIP] Accept error: %v", err)
				continue
			}
		}

		go m.handleConnection(conn)
	}
}

// handleConnection handles an incoming gossip connection
func (m *Membership) handleConnection(conn net.Conn) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	// Read message
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		return
	}

	// Parse message
	var msg GossipMessage
	if err := json.Unmarshal(buf[:n], &msg); err != nil {
		log.Printf("[MEMBERSHIP] Failed to parse gossip message: %v", err)
		return
	}

	// Handle message based on type
	switch msg.Type {
	case "join":
		m.handleJoinRequest(conn, &msg)
	case "heartbeat":
		m.handleHeartbeatMessage(&msg)
	case "state":
		m.handleStateRequest(conn)
	case "node_joined":
		m.handleNodeJoinedBroadcast(&msg)
	default:
		log.Printf("[MEMBERSHIP] Unknown message type: %s", msg.Type)
	}
}

// GossipMessage is the message format for gossip protocol
type GossipMessage struct {
	Type       string `json:"type"`
	NodeID     string `json:"node_id"`
	Address    string `json:"address"`     // gRPC address
	GossipAddr string `json:"gossip_addr"` // Gossip address for heartbeats
	RaftAddr   string `json:"raft_addr"`
	Timestamp  int64  `json:"timestamp"`
}

// handleJoinRequest handles a node join request
func (m *Membership) handleJoinRequest(conn net.Conn, msg *GossipMessage) {
	node := &Node{
		ID:         msg.NodeID,
		Address:    msg.Address,
		GossipAddr: msg.GossipAddr,
		RaftAddr:   msg.RaftAddr,
		State:      NodeStateAlive,
		Role:       NodeRoleFollower,
		JoinedAt:   time.Now(),
		UpdatedAt:  time.Now(),
	}

	if err := m.Join(node); err != nil {
		response := map[string]interface{}{"success": false, "error": err.Error()}
		json.NewEncoder(conn).Encode(response)
		return
	}

	// Notify other nodes about the new node
	go m.broadcastNodeJoined(node)

	// Send back cluster state
	m.mu.RLock()
	nodes := make([]*Node, 0, len(m.nodes))
	for _, n := range m.nodes {
		nodes = append(nodes, n)
	}
	m.mu.RUnlock()

	response := map[string]interface{}{
		"success": true,
		"nodes":   nodes,
	}
	json.NewEncoder(conn).Encode(response)
}

// handleHeartbeatMessage handles a heartbeat
func (m *Membership) handleHeartbeatMessage(msg *GossipMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if node, exists := m.nodes[msg.NodeID]; exists {
		node.UpdatedAt = time.Now()
		node.State = NodeStateAlive
	}
}

// handleNodeJoinedBroadcast handles notification that a new node joined
func (m *Membership) handleNodeJoinedBroadcast(msg *GossipMessage) {
	if msg.NodeID == m.localNode.ID {
		return // Ignore broadcast about ourselves
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.nodes[msg.NodeID]; exists {
		return // Already know about this node
	}

	node := &Node{
		ID:         msg.NodeID,
		Address:    msg.Address,
		GossipAddr: msg.GossipAddr,
		RaftAddr:   msg.RaftAddr,
		State:      NodeStateAlive,
		Role:       NodeRoleFollower,
		JoinedAt:   time.Now(),
		UpdatedAt:  time.Now(),
	}

	m.nodes[msg.NodeID] = node
	m.state.Nodes[msg.NodeID] = node
	log.Printf("[MEMBERSHIP] Learned about node %s at gossip=%s via broadcast", msg.NodeID, msg.GossipAddr)
}

// broadcastNodeJoined notifies all existing nodes about a new node
func (m *Membership) broadcastNodeJoined(newNode *Node) {
	m.mu.RLock()
	nodes := make([]*Node, 0, len(m.nodes))
	for _, n := range m.nodes {
		if n.ID != m.localNode.ID && n.ID != newNode.ID && n.GossipAddr != "" {
			nodes = append(nodes, n)
		}
	}
	m.mu.RUnlock()

	msg := GossipMessage{
		Type:       "node_joined",
		NodeID:     newNode.ID,
		Address:    newNode.Address,
		GossipAddr: newNode.GossipAddr,
		RaftAddr:   newNode.RaftAddr,
		Timestamp:  time.Now().UnixNano(),
	}

	for _, node := range nodes {
		go func(addr string) {
			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				return
			}
			defer conn.Close()
			json.NewEncoder(conn).Encode(msg)
		}(node.GossipAddr)
	}
}

// handleStateRequest sends current cluster state
func (m *Membership) handleStateRequest(conn net.Conn) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	json.NewEncoder(conn).Encode(m.state)
}

// Join adds a node to the cluster
func (m *Membership) Join(node *Node) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.nodes[node.ID]; exists {
		// Update existing node
		m.nodes[node.ID] = node
		m.state.Nodes[node.ID] = node
		m.emitEvent(EventTypeUpdate, node)
		return nil
	}

	node.JoinedAt = time.Now()
	node.UpdatedAt = time.Now()
	node.State = NodeStateAlive

	m.nodes[node.ID] = node
	m.state.Nodes[node.ID] = node
	m.state.UpdatedAt = time.Now()

	log.Printf("[MEMBERSHIP] Node %s joined the cluster", node.ID)
	m.emitEvent(EventTypeJoin, node)

	if m.onJoin != nil {
		go m.onJoin(node)
	}

	return nil
}

// Leave removes a node from the cluster
func (m *Membership) Leave(nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, exists := m.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	node.State = NodeStateLeft
	node.UpdatedAt = time.Now()
	delete(m.nodes, nodeID)
	delete(m.state.Nodes, nodeID)
	m.state.UpdatedAt = time.Now()

	log.Printf("[MEMBERSHIP] Node %s left the cluster", nodeID)
	m.emitEvent(EventTypeLeave, node)

	if m.onLeave != nil {
		go m.onLeave(node)
	}

	return nil
}

// MarkDead marks a node as dead
func (m *Membership) MarkDead(nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, exists := m.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	node.State = NodeStateDead
	node.UpdatedAt = time.Now()

	log.Printf("[MEMBERSHIP] Node %s marked as dead", nodeID)
	m.emitEvent(EventTypeFailed, node)

	return nil
}

// GetNode returns a node by ID
func (m *Membership) GetNode(nodeID string) (*Node, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	node, exists := m.nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	return node, nil
}

// GetNodes returns all nodes
func (m *Membership) GetNodes() []*Node {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := make([]*Node, 0, len(m.nodes))
	for _, node := range m.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// GetAliveNodes returns all alive nodes
func (m *Membership) GetAliveNodes() []*Node {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := make([]*Node, 0)
	for _, node := range m.nodes {
		if node.State == NodeStateAlive {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// GetLocalNode returns the local node
func (m *Membership) GetLocalNode() *Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.localNode
}

// GetClusterState returns the current cluster state
func (m *Membership) GetClusterState() *ClusterState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state
}

// SetLeader sets the cluster leader
func (m *Membership) SetLeader(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.state.LeaderID = nodeID
	m.state.UpdatedAt = time.Now()

	// Update node roles
	for id, node := range m.nodes {
		if id == nodeID {
			node.Role = NodeRoleLeader
		} else {
			node.Role = NodeRoleFollower
		}
	}

	log.Printf("[MEMBERSHIP] Leader set to %s", nodeID)
}

// IsLeader returns true if local node is the leader
func (m *Membership) IsLeader() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state.LeaderID == m.localNode.ID
}

// GetLeader returns the current leader node
func (m *Membership) GetLeader() *Node {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.state.LeaderID == "" {
		return nil
	}
	return m.nodes[m.state.LeaderID]
}

// Events returns the event channel
func (m *Membership) Events() <-chan MemberEvent {
	return m.eventCh
}

// OnJoin sets the callback for join events
func (m *Membership) OnJoin(fn func(node *Node)) {
	m.onJoin = fn
}

// OnLeave sets the callback for leave events
func (m *Membership) OnLeave(fn func(node *Node)) {
	m.onLeave = fn
}

// OnUpdate sets the callback for update events
func (m *Membership) OnUpdate(fn func(node *Node)) {
	m.onUpdate = fn
}

// emitEvent emits a membership event
func (m *Membership) emitEvent(eventType EventType, node *Node) {
	select {
	case m.eventCh <- MemberEvent{
		Type: eventType,
		Node: node,
		Time: time.Now(),
	}:
	default:
		// Channel full, skip event
	}
}

// heartbeatLoop sends periodic heartbeats to other nodes
func (m *Membership) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(m.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.sendHeartbeats()
		}
	}
}

// sendHeartbeats sends heartbeats to all known nodes
func (m *Membership) sendHeartbeats() {
	m.mu.RLock()
	nodes := make([]*Node, 0, len(m.nodes))
	for _, node := range m.nodes {
		if node.ID != m.localNode.ID && node.State == NodeStateAlive {
			nodes = append(nodes, node)
		}
	}
	m.mu.RUnlock()

	for _, node := range nodes {
		go m.sendHeartbeat(node)
	}
}

// sendHeartbeat sends a heartbeat to a specific node
func (m *Membership) sendHeartbeat(node *Node) {
	// Create heartbeat message
	hb := &GossipMessage{
		Type:      "heartbeat",
		NodeID:    m.localNode.ID,
		Address:   m.localNode.Address,
		Timestamp: time.Now().UnixMilli(),
	}

	// Use gossip address if available, otherwise fall back to gRPC address
	targetAddr := node.GossipAddr
	if targetAddr == "" {
		targetAddr = node.Address
	}

	// Try to connect and send heartbeat
	conn, err := net.DialTimeout("tcp", targetAddr, 2*time.Second)
	if err != nil {
		log.Printf("[MEMBERSHIP] Heartbeat to %s failed: %v", node.ID, err)
		return
	}
	defer conn.Close()

	// Send heartbeat as GossipMessage
	json.NewEncoder(conn).Encode(hb)
}

// failureDetectorLoop detects failed nodes
func (m *Membership) failureDetectorLoop(ctx context.Context) {
	ticker := time.NewTicker(m.config.HeartbeatInterval * 3)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.detectFailures()
		}
	}
}

// detectFailures checks for failed nodes
func (m *Membership) detectFailures() {
	m.mu.Lock()
	defer m.mu.Unlock()

	timeout := m.config.HeartbeatInterval * 5
	now := time.Now()

	for _, node := range m.nodes {
		if node.ID == m.localNode.ID {
			continue
		}

		if node.State == NodeStateAlive && now.Sub(node.UpdatedAt) > timeout {
			log.Printf("[MEMBERSHIP] Node %s suspected (no heartbeat for %v)", node.ID, timeout)
			node.State = NodeStateSuspect
			m.emitEvent(EventTypeFailed, node)
		} else if node.State == NodeStateSuspect && now.Sub(node.UpdatedAt) > timeout*2 {
			log.Printf("[MEMBERSHIP] Node %s marked dead", node.ID)
			node.State = NodeStateDead
		}
	}
}

// joinSeedNodes attempts to join the cluster via seed nodes
func (m *Membership) joinSeedNodes(ctx context.Context) {
	for _, seedAddr := range m.config.SeedNodes {
		if err := m.joinViaNode(ctx, seedAddr); err != nil {
			log.Printf("[MEMBERSHIP] Failed to join via %s: %v", seedAddr, err)
			continue
		}
		log.Printf("[MEMBERSHIP] Successfully joined cluster via %s", seedAddr)
		return
	}
	log.Printf("[MEMBERSHIP] Could not join via any seed node, starting as standalone")
}

// joinViaNode attempts to join the cluster via a specific node
func (m *Membership) joinViaNode(ctx context.Context, addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", addr, err)
	}
	defer conn.Close()

	// Send join request using GossipMessage format
	joinMsg := &GossipMessage{
		Type:       "join",
		NodeID:     m.localNode.ID,
		Address:    m.localNode.Address,
		GossipAddr: m.localNode.GossipAddr,
		RaftAddr:   m.localNode.RaftAddr,
		Timestamp:  time.Now().UnixMilli(),
	}
	if err := json.NewEncoder(conn).Encode(joinMsg); err != nil {
		return fmt.Errorf("send join request: %w", err)
	}

	// Read response
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var response map[string]interface{}
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		return fmt.Errorf("read join response: %w", err)
	}

	if success, ok := response["success"].(bool); !ok || !success {
		errMsg := "unknown error"
		if e, ok := response["error"].(string); ok {
			errMsg = e
		}
		return fmt.Errorf("join rejected: %s", errMsg)
	}

	// Process nodes from response
	if nodes, ok := response["nodes"].([]interface{}); ok {
		for _, nodeData := range nodes {
			if nodeMap, ok := nodeData.(map[string]interface{}); ok {
				nodeID := getString(nodeMap, "id") // lowercase per JSON tag
				if nodeID != "" && nodeID != m.localNode.ID {
					node := &Node{
						ID:         nodeID,
						Address:    getString(nodeMap, "address"),
						GossipAddr: getString(nodeMap, "gossip_addr"),
						HTTPAddr:   getString(nodeMap, "http_addr"),
						RaftAddr:   getString(nodeMap, "raft_addr"),
						State:      NodeStateAlive,
						JoinedAt:   time.Now(),
						UpdatedAt:  time.Now(),
					}
					m.mu.Lock()
					m.nodes[nodeID] = node
					m.state.Nodes[nodeID] = node
					m.mu.Unlock()
					log.Printf("[MEMBERSHIP] Discovered node %s at gossip=%s", nodeID, node.GossipAddr)
				}
			}
		}
	}

	return nil
}

// getString safely extracts a string from a map
func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

// Heartbeat represents a heartbeat message
type Heartbeat struct {
	NodeID    string    `json:"node_id"`
	Address   string    `json:"address"`
	Timestamp int64     `json:"timestamp"`
	State     NodeState `json:"state"`
}

// JoinRequest represents a join request
type JoinRequest struct {
	NodeID   string `json:"node_id"`
	Address  string `json:"address"`
	HTTPAddr string `json:"http_addr"`
	RaftAddr string `json:"raft_addr"`
}

// JoinResponse represents a join response
type JoinResponse struct {
	Success      bool          `json:"success"`
	Error        string        `json:"error,omitempty"`
	ClusterState *ClusterState `json:"cluster_state,omitempty"`
}

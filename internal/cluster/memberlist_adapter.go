package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

// MemberlistMembership is a Membership implementation backed by HashiCorp Memberlist (SWIM protocol).
// It provides production-grade failure detection with lower overhead than custom TCP heartbeats.
type MemberlistMembership struct {
	config    *ClusterConfig
	list      *memberlist.Memberlist
	localNode *Node
	nodes     map[string]*Node
	nodesMu   sync.RWMutex
	onJoinCb  func(node *Node)
	onLeaveCb func(node *Node)
	started   bool
	wg        sync.WaitGroup
	eventCh   chan MemberEvent
	quit      chan struct{}
}

// NewMemberlistMembership creates a new membership using HashiCorp Memberlist.
func NewMemberlistMembership(config *ClusterConfig) (*MemberlistMembership, error) {
	localNode := &Node{
		ID:         config.NodeID,
		Address:    config.GRPCAddr,
		GossipAddr: config.BindAddr,
		HTTPAddr:   config.HTTPAddr,
		RaftAddr:   config.RaftAddr,
		State:      NodeStateAlive,
		Role:       NodeRoleFollower,
		Meta:       make(map[string]string),
		JoinedAt:   time.Now(),
		UpdatedAt:  time.Now(),
	}

	mm := &MemberlistMembership{
		config:    config,
		localNode: localNode,
		nodes:     make(map[string]*Node),
		eventCh:   make(chan MemberEvent, 100),
		quit:      make(chan struct{}),
	}
	mm.nodes[localNode.ID] = localNode

	return mm, nil
}

// Start initializes memberlist and joins the cluster.
func (mm *MemberlistMembership) Start(ctx context.Context) error {
	if mm.started {
		return fmt.Errorf("already started")
	}

	conf := memberlist.DefaultLANConfig()
	conf.Name = mm.config.NodeID
	conf.BindAddr = mm.bindHost()
	conf.BindPort = mm.bindPort()
	conf.AdvertiseAddr = mm.advertiseHost()
	conf.AdvertisePort = mm.advertisePort()
	conf.ProbeInterval = mm.config.HeartbeatInterval
	conf.ProbeTimeout = mm.config.FailureTimeout
	conf.SuspicionMult = 3
	conf.Delegate = &mlDelegate{mm: mm}
	conf.Events = &mlEventDelegate{mm: mm}

	list, err := memberlist.Create(conf)
	if err != nil {
		return fmt.Errorf("create memberlist: %w", err)
	}
	mm.list = list

	// Join existing cluster if seeds provided
	if len(mm.config.SeedNodes) > 0 {
		n, err := list.Join(mm.config.SeedNodes)
		if err != nil {
			log.Printf("[MEMBERSHIP] Failed to join all seeds: %v (joined %d)", err, n)
		} else {
			log.Printf("[MEMBERSHIP] Joined %d seed nodes", n)
		}
	}

	mm.started = true
	log.Printf("[MEMBERSHIP] Memberlist started on %s:%d", conf.BindAddr, conf.BindPort)
	return nil
}

// Stop gracefully leaves the cluster and shuts down memberlist.
func (mm *MemberlistMembership) Stop() {
	if !mm.started {
		return
	}
	close(mm.quit)
	if mm.list != nil {
		if err := mm.list.Leave(5 * time.Second); err != nil {
			log.Printf("[MEMBERSHIP] Leave error: %v", err)
		}
		if err := mm.list.Shutdown(); err != nil {
			log.Printf("[MEMBERSHIP] Shutdown error: %v", err)
		}
	}
	mm.wg.Wait()
	mm.started = false
}

// Join is a no-op for memberlist (discovery is automatic via gossip).
func (mm *MemberlistMembership) Join(node *Node) error {
	// Memberlist auto-discovers via gossip; manual join not needed.
	return nil
}

// Leave removes a node from the cluster view.
func (mm *MemberlistMembership) Leave(nodeID string) error {
	mm.nodesMu.Lock()
	delete(mm.nodes, nodeID)
	mm.nodesMu.Unlock()
	return nil
}

// GetNodes returns all known nodes.
func (mm *MemberlistMembership) GetNodes() []*Node {
	mm.nodesMu.RLock()
	defer mm.nodesMu.RUnlock()

	nodes := make([]*Node, 0, len(mm.nodes))
	for _, n := range mm.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

// GetAliveNodes returns only alive nodes.
func (mm *MemberlistMembership) GetAliveNodes() []*Node {
	mm.nodesMu.RLock()
	defer mm.nodesMu.RUnlock()

	var alive []*Node
	for _, n := range mm.nodes {
		if n.State == NodeStateAlive || n.State == NodeStateSuspect {
			alive = append(alive, n)
		}
	}
	return alive
}

// GetLocalNode returns the local node.
func (mm *MemberlistMembership) GetLocalNode() *Node {
	mm.nodesMu.RLock()
	defer mm.nodesMu.RUnlock()
	return mm.localNode
}

// GetNode returns a specific node by ID.
func (mm *MemberlistMembership) GetNode(nodeID string) (*Node, error) {
	mm.nodesMu.RLock()
	defer mm.nodesMu.RUnlock()
	node, ok := mm.nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}
	return node, nil
}

// Events returns a channel of membership events.
func (mm *MemberlistMembership) Events() <-chan MemberEvent {
	return mm.eventCh
}

// GetClusterState returns the cluster state.
func (mm *MemberlistMembership) GetClusterState() *ClusterState {
	mm.nodesMu.RLock()
	defer mm.nodesMu.RUnlock()

	nodesCopy := make(map[string]*Node, len(mm.nodes))
	for k, v := range mm.nodes {
		nodesCopy[k] = v
	}

	return &ClusterState{
		ClusterID: mm.config.ClusterID,
		Nodes:     nodesCopy,
		UpdatedAt: time.Now(),
	}
}

// OnJoin registers a callback for node join events.
func (mm *MemberlistMembership) OnJoin(cb func(node *Node)) {
	mm.onJoinCb = cb
}

// OnLeave registers a callback for node leave events.
func (mm *MemberlistMembership) OnLeave(cb func(node *Node)) {
	mm.onLeaveCb = cb
}

// --- internal helpers ---

func (mm *MemberlistMembership) bindHost() string {
	host, _, _ := net.SplitHostPort(mm.config.BindAddr)
	if host == "" {
		host = "0.0.0.0"
	}
	return host
}

func (mm *MemberlistMembership) bindPort() int {
	_, portStr, err := net.SplitHostPort(mm.config.BindAddr)
	if err != nil {
		return 7946
	}
	var port int
	fmt.Sscanf(portStr, "%d", &port)
	if port == 0 {
		port = 7946
	}
	return port
}

func (mm *MemberlistMembership) advertiseHost() string {
	host, _, _ := net.SplitHostPort(mm.config.AdvertiseAddr)
	if host == "" {
		host = mm.bindHost()
	}
	return host
}

func (mm *MemberlistMembership) advertisePort() int {
	return mm.bindPort()
}

func (mm *MemberlistMembership) nodeFromMember(m *memberlist.Node) *Node {
	node := &Node{
		ID:        m.Name,
		State:     NodeStateAlive,
		JoinedAt:  time.Now(),
		UpdatedAt: time.Now(),
		Meta:      make(map[string]string),
	}
	// Try to decode metadata
	if len(m.Meta) > 0 {
		var meta nodeMeta
		if err := json.Unmarshal(m.Meta, &meta); err == nil {
			node.Address = meta.GRPCAddr
			node.GossipAddr = meta.GossipAddr
			node.HTTPAddr = meta.HTTPAddr
			node.RaftAddr = meta.RaftAddr
		}
	}
	return node
}

func (mm *MemberlistMembership) metaBytes() []byte {
	meta := nodeMeta{
		NodeID:     mm.config.NodeID,
		GRPCAddr:   mm.config.GRPCAddr,
		GossipAddr: mm.config.BindAddr,
		HTTPAddr:   mm.config.HTTPAddr,
		RaftAddr:   mm.config.RaftAddr,
	}
	data, _ := json.Marshal(meta)
	return data
}

// nodeMeta is the metadata exchanged via memberlist.
type nodeMeta struct {
	NodeID     string `json:"node_id"`
	GRPCAddr   string `json:"grpc_addr"`
	GossipAddr string `json:"gossip_addr"`
	HTTPAddr   string `json:"http_addr"`
	RaftAddr   string `json:"raft_addr"`
}

// mlDelegate implements memberlist.Delegate for metadata exchange.
type mlDelegate struct {
	mm *MemberlistMembership
}

func (d *mlDelegate) NodeMeta(limit int) []byte {
	return d.mm.metaBytes()
}

func (d *mlDelegate) NotifyMsg([]byte)                           {}
func (d *mlDelegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }
func (d *mlDelegate) LocalState(join bool) []byte                { return nil }
func (d *mlDelegate) MergeRemoteState(buf []byte, join bool)     {}

// mlEventDelegate implements memberlist.EventDelegate for join/leave notifications.
type mlEventDelegate struct {
	mm *MemberlistMembership
}

func (e *mlEventDelegate) NotifyJoin(member *memberlist.Node) {
	e.mm.nodesMu.Lock()
	node := e.mm.nodeFromMember(member)
	e.mm.nodes[node.ID] = node
	e.mm.nodesMu.Unlock()

	select {
	case e.mm.eventCh <- MemberEvent{Type: EventTypeJoin, Node: node, Time: time.Now()}:
	default:
	}

	if e.mm.onJoinCb != nil {
		e.mm.onJoinCb(node)
	}
	log.Printf("[MEMBERSHIP] Node joined: %s at %s", member.Name, member.Address())
}

func (e *mlEventDelegate) NotifyLeave(member *memberlist.Node) {
	e.mm.nodesMu.Lock()
	if node, ok := e.mm.nodes[member.Name]; ok {
		node.State = NodeStateDead
	}
	e.mm.nodesMu.Unlock()

	select {
	case e.mm.eventCh <- MemberEvent{Type: EventTypeLeave, Node: &Node{ID: member.Name}, Time: time.Now()}:
	default:
	}

	if e.mm.onLeaveCb != nil {
		e.mm.onLeaveCb(&Node{ID: member.Name})
	}
	log.Printf("[MEMBERSHIP] Node left: %s", member.Name)
}

func (e *mlEventDelegate) NotifyUpdate(member *memberlist.Node) {
	node := e.mm.nodeFromMember(member)
	e.mm.nodesMu.Lock()
	e.mm.nodes[node.ID] = node
	e.mm.nodesMu.Unlock()
}

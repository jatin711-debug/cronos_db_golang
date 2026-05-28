package cluster

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"strconv"
	"sync"

	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
)

// HashRing implements consistent hashing with virtual nodes
type HashRing struct {
	mu           sync.RWMutex
	ring         []uint64          // Sorted hash positions
	nodeMap      map[uint64]string // Hash position -> node ID
	nodes        map[string]int    // Node ID -> virtual node count
	virtualNodes int               // Default virtual nodes per physical node
	replicas     int               // Replication factor

	// Topology-aware placement
	topology map[string]NodeTopology // node ID -> rack/zone/region
}

// NodeTopology holds topology labels for a node.
type NodeTopology struct {
	Rack   string
	Zone   string
	Region string
}

// NewHashRing creates a new consistent hash ring
func NewHashRing(virtualNodes, replicas int) *HashRing {
	if virtualNodes <= 0 {
		virtualNodes = 150 // Good default for balance
	}
	if replicas <= 0 {
		replicas = 3
	}

	return &HashRing{
		ring:         make([]uint64, 0),
		nodeMap:      make(map[uint64]string),
		nodes:        make(map[string]int),
		virtualNodes: virtualNodes,
		replicas:     replicas,
		topology:     make(map[string]NodeTopology),
	}
}

// SetNodeTopology sets topology labels for a node.
func (h *HashRing) SetNodeTopology(nodeID string, topo NodeTopology) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.topology[nodeID] = topo
}

// AddNode adds a node to the ring
func (h *HashRing) AddNode(nodeID string) {
	h.AddNodeWithWeight(nodeID, h.virtualNodes)
}

// AddNodeWithWeight adds a node with a specific number of virtual nodes
func (h *HashRing) AddNodeWithWeight(nodeID string, weight int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, exists := h.nodes[nodeID]; exists {
		return // Already exists
	}

	h.nodes[nodeID] = weight

	// Add virtual nodes — use strconv instead of fmt.Sprintf to avoid allocation
	for i := 0; i < weight; i++ {
		key := nodeID + "-" + strconv.Itoa(i)
		hash := h.hash(key)
		h.ring = append(h.ring, hash)
		h.nodeMap[hash] = nodeID
	}

	// Sort the ring
	sort.Slice(h.ring, func(i, j int) bool {
		return h.ring[i] < h.ring[j]
	})
}

// RemoveNode removes a node from the ring
func (h *HashRing) RemoveNode(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	weight, exists := h.nodes[nodeID]
	if !exists {
		return
	}

	delete(h.nodes, nodeID)

	// Remove virtual nodes
	for i := 0; i < weight; i++ {
		key := nodeID + "-" + strconv.Itoa(i)
		hash := h.hash(key)
		delete(h.nodeMap, hash)
	}

	// Rebuild ring without removed node's hashes
	newRing := make([]uint64, 0, len(h.ring)-weight)
	for _, hash := range h.ring {
		if _, exists := h.nodeMap[hash]; exists {
			newRing = append(newRing, hash)
		}
	}
	h.ring = newRing
}

// GetNode returns the node responsible for a key
func (h *HashRing) GetNode(key string) string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 {
		return ""
	}

	hash := h.hash(key)
	idx := h.search(hash)
	return h.nodeMap[h.ring[idx]]
}

// GetNodes returns n nodes responsible for a key (for replication)
func (h *HashRing) GetNodes(key string, n int) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.getNodesLocked(key, n)
}

// getNodesLocked returns n nodes for a key (caller must hold at least RLock)
func (h *HashRing) getNodesLocked(key string, n int) []string {
	if len(h.ring) == 0 {
		return nil
	}

	if n > len(h.nodes) {
		n = len(h.nodes)
	}

	hash := h.hash(key)
	idx := h.search(hash)

	// Collect unique nodes using slice scan instead of map.
	// For typical replication factors (n=3), linear scan is ~20x faster
	// than map allocation + lookup.
	nodes := make([]string, 0, n)
	usedRacks := make(map[string]struct{})

	for i := 0; i < len(h.ring) && len(nodes) < n; i++ {
		nodeIdx := (idx + i) % len(h.ring)
		nodeID := h.nodeMap[h.ring[nodeIdx]]
		// Linear scan for dedup — faster than map for n <= ~10
		found := false
		for _, existing := range nodes {
			if existing == nodeID {
				found = true
				break
			}
		}
		if found {
			continue
		}

		// Topology-aware placement: avoid same rack if possible
		if topo, ok := h.topology[nodeID]; ok && topo.Rack != "" {
			if _, used := usedRacks[topo.Rack]; used {
				// Only skip same-rack if we have enough candidates left
				remaining := len(h.nodes) - len(nodes)
				if remaining > n-len(nodes) {
					continue
				}
			}
			usedRacks[topo.Rack] = struct{}{}
		}

		nodes = append(nodes, nodeID)
	}

	return nodes
}

// GetPartitionNode returns the node responsible for a partition
func (h *HashRing) GetPartitionNode(partitionID int32) string {
	return h.GetNode("partition-" + strconv.FormatInt(int64(partitionID), 10))
}

// GetPartitionNodes returns nodes for a partition (leader + replicas)
func (h *HashRing) GetPartitionNodes(partitionID int32) []string {
	return h.GetNodes("partition-"+strconv.FormatInt(int64(partitionID), 10), h.replicas)
}

// getPartitionNodesLocked returns nodes for a partition (caller must hold RLock).
// This is the lock-free internal version to prevent recursive RLock deadlock.
func (h *HashRing) getPartitionNodesLocked(partitionID int32) []string {
	return h.getNodesLocked("partition-"+strconv.FormatInt(int64(partitionID), 10), h.replicas)
}

// GetTopicPartition returns the partition ID for a topic
func (h *HashRing) GetTopicPartition(topic string, numPartitions int) int32 {
	return utils.HashToPartitionID(topic, numPartitions)
}

// GetKeyPartition returns the partition ID for a key
func (h *HashRing) GetKeyPartition(key string, numPartitions int) int32 {
	return utils.HashToPartitionID(key, numPartitions)
}

// search finds the index of the first node with hash >= target
func (h *HashRing) search(hash uint64) int {
	idx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i] >= hash
	})

	// Wrap around
	if idx >= len(h.ring) {
		idx = 0
	}

	return idx
}

// hash computes hash of a key using SHA-256 for uniform distribution
func (h *HashRing) hash(key string) uint64 {
	has := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(has[:8])
}

// Size returns the number of physical nodes
func (h *HashRing) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.nodes)
}

// Nodes returns all node IDs
func (h *HashRing) Nodes() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	nodes := make([]string, 0, len(h.nodes))
	for nodeID := range h.nodes {
		nodes = append(nodes, nodeID)
	}
	return nodes
}

// GetNodePartitions returns all partitions assigned to a node.
// FIX: Previously called GetPartitionNodes() which re-acquired RLock,
// causing deadlock since Go's sync.RWMutex is not reentrant.
// Now uses getPartitionNodesLocked() which skips locking.
func (h *HashRing) GetNodePartitions(nodeID string, numPartitions int) []int32 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	partitions := make([]int32, 0)
	for i := 0; i < numPartitions; i++ {
		nodes := h.getPartitionNodesLocked(int32(i))
		for _, n := range nodes {
			if n == nodeID {
				partitions = append(partitions, int32(i))
				break
			}
		}
	}
	return partitions
}

// GetPartitionAssignments returns partition assignments for all nodes.
// FIX: Same recursive RLock deadlock as GetNodePartitions.
func (h *HashRing) GetPartitionAssignments(numPartitions int) map[int32][]string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	assignments := make(map[int32][]string)
	for i := 0; i < numPartitions; i++ {
		assignments[int32(i)] = h.getPartitionNodesLocked(int32(i))
	}
	return assignments
}

// Rebalance returns the partition movements needed when a node joins/leaves
func (h *HashRing) Rebalance(oldAssignments map[int32][]string, numPartitions int) []PartitionMove {
	newAssignments := h.GetPartitionAssignments(numPartitions)

	moves := make([]PartitionMove, 0)
	for partitionID, newNodes := range newAssignments {
		oldNodes, exists := oldAssignments[partitionID]
		if !exists {
			// New partition
			for i, nodeID := range newNodes {
				moves = append(moves, PartitionMove{
					PartitionID: partitionID,
					FromNode:    "",
					ToNode:      nodeID,
					IsLeader:    i == 0,
				})
			}
			continue
		}

		// Check for changes in leader
		if len(oldNodes) > 0 && len(newNodes) > 0 && oldNodes[0] != newNodes[0] {
			moves = append(moves, PartitionMove{
				PartitionID: partitionID,
				FromNode:    oldNodes[0],
				ToNode:      newNodes[0],
				IsLeader:    true,
			})
		}

		// Check for replica changes
		oldSet := make(map[string]bool)
		for _, n := range oldNodes {
			oldSet[n] = true
		}

		for i, newNode := range newNodes {
			if !oldSet[newNode] {
				// New replica
				moves = append(moves, PartitionMove{
					PartitionID: partitionID,
					FromNode:    "",
					ToNode:      newNode,
					IsLeader:    i == 0,
				})
			}
		}
	}

	return moves
}

// PartitionMove represents a partition movement during rebalancing
type PartitionMove struct {
	PartitionID int32
	FromNode    string
	ToNode      string
	IsLeader    bool
}

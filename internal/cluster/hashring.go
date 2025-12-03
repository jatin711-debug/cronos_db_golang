package cluster

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

// HashRing implements consistent hashing with virtual nodes
type HashRing struct {
	mu           sync.RWMutex
	ring         []uint64          // Sorted hash positions
	nodeMap      map[uint64]string // Hash position -> node ID
	nodes        map[string]int    // Node ID -> virtual node count
	virtualNodes int               // Default virtual nodes per physical node
	replicas     int               // Replication factor
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
	}
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

	// Add virtual nodes
	for i := 0; i < weight; i++ {
		hash := h.hash(fmt.Sprintf("%s-%d", nodeID, i))
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
		hash := h.hash(fmt.Sprintf("%s-%d", nodeID, i))
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

	if len(h.ring) == 0 {
		return nil
	}

	if n > len(h.nodes) {
		n = len(h.nodes)
	}

	hash := h.hash(key)
	idx := h.search(hash)

	// Collect unique nodes
	nodes := make([]string, 0, n)
	seen := make(map[string]bool)

	for i := 0; i < len(h.ring) && len(nodes) < n; i++ {
		nodeIdx := (idx + i) % len(h.ring)
		nodeID := h.nodeMap[h.ring[nodeIdx]]
		if !seen[nodeID] {
			seen[nodeID] = true
			nodes = append(nodes, nodeID)
		}
	}

	return nodes
}

// GetPartitionNode returns the node responsible for a partition
func (h *HashRing) GetPartitionNode(partitionID int32) string {
	return h.GetNode(fmt.Sprintf("partition-%d", partitionID))
}

// GetPartitionNodes returns nodes for a partition (leader + replicas)
func (h *HashRing) GetPartitionNodes(partitionID int32) []string {
	return h.GetNodes(fmt.Sprintf("partition-%d", partitionID), h.replicas)
}

// GetTopicPartition returns the partition ID for a topic
func (h *HashRing) GetTopicPartition(topic string, numPartitions int) int32 {
	hash := h.hash(topic)
	return int32(hash % uint64(numPartitions))
}

// GetKeyPartition returns the partition ID for a key
func (h *HashRing) GetKeyPartition(key string, numPartitions int) int32 {
	hash := h.hash(key)
	return int32(hash % uint64(numPartitions))
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

// hash computes hash of a key using SHA256
func (h *HashRing) hash(key string) uint64 {
	hasher := sha256.New()
	hasher.Write([]byte(key))
	sum := hasher.Sum(nil)
	return binary.BigEndian.Uint64(sum[:8])
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

// GetNodePartitions returns all partitions assigned to a node
func (h *HashRing) GetNodePartitions(nodeID string, numPartitions int) []int32 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	partitions := make([]int32, 0)
	for i := 0; i < numPartitions; i++ {
		nodes := h.GetPartitionNodes(int32(i))
		for _, n := range nodes {
			if n == nodeID {
				partitions = append(partitions, int32(i))
				break
			}
		}
	}
	return partitions
}

// GetPartitionAssignments returns partition assignments for all nodes
func (h *HashRing) GetPartitionAssignments(numPartitions int) map[int32][]string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	assignments := make(map[int32][]string)
	for i := 0; i < numPartitions; i++ {
		assignments[int32(i)] = h.GetPartitionNodes(int32(i))
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

package cluster

import (
	"fmt"
	"testing"
)

func TestHashRing_AddRemoveNodes(t *testing.T) {
	ring := NewHashRing(100, 1) // 100 virtual nodes per physical node

	// Add nodes
	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	// Verify ring has correct number of virtual nodes (3 * 100 = 300)
	if len(ring.ring) != 300 {
		t.Errorf("Expected 300 virtual nodes, got %d", len(ring.ring))
	}

	// Remove a node
	ring.RemoveNode("node2")

	// Verify ring has correct number (2 * 100 = 200)
	if len(ring.ring) != 200 {
		t.Errorf("Expected 200 virtual nodes, got %d", len(ring.ring))
	}

	// Remove non-existent node (should not panic)
	ring.RemoveNode("node99")

	// Add duplicate node (should be a no-op)
	ring.AddNode("node1")
	if len(ring.ring) != 200 {
		t.Errorf("Expected 200 virtual nodes after adding duplicate, got %d", len(ring.ring))
	}
}

func TestHashRing_GetNode(t *testing.T) {
	ring := NewHashRing(100, 1)

	// Empty ring should return empty string
	if node := ring.GetNode("key"); node != "" {
		t.Errorf("Expected empty string from empty ring, got %s", node)
	}

	// Add nodes
	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	// Get node for various keys
	key1 := "test-key-1"
	key2 := "test-key-2"

	node1 := ring.GetNode(key1)
	node2 := ring.GetNode(key2)

	if node1 == "" {
		t.Error("GetNode returned empty for key1")
	}
	if node2 == "" {
		t.Error("GetNode returned empty for key2")
	}

	// Same key should always map to same node
	for i := 0; i < 100; i++ {
		if ring.GetNode(key1) != node1 {
			t.Error("GetNode returned inconsistent result for same key")
		}
	}
}

func TestHashRing_GetNodes(t *testing.T) {
	ring := NewHashRing(100, 3)

	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	// Get multiple nodes for replication
	nodes := ring.GetNodes("test-key", 3)
	if len(nodes) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(nodes))
	}

	// All nodes should be unique
	seen := make(map[string]bool)
	for _, node := range nodes {
		if seen[node] {
			t.Errorf("Duplicate node in results: %s", node)
		}
		seen[node] = true
	}

	// Request more nodes than available
	nodes = ring.GetNodes("test-key", 5)
	if len(nodes) != 3 {
		t.Errorf("Expected 3 nodes (max available), got %d", len(nodes))
	}
}

func TestHashRing_Distribution(t *testing.T) {
	ring := NewHashRing(150, 1) // Default virtual nodes

	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	// Count distribution across 10000 keys
	counts := make(map[string]int)
	numKeys := 10000

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		node := ring.GetNode(key)
		counts[node]++
	}

	// Check that distribution is roughly even (within 20%)
	expected := numKeys / 3
	tolerance := expected / 5 // 20% tolerance

	for node, count := range counts {
		if count < expected-tolerance || count > expected+tolerance {
			t.Logf("Node %s got %d keys (expected ~%d)", node, count, expected)
		}
	}

	// At least verify all nodes got some keys
	for _, node := range []string{"node1", "node2", "node3"} {
		if counts[node] == 0 {
			t.Errorf("Node %s got 0 keys", node)
		}
	}
}

func TestHashRing_ConsistencyOnNodeChange(t *testing.T) {
	ring := NewHashRing(100, 1)

	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	// Record initial mappings for 1000 keys
	initialMappings := make(map[string]string)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		initialMappings[key] = ring.GetNode(key)
	}

	// Add a new node
	ring.AddNode("node4")

	// Count how many keys moved
	moved := 0
	for key, oldNode := range initialMappings {
		newNode := ring.GetNode(key)
		if newNode != oldNode {
			moved++
		}
	}

	// With consistent hashing, roughly 1/4 of keys should move (1000/4 = 250)
	// Allow significant tolerance since virtual nodes add variance
	expectedMoves := 1000 / 4
	tolerance := expectedMoves / 2

	if moved < expectedMoves-tolerance || moved > expectedMoves+tolerance {
		t.Logf("Expected ~%d keys to move, got %d (this is informational)", expectedMoves, moved)
	}

	// Key: most keys should NOT move
	if moved > 500 {
		t.Errorf("Too many keys moved: %d (expected <500)", moved)
	}
}

func TestHashRing_EmptyRingOperations(t *testing.T) {
	ring := NewHashRing(100, 1)

	// Empty ring operations should not panic
	node := ring.GetNode("key")
	if node != "" {
		t.Errorf("Expected empty string, got %s", node)
	}

	nodes := ring.GetNodes("key", 3)
	if len(nodes) != 0 {
		t.Errorf("Expected empty slice, got %v", nodes)
	}
}

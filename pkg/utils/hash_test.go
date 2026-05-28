package utils

import (
	"testing"
)

func TestNewCRC32Calculator(t *testing.T) {
	c := NewCRC32Calculator()
	if c == nil {
		t.Fatal("NewCRC32Calculator should not return nil")
	}
}

func TestCRC32Calculator_Calculate(t *testing.T) {
	c := NewCRC32Calculator()
	data := []byte("hello world")
	checksum := c.Calculate(data)
	if checksum == 0 {
		t.Error("checksum should not be 0")
	}

	// Same data should give same checksum
	checksum2 := c.Calculate(data)
	if checksum != checksum2 {
		t.Error("same data should give same checksum")
	}
}

func TestCRC32Calculator_Verify(t *testing.T) {
	c := NewCRC32Calculator()
	data := []byte("test data")
	checksum := c.Calculate(data)

	if !c.Verify(data, checksum) {
		t.Error("should verify correct checksum")
	}
	if c.Verify(data, checksum+1) {
		t.Error("should not verify incorrect checksum")
	}
}

func TestNewSHA1Hasher(t *testing.T) {
	h := NewSHA1Hasher()
	if h == nil {
		t.Fatal("NewSHA1Hasher should not return nil")
	}
}

func TestSHA1Hasher_Hash(t *testing.T) {
	h := NewSHA1Hasher()
	hash1 := h.Hash([]byte("hello"))
	hash2 := h.Hash([]byte("hello"))
	hash3 := h.Hash([]byte("world"))

	if hash1 == "" {
		t.Error("hash should not be empty")
	}
	if hash1 != hash2 {
		t.Error("same data should give same hash")
	}
	if hash1 == hash3 {
		t.Error("different data should give different hash")
	}
	if len(hash1) != 40 {
		t.Errorf("expected 40 hex chars for SHA1, got %d", len(hash1))
	}
}

func TestSHA1Hasher_Verify(t *testing.T) {
	h := NewSHA1Hasher()
	data := []byte("verify me")
	hash := h.Hash(data)

	if !h.Verify(data, hash) {
		t.Error("should verify correct hash")
	}
	if h.Verify(data, hash+"extra") {
		t.Error("should not verify incorrect hash")
	}
}

func TestNewConsistentHash(t *testing.T) {
	c := NewConsistentHash()
	if c == nil {
		t.Fatal("NewConsistentHash should not return nil")
	}
}

func TestConsistentHash_Add(t *testing.T) {
	c := NewConsistentHash()
	c.Add("node-1")
	c.Add("node-2")
	c.Add("node-3")

	if len(c.ring) == 0 {
		t.Error("ring should not be empty after adding nodes")
	}
}

func TestConsistentHash_Get(t *testing.T) {
	c := NewConsistentHash()
	c.Add("node-1")
	c.Add("node-2")

	node := c.Get("key-1")
	if node == "" {
		t.Error("should return a node")
	}

	// Same key should map to same node
	node2 := c.Get("key-1")
	if node != node2 {
		t.Error("same key should map to same node")
	}
}

func TestConsistentHash_Get_Empty(t *testing.T) {
	c := NewConsistentHash()
	node := c.Get("key-1")
	if node != "" {
		t.Error("empty ring should return empty string")
	}
}

func TestConsistentHash_Remove(t *testing.T) {
	c := NewConsistentHash()
	c.Add("node-1")
	c.Add("node-2")

	c.Remove("node-1")

	node := c.Get("any-key")
	if node == "node-1" {
		t.Error("should not return removed node")
	}
}

func TestConsistentHash_GetN(t *testing.T) {
	c := NewConsistentHash()
	c.Add("node-1")
	c.Add("node-2")
	c.Add("node-3")

	nodes := c.GetN("key-1", 2)
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(nodes))
	}

	// Should deduplicate
	if nodes[0] == nodes[1] {
		t.Error("GetN should return distinct nodes")
	}
}

func TestConsistentHash_GetN_MoreThanAvailable(t *testing.T) {
	c := NewConsistentHash()
	c.Add("node-1")

	nodes := c.GetN("key-1", 5)
	// GetN fills remaining slots by wrapping around
	if len(nodes) != 5 {
		t.Errorf("expected 5 nodes (filled by wrapping), got %d", len(nodes))
	}
}

func TestHashToPartitionID(t *testing.T) {
	id1 := HashToPartitionID("topic-a", 8)
	id2 := HashToPartitionID("topic-b", 8)
	id1Again := HashToPartitionID("topic-a", 8)

	if id1 < 0 || id1 >= 8 {
		t.Errorf("id1 out of range: %d", id1)
	}
	if id2 < 0 || id2 >= 8 {
		t.Errorf("id2 out of range: %d", id2)
	}
	if id1 != id1Again {
		t.Errorf("same input should give same id, got %d and %d", id1, id1Again)
	}
}

func TestHashToPartitionID_Deterministic(t *testing.T) {
	inputs := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	for _, input := range inputs {
		id := HashToPartitionID(input, 8)
		if id < 0 || id >= 8 {
			t.Errorf("%s -> %d out of range", input, id)
		}
	}
}

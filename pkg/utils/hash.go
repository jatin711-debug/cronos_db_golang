// Package utils provides small shared helpers used by the server and client SDK:
// hashing, panic-safe goroutines, and atomic file writes.
package utils

import (
	"crypto/sha1"
	"encoding/hex"
	"hash"
	"hash/crc32"
	"hash/fnv"
	"slices"
)

// Hash is a minimal hashing interface used by older helpers.
type Hash interface {
	// Sum appends the current hash to b and returns the resulting slice.
	Sum(b []byte) []byte
	// Reset clears the hasher state.
	Reset()
}

// CRC32Calculator computes and verifies IEEE CRC32 checksums (WAL/record integrity).
type CRC32Calculator struct {
	crc uint32 // reserved for streaming use; Calculate uses IEEE over full buffers
}

// NewCRC32Calculator returns a new CRC32Calculator.
func NewCRC32Calculator() *CRC32Calculator {
	return &CRC32Calculator{}
}

// Calculate returns the IEEE CRC32 of data.
func (c *CRC32Calculator) Calculate(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// Verify reports whether data's CRC32 matches expected.
func (c *CRC32Calculator) Verify(data []byte, expected uint32) bool {
	return c.Calculate(data) == expected
}

// SHA1Hasher computes hex-encoded SHA-1 digests.
type SHA1Hasher struct {
	hasher hash.Hash // reusable SHA-1 state
}

// NewSHA1Hasher returns a SHA1Hasher with a fresh SHA-1 instance.
func NewSHA1Hasher() *SHA1Hasher {
	return &SHA1Hasher{
		hasher: sha1.New(),
	}
}

// Hash returns the hex-encoded SHA-1 of data.
func (h *SHA1Hasher) Hash(data []byte) string {
	h.hasher.Reset()
	h.hasher.Write(data)
	hash := h.hasher.Sum(nil)
	return hex.EncodeToString(hash)
}

// Verify reports whether data's SHA-1 hex digest equals hashStr.
func (h *SHA1Hasher) Verify(data []byte, hashStr string) bool {
	calculated := h.Hash(data)
	return calculated == hashStr
}

// ConsistentHash is a simple CRC32-based consistent hash ring (single vnode per node).
// Prefer the cluster hashring for production placement; this helper is a lightweight utility.
type ConsistentHash struct {
	ring       map[uint32]string // hash position → node name
	sortedKeys []uint32          // sorted ring positions for clockwise walks
}

// NewConsistentHash creates an empty consistent hash ring.
func NewConsistentHash() *ConsistentHash {
	return &ConsistentHash{
		ring:       make(map[uint32]string),
		sortedKeys: []uint32{},
	}
}

// Add inserts a node onto the ring at CRC32(node).
func (c *ConsistentHash) Add(node string) {
	// Simplified - would use multiple virtual nodes in production
	hash := crc32.ChecksumIEEE([]byte(node))
	c.ring[hash] = node
	c.sortedKeys = append(c.sortedKeys, hash)
}

// Remove deletes a node from the ring by its CRC32(node) position.
func (c *ConsistentHash) Remove(node string) {
	hash := crc32.ChecksumIEEE([]byte(node))
	delete(c.ring, hash)
	// Simplified - would rebuild sortedKeys in production
}

// Get returns the node responsible for key (first ring position clockwise), or "".
func (c *ConsistentHash) Get(key string) string {
	if len(c.ring) == 0 {
		return ""
	}

	hash := crc32.ChecksumIEEE([]byte(key))

	// Find the first node clockwise from the hash
	for _, ringHash := range c.sortedKeys {
		if hash <= ringHash {
			return c.ring[ringHash]
		}
	}

	// Wrap around to the first node
	return c.ring[c.sortedKeys[0]]
}

// GetN returns up to n distinct nodes for key (replication-style placement).
// This implementation is simplified and may duplicate nodes if the ring is small.
func (c *ConsistentHash) GetN(key string, n int) []string {
	if len(c.ring) == 0 {
		return nil
	}

	hash := crc32.ChecksumIEEE([]byte(key))
	result := make([]string, 0, n)
	idx := 0

	for len(result) < n && idx < len(c.sortedKeys) {
		for _, ringHash := range c.sortedKeys[idx:] {
			if hash <= ringHash {
				node := c.ring[ringHash]
				// Check if node is already in result
				exists := slices.Contains(result, node)
				if !exists {
					result = append(result, node)
					break
				}
			}
		}
		idx++
	}

	// If we don't have enough unique nodes, wrap around
	// This is a simplified implementation
	for len(result) < n && len(result) > 0 {
		result = append(result, result[0])
		if len(result) >= n {
			break
		}
	}

	return result
}

// HashToPartitionID computes a stable partition ID from an input string using
// FNV-1a (fast, zero-alloc, excellent distribution for routing).
// All nodes derive the same partition ID for a given topic/key since FNV-1a
// is deterministic. Replaced SHA-256 (~400ns, heap alloc) which was overkill
// for non-cryptographic partition routing.
func HashToPartitionID(input string, numPartitions int) int32 {
	if numPartitions <= 0 {
		return 0
	}
	h := fnv.New64a()
	h.Write([]byte(input))
	return int32(h.Sum64() % uint64(numPartitions))
}

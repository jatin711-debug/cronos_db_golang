package utils

import (
	"crypto/sha1"
	"encoding/hex"
	"hash"
	"hash/crc32"
)

// Hash is a simple interface for hashing
type Hash interface {
	Sum(b []byte) []byte
	Reset()
}

// CRC32Calculator calculates CRC32 checksums
type CRC32Calculator struct {
	crc uint32
}

func NewCRC32Calculator() *CRC32Calculator {
	return &CRC32Calculator{}
}

func (c *CRC32Calculator) Calculate(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func (c *CRC32Calculator) Verify(data []byte, expected uint32) bool {
	return c.Calculate(data) == expected
}

// SHA1Hasher calculates SHA1 hashes
type SHA1Hasher struct {
	hasher hash.Hash
}

func NewSHA1Hasher() *SHA1Hasher {
	return &SHA1Hasher{
		hasher: sha1.New(),
	}
}

func (h *SHA1Hasher) Hash(data []byte) string {
	h.hasher.Reset()
	h.hasher.Write(data)
	hash := h.hasher.Sum(nil)
	return hex.EncodeToString(hash)
}

func (h *SHA1Hasher) Verify(data []byte, hashStr string) bool {
	calculated := h.Hash(data)
	return calculated == hashStr
}

// ConsistentHash represents a consistent hashing ring
type ConsistentHash struct {
	ring       map[uint32]string
	sortedKeys []uint32
}

// NewConsistentHash creates a new consistent hash
func NewConsistentHash() *ConsistentHash {
	return &ConsistentHash{
		ring:       make(map[uint32]string),
		sortedKeys: []uint32{},
	}
}

// Add adds a node to the hash ring
func (c *ConsistentHash) Add(node string) {
	// Simplified - would use multiple virtual nodes in production
	hash := crc32.ChecksumIEEE([]byte(node))
	c.ring[hash] = node
	c.sortedKeys = append(c.sortedKeys, hash)
}

// Remove removes a node from the hash ring
func (c *ConsistentHash) Remove(node string) {
	hash := crc32.ChecksumIEEE([]byte(node))
	delete(c.ring, hash)
	// Simplified - would rebuild sortedKeys in production
}

// Get finds the node responsible for a key
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

// GetN finds N nodes for a key (replication)
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
				exists := false
				for _, n := range result {
					if n == node {
						exists = true
						break
					}
				}
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

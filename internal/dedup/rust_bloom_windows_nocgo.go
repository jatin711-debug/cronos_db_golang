//go:build windows && !cgo

package dedup

// RustBloomFilter is a stub when cgo is disabled on Windows.
// NewRustBloomFilter always returns nil so callers use GoBloomFilter instead.
type RustBloomFilter struct{}

// NewRustBloomFilter returns nil so callers fall back to the pure-Go bloom filter.
func NewRustBloomFilter(expectedItems uint64, fpr float64) *RustBloomFilter {
	return nil
}

// Add is a no-op on the nocgo stub.
func (bf *RustBloomFilter) Add(key string) {}

// MayContain always returns false on the nocgo stub.
func (bf *RustBloomFilter) MayContain(key string) bool { return false }

// MayContainBatch returns a zeroed result slice on the nocgo stub.
func (bf *RustBloomFilter) MayContainBatch(keys []string) []bool {
	out := make([]bool, len(keys))
	return out
}

// Count always returns 0 on the nocgo stub.
func (bf *RustBloomFilter) Count() uint64 { return 0 }

// Reset is a no-op on the nocgo stub.
func (bf *RustBloomFilter) Reset() {}

// MemoryUsageBytes always returns 0 on the nocgo stub.
func (bf *RustBloomFilter) MemoryUsageBytes() uint64 { return 0 }

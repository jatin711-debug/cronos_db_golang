//go:build windows && !cgo

package dedup

// RustBloomFilter is unavailable when cgo is disabled on Windows.
type RustBloomFilter struct{}

// NewRustBloomFilter returns nil so callers fall back to the pure-Go bloom filter.
func NewRustBloomFilter(expectedItems uint64, fpr float64) *RustBloomFilter {
	return nil
}

func (bf *RustBloomFilter) Add(key string)             {}
func (bf *RustBloomFilter) MayContain(key string) bool { return false }
func (bf *RustBloomFilter) MayContainBatch(keys []string) []bool {
	out := make([]bool, len(keys))
	return out
}
func (bf *RustBloomFilter) Count() uint64            { return 0 }
func (bf *RustBloomFilter) Reset()                   {}
func (bf *RustBloomFilter) MemoryUsageBytes() uint64 { return 0 }

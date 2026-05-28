//go:build windows

package dedup

import "fmt"

// RustBloomFilter is a stub on Windows — the Rust FFI library is not available.
type RustBloomFilter struct{}

func NewRustBloomFilter(expectedItems uint64, fpr float64) *RustBloomFilter {
	return nil
}

func (bf *RustBloomFilter) Add(key string) {
	panic(fmt.Errorf("RustBloomFilter is not available on Windows"))
}

func (bf *RustBloomFilter) MayContain(key string) bool {
	panic(fmt.Errorf("RustBloomFilter is not available on Windows"))
}

func (bf *RustBloomFilter) MayContainBatch(keys []string) []bool {
	panic(fmt.Errorf("RustBloomFilter is not available on Windows"))
}

func (bf *RustBloomFilter) Count() uint64 { return 0 }

func (bf *RustBloomFilter) Reset() {}

func (bf *RustBloomFilter) MemoryUsageBytes() uint64 { return 0 }

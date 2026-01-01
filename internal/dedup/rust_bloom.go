package dedup

/*
#cgo LDFLAGS: -L./rust/target/release -lcronos_dedup -lm
// Windows specific: might need Reference to .lib?
// Go on Windows usually links against .dll if -l specifies it?
// Usually needs import library. Rust produces .dll.lib for .dll.

#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>

void* bloom_new(unsigned long long expected_items, double fpr);
void bloom_free(void* ptr);
void bloom_add(void* ptr, const unsigned char* key, size_t len);
bool bloom_check(void* ptr, const unsigned char* key, size_t len);
void bloom_check_batch(void* ptr, const unsigned char** keys, const size_t* keys_lens, size_t num_keys, bool* results);
*/
import "C"
import (
	"runtime"
	"unsafe"
)

// RustBloomFilter is a wrapper around the Rust implementation
type RustBloomFilter struct {
	ptr unsafe.Pointer
}

// NewRustBloomFilter creates a new Rust-backed bloom filter
func NewRustBloomFilter(expectedItems uint64, fpr float64) *RustBloomFilter {
	ptr := C.bloom_new(C.ulonglong(expectedItems), C.double(fpr))
	bf := &RustBloomFilter{ptr: ptr}

	// Ensure memory is freed when Go object is GC'd
	runtime.SetFinalizer(bf, func(f *RustBloomFilter) {
		C.bloom_free(f.ptr)
	})

	return bf
}

func (bf *RustBloomFilter) Add(key string) {
	// Unsafe string data access (no copy)
	// Key is not stored by Rust, only read, so this is safe for duration of call
	p := unsafe.StringData(key)
	C.bloom_add(bf.ptr, (*C.uchar)(unsafe.Pointer(p)), C.size_t(len(key)))
}

func (bf *RustBloomFilter) MayContain(key string) bool {
	p := unsafe.StringData(key)
	return bool(C.bloom_check(bf.ptr, (*C.uchar)(unsafe.Pointer(p)), C.size_t(len(key))))
}

func (bf *RustBloomFilter) MayContainBatch(keys []string) []bool {
	n := len(keys)
	if n == 0 {
		return nil
	}

	// Allocate arrays for C transfer
	// Note: optimization possible with arena / sync.Pool
	keyPtrs := make([]*C.uchar, n)
	keyLens := make([]C.size_t, n)
	results := make([]C.bool, n)

	for i, k := range keys {
		keyPtrs[i] = (*C.uchar)(unsafe.Pointer(unsafe.StringData(k)))
		keyLens[i] = C.size_t(len(k))
	}

	C.bloom_check_batch(
		bf.ptr,
		(**C.uchar)(unsafe.Pointer(&keyPtrs[0])),
		(*C.size_t)(unsafe.Pointer(&keyLens[0])),
		C.size_t(n),
		(*C.bool)(unsafe.Pointer(&results[0])),
	)

	goResults := make([]bool, n)
	for i, r := range results {
		goResults[i] = bool(r)
	}

	return goResults
}

func (bf *RustBloomFilter) Count() uint64 {
	// Not implemented in Rust side yet, return 0 or track in Go
	return 0
}

func (bf *RustBloomFilter) Reset() {
	// Not implemented in Rust side yet
	// Re-create?
}

func (bf *RustBloomFilter) MemoryUsageBytes() uint64 {
	return 0 // TODO
}

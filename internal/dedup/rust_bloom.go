package dedup

/*
#cgo LDFLAGS: -L${SRCDIR}/rust/target/release -lcronos_dedup -lm
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
unsigned long long bloom_count(void* ptr);
unsigned long long bloom_memory_usage(void* ptr);
void bloom_reset(void* ptr);
*/
import "C"
import (
	"runtime"
	"sync"
	"unsafe"
)

// RustBloomFilter is a wrapper around the Rust implementation
type RustBloomFilter struct {
	ptr unsafe.Pointer
}

type rustBatchScratch struct {
	keyPtrs  []*C.uchar
	keyLens  []C.size_t
	results  []C.bool
	goResult []bool
}

var rustBatchScratchPool = sync.Pool{
	New: func() interface{} {
		return &rustBatchScratch{}
	},
}

func ensureBatchCap[T any](s []T, n int) []T {
	if cap(s) < n {
		return make([]T, n)
	}
	return s[:n]
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

	scratch := rustBatchScratchPool.Get().(*rustBatchScratch)
	defer rustBatchScratchPool.Put(scratch)

	keyPtrs := ensureBatchCap(scratch.keyPtrs, n)
	keyLens := ensureBatchCap(scratch.keyLens, n)
	results := ensureBatchCap(scratch.results, n)
	goResults := ensureBatchCap(scratch.goResult, n)

	scratch.keyPtrs = keyPtrs
	scratch.keyLens = keyLens
	scratch.results = results
	scratch.goResult = goResults

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

	for i, r := range results {
		goResults[i] = bool(r)
	}

	out := make([]bool, n)
	copy(out, goResults)
	return out
}

func (bf *RustBloomFilter) Count() uint64 {
	return uint64(C.bloom_count(bf.ptr))
}

func (bf *RustBloomFilter) Reset() {
	C.bloom_reset(bf.ptr)
}

func (bf *RustBloomFilter) MemoryUsageBytes() uint64 {
	return uint64(C.bloom_memory_usage(bf.ptr))
}

//go:build windows && cgo

package dedup

/*
#cgo LDFLAGS: -L${SRCDIR}/rust/target/release -lcronos_dedup

#include <stdlib.h>
#include <string.h>
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
	"unsafe"
)

// RustBloomFilter is a wrapper around the Rust implementation.
// On Windows this binds to the staged cronos_dedup.dll via its import library.
type RustBloomFilter struct {
	ptr unsafe.Pointer
}

// NewRustBloomFilter creates a new Rust-backed bloom filter.
func NewRustBloomFilter(expectedItems uint64, fpr float64) *RustBloomFilter {
	ptr := C.bloom_new(C.ulonglong(expectedItems), C.double(fpr))
	if ptr == nil {
		return nil
	}
	bf := &RustBloomFilter{ptr: ptr}

	// Ensure memory is freed when the Go object is GC'd.
	runtime.SetFinalizer(bf, func(f *RustBloomFilter) {
		C.bloom_free(f.ptr)
	})

	return bf
}

func (bf *RustBloomFilter) Add(key string) {
	p := unsafe.StringData(key)
	C.bloom_add(bf.ptr, (*C.uchar)(unsafe.Pointer(p)), C.size_t(len(key)))
	runtime.KeepAlive(key)
	runtime.KeepAlive(bf)
}

func (bf *RustBloomFilter) MayContain(key string) bool {
	p := unsafe.StringData(key)
	result := bool(C.bloom_check(bf.ptr, (*C.uchar)(unsafe.Pointer(p)), C.size_t(len(key))))
	runtime.KeepAlive(key)
	runtime.KeepAlive(bf)
	return result
}

func (bf *RustBloomFilter) MayContainBatch(keys []string) []bool {
	n := len(keys)
	if n == 0 {
		return nil
	}

	totalLen := 0
	for _, k := range keys {
		totalLen += len(k)
	}
	keySlab := C.malloc(C.size_t(totalLen + 1))
	if keySlab == nil {
		panic("bloom_check_batch: cgo slab malloc failed")
	}
	defer C.free(keySlab)

	keyPtrs := make([]*C.uchar, n)
	keyLens := make([]C.size_t, n)
	results := make([]C.bool, n)

	offset := 0
	for i, k := range keys {
		keyPtrs[i] = (*C.uchar)(unsafe.Add(keySlab, offset))
		keyLens[i] = C.size_t(len(k))
		if len(k) > 0 {
			C.memcpy(
				unsafe.Pointer(keyPtrs[i]),
				unsafe.Pointer(unsafe.StringData(k)),
				C.size_t(len(k)),
			)
		}
		offset += len(k)
	}

	C.bloom_check_batch(
		bf.ptr,
		(**C.uchar)(unsafe.Pointer(&keyPtrs[0])),
		(*C.size_t)(unsafe.Pointer(&keyLens[0])),
		C.size_t(n),
		(*C.bool)(unsafe.Pointer(&results[0])),
	)

	out := make([]bool, n)
	for i, r := range results {
		out[i] = bool(r)
	}
	runtime.KeepAlive(bf)
	return out
}

func (bf *RustBloomFilter) Count() uint64 {
	result := uint64(C.bloom_count(bf.ptr))
	runtime.KeepAlive(bf)
	return result
}

func (bf *RustBloomFilter) Reset() {
	C.bloom_reset(bf.ptr)
	runtime.KeepAlive(bf)
}

func (bf *RustBloomFilter) MemoryUsageBytes() uint64 {
	result := uint64(C.bloom_memory_usage(bf.ptr))
	runtime.KeepAlive(bf)
	return result
}

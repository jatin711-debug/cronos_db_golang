//go:build !windows

package dedup

/*
#cgo LDFLAGS: -L${SRCDIR}/rust/target/release -lcronos_dedup -lm
// Windows specific: might need Reference to .lib?
// Go on Windows usually links against .dll if -l specifies it?
// Usually needs import library. Rust produces .dll.lib for .dll.

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

// RustBloomFilter is a cgo wrapper around the Rust cronos_dedup bloom filter.
// Keys are not retained by Rust; only bits are updated during Add/check.
type RustBloomFilter struct {
	ptr unsafe.Pointer // opaque handle owned by the Rust library
}

// NewRustBloomFilter creates a Rust-backed bloom filter sized for expectedItems at fpr.
// A finalizer frees the native allocation when the Go object is GC'd.
func NewRustBloomFilter(expectedItems uint64, fpr float64) *RustBloomFilter {
	ptr := C.bloom_new(C.ulonglong(expectedItems), C.double(fpr))
	bf := &RustBloomFilter{ptr: ptr}

	// Ensure memory is freed when Go object is GC'd
	runtime.SetFinalizer(bf, func(f *RustBloomFilter) {
		C.bloom_free(f.ptr)
	})

	return bf
}

// Add inserts key into the native bloom filter without copying the string.
func (bf *RustBloomFilter) Add(key string) {
	// Unsafe string data access (no copy)
	// Key is not stored by Rust, only read, so this is safe for duration of call
	p := unsafe.StringData(key)
	C.bloom_add(bf.ptr, (*C.uchar)(unsafe.Pointer(p)), C.size_t(len(key)))
}

// MayContain reports whether key might be present in the native bloom filter.
func (bf *RustBloomFilter) MayContain(key string) bool {
	p := unsafe.StringData(key)
	return bool(C.bloom_check(bf.ptr, (*C.uchar)(unsafe.Pointer(p)), C.size_t(len(key))))
}

// MayContainBatch checks multiple keys in a single cgo crossing.
func (bf *RustBloomFilter) MayContainBatch(keys []string) []bool {
	n := len(keys)
	if n == 0 {
		return nil
	}

	// CGo's pointer-passing rules forbid passing a Go pointer to memory that
	// itself contains Go pointers (e.g. a []*C.uchar whose elements point at
	// Go string buffers). However, a Go slice that contains ONLY C pointers
	// (from C.malloc) is compliant. We exploit this to keep the pointer,
	// length, and result arrays on the Go heap (fast, GC-managed) and copy
	// all key bytes into a single contiguous C-allocated slab — collapsing
	// what was previously N+3 C mallocs + N memcpys + N+3 frees down to a
	// single C malloc + N memcpys + a single free per batch.

	// 1. Compute total key bytes so we can allocate one contiguous slab.
	totalLen := 0
	for _, k := range keys {
		totalLen += len(k)
	}
	// Allocate at least 1 byte so empty-key batches still have a non-nil
	// pointer to pass into Rust (slice::from_raw_parts requires non-null).
	keySlab := C.malloc(C.size_t(totalLen + 1))
	if keySlab == nil {
		panic("bloom_check_batch: cgo slab malloc failed")
	}
	defer C.free(keySlab)

	// 2. Go-heap arrays holding C pointers / scalar values — cgo-rule-clean.
	keyPtrs := make([]*C.uchar, n)
	keyLens := make([]C.size_t, n)
	results := make([]C.bool, n)

	// 3. Copy each key back-to-back into the slab, recording its pointer + length.
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

	// 4. Single cgo crossing: pass the Go arrays (containing only C pointers /
	//    scalars) into Rust. &keyPtrs[0] points at Go memory holding C pointers,
	//    which is the allowed case under the cgo pointer rules.
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
	return out
}

// Count returns the approximate number of items added to the native filter.
func (bf *RustBloomFilter) Count() uint64 {
	return uint64(C.bloom_count(bf.ptr))
}

// Reset clears the native bloom filter.
func (bf *RustBloomFilter) Reset() {
	C.bloom_reset(bf.ptr)
}

// MemoryUsageBytes returns the native filter's approximate memory footprint.
func (bf *RustBloomFilter) MemoryUsageBytes() uint64 {
	return uint64(C.bloom_memory_usage(bf.ptr))
}

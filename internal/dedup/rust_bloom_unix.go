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

	// CGo's pointer-passing rules forbid passing a Go pointer to memory that
	// itself contains Go pointers (e.g. a [](*C.uchar) whose elements point at
	// Go string buffers). To stay compliant on Go 1.23+ (which enforces this at
	// runtime and panics otherwise), allocate the pointer/length/result arrays
	// and per-key buffers in C and copy the key bytes across before the call.
	ptrSize := C.size_t(unsafe.Sizeof(uintptr(0)))
	sizeSize := C.size_t(unsafe.Sizeof(C.size_t(0)))
	boolSize := C.size_t(unsafe.Sizeof(C.bool(false)))

	cKeys := C.calloc(C.size_t(n), ptrSize)
	cLens := C.calloc(C.size_t(n), sizeSize)
	cResults := C.calloc(C.size_t(n), boolSize)
	if cKeys == nil || cLens == nil || cResults == nil {
		if cKeys != nil {
			C.free(cKeys)
		}
		if cLens != nil {
			C.free(cLens)
		}
		if cResults != nil {
			C.free(cResults)
		}
		panic("bloom_check_batch: cgo malloc failed")
	}
	defer C.free(cKeys)
	defer C.free(cLens)
	defer C.free(cResults)

	keyPtrArray := (*[1 << 30]*C.uchar)(cKeys)[:n:n]
	lensArray := (*[1 << 30]C.size_t)(cLens)[:n:n]
	resultsArray := (*[1 << 30]C.bool)(cResults)[:n:n]

	keyBuffers := make([]unsafe.Pointer, n)
	for i, k := range keys {
		buf := C.malloc(C.size_t(len(k)))
		if buf == nil {
			for _, b := range keyBuffers[:i] {
				if b != nil {
					C.free(b)
				}
			}
			panic("bloom_check_batch: cgo key malloc failed")
		}
		keyBuffers[i] = buf
		if len(k) == 0 {
			keyPtrArray[i] = (*C.uchar)(buf)
			lensArray[i] = 0
			continue
		}
		C.memcpy(buf, unsafe.Pointer(unsafe.StringData(k)), C.size_t(len(k)))
		keyPtrArray[i] = (*C.uchar)(buf)
		lensArray[i] = C.size_t(len(k))
	}
	defer func() {
		for _, b := range keyBuffers {
			if b != nil {
				C.free(b)
			}
		}
	}()

	C.bloom_check_batch(
		bf.ptr,
		(**C.uchar)(unsafe.Pointer(cKeys)),
		(*C.size_t)(unsafe.Pointer(cLens)),
		C.size_t(n),
		(*C.bool)(unsafe.Pointer(cResults)),
	)

	out := make([]bool, n)
	for i, r := range resultsArray {
		out[i] = bool(r)
	}
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

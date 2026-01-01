use libc::{size_t, c_void};
use std::slice;
use std::sync::atomic::{AtomicU64, Ordering};
use std::hash::Hasher;
use rayon::prelude::*;
use twox_hash::XxHash64;

// Blocked Bloom Filter for better cache locality could be better, 
// but sticking to standard global bitset for simplicity and match with Go logic initially.
// We use AtomicU64 for concurrent lock-free sets, similar to the Go implementation.

pub struct BloomFilter {
    bits: Vec<AtomicU64>,
    size_bits: u64,
    num_hashes: u64,
}

impl BloomFilter {
    fn new(expected_items: u64, fpr: f64) -> Self {
        let ln2 = 0.69314718056;
        let bits_per_item = (-1.0 * fpr.ln() / (ln2 * ln2)).ceil() as u64;
        let num_hashes = (bits_per_item as f64 * ln2).ceil() as u64;
        
        // Ensure size is multiple of 64 for atomic access
        let mut size_bits = expected_items * bits_per_item;
        size_bits = (size_bits + 63) / 64 * 64;
        
        let num_u64 = (size_bits / 64) as usize;
        let mut bits = Vec::with_capacity(num_u64);
        for _ in 0..num_u64 {
            bits.push(AtomicU64::new(0));
        }

        BloomFilter {
            bits,
            size_bits,
            num_hashes,
        }
    }

    // Double hashing: h1 + i*h2
    #[inline]
    fn get_hash_values(key: &[u8]) -> (u64, u64) {
        let mut hasher1 = XxHash64::with_seed(0);
        hasher1.write(key);
        let h1 = hasher1.finish();

        let mut hasher2 = XxHash64::with_seed(14695981039346656037); // FNV offset basis as random seed
        hasher2.write(key);
        let h2 = hasher2.finish();
        
        (h1, h2)
    }

    fn add(&self, key: &[u8]) {
        let (h1, h2) = Self::get_hash_values(key);
        
        for i in 0..self.num_hashes {
            let idx = (h1.wrapping_add(i.wrapping_mul(h2))) % self.size_bits;
            let word_idx = (idx / 64) as usize;
            let bit_mask = 1u64 << (idx % 64);
            
            // Lock-free set
            let atom = &self.bits[word_idx];
            // Relaxed ordering might be okay for bloom filters, but using Relaxed/Release consistency
            // Standard idiom is ORing. 
            // We use fetch_or which is atomic.
            atom.fetch_or(bit_mask, Ordering::Relaxed);
        }
    }

    fn contains(&self, key: &[u8]) -> bool {
        let (h1, h2) = Self::get_hash_values(key);
        
        for i in 0..self.num_hashes {
            let idx = (h1.wrapping_add(i.wrapping_mul(h2))) % self.size_bits;
            let word_idx = (idx / 64) as usize;
            let bit_mask = 1u64 << (idx % 64);
            
            if self.bits[word_idx].load(Ordering::Relaxed) & bit_mask == 0 {
                return false;
            }
        }
        true
    }
}

// FFI Interface

#[no_mangle]
pub extern "C" fn bloom_new(expected_items: u64, fpr: f64) -> *mut BloomFilter {
    let bf = BloomFilter::new(expected_items, fpr);
    Box::into_raw(Box::new(bf))
}

#[no_mangle]
pub unsafe extern "C" fn bloom_free(ptr: *mut BloomFilter) {
    if !ptr.is_null() {
        drop(Box::from_raw(ptr));
    }
}

#[no_mangle]
pub unsafe extern "C" fn bloom_add(ptr: *mut BloomFilter, key_ptr: *const u8, key_len: size_t) {
    let bf = &*ptr;
    let key = slice::from_raw_parts(key_ptr, key_len);
    bf.add(key);
}

#[no_mangle]
pub unsafe extern "C" fn bloom_check(ptr: *mut BloomFilter, key_ptr: *const u8, key_len: size_t) -> bool {
    let bf = &*ptr;
    let key = slice::from_raw_parts(key_ptr, key_len);
    bf.contains(key)
}

// Wrapper for raw pointer to make it Sync/Send for Rayon
#[repr(transparent)]
#[derive(Clone, Copy)]
struct SyncPtr(*const u8);
unsafe impl Sync for SyncPtr {}
unsafe impl Send for SyncPtr {}

// Batch check implementation - Parallelized with Rayon
#[no_mangle]
pub unsafe extern "C" fn bloom_check_batch(
    ptr: *mut BloomFilter,
    keys_array: *const *const u8,
    keys_lens: *const size_t,
    num_keys: size_t,
    results_out: *mut bool,
) {
    let bf = &*ptr;
    let safe_keys = slice::from_raw_parts(keys_array as *const SyncPtr, num_keys);
    let lens = slice::from_raw_parts(keys_lens, num_keys);
    let results = slice::from_raw_parts_mut(results_out, num_keys);

    // If batch size is small, serial is faster. If large, use parallel.
    if num_keys < 100 {
        for i in 0..num_keys {
            let key = slice::from_raw_parts(safe_keys[i].0, lens[i]);
            results[i] = bf.contains(key);
        }
    } else {
        // Parallel iterator
        results.par_iter_mut().enumerate().for_each(|(i, res)| {
            let key = unsafe { slice::from_raw_parts(safe_keys[i].0, lens[i]) };
            *res = bf.contains(key);
        });
    }
}

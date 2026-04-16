package dedup

import (
	"fmt"
	"testing"
)

func TestRustBloomIntegration(t *testing.T) {
	// Create Rust-backed bloom filter
	// 1000 items, 0.01 FPR
	bf := NewRustBloomFilter(1000, 0.01)

	// Basic Add/Check
	key1 := "test-key-1"
	key2 := "test-key-2"

	bf.Add(key1)

	if !bf.MayContain(key1) {
		t.Errorf("Expected to contain %s", key1)
	}

	if bf.MayContain(key2) {
		t.Errorf("Expected NOT to contain %s (false positive highly unlikely)", key2)
	}

	// Batch Check
	batchKeys := []string{"batch-1", "batch-2", "batch-3", "batch-4", "batch-5"}
	// Add 1, 3, 5
	bf.Add(batchKeys[0])
	bf.Add(batchKeys[2])
	bf.Add(batchKeys[4])

	results := bf.MayContainBatch(batchKeys)

	expected := []bool{true, false, true, false, true}

	for i, res := range results {
		if res != expected[i] {
			t.Errorf("Batch check failed at index %d: expected %v, got %v", i, expected[i], res)
		}
	}

	fmt.Println("✅ Rust Bloom Filter integration verified!")
}

func TestBloomPebbleStore_WithRust(t *testing.T) {
	tmpDir := t.TempDir()

	// NewBloomPebbleStore uses NewBloomFilter which calls NewRustBloomFilter
	store, err := NewBloomPebbleStore(tmpDir, 100, 1, 10000, 0.01)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Add generic item
	isDup, err := store.CheckAndStore("msg-rust-1", 100)
	if err != nil {
		t.Fatalf("CheckAndStore failed: %v", err)
	}
	if isDup {
		t.Error("First add should not be dup")
	}

	// Check dup
	isDup, err = store.CheckAndStore("msg-rust-1", 100)
	if !isDup {
		t.Error("Second add should be dup")
	}

	// Stats should reflect Bloom hits
	// First add: Bloom said NO (false), added to Bloom. Pebble store.
	// Second add: Bloom said YES (true), Checked Pebble -> YES.

	stats, _ := store.GetStats()
	if stats.PebbleHits != 1 {
		t.Errorf("Expected 1 Pebble hit, got %d", stats.PebbleHits)
	}

	fmt.Println("✅ BloomPebbleStore integration verified!")
}

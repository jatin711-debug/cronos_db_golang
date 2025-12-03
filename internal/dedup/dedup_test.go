package dedup

import (
	"sync"
	"testing"
)

func TestPebbleStore_CheckAndStore(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := NewPebbleStore(tmpDir, 0, 1) // 1 hour TTL
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// First check should be false (not a duplicate)
	isDup, err := store.CheckAndStore("msg-1", 0)
	if err != nil {
		t.Fatalf("CheckAndStore failed: %v", err)
	}
	if isDup {
		t.Error("Expected first check to return false (not duplicate)")
	}

	// Second check with same ID should be true (duplicate)
	isDup, err = store.CheckAndStore("msg-1", 1)
	if err != nil {
		t.Fatalf("CheckAndStore failed: %v", err)
	}
	if !isDup {
		t.Error("Expected second check to return true (duplicate)")
	}
}

func TestPebbleStore_Exists(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := NewPebbleStore(tmpDir, 0, 1)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Check non-existent
	exists, err := store.Exists("nonexistent")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists {
		t.Error("Expected non-existent key to return false")
	}

	// Store and check
	_, err = store.CheckAndStore("exists-test", 0)
	if err != nil {
		t.Fatalf("CheckAndStore failed: %v", err)
	}

	exists, err = store.Exists("exists-test")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("Expected existing key to return true")
	}
}

func TestPebbleStore_GetOffset(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := NewPebbleStore(tmpDir, 0, 1)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Store with specific offset
	_, err = store.CheckAndStore("offset-test", 42)
	if err != nil {
		t.Fatalf("CheckAndStore failed: %v", err)
	}

	// Get offset back
	offset, found, err := store.GetOffset("offset-test")
	if err != nil {
		t.Fatalf("GetOffset failed: %v", err)
	}
	if !found {
		t.Error("Expected to find stored message")
	}
	if offset != 42 {
		t.Errorf("Expected offset 42, got %d", offset)
	}
}

func TestPebbleStore_MultiplePartitions(t *testing.T) {
	tmpDir := t.TempDir()

	store1, err := NewPebbleStore(tmpDir, 0, 1)
	if err != nil {
		t.Fatalf("Failed to create store 1: %v", err)
	}
	defer store1.Close()

	store2, err := NewPebbleStore(tmpDir, 1, 1)
	if err != nil {
		t.Fatalf("Failed to create store 2: %v", err)
	}
	defer store2.Close()

	// Store in partition 0
	_, err = store1.CheckAndStore("partition-test", 0)
	if err != nil {
		t.Fatalf("CheckAndStore failed: %v", err)
	}

	// Check in partition 1 - should be separate namespace
	isDup, err := store2.CheckAndStore("partition-test", 0)
	if err != nil {
		t.Fatalf("CheckAndStore failed: %v", err)
	}
	if isDup {
		t.Error("Partitions should have separate namespaces")
	}
}

func TestPebbleStore_GetStats(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := NewPebbleStore(tmpDir, 0, 1)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Add some entries
	store.CheckAndStore("stats-1", 0)
	store.CheckAndStore("stats-2", 1)
	store.CheckAndStore("stats-1", 2) // Duplicate

	stats, err := store.GetStats()
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}

	// Should have 2 unique entries
	if stats.ApproximateCount != 2 {
		t.Errorf("Expected 2 entries, got %d", stats.ApproximateCount)
	}
}

func TestPebbleStore_CloseAndReopen(t *testing.T) {
	tmpDir := t.TempDir()

	// Create store and add entry
	store1, err := NewPebbleStore(tmpDir, 0, 1)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	_, err = store1.CheckAndStore("persist-test", 0)
	if err != nil {
		t.Fatalf("CheckAndStore failed: %v", err)
	}

	store1.Close()

	// Reopen and verify persistence
	store2, err := NewPebbleStore(tmpDir, 0, 1)
	if err != nil {
		t.Fatalf("Failed to reopen store: %v", err)
	}
	defer store2.Close()

	exists, err := store2.Exists("persist-test")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("Expected persisted entry to exist after reopen")
	}
}

func TestPebbleStore_ConcurrentAccess(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := NewPebbleStore(tmpDir, 0, 1)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Concurrent writes
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := "concurrent-" + string(rune('a'+id)) + "-" + string(rune('0'+j%10))
				store.CheckAndStore(key, int64(id*100+j))
			}
		}(i)
	}

	wg.Wait()

	// Verify some entries exist
	exists, _ := store.Exists("concurrent-a-0")
	if !exists {
		t.Error("Expected concurrent entry to exist")
	}
}

func TestPebbleStore_PruneExpired(t *testing.T) {
	tmpDir := t.TempDir()

	// Create store with 0 hour TTL (immediate expiration)
	store, err := NewPebbleStore(tmpDir, 0, 0) // 0 hour TTL means entries expire immediately
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Store entry (will be expired since TTL is 0)
	_, err = store.CheckAndStore("prune-test", 0)
	if err != nil {
		t.Fatalf("CheckAndStore failed: %v", err)
	}

	// Prune expired entries
	pruned, err := store.PruneExpired()
	if err != nil {
		t.Fatalf("PruneExpired failed: %v", err)
	}

	// Should have pruned 1 entry
	if pruned != 1 {
		t.Errorf("Expected 1 pruned entry, got %d", pruned)
	}

	// Entry should no longer exist
	exists, _ := store.Exists("prune-test")
	if exists {
		t.Error("Expected pruned entry to not exist")
	}
}

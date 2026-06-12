package scheduler

import (
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestColdStoreStoreAndScan(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewColdStore(dir, nil)
	if err != nil {
		t.Fatalf("create cold store: %v", err)
	}
	defer cs.Close()

	// Store some entries
	entries := []struct {
		Offset     int64
		ScheduleTS int64
	}{
		{Offset: 100, ScheduleTS: 1000},
		{Offset: 200, ScheduleTS: 2000},
		{Offset: 300, ScheduleTS: 3000},
		{Offset: 150, ScheduleTS: 1500},
	}

	for _, e := range entries {
		if err := cs.Store(e.Offset, e.ScheduleTS); err != nil {
			t.Fatalf("store failed: %v", err)
		}
	}

	if cs.Count() != 4 {
		t.Fatalf("expected count 4, got %d", cs.Count())
	}

	// Scan partial range — should include start and end boundaries
	offsets, err := cs.ScanRange(1000, 2000)
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}

	if len(offsets) != 3 {
		t.Fatalf("expected 3 offsets in range [1000,2000], got %d: %v", len(offsets), offsets)
	}

	// Should be sorted by schedule_ts: 100, 150, 200
	expected := []int64{100, 150, 200}
	for i, exp := range expected {
		if offsets[i] != exp {
			t.Fatalf("offset mismatch at %d: expected %d, got %d", i, exp, offsets[i])
		}
	}
}

func TestColdStoreScanExcludesBeyondEnd(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewColdStore(dir, nil)
	if err != nil {
		t.Fatalf("create cold store: %v", err)
	}
	defer cs.Close()

	cs.Store(100, 1000)
	cs.Store(200, 2000)
	cs.Store(300, 2001) // just past endTS

	offsets, err := cs.ScanRange(1000, 2000)
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if len(offsets) != 2 {
		t.Fatalf("expected 2 offsets, got %d: %v", len(offsets), offsets)
	}
}

func TestColdStoreBatchOperations(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewColdStore(dir, nil)
	if err != nil {
		t.Fatalf("create cold store: %v", err)
	}
	defer cs.Close()

	entries := []struct {
		Offset     int64
		ScheduleTS int64
	}{
		{Offset: 10, ScheduleTS: 100},
		{Offset: 20, ScheduleTS: 200},
		{Offset: 30, ScheduleTS: 300},
	}

	if err := cs.StoreBatch(entries); err != nil {
		t.Fatalf("store batch failed: %v", err)
	}

	if cs.Count() != 3 {
		t.Fatalf("expected count 3, got %d", cs.Count())
	}

	// Delete batch
	if err := cs.DeleteBatch(entries[:2]); err != nil {
		t.Fatalf("delete batch failed: %v", err)
	}

	if cs.Count() != 1 {
		t.Fatalf("expected count 1 after delete, got %d", cs.Count())
	}

	remaining, err := cs.ScanRange(0, 500)
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if len(remaining) != 1 || remaining[0] != 30 {
		t.Fatalf("expected remaining [30], got %v", remaining)
	}
}

func TestColdStoreEmptyScan(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewColdStore(dir, nil)
	if err != nil {
		t.Fatalf("create cold store: %v", err)
	}
	defer cs.Close()

	offsets, err := cs.ScanRange(0, 1000)
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if len(offsets) != 0 {
		t.Fatalf("expected empty scan, got %v", offsets)
	}
}

func TestColdStoreSharedCache(t *testing.T) {
	dir := t.TempDir()
	cache := pebble.NewCache(8 * 1024 * 1024)
	defer cache.Unref()

	cs, err := NewColdStore(dir, cache)
	if err != nil {
		t.Fatalf("create cold store with cache: %v", err)
	}
	defer cs.Close()

	if err := cs.Store(1, 100); err != nil {
		t.Fatalf("store failed: %v", err)
	}
}

func TestColdStoreInitCount(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewColdStore(dir, nil)
	if err != nil {
		t.Fatalf("create cold store: %v", err)
	}
	defer cs.Close()

	for i := int64(0); i < 100; i++ {
		if err := cs.Store(i, i*10); err != nil {
			t.Fatalf("store failed: %v", err)
		}
	}

	if cs.Count() != 100 {
		t.Fatalf("expected count 100, got %d", cs.Count())
	}
}

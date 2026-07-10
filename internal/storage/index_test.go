package storage

import (
	"os"
	"path/filepath"
	"testing"
)

// TestIndex_Truncate verifies that Truncate drops entries past the given
// offset and rewrites the on-disk file so the truncation survives a reopen.
func TestIndex_Truncate(t *testing.T) {
	tmpDir := t.TempDir()

	idx, err := NewIndex(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}

	// Add 5 entries at offsets 0..4 (positions are arbitrary but increasing).
	for i := int64(0); i < 5; i++ {
		if err := idx.AddEntry(1000+i, i, 64+i*50); err != nil {
			t.Fatalf("AddEntry %d: %v", i, err)
		}
	}

	// Truncate to offset 2 — entries for offsets 3 and 4 must be dropped.
	if err := idx.Truncate(2); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	entries := idx.GetEntries()
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries after truncate, got %d", len(entries))
	}
	for _, e := range entries {
		if e.Offset > 2 {
			t.Errorf("entry offset %d survived truncate to 2", e.Offset)
		}
	}

	// Reopen and confirm the on-disk file matches (truncation is durable).
	if err := idx.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	idx2, err := NewIndex(tmpDir, 0)
	if err != nil {
		t.Fatalf("reopen NewIndex: %v", err)
	}
	defer idx2.Close()

	reopened := idx2.GetEntries()
	if len(reopened) != 3 {
		t.Fatalf("expected 3 entries after reopen, got %d (truncate not persisted)", len(reopened))
	}
	if got := reopened[len(reopened)-1].Offset; got != 2 {
		t.Errorf("last entry offset after reopen = %d, want 2", got)
	}
}

// TestIndex_Truncate_NoopBeyondTail asserts that truncating to a value that
// keeps all entries is a no-op (no entries dropped, no error).
func TestIndex_Truncate_NoopBeyondTail(t *testing.T) {
	tmpDir := t.TempDir()
	idx, err := NewIndex(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	defer idx.Close()

	for i := int64(0); i < 3; i++ {
		if err := idx.AddEntry(i, i, 64+i*10); err != nil {
			t.Fatalf("AddEntry: %v", err)
		}
	}
	if err := idx.Truncate(100); err != nil {
		t.Fatalf("Truncate beyond tail: %v", err)
	}
	if got := idx.Count(); got != 3 {
		t.Errorf("expected 3 entries after noop truncate, got %d", got)
	}
}

// TestIndex_Truncate_FileRewritten checks the on-disk file size reflects the
// truncated entry count (the file was actually rewritten, not just the slice).
func TestIndex_Truncate_FileRewritten(t *testing.T) {
	tmpDir := t.TempDir()
	idx, err := NewIndex(tmpDir, 0)
	if err != nil {
		t.Fatalf("NewIndex: %v", err)
	}
	defer idx.Close()
	for i := int64(0); i < 10; i++ {
		if err := idx.AddEntry(i, i, i); err != nil {
			t.Fatalf("AddEntry: %v", err)
		}
	}
	// Flush so the on-disk file has all 10 entries written before truncation.
	if err := idx.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if err := idx.Truncate(4); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	path := filepath.Join(tmpDir, "index", "00000000000000000000.index")
	// After Truncate the file is rewritten; flush is implicit in rewriteLocked.
	// 5 entries (offsets 0..4) => 5 * indexEntrySize bytes.
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat index file: %v", err)
	}
	want := int64(5 * indexEntrySize)
	if fi.Size() != want {
		t.Errorf("index file size = %d, want %d (rewrite did not shrink file)", fi.Size(), want)
	}
}

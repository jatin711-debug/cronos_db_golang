package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	indexEntrySize    = 24 // timestamp(8) + offset(8) + filePosition(8)
	indexMagic        = "CRNIDX1"
	indexVersion      = 1
	indexSyncInterval = 100 // Sync every N entries for performance
)

// IndexEntry is one sparse index entry mapping an event to a byte position.
type IndexEntry struct {
	// Timestamp is the event schedule timestamp (Unix milliseconds).
	Timestamp int64
	// Offset is the event offset within the partition.
	Offset int64
	// FilePosition is the byte position of the record in the segment file.
	FilePosition int64
}

// Index manages a sparse index file for a single WAL segment.
// Entries are appended periodically (every N events) so range reads can
// seek near the target offset or timestamp without a full scan.
type Index struct {
	mu       sync.RWMutex
	entries  []IndexEntry  // in-memory copy of on-disk entries (offset-ordered)
	file     *os.File      // O_APPEND handle for durable entry writes
	writer   *bufio.Writer // buffered writer; eliminates Seek+Write per entry
	filePath string        // absolute path to the .index file
}

// NewIndex creates or opens the sparse index file for the segment whose first
// offset is segmentFirstOffset.
func NewIndex(dataDir string, segmentFirstOffset int64) (*Index, error) {
	indexDir := filepath.Join(dataDir, "index")
	if err := os.MkdirAll(indexDir, 0755); err != nil {
		return nil, fmt.Errorf("create index dir: %w", err)
	}

	filename := fmt.Sprintf("%020d.index", segmentFirstOffset)
	filePath := filepath.Join(indexDir, filename)

	// O_APPEND eliminates the Seek call before each Write
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open index file: %w", err)
	}

	idx := &Index{
		entries:  make([]IndexEntry, 0),
		file:     file,
		writer:   bufio.NewWriterSize(file, 64*1024), // 64KB buffer
		filePath: filePath,
	}

	// Load existing entries
	if err := idx.load(); err != nil {
		file.Close()
		return nil, fmt.Errorf("load index: %w", err)
	}

	return idx, nil
}

// load reads index entries from disk
func (idx *Index) load() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	stat, err := idx.file.Stat()
	if err != nil {
		return fmt.Errorf("stat index: %w", err)
	}

	if stat.Size() == 0 {
		return nil // Empty index
	}

	// Use a separate reader for loading since file is O_APPEND
	readFile, err := os.Open(idx.filePath)
	if err != nil {
		return fmt.Errorf("open for reading: %w", err)
	}
	defer readFile.Close()

	// Read all entries
	entryCount := stat.Size() / indexEntrySize
	idx.entries = make([]IndexEntry, 0, entryCount)

	for i := int64(0); i < entryCount; i++ {
		var entryBytes [indexEntrySize]byte
		if _, err := io.ReadFull(readFile, entryBytes[:]); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read entry: %w", err)
		}

		entry := IndexEntry{
			Timestamp:    int64(binary.BigEndian.Uint64(entryBytes[0:8])),
			Offset:       int64(binary.BigEndian.Uint64(entryBytes[8:16])),
			FilePosition: int64(binary.BigEndian.Uint64(entryBytes[16:24])),
		}
		idx.entries = append(idx.entries, entry)
	}

	return nil
}

// AddEntry adds an index entry (thread-safe, acquires lock).
func (idx *Index) AddEntry(timestamp, offset, filePosition int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	return idx.addEntryLocked(timestamp, offset, filePosition)
}

// AddEntryUnsafe adds an index entry. The caller must hold the parent WAL lock
// to ensure ordering, but this function acquires the index's own mutex for the
// short in-memory+write step. Keeping the index mutex separate lets the
// background flush loop sync the index file without holding WAL.mu.
func (idx *Index) AddEntryUnsafe(timestamp, offset, filePosition int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	return idx.addEntryLocked(timestamp, offset, filePosition)
}

// addEntryLocked is the internal implementation. Caller must hold idx.mu.
func (idx *Index) addEntryLocked(timestamp, offset, filePosition int64) error {
	entry := IndexEntry{
		Timestamp:    timestamp,
		Offset:       offset,
		FilePosition: filePosition,
	}

	// Append to in-memory entries
	idx.entries = append(idx.entries, entry)

	// Write to buffered writer (O_APPEND eliminates Seek)
	var entryBytes [indexEntrySize]byte
	binary.BigEndian.PutUint64(entryBytes[0:8], uint64(timestamp))
	binary.BigEndian.PutUint64(entryBytes[8:16], uint64(offset))
	binary.BigEndian.PutUint64(entryBytes[16:24], uint64(filePosition))

	if _, err := idx.writer.Write(entryBytes[:]); err != nil {
		return fmt.Errorf("write entry: %w", err)
	}

	return nil
}

// FindByOffset finds the closest index entry at or before targetOffset.
// Returns the file position to start scanning from; found is false when the
// scan should start after the segment header (no suitable entry).
func (idx *Index) FindByOffset(targetOffset int64) (filePosition int64, found bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.entries) == 0 {
		return 64, false // Start after header
	}

	// Binary search for the largest offset <= targetOffset
	i := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].Offset > targetOffset
	})

	if i == 0 {
		// All entries are greater than target, start from beginning
		return 64, false
	}

	// Return the entry just before where targetOffset would be inserted
	return idx.entries[i-1].FilePosition, true
}

// FindByTimestamp finds the closest index entry at or before targetTS
// (Unix milliseconds). Returns the file position to start scanning from.
func (idx *Index) FindByTimestamp(targetTS int64) (filePosition int64, found bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.entries) == 0 {
		return 64, false // Start after header
	}

	// Binary search for the largest timestamp <= targetTS
	i := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].Timestamp > targetTS
	})

	if i == 0 {
		return 64, false
	}

	return idx.entries[i-1].FilePosition, true
}

// GetEntries returns a copy of all index entries (for debugging/testing).
func (idx *Index) GetEntries() []IndexEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	entries := make([]IndexEntry, len(idx.entries))
	copy(entries, idx.entries)
	return entries
}

// Truncate drops every index entry whose Offset is strictly greater than
// maxOffset, and rewrites the on-disk index file to match. This is called
// during segment recovery (Segment.scan) after a corrupt tail has truncated
// the segment file: without it, the index can still contain entries whose
// FilePosition points past the new (shorter) segment EOF, causing later reads
// to dereference uninitialized/garbage bytes.
//
// The rewrite is atomic (write .tmp, fsync, rename) so a crash mid-rewrite
// cannot corrupt the index; a leftover stale index would only make reads
// slower (they'd re-scan from the previous good entry), never incorrect.
func (idx *Index) Truncate(maxOffset int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Find the first entry to drop. Entries are appended in offset order, so
	// the slice is sorted by Offset and we can binary-search the cut point.
	cut := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].Offset > maxOffset
	})
	if cut >= len(idx.entries) {
		// Nothing to drop — no entries beyond maxOffset.
		return nil
	}

	idx.entries = append(idx.entries[:0:0], idx.entries[:cut]...) // truncate to first `cut` entries

	// Rewrite the index file from scratch. We must reopen it without O_APPEND
	// so the rewrite truncates the old contents.
	if err := idx.rewriteLocked(); err != nil {
		return fmt.Errorf("rewrite index after truncate: %w", err)
	}
	return nil
}

// rewriteLocked rewrites the entire index file from the in-memory entries.
// Caller must hold idx.mu. The write is atomic: a temp file is fsynced and
// renamed over the index path, then the index's file handle is reopened.
func (idx *Index) rewriteLocked() error {
	tmpPath := idx.filePath + ".tmp"

	// Flush any buffered writes on the old handle first so we don't lose them,
	// then close it so the rename can succeed on Windows.
	if idx.writer != nil {
		_ = idx.writer.Flush()
	}
	oldFile := idx.file
	if oldFile != nil {
		_ = oldFile.Close()
	}

	// Write all entries to a fresh temp file (O_TRUNC ensures no stale tail).
	tmpFile, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("create temp index: %w", err)
	}

	var entryBytes [indexEntrySize]byte
	for _, e := range idx.entries {
		binary.BigEndian.PutUint64(entryBytes[0:8], uint64(e.Timestamp))
		binary.BigEndian.PutUint64(entryBytes[8:16], uint64(e.Offset))
		binary.BigEndian.PutUint64(entryBytes[16:24], uint64(e.FilePosition))
		if _, err := tmpFile.Write(entryBytes[:]); err != nil {
			tmpFile.Close()
			return fmt.Errorf("write temp index: %w", err)
		}
	}
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		return fmt.Errorf("fsync temp index: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("close temp index: %w", err)
	}

	// Atomic replace.
	if err := os.Rename(tmpPath, idx.filePath); err != nil {
		return fmt.Errorf("rename temp index: %w", err)
	}

	// Reopen the (now replaced) index file with the same flags as NewIndex.
	newFile, err := os.OpenFile(idx.filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("reopen index: %w", err)
	}
	idx.file = newFile
	idx.writer = bufio.NewWriterSize(newFile, 64*1024)
	return nil
}

// Count returns the number of index entries currently loaded.
func (idx *Index) Count() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.entries)
}

// Flush flushes the buffered writer and fsyncs the index file to disk.
func (idx *Index) Flush() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.writer != nil {
		if err := idx.writer.Flush(); err != nil {
			return err
		}
	}
	if idx.file != nil {
		return idx.file.Sync()
	}
	return nil
}

// Close flushes, fsyncs, and closes the index file.
func (idx *Index) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.writer != nil {
		idx.writer.Flush()
	}
	if idx.file != nil {
		// Sync before close to ensure all data is persisted
		idx.file.Sync()
		return idx.file.Close()
	}
	return nil
}

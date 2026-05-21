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

// IndexEntry represents a sparse index entry
type IndexEntry struct {
	Timestamp    int64 // Event schedule timestamp
	Offset       int64 // Event offset in partition
	FilePosition int64 // Byte position in segment file
}

// Index manages sparse index for a segment
type Index struct {
	mu       sync.RWMutex
	entries  []IndexEntry
	file     *os.File
	writer   *bufio.Writer // Buffered writer eliminates Seek+Write per entry
	filePath string
}

// NewIndex creates or opens an index file
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

// AddEntry adds an index entry (thread-safe, acquires lock)
func (idx *Index) AddEntry(timestamp, offset, filePosition int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	return idx.addEntryLocked(timestamp, offset, filePosition)
}

// AddEntryUnsafe adds an index entry without acquiring the lock.
// The caller MUST guarantee exclusive access (e.g. parent WAL.mu is held).
// This eliminates the triple-nested lock hierarchy WAL→Segment→Index.
func (idx *Index) AddEntryUnsafe(timestamp, offset, filePosition int64) error {
	return idx.addEntryLocked(timestamp, offset, filePosition)
}

// addEntryLocked is the internal implementation (caller must ensure exclusion)
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

	// Batch sync: sync every N entries instead of every entry for performance
	if len(idx.entries)%indexSyncInterval == 0 {
		if err := idx.writer.Flush(); err != nil {
			return err
		}
		return idx.file.Sync()
	}
	return nil
}

// FindByOffset finds the closest index entry at or before the target offset
// Returns the file position to start reading from
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

// FindByTimestamp finds the closest index entry at or before the target timestamp
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

// GetEntries returns all index entries (for debugging/testing)
func (idx *Index) GetEntries() []IndexEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	entries := make([]IndexEntry, len(idx.entries))
	copy(entries, idx.entries)
	return entries
}

// Count returns the number of index entries
func (idx *Index) Count() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.entries)
}

// Flush syncs any pending writes to disk
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

// Close closes the index file
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

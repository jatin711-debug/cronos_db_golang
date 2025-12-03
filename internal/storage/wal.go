package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"cronos_db/pkg/types"
)

// WAL represents Write-Ahead Log with segmented storage
type WAL struct {
	mu            sync.RWMutex
	dataDir       string
	partitionID   int32
	segments      []*Segment
	activeSegment *Segment
	nextOffset    int64
	highWatermark int64
	config        *WALConfig
}

// WALConfig represents WAL configuration
type WALConfig struct {
	SegmentSizeBytes int64
	IndexInterval    int64
	FsyncMode        string
	FlushIntervalMS  int32
}

// NewWAL creates a new WAL
func NewWAL(dataDir string, partitionID int32, config *WALConfig) (*WAL, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	wal := &WAL{
		dataDir:       dataDir,
		partitionID:   partitionID,
		segments:      make([]*Segment, 0),
		nextOffset:    0,
		highWatermark: 0,
		config:        config,
	}

	// Load existing segments
	if err := wal.loadSegments(); err != nil {
		return nil, fmt.Errorf("load segments: %w", err)
	}

	// Open or create active segment
	if err := wal.openActiveSegment(); err != nil {
		return nil, fmt.Errorf("open active segment: %w", err)
	}

	return wal, nil
}

// loadSegments loads existing segments
func (w *WAL) loadSegments() error {
	segmentsDir := filepath.Join(w.dataDir, "segments")
	if _, err := os.Stat(segmentsDir); os.IsNotExist(err) {
		// No segments exist, start fresh
		return nil
	}

	entries, err := os.ReadDir(segmentsDir)
	if err != nil {
		return fmt.Errorf("read segments dir: %w", err)
	}

	segmentFiles := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) == ".log" {
			segmentFiles = append(segmentFiles, entry.Name())
		}
	}

	// Sort by offset
	sort.Strings(segmentFiles)

	// Load each segment
	for _, filename := range segmentFiles {
		segment, err := OpenSegment(w.dataDir, filename)
		if err != nil {
			return fmt.Errorf("open segment %s: %w", filename, err)
		}
		w.segments = append(w.segments, segment)
		w.nextOffset = segment.GetLastOffset() + 1
		w.highWatermark = segment.GetLastOffset()
	}

	return nil
}

// openActiveSegment opens or creates active segment
func (w *WAL) openActiveSegment() error {
	if len(w.segments) > 0 {
		lastSegment := w.segments[len(w.segments)-1]
		if lastSegment.IsActive() {
			w.activeSegment = lastSegment
			return nil
		}
	}

	// Create new active segment
	segment, err := NewSegment(w.dataDir, w.nextOffset, true)
	if err != nil {
		return fmt.Errorf("create new segment: %w", err)
	}
	w.segments = append(w.segments, segment)
	w.activeSegment = segment

	return nil
}

// AppendEvent appends an event to WAL
func (w *WAL) AppendEvent(event *types.Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Set offset
	event.Offset = w.nextOffset
	event.PartitionId = w.partitionID

	// Append to active segment
	if err := w.activeSegment.AppendEvent(event, w.config.IndexInterval); err != nil {
		return fmt.Errorf("append to segment: %w", err)
	}

	w.nextOffset++
	w.highWatermark = event.Offset

	// Rotate segment if full
	if w.activeSegment.IsFull(w.config.SegmentSizeBytes) {
		if err := w.rotateSegment(); err != nil {
			return fmt.Errorf("rotate segment: %w", err)
		}
	}

	return nil
}

// rotateSegment rotates to new active segment
func (w *WAL) rotateSegment() error {
	// Close current active segment
	if err := w.activeSegment.Close(); err != nil {
		return fmt.Errorf("close active segment: %w", err)
	}

	// Create new active segment
	newSegment, err := NewSegment(w.dataDir, w.nextOffset, true)
	if err != nil {
		return fmt.Errorf("create new active segment: %w", err)
	}
	w.segments = append(w.segments, newSegment)
	w.activeSegment = newSegment

	return nil
}

// ReadEvents reads events in offset range
func (w *WAL) ReadEvents(startOffset, endOffset int64) ([]*types.Event, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	result := make([]*types.Event, 0)

	// Find segments that contain the range
	for _, segment := range w.segments {
		if segment.GetLastOffset() < startOffset || segment.GetFirstOffset() > endOffset {
			continue
		}

		// Read events from this segment
		// Start from max(startOffset, segment.firstOffset)
		readOffset := max(startOffset, segment.GetFirstOffset())

		// End at min(endOffset, segment.lastOffset)
		endForSegment := min(endOffset, segment.GetLastOffset())

		// Read each event
		for offset := readOffset; offset <= endForSegment; offset++ {
			event, err := segment.ReadEvent(offset)
			if err != nil {
				// Skip if event not found (might be truncated)
				continue
			}
			event.PartitionId = w.partitionID
			result = append(result, event)
		}
	}

	return result, nil
}

// ReadEventsByTime reads events in timestamp range
func (w *WAL) ReadEventsByTime(startTS, endTS int64) ([]*types.Event, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	result := make([]*types.Event, 0)

	// Find segments that contain the timestamp range
	for _, segment := range w.segments {
		if segment.GetLastTS() < startTS || segment.GetFirstTS() > endTS {
			continue
		}

		// TODO: Read events using index
	}

	return result, nil
}

// Flush flushes all pending writes
func (w *WAL) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.activeSegment == nil {
		return nil
	}

	if err := w.activeSegment.Flush(); err != nil {
		return err
	}

	// Conditionally sync based on FsyncMode
	if w.config.FsyncMode == "every_event" {
		return w.activeSegment.Sync()
	}
	return nil
}

// Close closes WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.activeSegment != nil {
		if err := w.activeSegment.Close(); err != nil {
			return err
		}
	}

	return nil
}

// GetSegments returns all segments
func (w *WAL) GetSegments() []*Segment {
	w.mu.RLock()
	defer w.mu.RUnlock()

	segments := make([]*Segment, len(w.segments))
	copy(segments, w.segments)
	return segments
}

// GetActiveSegment returns active segment
func (w *WAL) GetActiveSegment() *Segment {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.activeSegment
}

// GetNextOffset returns next offset to write
func (w *WAL) GetNextOffset() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.nextOffset
}

// GetHighWatermark returns high watermark
func (w *WAL) GetHighWatermark() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.highWatermark
}

// GetLastOffset returns last offset in WAL
func (w *WAL) GetLastOffset() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if len(w.segments) == 0 {
		return -1
	}

	return w.segments[len(w.segments)-1].GetLastOffset()
}

// Compact compacts old segments
func (w *WAL) Compact(beforeOffset int64, consumerOffsets map[int64]bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Keep all segments with offset >= min_offset
	// where min_offset is min(all consumer offsets)
	// TODO: Implement full compaction logic
	return nil
}

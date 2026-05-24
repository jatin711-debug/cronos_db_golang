package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// FsyncMode controls when the WAL performs fsync.
// Using int enum instead of string comparison eliminates per-write overhead.
type FsyncMode int

const (
	FsyncPeriodic   FsyncMode = iota // Sync in background flush loop
	FsyncEveryEvent                  // Sync after every write (safest, slowest)
	FsyncBatch                       // Sync in background flush loop (like periodic)
)

// ParseFsyncMode converts a string to FsyncMode enum
func ParseFsyncMode(s string) FsyncMode {
	switch s {
	case "every_event":
		return FsyncEveryEvent
	case "batch":
		return FsyncBatch
	default:
		return FsyncPeriodic
	}
}

// WAL represents Write-Ahead Log with segmented storage
type WAL struct {
	mu                 sync.RWMutex
	dataDir            string
	partitionID        int32
	segments           []*Segment
	activeSegment      *Segment
	nextSegment        *Segment    // Pre-created next segment for fast rotation
	nextSegMu          sync.Mutex  // Protects nextSegment
	preCreateTriggered atomic.Bool // Atomic flag to avoid duplicate pre-creation goroutines
	nextOffset         int64
	highWatermark      int64
	config             *WALConfig
	fsyncMode          FsyncMode   // Parsed once at init, avoids per-write string cmp
	dirty              atomic.Bool // Set on write, cleared on flush
	quit               chan struct{}
	wg                 sync.WaitGroup
}

// WALConfig represents WAL configuration
type WALConfig struct {
	SegmentSizeBytes int64
	IndexInterval    int64
	FsyncMode        string // Raw string config, parsed to enum at WAL init
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
		fsyncMode:     ParseFsyncMode(config.FsyncMode),
		quit:          make(chan struct{}),
	}

	// Load existing segments
	if err := wal.loadSegments(); err != nil {
		return nil, fmt.Errorf("load segments: %w", err)
	}

	// Open or create active segment
	if err := wal.openActiveSegment(); err != nil {
		return nil, fmt.Errorf("open active segment: %w", err)
	}

	// Start periodic background flush if configured
	if config.FlushIntervalMS > 0 {
		wal.wg.Add(1)
		go wal.periodicFlushLoop()
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

// PreparedRecord holds pre-encoded record buffer for fast serialization
type PreparedRecord struct {
	Event *types.Event
	Buf   []byte
}

var recordBufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 4096)
	},
}

// PrepareRecord serializes an event to a byte slice with placeholders for offset and CRC
func PrepareRecord(event *types.Event) (*PreparedRecord, error) {
	msgIDLen := len(event.GetMessageId())
	topicLen := len(event.Topic)
	payloadLen := len(event.Payload)
	metaCount := len(event.Meta)

	// Calculate size
	size := 4 + 4 + 8 + 8 + 2 + msgIDLen + 2 + topicLen + 4 + payloadLen + 2
	for k, v := range event.Meta {
		metaEntrySize := 2 + len(k) + 2 + len(v)
		if size > (1<<63-1)-metaEntrySize {
			return nil, fmt.Errorf("event record too large: overflow in size calculation")
		}
		size += metaEntrySize
	}

	var buf []byte
	if size <= 4096 {
		x := recordBufPool.Get().([]byte)
		buf = x[:size]
	} else {
		buf = make([]byte, size)
	}

	offset := 0

	// Length (4 bytes)
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(size))
	offset += 4

	// CRC32 (4 bytes) - placeholder
	offset += 4

	// Offset (8 bytes) - placeholder
	offset += 8

	// Schedule timestamp (8 bytes)
	binary.BigEndian.PutUint64(buf[offset:offset+8], uint64(event.GetScheduleTs()))
	offset += 8

	// Message ID length (2 bytes)
	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(msgIDLen))
	offset += 2
	copy(buf[offset:offset+msgIDLen], event.GetMessageId())
	offset += msgIDLen

	// Topic length (2 bytes)
	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(topicLen))
	offset += 2
	copy(buf[offset:offset+topicLen], event.Topic)
	offset += topicLen

	// Payload length (4 bytes)
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(payloadLen))
	offset += 4
	copy(buf[offset:offset+payloadLen], event.Payload)
	offset += payloadLen

	// Meta count (2 bytes)
	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(metaCount))
	offset += 2

	for k, v := range event.Meta {
		binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(k)))
		offset += 2
		copy(buf[offset:offset+len(k)], k)
		offset += len(k)

		binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(v)))
		offset += 2
		copy(buf[offset:offset+len(v)], v)
		offset += len(v)
	}

	return &PreparedRecord{
		Event: event,
		Buf:   buf,
	}, nil
}

// AppendBatch appends a batch of events to WAL
func (w *WAL) AppendBatch(events []*types.Event) error {
	if len(events) == 0 {
		return nil
	}

	// 1. Prepare records OUTSIDE the lock
	prepared := make([]*PreparedRecord, len(events))
	for i, event := range events {
		prep, err := PrepareRecord(event)
		if err != nil {
			// If error, return already allocated buffers to pool
			for j := 0; j < i; j++ {
				if len(prepared[j].Buf) <= 4096 {
					recordBufPool.Put(prepared[j].Buf)
				}
			}
			return err
		}
		prepared[i] = prep
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// 2. Set offsets and partition IDs, fill them in the prepared records
	for _, prep := range prepared {
		prep.Event.Offset = w.nextOffset
		prep.Event.PartitionId = w.partitionID
		w.nextOffset++

		// Fill offset (bytes 8-16)
		binary.BigEndian.PutUint64(prep.Buf[8:16], uint64(prep.Event.Offset))

		// Compute and fill CRC32 (bytes 4-8)
		crc := crc32.ChecksumIEEE(prep.Buf[8:])
		binary.BigEndian.PutUint32(prep.Buf[4:8], crc)
	}

	// 3. Append to active segment — use Unsafe since we hold w.mu
	if err := w.activeSegment.AppendPreparedBatchUnsafe(prepared, w.config.IndexInterval); err != nil {
		// Return buffers to pool
		for _, prep := range prepared {
			if len(prep.Buf) <= 4096 {
				recordBufPool.Put(prep.Buf)
			}
		}
		return fmt.Errorf("append prepared batch to segment: %w", err)
	}

	w.highWatermark = events[len(events)-1].Offset
	w.dirty.Store(true)

	// Sync inline only for every_event mode.
	if w.fsyncMode == FsyncEveryEvent {
		if err := w.activeSegment.FlushBuffer(); err != nil {
			// Return buffers to pool
			for _, prep := range prepared {
				if len(prep.Buf) <= 4096 {
					recordBufPool.Put(prep.Buf)
				}
			}
			return fmt.Errorf("flush batch: %w", err)
		}
		if err := w.activeSegment.Sync(); err != nil {
			// Return buffers to pool
			for _, prep := range prepared {
				if len(prep.Buf) <= 4096 {
					recordBufPool.Put(prep.Buf)
				}
			}
			return fmt.Errorf("sync batch: %w", err)
		}
	}

	// 4. Return buffers to pool
	for _, prep := range prepared {
		if len(prep.Buf) <= 4096 {
			recordBufPool.Put(prep.Buf)
		}
	}

	// Rotate segment if full
	if w.activeSegment.IsFull(w.config.SegmentSizeBytes) {
		if err := w.rotateSegment(); err != nil {
			return fmt.Errorf("rotate segment: %w", err)
		}
	} else {
		// Trigger background pre-creation at 90% capacity
		threshold := int64(float64(w.config.SegmentSizeBytes) * 0.9)
		if w.activeSegment.GetSize() >= threshold && w.preCreateTriggered.CompareAndSwap(false, true) {
			go w.maybePreCreateNextSegment(w.nextOffset)
		}
	}

	return nil
}

// maybePreCreateNextSegment creates the next segment in the background
// so that rotation can swap it in with minimal lock hold time.
func (w *WAL) maybePreCreateNextSegment(nextOffset int64) {
	w.nextSegMu.Lock()
	if w.nextSegment != nil {
		w.nextSegMu.Unlock()
		return
	}
	w.nextSegMu.Unlock()

	seg, err := NewSegment(w.dataDir, nextOffset, true)
	if err != nil {
		log.Printf("[WAL-%d] failed to pre-create next segment: %v", w.partitionID, err)
		w.preCreateTriggered.Store(false)
		return
	}

	w.nextSegMu.Lock()
	if w.nextSegment == nil {
		w.nextSegment = seg
	} else {
		_ = seg.Close() // Another goroutine won the race
	}
	w.nextSegMu.Unlock()
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
	// Prepare record outside the lock
	prep, err := PrepareRecord(event)
	if err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Set offset
	event.Offset = w.nextOffset
	event.PartitionId = w.partitionID
	w.nextOffset++

	// Fill offset (bytes 8-16)
	binary.BigEndian.PutUint64(prep.Buf[8:16], uint64(event.Offset))

	// Compute and fill CRC32 (bytes 4-8)
	crc := crc32.ChecksumIEEE(prep.Buf[8:])
	binary.BigEndian.PutUint32(prep.Buf[4:8], crc)

	// Append to active segment — use Unsafe since we hold w.mu
	if err := w.activeSegment.AppendPreparedBatchUnsafe([]*PreparedRecord{prep}, w.config.IndexInterval); err != nil {
		if len(prep.Buf) <= 4096 {
			recordBufPool.Put(prep.Buf)
		}
		return fmt.Errorf("append to segment: %w", err)
	}

	w.highWatermark = event.Offset
	w.dirty.Store(true)

	// Sync inline for every_event mode
	if w.fsyncMode == FsyncEveryEvent {
		if err := w.activeSegment.FlushBuffer(); err != nil {
			if len(prep.Buf) <= 4096 {
				recordBufPool.Put(prep.Buf)
			}
			return fmt.Errorf("flush event: %w", err)
		}
		if err := w.activeSegment.Sync(); err != nil {
			if len(prep.Buf) <= 4096 {
				recordBufPool.Put(prep.Buf)
			}
			return fmt.Errorf("sync event: %w", err)
		}
	}

	if len(prep.Buf) <= 4096 {
		recordBufPool.Put(prep.Buf)
	}

	// Rotate segment if full
	if w.activeSegment.IsFull(w.config.SegmentSizeBytes) {
		if err := w.rotateSegment(); err != nil {
			return fmt.Errorf("rotate segment: %w", err)
		}
	} else {
		// Trigger background pre-creation at 90% capacity
		threshold := int64(float64(w.config.SegmentSizeBytes) * 0.9)
		if w.activeSegment.GetSize() >= threshold && w.preCreateTriggered.CompareAndSwap(false, true) {
			go w.maybePreCreateNextSegment(w.nextOffset)
		}
	}

	return nil
}

// rotateSegment swaps to the pre-created next segment if available,
// otherwise falls back to synchronous creation.
func (w *WAL) rotateSegment() error {
	// Close current active segment
	if err := w.activeSegment.Close(); err != nil {
		return fmt.Errorf("close active segment: %w", err)
	}

	// Try to use pre-created segment
	w.nextSegMu.Lock()
	if w.nextSegment != nil {
		w.segments = append(w.segments, w.nextSegment)
		w.activeSegment = w.nextSegment
		w.nextSegment = nil
		w.nextSegMu.Unlock()
		w.preCreateTriggered.Store(false)
		return nil
	}
	w.nextSegMu.Unlock()

	// Fallback: create synchronously
	newSegment, err := NewSegment(w.dataDir, w.nextOffset, true)
	if err != nil {
		return fmt.Errorf("create new active segment: %w", err)
	}
	w.segments = append(w.segments, newSegment)
	w.activeSegment = newSegment
	w.preCreateTriggered.Store(false)

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
		readOffset := max(startOffset, segment.GetFirstOffset())
		endForSegment := min(endOffset, segment.GetLastOffset())

		events, err := segment.ReadEventsByOffsetRange(readOffset, endForSegment)
		if err != nil {
			return nil, fmt.Errorf("read offset range from segment %s: %w", segment.GetFilename(), err)
		}

		for _, event := range events {
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

		events, err := segment.ReadEventsByTime(startTS, endTS)
		if err != nil {
			return nil, fmt.Errorf("read from segment %s: %w", segment.GetFilename(), err)
		}

		for _, event := range events {
			event.PartitionId = w.partitionID
			result = append(result, event)
		}
	}

	return result, nil
}

// periodicFlushLoop runs in the background to flush and optionally sync
// the active segment at regular intervals. This eliminates syscall spikes
// from inline syncing and provides predictable latency.
func (w *WAL) periodicFlushLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(time.Duration(w.config.FlushIntervalMS) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// FIX: Skip flush if nothing was written since last flush.
			// This eliminates unnecessary RLock + syscall overhead on idle partitions.
			if !w.dirty.CompareAndSwap(true, false) {
				continue
			}

			// Flush buffer under lock (fast — just memcpy to kernel page cache)
			w.mu.Lock()
			seg := w.activeSegment
			var flushErr error
			if seg != nil {
				flushErr = seg.FlushBuffer()
			}
			w.mu.Unlock()

			if seg == nil || flushErr != nil {
				if flushErr != nil {
					log.Printf("[WAL-%d] background flush error: %v", w.partitionID, flushErr)
				}
				continue
			}

			// Sync OUTSIDE the lock — fsync doesn't need to block writers
			// Writers can continue appending to the bufio.Writer while we sync.
			if w.fsyncMode == FsyncPeriodic || w.fsyncMode == FsyncBatch {
				if err := seg.Sync(); err != nil {
					log.Printf("[WAL-%d] background sync error: %v", w.partitionID, err)
				}
			}

		case <-w.quit:
			return
		}
	}
}

// Flush flushes all pending writes to disk.
// This performs both buffer flush and fsync regardless of mode,
// and is intended for explicit durability checkpoints (e.g. shutdown).
func (w *WAL) Flush() error {
	w.mu.Lock()
	seg := w.activeSegment
	var err error
	if seg != nil {
		err = seg.FlushBuffer()
	}
	w.mu.Unlock()

	if seg == nil {
		return nil
	}

	if err != nil {
		return err
	}
	return seg.Sync()
}

// Close stops the background flush loop, flushes and syncs the active segment,
// and closes all underlying files.
func (w *WAL) Close() error {
	// Signal background loop to stop
	close(w.quit)
	w.wg.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.activeSegment != nil {
		// Final flush + sync before close
		_ = w.activeSegment.FlushBuffer()
		_ = w.activeSegment.Sync()
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

// CompactByOffset removes all segments whose last offset is less than upToOffset.
// Returns the number of segments deleted.
func (w *WAL) CompactByOffset(upToOffset int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.segments) <= 1 {
		return 0, nil // Never delete the active/only segment
	}

	// Build a new slice with only segments to keep - avoids in-place deletion bugs
	kept := make([]*Segment, 0, len(w.segments))
	deletedCount := 0

	for _, segment := range w.segments {
		// Never delete the active segment
		if segment == w.activeSegment {
			kept = append(kept, segment)
			continue
		}

		// Check if segment should be deleted (all offsets before upToOffset)
		if segment.GetLastOffset() < upToOffset && segment.GetLastOffset() > 0 {
			if err := segment.Delete(); err != nil {
				return deletedCount, fmt.Errorf("failed to delete segment %s: %w", segment.GetFilename(), err)
			}
			deletedCount++
		} else {
			kept = append(kept, segment)
		}
	}

	w.segments = kept
	return deletedCount, nil
}

// CompactByTimestamp removes all segments whose last timestamp is less than upToTS.
// Returns the number of segments deleted.
func (w *WAL) CompactByTimestamp(upToTS int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.segments) <= 1 {
		return 0, nil // Never delete the active/only segment
	}

	// Build a new slice with only segments to keep - avoids in-place deletion bugs
	kept := make([]*Segment, 0, len(w.segments))
	deletedCount := 0

	for _, segment := range w.segments {
		// Never delete the active segment
		if segment == w.activeSegment {
			kept = append(kept, segment)
			continue
		}

		// Check if segment should be deleted (all timestamps before upToTS)
		if segment.GetLastTS() < upToTS && segment.GetLastTS() > 0 {
			if err := segment.Delete(); err != nil {
				return deletedCount, fmt.Errorf("failed to delete segment %s: %w", segment.GetFilename(), err)
			}
			deletedCount++
		} else {
			kept = append(kept, segment)
		}
	}

	w.segments = kept
	return deletedCount, nil
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

// GetDataDir returns the WAL data directory
func (w *WAL) GetDataDir() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.dataDir
}

// ReloadSegments reloads segments from disk (used after bulk file sync)
func (w *WAL) ReloadSegments() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Close existing segments
	for _, seg := range w.segments {
		seg.Close()
	}
	w.segments = make([]*Segment, 0)
	w.activeSegment = nil

	// Reload from disk
	if err := w.loadSegments(); err != nil {
		return fmt.Errorf("reload segments: %w", err)
	}

	// Open or create active segment
	if err := w.openActiveSegment(); err != nil {
		return fmt.Errorf("reload open active segment: %w", err)
	}

	return nil
}

// Compact compacts old segments based on consumer offsets.
// It removes segments whose last offset is less than the minimum
// offset that any consumer has processed.
func (w *WAL) Compact(beforeOffset int64, consumerOffsets map[int64]bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(consumerOffsets) == 0 {
		return nil
	}

	// Find minimum offset across all consumers
	minOffset := int64(^uint64(0) >> 1) // Max int64
	for offset := range consumerOffsets {
		if offset < minOffset {
			minOffset = offset
		}
	}

	// Nothing to compact if no progress or all are at beginning
	if minOffset == 0 || minOffset >= w.highWatermark {
		return nil
	}

	// Use CompactByOffset which handles segment deletion properly
	deleted, err := w.compactByOffsetUnsafe(minOffset)
	if err != nil {
		return err
	}

	if deleted > 0 {
		log.Printf("WAL compaction deleted %d segments, minConsumerOffset=%d", deleted, minOffset)
	}

	return nil
}

// compactByOffsetUnsafe compacts without acquiring lock (caller must hold lock)
func (w *WAL) compactByOffsetUnsafe(upToOffset int64) (int, error) {
	if len(w.segments) <= 1 {
		return 0, nil
	}

	kept := make([]*Segment, 0, len(w.segments))
	deletedCount := 0

	for _, segment := range w.segments {
		if segment == w.activeSegment {
			kept = append(kept, segment)
			continue
		}

		if segment.GetLastOffset() < upToOffset && segment.GetLastOffset() > 0 {
			if err := segment.Delete(); err != nil {
				return deletedCount, fmt.Errorf("failed to delete segment %s: %w", segment.GetFilename(), err)
			}
			deletedCount++
		} else {
			kept = append(kept, segment)
		}
	}

	w.segments = kept
	return deletedCount, nil
}

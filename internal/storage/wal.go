package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/metrics"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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

// walFlushErrorsTotal tracks flush/sync failures in the WAL background loop.
var walFlushErrorsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "cronos_wal_flush_errors_total",
		Help: "Total number of WAL background flush/sync errors per partition",
	},
	[]string{"partition"},
)

// WAL represents Write-Ahead Log with segmented storage
type WAL struct {
	mu                 sync.RWMutex
	dataDir            string
	partitionID        int32
	partitionLabel     string // cached string for metrics
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
	flushErrors        atomic.Int64
	quit               chan struct{}
	quitOnce           sync.Once
	wg                 sync.WaitGroup
	cipher             *SegmentCipher
	appendHook         func(event *types.Event) // Called after successful append (e.g. CDC)
}

// WALConfig represents WAL configuration
type WALConfig struct {
	SegmentSizeBytes int64
	IndexInterval    int64
	FsyncMode        string // Raw string config, parsed to enum at WAL init
	FlushIntervalMS  int32
}

// NewWAL creates a new WAL
func NewWAL(dataDir string, partitionID int32, config *WALConfig, cipher *SegmentCipher) (*WAL, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	wal := &WAL{
		dataDir:        dataDir,
		partitionID:    partitionID,
		partitionLabel: strconv.FormatInt(int64(partitionID), 10),
		segments:       make([]*Segment, 0),
		nextOffset:     0,
		highWatermark:  0,
		config:         config,
		fsyncMode:      ParseFsyncMode(config.FsyncMode),
		quit:           make(chan struct{}),
		cipher:         cipher,
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

// SetAppendHook registers a callback invoked after each successful event append.
func (w *WAL) SetAppendHook(hook func(event *types.Event)) {
	w.appendHook = hook
}

// loadSegments loads existing segments and verifies their integrity on startup.
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

	// Load each segment with verification
	for _, filename := range segmentFiles {
		segment, err := OpenSegment(w.dataDir, filename, w.cipher)
		if err != nil {
			log.Printf("[WAL-%d] WARNING: Failed to open segment %s: %v", w.partitionID, filename, err)
			// Try to recover by skipping corrupt segment
			continue
		}

		// Verify segment integrity by reading all events and checking CRCs
		if err := w.verifySegment(segment); err != nil {
			closeErr := segment.Close()
			if closeErr != nil {
				log.Printf("[WAL-%d] WARNING: Segment %s verification failed: %v; additionally failed to close: %v", w.partitionID, filename, err, closeErr)
			} else {
				log.Printf("[WAL-%d] WARNING: Segment %s verification failed: %v", w.partitionID, filename, err)
			}
			// Skip corrupt segment - data before this segment is still valid
			continue
		}

		w.segments = append(w.segments, segment)
		w.nextOffset = segment.GetLastOffset() + 1
		w.highWatermark = segment.GetLastOffset()
	}

	if len(w.segments) > 0 {
		log.Printf("[WAL-%d] Loaded %d segments, nextOffset=%d", w.partitionID, len(w.segments), w.nextOffset)
	}

	return nil
}

// verifySegment reads all events from a segment to verify CRC integrity.
// This is called on startup to detect data corruption.
func (w *WAL) verifySegment(segment *Segment) error {
	firstOffset := segment.GetFirstOffset()
	lastOffset := segment.GetLastOffset()

	if firstOffset > lastOffset {
		return nil // Empty segment
	}

	events, err := segment.ReadEventsByOffsetRange(firstOffset, lastOffset)
	if err != nil {
		return fmt.Errorf("read events for verification: %w", err)
	}

	// Verify each event's CRC by re-parsing the raw record
	for _, event := range events {
		if event == nil {
			return fmt.Errorf("nil event in segment")
		}
		// The event was successfully parsed, which means CRC passed
		// Additional check: verify offset continuity
		if event.Offset < firstOffset || event.Offset > lastOffset {
			return fmt.Errorf("event offset %d out of segment range [%d, %d]", event.Offset, firstOffset, lastOffset)
		}
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

// preparedRecordPool recycles the []*PreparedRecord slice used by AppendBatch and
// AppendReplicatedBatch. This avoids one allocation per WAL append.
var preparedRecordPool = sync.Pool{
	New: func() interface{} {
		return make([]*PreparedRecord, 0, 64)
	},
}

func acquirePreparedSlice(n int) []*PreparedRecord {
	p := preparedRecordPool.Get().([]*PreparedRecord)
	if cap(p) < n {
		// Pool slot is too small for this batch; allocate a dedicated slice and
		// return the small one so it can be reused by smaller batches.
		preparedRecordPool.Put(p)
		return make([]*PreparedRecord, n)
	}
	return p[:n]
}

func releasePreparedSlice(prepared []*PreparedRecord) {
	// Don't retain oversized slices in the pool; they likely won't be reused.
	if cap(prepared) <= 4096 {
		preparedRecordPool.Put(prepared[:0])
	}
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

	start := time.Now()
	defer func() {
		metrics.ObserveWALAppend(strconv.FormatInt(int64(w.partitionID), 10), time.Since(start))
	}()

	// 1. Prepare records OUTSIDE the lock
	prepared := acquirePreparedSlice(len(events))
	defer releasePreparedSlice(prepared)

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
		w.mu.Unlock()
		returnBuffersToPool(prepared)
		return fmt.Errorf("append prepared batch to segment: %w", err)
	}

	w.highWatermark = events[len(events)-1].Offset
	w.dirty.Store(true)

	// Collect hook events to fire after unlock
	var hookEvents []*types.Event
	if w.appendHook != nil {
		hookEvents = append(hookEvents, events...)
	}

	// Flush under the WAL lock so appends and flushes do not interleave on the
	// segment's bufio writer. For every_event we also sync inline; for batch we
	// sync outside the lock to let other writers pipeline during the fsync.
	syncSegment := w.activeSegment
	if w.fsyncMode == FsyncEveryEvent {
		if err := syncSegment.FlushBuffer(); err != nil {
			w.mu.Unlock()
			returnBuffersToPool(prepared)
			return fmt.Errorf("flush batch: %w", err)
		}
		if err := syncSegment.Sync(); err != nil {
			w.mu.Unlock()
			returnBuffersToPool(prepared)
			return fmt.Errorf("sync batch: %w", err)
		}
	} else if w.fsyncMode == FsyncBatch {
		if err := syncSegment.FlushBuffer(); err != nil {
			w.mu.Unlock()
			returnBuffersToPool(prepared)
			return fmt.Errorf("flush batch: %w", err)
		}
	}

	// Rotate segment if full
	if w.activeSegment.IsFull(w.config.SegmentSizeBytes) {
		if err := w.rotateSegment(); err != nil {
			w.mu.Unlock()
			returnBuffersToPool(prepared)
			return fmt.Errorf("rotate segment: %w", err)
		}
	} else {
		// Trigger background pre-creation at 90% capacity
		threshold := int64(float64(w.config.SegmentSizeBytes) * 0.9)
		if w.activeSegment.GetSize() >= threshold && w.preCreateTriggered.CompareAndSwap(false, true) {
			go w.maybePreCreateNextSegment(w.nextOffset)
		}
	}

	w.mu.Unlock()

	// For batch mode, the buffer was already flushed under the lock; now sync the
	// file descriptor outside the WAL lock so other writers can continue appending.
	if w.fsyncMode == FsyncBatch {
		if err := syncSegment.Sync(); err != nil {
			returnBuffersToPool(prepared)
			return fmt.Errorf("sync batch: %w", err)
		}
	}

	// 4. Return buffers to pool
	returnBuffersToPool(prepared)

	// Fire hooks AFTER releasing the lock to avoid blocking writers
	for _, e := range hookEvents {
		w.appendHook(e)
	}

	return nil
}

// returnBuffersToPool returns prepared record buffers to the sync.Pool if they
// are small enough to be reused.
func returnBuffersToPool(prepared []*PreparedRecord) {
	for _, prep := range prepared {
		if prep != nil && len(prep.Buf) <= 4096 {
			recordBufPool.Put(prep.Buf)
		}
	}
}

// AppendReplicatedBatch appends a batch of events that already carry their leader-assigned
// offsets. It verifies that the batch starts at the WAL's next expected offset and that
// the offsets are contiguous, then writes the records without reassigning offsets.
// This is the path used by followers during leader-follower replication.
func (w *WAL) AppendReplicatedBatch(events []*types.Event) error {
	if len(events) == 0 {
		return nil
	}

	start := time.Now()
	defer func() {
		metrics.ObserveWALAppend(strconv.FormatInt(int64(w.partitionID), 10), time.Since(start))
	}()

	prepared := acquirePreparedSlice(len(events))
	defer releasePreparedSlice(prepared)

	for i, event := range events {
		prep, err := PrepareRecord(event)
		if err != nil {
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

	// Verify contiguity and starting offset against WAL's expected next offset.
	if events[0].Offset != w.nextOffset {
		w.mu.Unlock()
		returnBuffersToPool(prepared)
		return fmt.Errorf("replicated batch gap: expected start offset %d, got %d", w.nextOffset, events[0].Offset)
	}
	for i := 1; i < len(events); i++ {
		if events[i].Offset != events[i-1].Offset+1 {
			w.mu.Unlock()
			returnBuffersToPool(prepared)
			return fmt.Errorf("non-contiguous replicated offsets: %d followed by %d", events[i-1].Offset, events[i].Offset)
		}
	}

	for _, prep := range prepared {
		prep.Event.PartitionId = w.partitionID
		binary.BigEndian.PutUint64(prep.Buf[8:16], uint64(prep.Event.Offset))
		crc := crc32.ChecksumIEEE(prep.Buf[8:])
		binary.BigEndian.PutUint32(prep.Buf[4:8], crc)
		w.nextOffset = prep.Event.Offset + 1
	}

	if err := w.activeSegment.AppendPreparedBatchUnsafe(prepared, w.config.IndexInterval); err != nil {
		w.mu.Unlock()
		returnBuffersToPool(prepared)
		return fmt.Errorf("append replicated batch to segment: %w", err)
	}

	w.highWatermark = events[len(events)-1].Offset
	w.dirty.Store(true)

	var hookEvents []*types.Event
	if w.appendHook != nil {
		hookEvents = append(hookEvents, events...)
	}

	// Flush under the WAL lock for every_event; for batch, flush under the lock
	// and sync outside so followers can continue appending while the fsync runs.
	syncSegment := w.activeSegment
	if w.fsyncMode == FsyncEveryEvent {
		if err := syncSegment.FlushBuffer(); err != nil {
			w.mu.Unlock()
			returnBuffersToPool(prepared)
			return fmt.Errorf("flush replicated batch: %w", err)
		}
		if err := syncSegment.Sync(); err != nil {
			w.mu.Unlock()
			returnBuffersToPool(prepared)
			return fmt.Errorf("sync replicated batch: %w", err)
		}
	} else if w.fsyncMode == FsyncBatch {
		if err := syncSegment.FlushBuffer(); err != nil {
			w.mu.Unlock()
			returnBuffersToPool(prepared)
			return fmt.Errorf("flush replicated batch: %w", err)
		}
	}

	if w.activeSegment.IsFull(w.config.SegmentSizeBytes) {
		if err := w.rotateSegment(); err != nil {
			w.mu.Unlock()
			returnBuffersToPool(prepared)
			return fmt.Errorf("rotate segment: %w", err)
		}
	} else {
		threshold := int64(float64(w.config.SegmentSizeBytes) * 0.9)
		if w.activeSegment.GetSize() >= threshold && w.preCreateTriggered.CompareAndSwap(false, true) {
			go w.maybePreCreateNextSegment(w.nextOffset)
		}
	}

	w.mu.Unlock()

	if w.fsyncMode == FsyncBatch {
		if err := syncSegment.Sync(); err != nil {
			returnBuffersToPool(prepared)
			return fmt.Errorf("sync replicated batch: %w", err)
		}
	}

	returnBuffersToPool(prepared)

	for _, e := range hookEvents {
		w.appendHook(e)
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

	seg, err := NewSegment(w.dataDir, nextOffset, true, w.cipher)
	if err != nil {
		log.Printf("[WAL-%d] failed to pre-create next segment: %v", w.partitionID, err)
		w.preCreateTriggered.Store(false)
		return
	}

	w.nextSegMu.Lock()
	if w.nextSegment == nil {
		w.nextSegment = seg
		} else {
			if err := seg.Close(); err != nil {
				log.Printf("[WAL-%d] failed to close unused pre-created segment: %v", w.partitionID, err)
			}
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
	segment, err := NewSegment(w.dataDir, w.nextOffset, true, w.cipher)
	if err != nil {
		return fmt.Errorf("create new segment: %w", err)
	}
	w.segments = append(w.segments, segment)
	w.activeSegment = segment

	return nil
}

// AppendEvent appends an event to WAL
func (w *WAL) AppendEvent(event *types.Event) error {
	start := time.Now()
	defer func() {
		metrics.ObserveWALAppend(strconv.FormatInt(int64(w.partitionID), 10), time.Since(start))
	}()

	// Prepare record outside the lock
	prep, err := PrepareRecord(event)
	if err != nil {
		return err
	}

	w.mu.Lock()

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
		w.mu.Unlock()
		if len(prep.Buf) <= 4096 {
			recordBufPool.Put(prep.Buf)
		}
		return fmt.Errorf("append to segment: %w", err)
	}

	w.highWatermark = event.Offset
	w.dirty.Store(true)

	// Collect hook events to fire after unlock
	var hookEvents []*types.Event
	if w.appendHook != nil {
		hookEvents = append(hookEvents, event)
	}

	syncSegment := w.activeSegment
	// For every_event, flush + sync inline under the lock. For batch, flush
	// under the lock (so we don't race with concurrent appends) and sync after
	// releasing the lock to keep the syscall out of the critical section.
	if w.fsyncMode == FsyncEveryEvent {
		if err := syncSegment.FlushBuffer(); err != nil {
			w.mu.Unlock()
			if len(prep.Buf) <= 4096 {
				recordBufPool.Put(prep.Buf)
			}
			return fmt.Errorf("flush event: %w", err)
		}
		if err := syncSegment.Sync(); err != nil {
			w.mu.Unlock()
			if len(prep.Buf) <= 4096 {
				recordBufPool.Put(prep.Buf)
			}
			return fmt.Errorf("sync event: %w", err)
		}
	} else if w.fsyncMode == FsyncBatch {
		if err := syncSegment.FlushBuffer(); err != nil {
			w.mu.Unlock()
			if len(prep.Buf) <= 4096 {
				recordBufPool.Put(prep.Buf)
			}
			return fmt.Errorf("flush event: %w", err)
		}
	}

	// Rotate segment if full
	if w.activeSegment.IsFull(w.config.SegmentSizeBytes) {
		if err := w.rotateSegment(); err != nil {
			w.mu.Unlock()
			if len(prep.Buf) <= 4096 {
				recordBufPool.Put(prep.Buf)
			}
			return fmt.Errorf("rotate segment: %w", err)
		}
	} else {
		// Trigger background pre-creation at 90% capacity
		threshold := int64(float64(w.config.SegmentSizeBytes) * 0.9)
		if w.activeSegment.GetSize() >= threshold && w.preCreateTriggered.CompareAndSwap(false, true) {
			go w.maybePreCreateNextSegment(w.nextOffset)
		}
	}

	w.mu.Unlock()

	if w.fsyncMode == FsyncBatch {
		if err := syncSegment.Sync(); err != nil {
			if len(prep.Buf) <= 4096 {
				recordBufPool.Put(prep.Buf)
			}
			return fmt.Errorf("sync event: %w", err)
		}
	}

	if len(prep.Buf) <= 4096 {
		recordBufPool.Put(prep.Buf)
	}

	// Fire hooks AFTER releasing the lock to avoid blocking writers
	for _, e := range hookEvents {
		w.appendHook(e)
	}

	return nil
}

// rotateSegment swaps to the pre-created next segment if available,
// otherwise falls back to synchronous creation.
func (w *WAL) rotateSegment() error {
	start := time.Now()
	defer func() {
		metrics.ObserveSegmentRotation(strconv.FormatInt(int64(w.partitionID), 10), time.Since(start))
	}()

	var nextSeg *Segment

	// Try to use pre-created segment
	w.nextSegMu.Lock()
	if w.nextSegment != nil {
		nextSeg = w.nextSegment
		w.nextSegment = nil
	}
	w.nextSegMu.Unlock()

	if nextSeg == nil {
		// Fallback: create synchronously
		var err error
		nextSeg, err = NewSegment(w.dataDir, w.nextOffset, true, w.cipher)
		if err != nil {
			return fmt.Errorf("create new active segment: %w", err)
		}
	}

	// Now we have the new segment ready!
	oldActive := w.activeSegment
	w.segments = append(w.segments, nextSeg)
	w.activeSegment = nextSeg
	w.preCreateTriggered.Store(false)

	// Now close the old active segment safely
	if oldActive != nil {
		oldActive.mu.Lock()
		oldActive.isActive = false // mark old segment as inactive
		oldActive.mu.Unlock()
		if err := oldActive.Close(); err != nil {
			log.Printf("[WAL-%d] Failed to close rotated segment %s: %v", w.partitionID, oldActive.GetFilename(), err)
			// Don't fail the rotation because the new active segment is already open and active!
		}
	}

	return nil
}

// ReadEvents reads events in offset range
func (w *WAL) ReadEvents(startOffset, endOffset int64) ([]*types.Event, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	// Pre-allocate with a reasonable capacity estimate
	// (endOffset-startOffset+1) capped at 1024 to avoid over-allocation
	estimated := endOffset - startOffset + 1
	if estimated < 0 {
		estimated = 0
	}
	if estimated > 1024 {
		estimated = 1024
	}
	result := make([]*types.Event, 0, estimated)

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

// ReadEvent reads a single event by offset
func (w *WAL) ReadEvent(offset int64) (*types.Event, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	for _, segment := range w.segments {
		if segment.GetFirstOffset() > offset || segment.GetLastOffset() < offset {
			continue
		}
		event, err := segment.ReadEvent(offset)
		if err != nil {
			return nil, fmt.Errorf("read event at offset %d from segment %s: %w", offset, segment.GetFilename(), err)
		}
		if event != nil {
			event.PartitionId = w.partitionID
			return event, nil
		}
	}
	return nil, fmt.Errorf("event at offset %d not found", offset)
}

// ReadEventsByTime reads events in timestamp range
func (w *WAL) ReadEventsByTime(startTS, endTS int64) ([]*types.Event, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	// Pre-allocate with a reasonable capacity estimate
	result := make([]*types.Event, 0, 256)

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
					w.recordFlushError()
					log.Printf("[WAL-%d] background flush error: %v", w.partitionID, flushErr)
				}
				continue
			}

		// Sync OUTSIDE the lock — fsync doesn't need to block writers
		// Writers can continue appending to the bufio.Writer while we sync.
		if w.fsyncMode == FsyncPeriodic || w.fsyncMode == FsyncBatch {
			if err := seg.Sync(); err != nil {
				w.recordFlushError()
				log.Printf("[WAL-%d] background sync error: %v", w.partitionID, err)
				continue
			}
		}

		// The index is reconstructable from the segment, so we do not need to
		// sync it on every entry. Flushing it here (outside the WAL lock) moves
		// the index fsync out of the write hot path.
		if seg.index != nil {
			if err := seg.index.Flush(); err != nil {
				w.recordFlushError()
				log.Printf("[WAL-%d] background index flush error: %v", w.partitionID, err)
			}
		}


		case <-w.quit:
			return
		}
	}
}

// recordFlushError increments the local flush error counter and the Prometheus counter.
func (w *WAL) recordFlushError() {
	w.flushErrors.Add(1)
	walFlushErrorsTotal.WithLabelValues(w.partitionLabel).Inc()
}

// GetFlushErrors returns the number of background flush/sync errors observed.
func (w *WAL) GetFlushErrors() int64 {
	return w.flushErrors.Load()
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
// and closes all underlying files. Safe to call multiple times.
func (w *WAL) Close() error {
	// Signal background loop to stop
	w.quitOnce.Do(func() { close(w.quit) })
	w.wg.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()

	var errs []error

	if w.activeSegment != nil {
		// Final flush + sync before close
		if err := w.activeSegment.FlushBuffer(); err != nil {
			errs = append(errs, fmt.Errorf("flush active segment: %w", err))
		}
		if err := w.activeSegment.Sync(); err != nil {
			errs = append(errs, fmt.Errorf("sync active segment: %w", err))
		}
		if err := w.activeSegment.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close active segment: %w", err))
		}
	}

	// Close any remaining non-active segments (e.g., after rotation).
	for _, seg := range w.segments {
		if seg == w.activeSegment {
			continue
		}
		if err := seg.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close segment %s: %w", seg.GetFilename(), err))
		}
	}

	// Close pre-created next segment if it was prepared but never activated.
	w.nextSegMu.Lock()
	if w.nextSegment != nil {
		if err := w.nextSegment.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close pre-created segment: %w", err))
		}
		w.nextSegment = nil
	}
	w.nextSegMu.Unlock()

	return errors.Join(errs...)
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
	var closeErrs []error
	for _, seg := range w.segments {
		if err := seg.Close(); err != nil {
			closeErrs = append(closeErrs, fmt.Errorf("close segment %s: %w", seg.GetFilename(), err))
		}
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

	return errors.Join(closeErrs...)
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

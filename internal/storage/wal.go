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
	FsyncBatch                       // Sync after each append request via group commit
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
	nextSegment        *Segment     // Pre-created next segment for fast rotation
	nextSegMu          sync.Mutex   // Protects nextSegment
	preCreateTriggered atomic.Bool  // Atomic flag to avoid duplicate pre-creation goroutines
	nextOffset         atomic.Int64 // Next offset to assign; atomically reserved outside w.mu
	highWatermark      int64
	appendSeq          atomic.Int64 // Next offset that must be appended; enforces in-order segment writes
	appendSeqMu        sync.Mutex
	appendSeqCond      *sync.Cond
	config             *WALConfig
	fsyncMode          FsyncMode   // Parsed once at init, avoids per-write string cmp
	dirty              atomic.Bool // Set on write, cleared on flush
	flushErrors        atomic.Int64
	quit               chan struct{}
	quitOnce           sync.Once
	wg                 sync.WaitGroup
	cipher             *SegmentCipher
	currentTerm        int64                    // Raft term used for new records
	appendHook         func(event *types.Event) // Called after successful append (e.g. CDC)
	coalescer          *FsyncCoalescer          // Optional global fsync coalescer

	// Group-commit coordinator for FsyncBatch mode. When multiple concurrent
	// writers need to fsync the same segment, the first becomes the leader and
	// performs a single fsync; followers register and wait for the leader's
	// result. The fsync latency (~1ms on SSD) is the natural coalescing window.
	// This preserves the "durable after AppendBatch returns" guarantee of
	// FsyncBatch while collapsing N concurrent fsyncs into one.
	gcMu        sync.Mutex
	gcLeaderSeg *Segment    // non-nil while a leader is mid-sync
	gcFollowers []*gcWaiter // writers waiting for the leader's sync result
}

// gcWaiter is a follower in the group-commit protocol. The leader fills err
// and closes done after its fsync completes.
type gcWaiter struct {
	done chan struct{}
	err  error
}

// WALConfig represents WAL configuration
type WALConfig struct {
	SegmentSizeBytes int64
	IndexInterval    int64
	FsyncMode        string // Raw string config, parsed to enum at WAL init
	FlushIntervalMS  int32
}

// NewWAL creates a new WAL using a per-WAL background flush loop.
// For production use with many partitions, prefer NewWALWithCoalescer.
func NewWAL(dataDir string, partitionID int32, config *WALConfig, cipher *SegmentCipher) (*WAL, error) {
	return newWAL(dataDir, partitionID, config, cipher, nil)
}

// NewWALWithCoalescer creates a new WAL that relies on the provided global
// FsyncCoalescer for periodic buffer flush and fsync instead of spawning its own
// background goroutine.
func NewWALWithCoalescer(dataDir string, partitionID int32, config *WALConfig, cipher *SegmentCipher, coalescer *FsyncCoalescer) (*WAL, error) {
	return newWAL(dataDir, partitionID, config, cipher, coalescer)
}

func newWAL(dataDir string, partitionID int32, config *WALConfig, cipher *SegmentCipher, coalescer *FsyncCoalescer) (*WAL, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	wal := &WAL{
		dataDir:        dataDir,
		partitionID:    partitionID,
		partitionLabel: strconv.FormatInt(int64(partitionID), 10),
		segments:       make([]*Segment, 0),
		highWatermark:  0,
		config:         config,
		fsyncMode:      ParseFsyncMode(config.FsyncMode),
		quit:           make(chan struct{}),
		cipher:         cipher,
		coalescer:      coalescer,
	}
	wal.appendSeqCond = sync.NewCond(&wal.appendSeqMu)

	// Load existing segments
	if err := wal.loadSegments(); err != nil {
		return nil, fmt.Errorf("load segments: %w", err)
	}

	// Open or create active segment
	if err := wal.openActiveSegment(); err != nil {
		return nil, fmt.Errorf("open active segment: %w", err)
	}

	// The append sequencer must start at the same value as the reservation
	// counter so that local appends append segments in strict offset order.
	wal.appendSeq.Store(wal.nextOffset.Load())

	if coalescer != nil {
		coalescer.Register(wal)
	} else if config.FlushIntervalMS > 0 {
		// Start periodic background flush only if no coalescer is provided.
		wal.wg.Add(1)
		go wal.periodicFlushLoop()
	}

	return wal, nil
}

// SetCurrentTerm sets the Raft term used for new records. The replication layer
// calls this whenever a partition becomes leader or appends entries under a
// leader's term.
func (w *WAL) SetCurrentTerm(term int64) {
	atomic.StoreInt64(&w.currentTerm, term)
}

// GetCurrentTerm returns the term used for new records.
func (w *WAL) GetCurrentTerm() int64 {
	return atomic.LoadInt64(&w.currentTerm)
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
		w.nextOffset.Store(segment.GetLastOffset() + 1)
		w.highWatermark = segment.GetLastOffset()
	}

	if len(w.segments) > 0 {
		log.Printf("[WAL-%d] Loaded %d segments, nextOffset=%d", w.partitionID, len(w.segments), w.nextOffset.Load())
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
	Term  int64
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

// PrepareRecord serializes an event to a byte slice with placeholders for offset and CRC.
// The record format v2 is: [crc32 4][term 8][offset 8][schedule_ts 8][msgID len 2][msgID]
// [topic len 2][topic][payload len 4][payload][checksum 4][meta count 2][meta...].
func PrepareRecord(event *types.Event, term int64) (*PreparedRecord, error) {
	if event == nil {
		return nil, fmt.Errorf("event is nil")
	}

	// Serialize local values instead of mutating the caller's event. Producers
	// may reuse an event object across concurrent appenders; WAL preparation must
	// not introduce a data race on Term or Checksum.
	eventTerm := event.Term
	if eventTerm == 0 {
		eventTerm = term
	}
	eventChecksum := event.Checksum
	if eventChecksum == 0 && len(event.Payload) > 0 {
		eventChecksum = crc32.ChecksumIEEE(event.Payload)
	}

	msgIDLen := len(event.GetMessageId())
	topicLen := len(event.Topic)
	payloadLen := len(event.Payload)
	metaCount := len(event.Meta)
	maxUint16 := int(^uint16(0))
	if msgIDLen > maxUint16 || topicLen > maxUint16 || metaCount > maxUint16 {
		return nil, fmt.Errorf("event record field exceeds uint16 limit")
	}
	for key, value := range event.Meta {
		if len(key) > maxUint16 || len(value) > maxUint16 {
			return nil, fmt.Errorf("event metadata field exceeds uint16 limit")
		}
	}

	// Calculate size
	size := 4 + 4 + 8 + 8 + 8 + 2 + msgIDLen + 2 + topicLen + 4 + payloadLen + 4 + 2
	for k, v := range event.Meta {
		metaEntrySize := 2 + len(k) + 2 + len(v)
		if size > (1<<63-1)-metaEntrySize {
			return nil, fmt.Errorf("event record too large: overflow in size calculation")
		}
		size += metaEntrySize
	}
	if size > int(^uint32(0)) {
		return nil, fmt.Errorf("event record exceeds uint32 length limit")
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

	// Term (8 bytes)
	binary.BigEndian.PutUint64(buf[offset:offset+8], uint64(eventTerm))
	offset += 8

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

	// Payload checksum (4 bytes)
	binary.BigEndian.PutUint32(buf[offset:offset+4], eventChecksum)
	offset += 4

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
		Term:  eventTerm,
	}, nil
}

// termForEvent returns the term to use when writing an event. Replicated events
// may already carry their term; locally produced events use the WAL's current term.
func (w *WAL) termForEvent(event *types.Event) int64 {
	if event.Term != 0 {
		return event.Term
	}
	return w.GetCurrentTerm()
}

// waitAppendTurn blocks until the reserved startOffset is the next offset that
// must be appended. This preserves strict in-order segment writes when offsets
// are reserved outside w.mu.
func (w *WAL) waitAppendTurn(startOffset int64) {
	w.appendSeqMu.Lock()
	for w.appendSeq.Load() != startOffset {
		w.appendSeqCond.Wait()
	}
	w.appendSeqMu.Unlock()
}

// advanceAppendTurn releases the turn for the next waiter.
func (w *WAL) advanceAppendTurn(endOffset int64) {
	w.appendSeq.Store(endOffset)
	w.appendSeqMu.Lock()
	w.appendSeqCond.Broadcast()
	w.appendSeqMu.Unlock()
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

	// 1. Prepare records OUTSIDE the lock; serialization is the most expensive
	//    part of the write path and does not depend on the assigned offset.
	prepared := acquirePreparedSlice(len(events))
	defer releasePreparedSlice(prepared)

	for i, event := range events {
		prep, err := PrepareRecord(event, w.termForEvent(event))
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

	// 2. Reserve a contiguous offset range atomically. This is safe because the
	//    actual segment append is serialized by the append sequencer below.
	n := int64(len(events))
	endOffset := w.nextOffset.Add(n)
	startOffset := endOffset - n

	// 3. Fill offsets, partition IDs, and CRCs OUTSIDE the lock. CRC32 is
	//    hardware-accelerated but still measurable at high throughput.
	for i, prep := range prepared {
		offset := startOffset + int64(i)
		prep.Event.Offset = offset
		prep.Event.PartitionId = w.partitionID

		// Fill offset (bytes 16-24)
		binary.BigEndian.PutUint64(prep.Buf[16:24], uint64(offset))

		// Compute and fill CRC32 (bytes 4-8)
		crc := crc32.ChecksumIEEE(prep.Buf[8:])
		binary.BigEndian.PutUint32(prep.Buf[4:8], crc)
	}

	// 4. Wait until it is our turn to write to the segment. Offsets may be
	//    reserved out of order, but segment writes must remain strictly ordered.
	w.waitAppendTurn(startOffset)

	// 5. Append to active segment — caller holds w.mu, so use the lock-free variant.
	w.mu.Lock()
	if err := w.activeSegment.AppendPreparedBatchLocked(prepared, w.config.IndexInterval); err != nil {
		w.mu.Unlock()
		w.advanceAppendTurn(endOffset)
		returnBuffersToPool(prepared)
		return fmt.Errorf("append prepared batch to segment: %w", err)
	}

	if lastOffset := events[len(events)-1].Offset; lastOffset > w.highWatermark {
		w.highWatermark = lastOffset
	}
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
			w.advanceAppendTurn(endOffset)
			returnBuffersToPool(prepared)
			return fmt.Errorf("flush batch: %w", err)
		}
		if err := syncSegment.Sync(); err != nil {
			w.mu.Unlock()
			w.advanceAppendTurn(endOffset)
			returnBuffersToPool(prepared)
			return fmt.Errorf("sync batch: %w", err)
		}
	} else if w.fsyncMode == FsyncBatch {
		if err := syncSegment.FlushBuffer(); err != nil {
			w.mu.Unlock()
			w.advanceAppendTurn(endOffset)
			returnBuffersToPool(prepared)
			return fmt.Errorf("flush batch: %w", err)
		}
	}

	// Rotate segment if full
	if w.activeSegment.IsFull(w.config.SegmentSizeBytes) {
		if err := w.rotateSegment(); err != nil {
			w.mu.Unlock()
			w.advanceAppendTurn(endOffset)
			returnBuffersToPool(prepared)
			return fmt.Errorf("rotate segment: %w", err)
		}
	} else {
		// Trigger background pre-creation at 75% capacity so the next segment is
		// almost certainly ready before rotation is required.
		threshold := int64(float64(w.config.SegmentSizeBytes) * 0.75)
		if w.activeSegment.GetSize() >= threshold && w.preCreateTriggered.CompareAndSwap(false, true) {
			go w.maybePreCreateNextSegment(w.nextOffset.Load())
		}
	}

	// Advance the sequencer while still holding w.mu so the next writer cannot
	// interleave appends, then release both locks.
	w.advanceAppendTurn(endOffset)
	w.mu.Unlock()

	// For batch mode, the buffer was already flushed under the lock; now sync the
	// file descriptor outside the WAL lock so other writers can continue appending.
	// Group commit coalesces concurrent fsyncs into one: the first writer to
	// arrive becomes the leader and fsyncs; concurrent writers join as followers
	// and share the result, collapsing N fsyncs into one.
	if w.fsyncMode == FsyncBatch {
		if err := w.groupCommitSync(syncSegment); err != nil {
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
		prep, err := PrepareRecord(event, w.termForEvent(event))
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

	// Fill offsets, partition IDs, and CRCs outside the lock; offsets are already
	// leader-assigned so the expensive CRC work can overlap with other replication.
	for _, prep := range prepared {
		prep.Event.PartitionId = w.partitionID
		binary.BigEndian.PutUint64(prep.Buf[16:24], uint64(prep.Event.Offset))
		crc := crc32.ChecksumIEEE(prep.Buf[8:])
		binary.BigEndian.PutUint32(prep.Buf[4:8], crc)
	}

	w.mu.Lock()

	// Verify contiguity and starting offset against WAL's expected next offset.
	nextOffset := w.nextOffset.Load()
	if events[0].Offset != nextOffset {
		w.mu.Unlock()
		returnBuffersToPool(prepared)
		return fmt.Errorf("replicated batch gap: expected start offset %d, got %d", nextOffset, events[0].Offset)
	}
	for i := 1; i < len(events); i++ {
		if events[i].Offset != events[i-1].Offset+1 {
			w.mu.Unlock()
			returnBuffersToPool(prepared)
			return fmt.Errorf("non-contiguous replicated offsets: %d followed by %d", events[i-1].Offset, events[i].Offset)
		}
	}

	if err := w.activeSegment.AppendPreparedBatchLocked(prepared, w.config.IndexInterval); err != nil {
		w.mu.Unlock()
		returnBuffersToPool(prepared)
		return fmt.Errorf("append replicated batch to segment: %w", err)
	}

	endOffset := events[len(events)-1].Offset + 1
	w.nextOffset.Store(endOffset)
	w.appendSeq.Store(endOffset)
	if lastOffset := events[len(events)-1].Offset; lastOffset > w.highWatermark {
		w.highWatermark = lastOffset
	}
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
			go w.maybePreCreateNextSegment(w.nextOffset.Load())
		}
	}

	w.mu.Unlock()

	if w.fsyncMode == FsyncBatch {
		if err := w.groupCommitSync(syncSegment); err != nil {
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

	seg, err := NewSegmentWithSize(w.dataDir, nextOffset, true, w.cipher, w.config.SegmentSizeBytes)
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
	segment, err := NewSegmentWithSize(w.dataDir, w.nextOffset.Load(), true, w.cipher, w.config.SegmentSizeBytes)
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
	prep, err := PrepareRecord(event, w.termForEvent(event))
	if err != nil {
		return err
	}

	// Reserve a single offset atomically and fill it (plus CRC) outside the lock.
	endOffset := w.nextOffset.Add(1)
	offset := endOffset - 1
	event.Offset = offset
	event.PartitionId = w.partitionID
	binary.BigEndian.PutUint64(prep.Buf[16:24], uint64(offset))
	crc := crc32.ChecksumIEEE(prep.Buf[8:])
	binary.BigEndian.PutUint32(prep.Buf[4:8], crc)

	// Wait for our turn so segment writes remain ordered.
	w.waitAppendTurn(offset)

	// Append to active segment — caller holds w.mu, so use the lock-free variant.
	w.mu.Lock()
	if err := w.activeSegment.AppendPreparedBatchLocked([]*PreparedRecord{prep}, w.config.IndexInterval); err != nil {
		w.mu.Unlock()
		w.advanceAppendTurn(endOffset)
		if len(prep.Buf) <= 4096 {
			recordBufPool.Put(prep.Buf)
		}
		return fmt.Errorf("append to segment: %w", err)
	}

	if event.Offset > w.highWatermark {
		w.highWatermark = event.Offset
	}
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
			w.advanceAppendTurn(endOffset)
			if len(prep.Buf) <= 4096 {
				recordBufPool.Put(prep.Buf)
			}
			return fmt.Errorf("flush event: %w", err)
		}
		if err := syncSegment.Sync(); err != nil {
			w.mu.Unlock()
			w.advanceAppendTurn(endOffset)
			if len(prep.Buf) <= 4096 {
				recordBufPool.Put(prep.Buf)
			}
			return fmt.Errorf("sync event: %w", err)
		}
	} else if w.fsyncMode == FsyncBatch {
		if err := syncSegment.FlushBuffer(); err != nil {
			w.mu.Unlock()
			w.advanceAppendTurn(endOffset)
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
			w.advanceAppendTurn(endOffset)
			if len(prep.Buf) <= 4096 {
				recordBufPool.Put(prep.Buf)
			}
			return fmt.Errorf("rotate segment: %w", err)
		}
	} else {
		// Trigger background pre-creation at 75% capacity so the next segment is
		// almost certainly ready before rotation is required.
		threshold := int64(float64(w.config.SegmentSizeBytes) * 0.75)
		if w.activeSegment.GetSize() >= threshold && w.preCreateTriggered.CompareAndSwap(false, true) {
			go w.maybePreCreateNextSegment(w.nextOffset.Load())
		}
	}

	w.advanceAppendTurn(endOffset)
	w.mu.Unlock()

	if w.fsyncMode == FsyncBatch {
		if err := w.groupCommitSync(syncSegment); err != nil {
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
		nextSeg, err = NewSegmentWithSize(w.dataDir, w.nextOffset.Load(), true, w.cipher, w.config.SegmentSizeBytes)
		if err != nil {
			return fmt.Errorf("create new active segment: %w", err)
		}
	}

	// Now we have the new segment ready!
	oldActive := w.activeSegment
	w.segments = append(w.segments, nextSeg)
	w.activeSegment = nextSeg
	w.preCreateTriggered.Store(false)

	// Deactivate (not Close) the old active segment: release its write-side/mmap
	// resources but keep its read handle + index open so historical reads of this
	// now-rotated segment keep working (Replay, follower catch-up, cross-region
	// fetch). Closing the handle here previously made every read of a rotated
	// segment fail with os.ErrClosed. Retention/Delete closes it for good later.
	if oldActive != nil {
		if err := oldActive.Deactivate(); err != nil {
			log.Printf("[WAL-%d] Failed to deactivate rotated segment %s: %v", w.partitionID, oldActive.GetFilename(), err)
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

// backgroundFlushBuffer is invoked by the global FsyncCoalescer. It flushes the
// active segment's bufio buffer under the WAL lock and reports whether the
// segment should also be fsynced (periodic/batch modes). Returns nil when the
// WAL is clean since the last flush.
func (w *WAL) backgroundFlushBuffer() (*Segment, bool, error) {
	if !w.dirty.CompareAndSwap(true, false) {
		return nil, false, nil
	}

	w.mu.Lock()
	seg := w.activeSegment
	var err error
	if seg != nil {
		err = seg.FlushBuffer()
	}
	w.mu.Unlock()

	needsSync := w.fsyncMode == FsyncPeriodic || w.fsyncMode == FsyncBatch
	return seg, needsSync, err
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

// groupCommitSync coalesces concurrent fsync requests in FsyncBatch mode.
//
// When multiple writers call this method concurrently for the same segment,
// the first caller becomes the "leader" and performs a single fsync. All
// subsequent callers ("followers") that arrive while the leader is syncing
// register themselves and block until the leader completes, then return the
// leader's result. This turns N concurrent per-batch fsyncs into one shared
// fsync, with the fsync latency (~1ms on SSD) acting as the natural coalescing
// window — no timer or batching delay is needed.
//
// Durability semantics are unchanged: every caller still waits for a
// successful fsync before returning. The guarantee is "durable after
// AppendBatch returns" — group commit just makes N such guarantees cheaper.
//
// Callers targeting a DIFFERENT segment (e.g. after rotation) become leaders
// of their own group, so cross-segment fsyncs are never incorrectly shared.
func (w *WAL) groupCommitSync(seg *Segment) error {
	w.gcMu.Lock()
	// If a leader is already syncing THIS segment, join its group as a follower.
	if w.gcLeaderSeg == seg {
		waiter := &gcWaiter{done: make(chan struct{})}
		w.gcFollowers = append(w.gcFollowers, waiter)
		w.gcMu.Unlock()
		<-waiter.done
		return waiter.err
	}

	// No active leader for this segment — become the leader.
	w.gcLeaderSeg = seg
	// Detach the current follower list (followers that joined before us for a
	// prior leader were already woken when that leader finished; the slice is
	// empty at this point, but reset it for clarity).
	followers := w.gcFollowers
	w.gcFollowers = nil
	w.gcMu.Unlock()

	// Perform the actual fsync outside gcMu so new followers can register while
	// we sync (they will see gcLeaderSeg == seg and join us).
	syncErr := seg.Sync()

	// Wake all followers (including any that joined during the fsync above) and
	// clear leadership so the next caller can become a new leader.
	w.gcMu.Lock()
	w.gcLeaderSeg = nil
	// Merge followers that joined during our fsync with the snapshot taken earlier.
	allFollowers := followers
	if len(w.gcFollowers) > 0 {
		allFollowers = append(allFollowers, w.gcFollowers...)
		w.gcFollowers = nil
	}
	w.gcMu.Unlock()

	for _, fw := range allFollowers {
		fw.err = syncErr
		close(fw.done)
	}

	return syncErr
}

// Close stops the background flush loop, flushes and syncs the active segment,
// and closes all underlying files. Safe to call multiple times.
func (w *WAL) Close() error {
	// Signal background loop to stop
	w.quitOnce.Do(func() { close(w.quit) })
	w.wg.Wait()

	// Unregister from the global coalescer so it no longer touches this WAL.
	if w.coalescer != nil {
		w.coalescer.Unregister(w)
	}

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

// TruncateToOffset removes all events at or after `offset`, rewinding the WAL so
// the next appended event lands at `offset`. It is used for follower log
// truncation: when a higher-term leader's replicated log starts before the
// follower's next offset, the follower must discard its divergent tail before it
// can accept the leader's entries.
//
// Segments entirely at/after `offset` are deleted; the segment containing
// `offset` is truncated in place. The active segment is preserved (truncated, not
// deleted). This must only be invoked by an epoch-fenced caller — the replication
// Append handler verifies the leader's term before calling it.
func (w *WAL) TruncateToOffset(offset int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if offset < 0 {
		offset = 0
	}
	oldNext := w.nextOffset.Load()
	if offset >= oldNext {
		return 0, nil // nothing at/after offset
	}
	// The number of events removed is exactly oldNext - offset (offsets are dense).
	removed := int(oldNext - offset)

	// Reconcile segments. Segments entirely at/after `offset` are deleted (including
	// the current active segment). The segment straddling `offset` is truncated to
	// keep [.., offset-1] and stays as a read-only (rotated) segment. A brand-new
	// active segment starting at `offset` is then created so subsequent appends
	// land contiguously — this avoids the complexity of re-activating a rotated
	// segment's torn-down write path.
	kept := make([]*Segment, 0, len(w.segments))
	for _, segment := range w.segments {
		first := segment.GetFirstOffset()
		last := segment.GetLastOffset()

		switch {
		case first >= offset:
			// Entire segment is at/after offset — delete it (even if it is active).
			if err := segment.Delete(); err != nil {
				return removed, fmt.Errorf("delete segment %s during truncate: %w", segment.GetFilename(), err)
			}
		case last >= offset:
			// Straddles offset — truncate to keep through offset-1, keep read-only.
			if _, err := segment.TruncateAfterOffset(offset - 1); err != nil {
				return removed, fmt.Errorf("truncate segment %s to offset %d: %w", segment.GetFilename(), offset, err)
			}
			// Ensure it is not treated as active (its write path may still be live if
			// it was the active segment before truncation).
			if segment == w.activeSegment {
				if err := segment.Deactivate(); err != nil {
					log.Printf("[WAL-%d] deactivate straddling segment failed: %v", w.partitionID, err)
				}
			}
			kept = append(kept, segment)
		default:
			// Entirely before offset — keep as-is.
			kept = append(kept, segment)
		}
	}

	// Create a fresh active segment beginning at `offset`.
	newActive, err := NewSegmentWithSize(w.dataDir, offset, true, w.cipher, w.config.SegmentSizeBytes)
	if err != nil {
		return removed, fmt.Errorf("create new active segment at offset %d: %w", offset, err)
	}
	kept = append(kept, newActive)
	w.segments = kept
	w.activeSegment = newActive

	// Discard any pre-created next segment; its firstOffset is now stale.
	w.nextSegMu.Lock()
	if w.nextSegment != nil {
		_ = w.nextSegment.Close()
		w.nextSegment = nil
	}
	w.nextSegMu.Unlock()
	w.preCreateTriggered.Store(false)

	// Rewind offset counters so the next append lands exactly at `offset`.
	w.nextOffset.Store(offset)
	w.appendSeq.Store(offset)
	w.highWatermark = offset - 1
	w.dirty.Store(true)

	log.Printf("[WAL-%d] Truncated to offset %d, removed %d events", w.partitionID, offset, removed)
	return removed, nil
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
	return w.nextOffset.Load()
}

// GetTermForOffset returns the Raft term stored for the entry at the given offset.
func (w *WAL) GetTermForOffset(offset int64) (int64, error) {
	event, err := w.ReadEvent(offset)
	if err != nil {
		return 0, err
	}
	return event.GetTerm(), nil
}
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

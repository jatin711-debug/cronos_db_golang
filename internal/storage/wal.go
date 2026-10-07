// Package storage implements durable segmented write-ahead logging (WAL) for
// CronosDB partitions. It provides mmap-backed segment files, sparse indexes,
// optional AES-256-GCM at-rest encryption, incremental backups, and coalesced
// fsync across many partitions.
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
// Using an int enum instead of string comparison eliminates per-write overhead.
type FsyncMode int

const (
	// FsyncPeriodic syncs in the background flush loop (or FsyncCoalescer).
	FsyncPeriodic FsyncMode = iota
	// FsyncEveryEvent syncs after every write (safest, slowest).
	FsyncEveryEvent
	// FsyncBatch syncs after each append request via group-commit coalescing.
	FsyncBatch
)

// ParseFsyncMode converts a config string to FsyncMode.
// Accepted values: "every_event", "batch"; any other value maps to FsyncPeriodic.
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

// WAL is a segmented write-ahead log for a single partition.
// Offsets are reserved atomically, then written in strict order via an append
// sequencer so concurrent producers never interleave records on disk.
type WAL struct {
	mu             sync.RWMutex
	dataDir        string // root directory for segments/ and index/
	partitionID    int32  // owning partition ID
	partitionLabel string // cached string form of partitionID for metrics
	segments       []*Segment
	activeSegment  *Segment
	nextOffset     atomic.Int64 // next offset to assign; reserved outside w.mu
	highWatermark  int64        // highest durable offset written (inclusive)
	appendSeq      atomic.Int64 // next offset that must be appended (in-order writes)
	appendSeqMu    sync.Mutex
	appendSeqCond  *sync.Cond
	config         *WALConfig
	fsyncMode      FsyncMode   // parsed once at init; avoids per-write string cmp
	dirty          atomic.Bool // set on write, cleared on flush
	flushErrors    atomic.Int64
	quit           chan struct{}
	quitOnce       sync.Once
	wg             sync.WaitGroup
	cipher         *SegmentCipher           // optional at-rest encryption; nil = plaintext
	currentTerm    int64                    // Raft term stamped on newly produced records
	appendHook     func(event *types.Event) // called after successful append (e.g. CDC)
	coalescer      *FsyncCoalescer          // optional global fsync coalescer

	// Group-commit coordinator for FsyncBatch mode: writers that finish
	// appending while a sync is in flight share the next one. See groupCommitSync.
	gcMu      sync.Mutex
	gcCurrent *groupSync // sync in flight, nil when idle
	gcNext    *groupSync // sync that starts when gcCurrent finishes

	// checkpointMu is held while Checkpoint copies files, and by everything
	// that deletes segment files or cuts them short. It is taken before mu.
	checkpointMu sync.Mutex
	// afterCheckpointCut, when set by a test, runs once Checkpoint has released
	// the WAL lock and before it copies anything.
	afterCheckpointCut func()
}

// groupSync is one flush+fsync shared by a group of writers. The writer that
// created it runs it; the others wait on done and read err.
type groupSync struct {
	seg  *Segment
	done chan struct{}
	err  error
}

// WALConfig configures segment sizing, sparse indexing, and durability mode.
type WALConfig struct {
	// SegmentSizeBytes is the target maximum size of each segment file in bytes.
	// Segments rotate when they reach this size. Zero uses a library default.
	SegmentSizeBytes int64
	// IndexInterval is the sparse-index period in number of events
	// (one index entry every IndexInterval events).
	IndexInterval int64
	// FsyncMode is the raw string durability mode ("periodic", "every_event",
	// "batch"), parsed to FsyncMode at WAL construction.
	FsyncMode string
	// FlushIntervalMS is the background buffer-flush interval in milliseconds.
	// Used by the per-WAL flush loop or the shared FsyncCoalescer. Zero disables
	// periodic flush when no coalescer is provided.
	FlushIntervalMS int32
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

	if err := RecoverSnapshot(dataDir); err != nil {
		return nil, fmt.Errorf("recover snapshot install: %w", err)
	}
	// Load existing segments
	if err := wal.loadSegments(); err != nil {
		for _, seg := range wal.segments {
			_ = seg.Close()
		}
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
			return fmt.Errorf("open existing segment %s: %w", filename, err)
		}

		// Verify segment integrity by reading all events and checking CRCs
		if err := w.verifySegment(segment); err != nil {
			closeErr := segment.Close()
			if closeErr != nil {
				log.Printf("[WAL-%d] WARNING: Segment %s verification failed: %v; additionally failed to close: %v", w.partitionID, filename, err, closeErr)
			} else {
				log.Printf("[WAL-%d] WARNING: Segment %s verification failed: %v", w.partitionID, filename, err)
			}
			return fmt.Errorf("verify existing segment %s: %w", filename, err)
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

// PreparedRecord holds a pre-encoded WAL record buffer for fast serialization.
// Offset and CRC placeholders in Buf are filled after the offset is reserved.
type PreparedRecord struct {
	// Event is the source event; Offset and PartitionId are filled during append.
	Event *types.Event
	// Buf is the full on-disk record (length prefix through meta), with offset/CRC
	// placeholders until assignment.
	Buf []byte
	// Term is the Raft term encoded into the record.
	Term int64
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

// AppendBatch appends a batch of events to the WAL, assigning contiguous offsets
// and applying the configured fsync mode. Empty batches are a no-op.
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
		// The record's term travels with the event: replication sends it to
		// followers, which store the same term for the same offset.
		prep.Event.Term = prep.Term

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

	// every_event flushes and syncs inline, under the WAL lock. batch does both
	// after releasing it (groupCommitSync), so other writers keep appending
	// while the disk works and then share one sync.
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
	}

	// Rotate segment if full
	if w.activeSegment.IsFull(w.config.SegmentSizeBytes) {
		if err := w.rotateSegment(); err != nil {
			w.mu.Unlock()
			w.advanceAppendTurn(endOffset)
			returnBuffersToPool(prepared)
			return fmt.Errorf("rotate segment: %w", err)
		}
	}

	// Advance the sequencer while still holding w.mu so the next writer cannot
	// interleave appends, then release both locks.
	w.advanceAppendTurn(endOffset)
	w.mu.Unlock()

	// Batch mode: make the append durable outside the WAL lock, sharing the
	// flush and fsync with every writer that appended in the meantime.
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

	// Flush and sync under the WAL lock for every_event; batch does both after
	// the lock is released (groupCommitSync).
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
	}

	if w.activeSegment.IsFull(w.config.SegmentSizeBytes) {
		if err := w.rotateSegment(); err != nil {
			w.mu.Unlock()
			returnBuffersToPool(prepared)
			return fmt.Errorf("rotate segment: %w", err)
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

// openActiveSegment opens the last active segment or creates a new one.
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

// AppendEvent appends a single event to the WAL, assigning the next offset and
// applying the configured fsync mode.
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
	event.Term = prep.Term
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

// rotateSegment creates a segment at the actual appended offset boundary.
// The caller must hold w.mu.
func (w *WAL) rotateSegment() error {
	start := time.Now()
	defer func() {
		metrics.ObserveSegmentRotation(strconv.FormatInt(int64(w.partitionID), 10), time.Since(start))
	}()

	// nextOffset includes reservations by waiting writers. The new segment's
	// start must follow the last record actually appended, not those reservations.
	// Speculatively naming a segment at 75% capacity also gets this boundary wrong.
	firstOffset := w.activeSegment.GetLastOffset() + 1
	nextSeg, err := NewSegmentWithSize(w.dataDir, firstOffset, true, w.cipher, w.config.SegmentSizeBytes)
	if err != nil {
		return fmt.Errorf("create new active segment: %w", err)
	}

	// Now we have the new segment ready!
	oldActive := w.activeSegment
	w.segments = append(w.segments, nextSeg)
	w.activeSegment = nextSeg

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

// ReadEvents reads events in the inclusive offset range [startOffset, endOffset]
// across all segments that overlap the range.
func (w *WAL) ReadEvents(startOffset, endOffset int64) ([]*types.Event, error) {
	// Only the segment list needs the WAL lock. Reading under it would hold up
	// every append to this partition for the length of the read; each segment
	// guards its own data.
	w.mu.RLock()
	segments := make([]*Segment, 0, 2)
	for _, segment := range w.segments {
		if segment.GetLastOffset() < startOffset || segment.GetFirstOffset() > endOffset {
			continue
		}
		segments = append(segments, segment)
	}
	w.mu.RUnlock()

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

	for _, segment := range segments {
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

// ReadEvent reads a single event by absolute partition offset.
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

// ReadEventsByTime reads events whose schedule timestamp falls in the inclusive
// range [startTS, endTS] (milliseconds since epoch).
func (w *WAL) ReadEventsByTime(startTS, endTS int64) ([]*types.Event, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	// Pre-allocate with a reasonable capacity estimate
	result := make([]*types.Event, 0, 256)

	// Find segments that contain the timestamp range
	for _, segment := range w.segments {
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

	// Only the segment pointer needs the WAL lock. The flush itself is disk
	// I/O for mmap segments and is serialized by the segment's own locks;
	// doing it under w.mu would stall every append for its duration.
	w.mu.RLock()
	seg := w.activeSegment
	w.mu.RUnlock()
	var err error
	if seg != nil {
		err = seg.FlushBuffer()
	}

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

			// Flush outside the WAL lock: for mmap segments this is disk I/O,
			// and the segment serializes it with appends on its own.
			w.mu.RLock()
			seg := w.activeSegment
			w.mu.RUnlock()
			var flushErr error
			if seg != nil {
				flushErr = seg.FlushBuffer()
			}

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
	w.mu.RLock()
	seg := w.activeSegment
	w.mu.RUnlock()
	if seg == nil {
		return nil
	}
	if err := seg.FlushBuffer(); err != nil {
		return err
	}
	return seg.Sync()
}

// groupCommitSync makes a completed append durable in FsyncBatch mode, sharing
// the flush and fsync between concurrent writers.
//
// A writer may only rely on a sync that started after its append finished: a
// sync already in flight captured the segment's write position earlier and
// says nothing about later bytes. So a writer that finds no sync in flight
// runs one itself, and a writer that finds one in flight joins the next sync,
// which starts as soon as the current one ends. Everything appended during one
// fsync is therefore covered by a single following fsync, and no append is
// acknowledged before a sync that includes it has succeeded.
//
// A queued sync covers one segment. A writer whose segment differs (a rotation
// happened in between) flushes that segment directly.
func (w *WAL) groupCommitSync(seg *Segment) error {
	w.gcMu.Lock()
	current := w.gcCurrent
	if current == nil {
		current = &groupSync{seg: seg, done: make(chan struct{})}
		w.gcCurrent = current
		w.gcMu.Unlock()
		return w.runGroupSync(current)
	}
	next := w.gcNext
	if next == nil {
		next = &groupSync{seg: seg, done: make(chan struct{})}
		w.gcNext = next
		w.gcMu.Unlock()
		<-current.done // promoted to gcCurrent by the sync that just ended
		return w.runGroupSync(next)
	}
	w.gcMu.Unlock()
	if next.seg != seg {
		return seg.Flush()
	}
	<-next.done
	return next.err
}

// runGroupSync flushes and syncs the group's segment, then hands the turn to
// the queued sync, if any, before waking the group.
func (w *WAL) runGroupSync(group *groupSync) error {
	group.err = group.seg.Flush()
	w.gcMu.Lock()
	w.gcCurrent, w.gcNext = w.gcNext, nil
	w.gcMu.Unlock()
	close(group.done)
	return group.err
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

	return errors.Join(errs...)
}

// GetSegments returns a snapshot copy of all open segments (active and historical).
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
	w.checkpointMu.Lock()
	defer w.checkpointMu.Unlock()
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
	w.checkpointMu.Lock()
	defer w.checkpointMu.Unlock()
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
	w.checkpointMu.Lock()
	defer w.checkpointMu.Unlock()
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

// GetActiveSegment returns the segment currently accepting appends.
func (w *WAL) GetActiveSegment() *Segment {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.activeSegment
}

// GetNextOffset returns the next offset that will be assigned to a new event.
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

// GetHighWatermark returns the highest durable offset written to the WAL
// (inclusive). Zero-value before any writes is 0; an empty WAL may report
// lastOffset-based values via GetLastOffset instead.
func (w *WAL) GetHighWatermark() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.highWatermark
}

// GetLastOffset returns the offset of the last event in the log, or -1 if
// nothing has been written to it.
func (w *WAL) GetLastOffset() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if len(w.segments) == 0 {
		return -1
	}
	return logEndOf(w.segments[len(w.segments)-1])
}

// logEndOf returns the offset of the last event at or before the end of
// segment. A segment holds no events right after the rotation that created
// it, and the log then ends just before the segment's first offset.
func logEndOf(segment *Segment) int64 {
	if last := segment.GetLastOffset(); last >= 0 {
		return last
	}
	return segment.GetFirstOffset() - 1
}

// GetDataDir returns the WAL data directory path.
func (w *WAL) GetDataDir() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.dataDir
}

// ReloadSegments reloads segments from disk after bulk file sync (e.g. snapshot
// install). Existing open segment handles are closed first.
func (w *WAL) ReloadSegments() error {
	w.checkpointMu.Lock()
	defer w.checkpointMu.Unlock()
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
	w.checkpointMu.Lock()
	defer w.checkpointMu.Unlock()
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

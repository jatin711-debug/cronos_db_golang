package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// Segment is a single WAL segment file holding a contiguous offset range.
// Active segments accept appends via mmap (plaintext) or a buffered writer
// (encrypted); rotated segments remain readable until retention deletes them.
type Segment struct {
	mu              sync.RWMutex
	flushMu         sync.Mutex // serializes flush/sync with mmap remapping
	segmentFile     *os.File
	writer          *bufio.Writer // buffered path for encrypted or non-mmap writes; nil until one is made (bufferedWriter)
	reader          io.ReaderAt
	mmapData        []byte    // memory-mapped file view; nil when encrypted or unmapped
	mmapWritePos    int64     // current write cursor within mmapData (bytes)
	mmapSize        int64     // mapped length in bytes (tracks len(mmapData))
	mmapFlushedPos  int64     // mmap prefix already handed to the OS by syncMmap; guarded by flushMu
	writeErr        error     // first failed write-out or sync, returned by every later one; guarded by flushMu (see failedWriteLocked)
	firstOffset     int64     // first event offset stored in this segment
	lastOffset      int64     // last event offset; -1 when empty
	firstTS         int64     // schedule timestamp of first event (ms); 0 if unknown
	lastTS          int64     // schedule timestamp of last event (ms)
	times           timeRange // least and greatest schedule timestamp of its events
	nextOffset      int64     // next offset expected to be appended in this segment
	indexEntries    int64     // number of sparse index entries written
	nextIndexOffset int64     // relative event count threshold for next index entry
	sizeBytes       int64     // logical data end in bytes (header + records)
	createdTS       int64     // segment creation time (Unix ms)
	isActive        bool      // true while this segment accepts appends
	closed          bool      // true after Close(); reads/writes are rejected
	dataDir         string
	filename        string // e.g. "00000000000000000012.log"
	indexFilename   string // e.g. "00000000000000000012.index"
	index           *Index // sparse index for fast seeking by offset/timestamp
	recordBuf       []byte // reusable buffer for serialization
	cipher          *SegmentCipher

	// deleted is set by Delete: the segment has left the log.
	deleted atomic.Bool

	// invalidTail is set when the bytes after the last valid record are not
	// empty space: a record that was never finished, or one that no longer
	// reads. What that means depends on where the segment is in the log (see
	// WAL.loadSegments). invalidAt is where those bytes begin, invalidWhy what
	// was found there.
	invalidTail bool
	invalidAt   int64
	invalidWhy  string
	// validAfterTail is a verified indexed record beyond the scan boundary,
	// or -1 when the sparse index proves nothing about the tail.
	validAfterTail int64
	// predecessor is a durable boundary of the preceding segment, stored in
	// this segment's checksummed header. The first retained segment may omit it.
	predecessor    *segmentBoundary
	boundaryDamage bool
}

type segmentBoundary struct {
	first, last, size int64
}

const boundaryMagic = "BND1"

// SegmentHeaderVersion is the on-disk WAL segment format understood by this build.
const SegmentHeaderVersion = 2

var errBoundaryPublication = errors.New("segment boundary publication is uncertain")

const defaultSegmentPreallocSize = 64 * 1024 * 1024

// NewSegment creates a new segment using a conservative default preallocation
// size. WAL callers should use NewSegmentWithSize so the configured segment
// size is honored.
func NewSegment(dataDir string, firstOffset int64, isActive bool, cipher *SegmentCipher) (*Segment, error) {
	return NewSegmentWithSize(dataDir, firstOffset, isActive, cipher, defaultSegmentPreallocSize)
}

// NewSegmentWithSize creates a new segment and preallocates at most the
// configured segment size. Keeping this value tied to WALConfig avoids mapping
// 1GB for every partition regardless of the requested segment size.
func NewSegmentWithSize(dataDir string, firstOffset int64, isActive bool, cipher *SegmentCipher, segmentSize int64) (*Segment, error) {
	return newSegmentWithBoundary(dataDir, firstOffset, isActive, cipher, segmentSize, nil)
}

func newSegmentWithBoundary(dataDir string, firstOffset int64, isActive bool, cipher *SegmentCipher, segmentSize int64, predecessor *segmentBoundary) (*Segment, error) {
	filename := fmt.Sprintf("%020d.log", firstOffset)
	filePath := filepath.Join(dataDir, "segments", filename)

	// Create segments directory if it doesn't exist
	segmentsDir := filepath.Join(dataDir, "segments")
	if err := os.MkdirAll(segmentsDir, 0755); err != nil {
		return nil, fmt.Errorf("create segments dir: %w", err)
	}

	// Open or create segment file. We intentionally avoid os.O_APPEND; on Windows
	// O_APPEND can cause SetEndOfFile to return ERROR_ACCESS_DENIED during
	// pre-allocation, and the code already writes sequentially at the current offset.
	flags := os.O_RDWR | os.O_CREATE
	if predecessor != nil {
		flags |= os.O_EXCL
	}
	file, err := os.OpenFile(filePath, flags, 0644)
	if err != nil {
		return nil, fmt.Errorf("open segment file: %w", err)
	}

	// Determine whether the file is newly created *before* extending it.
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		if predecessor != nil {
			return nil, fmt.Errorf("%w: stat file: %v", errBoundaryPublication, err)
		}
		return nil, fmt.Errorf("stat file: %w", err)
	}
	isNewFile := stat.Size() == 0

	// Pre-allocate segment file for zero-allocation writes.
	preallocSize := segmentSize
	if preallocSize <= 0 {
		preallocSize = defaultSegmentPreallocSize
	}
	if err := preallocateFile(file, preallocSize); err != nil {
		// Pre-allocation is optional — log warning but continue
		log.Printf("[SEGMENT] Pre-allocation failed for %s: %v (continuing without pre-allocation)", filename, err)
	}

	// Get file stats after pre-allocation
	stat, err = file.Stat()
	if err != nil {
		file.Close()
		if predecessor != nil {
			return nil, fmt.Errorf("%w: stat preallocated file: %v", errBoundaryPublication, err)
		}
		return nil, fmt.Errorf("stat file after preallocate: %w", err)
	}

	createdTS := time.Now().UnixMilli()
	dataEnd := stat.Size()

	// For newly created files, write the header directly to the file before any
	// mmap or buffered writer is set up. This guarantees the header is at offset 0
	// and leaves the file position at the end of the header for subsequent writes.
	if isNewFile {
		if _, err := file.Write(buildSegmentHeaderWithBoundary(firstOffset, createdTS, predecessor)); err != nil {
			file.Close()
			if predecessor != nil {
				return nil, fmt.Errorf("%w: write header: %v", errBoundaryPublication, err)
			}
			return nil, fmt.Errorf("write header: %w", err)
		}
		if err := file.Sync(); err != nil {
			file.Close()
			if predecessor != nil {
				return nil, fmt.Errorf("%w: sync header: %v", errBoundaryPublication, err)
			}
			return nil, fmt.Errorf("sync header: %w", err)
		}
		if predecessor != nil {
			if err := SyncDirectory(segmentsDir); err != nil {
				file.Close()
				return nil, fmt.Errorf("%w: sync segment directory: %v", errBoundaryPublication, err)
			}
		}
		dataEnd = 64
	}

	// Create index
	index, err := NewIndex(dataDir, firstOffset)
	if err != nil {
		file.Close()
		if predecessor != nil {
			return nil, fmt.Errorf("%w: open index: %v", errBoundaryPublication, err)
		}
		return nil, fmt.Errorf("create index: %w", err)
	}

	// Mmap the file for zero-copy reads and writes. Encrypted segments use the
	// buffered writer path instead because records are transformed before being
	// persisted, so reads must come from the file, not a stale mmap view.
	var mmapData []byte
	if stat.Size() > 0 && cipher == nil {
		mmapData, _ = mmapFile(file, stat.Size())
		// We ignore mmap errors and fall back to file reading (mmapData will be nil if error)
	}

	segment := &Segment{
		segmentFile:     file,
		reader:          file, // io.ReaderAt doesn't have buffered option
		mmapData:        mmapData,
		mmapWritePos:    dataEnd, // Start appending after existing data / header
		mmapSize:        stat.Size(),
		sizeBytes:       dataEnd,
		firstOffset:     firstOffset,
		lastOffset:      -1, // no records yet; updated as events are appended
		nextOffset:      firstOffset,
		createdTS:       createdTS,
		isActive:        isActive,
		dataDir:         dataDir,
		filename:        filename,
		indexFilename:   fmt.Sprintf("%020d.index", firstOffset),
		nextIndexOffset: 0,
		indexEntries:    0,
		index:           index,
		recordBuf:       make([]byte, 0, 4096), // Pre-allocate 4KB
		cipher:          cipher,
		predecessor:     predecessor,
	}

	return segment, nil
}

// OpenSegment opens an existing segment file, validates its header, scans
// records for metadata, and recovers from a corrupt tail if needed.
func OpenSegment(dataDir string, filename string, cipher *SegmentCipher) (*Segment, error) {
	filePath := filepath.Join(dataDir, "segments", filename)

	// Open the existing segment without os.O_APPEND. O_APPEND can cause
	// SetEndOfFile to fail with ERROR_ACCESS_DENIED on Windows when the file
	// is opened for pre-allocation, and replay/write paths manage the offset
	// explicitly.
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open segment file: %w", err)
	}

	// Get file info
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("stat file: %w", err)
	}

	// Parse offset from filename
	var firstOffset int64
	fmt.Sscanf(filename, "%020d.log", &firstOffset)

	// Open or create index
	index, err := NewIndex(dataDir, firstOffset)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("open index: %w", err)
	}

	// Skip pre-allocation when reopening an existing segment for replay. Only
	// the active segment needs room to grow, and WAL replay opens every segment
	// as read-only. This avoids the noisy Windows SetEndOfFile warning.

	// Try to mmap the file for zero-copy reads. Encrypted segments read from the
	// file because records are transformed on the write path, so the mmap view
	// would not contain the plaintext record layout.
	var mmapData []byte
	mmapSize := stat.Size()
	if stat.Size() > 0 && cipher == nil {
		mmapData, _ = mmapFile(file, stat.Size())
		// Fallback to file reading if mmap fails
	}

	segment := &Segment{
		segmentFile:    file,
		reader:         file,
		mmapData:       mmapData,
		mmapWritePos:   stat.Size(), // Start writing at end of existing data
		mmapSize:       mmapSize,
		firstOffset:    firstOffset,
		createdTS:      time.Now().UnixMilli(),
		isActive:       true, // Opened segments are active by default
		sizeBytes:      stat.Size(),
		dataDir:        dataDir,
		filename:       filename,
		indexFilename:  fmt.Sprintf("%020d.index", firstOffset),
		index:          index,
		recordBuf:      make([]byte, 0, 4096),
		cipher:         cipher,
		validAfterTail: -1,
	}

	// Validate header before scanning records.
	if err := segment.readHeader(); err != nil {
		_ = segment.Close()
		return nil, fmt.Errorf("invalid segment header: %w", err)
	}

	// Scan segment to get metadata
	if err := segment.scan(); err != nil {
		_ = segment.Close()
		return nil, fmt.Errorf("scan segment: %w", err)
	}

	// Set nextOffset based on lastOffset found during scan
	segment.nextOffset = segment.lastOffset + 1

	// Reconcile the mmap write cursor with the real data end discovered by scan.
	// The constructor set mmapWritePos to the on-disk size, which for a segment
	// with a preallocated tail is far past the last real record. If scan trimmed
	// the tail, truncateToPosition already reset this; set it explicitly to cover
	// the no-truncation path and any future scan changes. Without this, the first
	// append after reopen writes at the preallocated EOF, leaving a zero gap that
	// silently truncates all subsequent range reads (data-loss on every restart).
	if segment.mmapData != nil {
		segment.mmapWritePos = segment.sizeBytes
	}

	// Position the file handle at the end of the data so the buffered writer
	// (used for encrypted records) appends instead of overwriting.
	if _, err := file.Seek(segment.sizeBytes, io.SeekStart); err != nil {
		_ = segment.Close()
		return nil, fmt.Errorf("seek to end of data: %w", err)
	}

	return segment, nil
}

// readHeader validates the 64-byte segment header and returns an error if the
// magic number, version, or header CRC is invalid.
func (s *Segment) readHeader() error {
	if s.segmentFile == nil {
		return fmt.Errorf("segment file not open")
	}
	if _, err := s.segmentFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek to header: %w", err)
	}
	header := make([]byte, 64)
	if _, err := io.ReadFull(s.segmentFile, header); err != nil {
		return fmt.Errorf("read header: %w", err)
	}
	if string(header[0:6]) != "CRNOS1" {
		return fmt.Errorf("bad header magic: %q", header[0:7])
	}
	if header[7] != SegmentHeaderVersion {
		return fmt.Errorf("unsupported segment version: %d (this build requires version %d; older WAL data is not migrated)", header[7], SegmentHeaderVersion)
	}
	stored := binary.BigEndian.Uint32(header[60:64])
	computed := crc32.ChecksumIEEE(header[0:60])
	if stored != computed {
		return fmt.Errorf("header CRC mismatch: stored=%08x computed=%08x", stored, computed)
	}
	if string(header[32:36]) == boundaryMagic {
		boundary := &segmentBoundary{
			first: int64(binary.BigEndian.Uint64(header[36:44])),
			last:  int64(binary.BigEndian.Uint64(header[44:52])),
			size:  int64(binary.BigEndian.Uint64(header[52:60])),
		}
		if boundary.first < 0 || boundary.last < boundary.first-1 || boundary.last >= s.firstOffset || boundary.size < 64 {
			return fmt.Errorf("invalid predecessor boundary in segment header")
		}
		s.predecessor = boundary
	}
	return nil
}

func buildSegmentHeaderWithBoundary(firstOffset, createdTS int64, predecessor *segmentBoundary) []byte {
	header := make([]byte, 64)

	// Magic number "CRNOS1" (7 bytes)
	copy(header[0:7], []byte("CRNOS1"))

	// Version (1 byte)
	header[7] = SegmentHeaderVersion

	// First offset (8 bytes)
	binary.BigEndian.PutUint64(header[8:16], uint64(firstOffset))

	// Created timestamp (8 bytes)
	binary.BigEndian.PutUint64(header[24:32], uint64(createdTS))

	if predecessor != nil {
		copy(header[32:36], boundaryMagic)
		binary.BigEndian.PutUint64(header[36:44], uint64(predecessor.first))
		binary.BigEndian.PutUint64(header[44:52], uint64(predecessor.last))
		binary.BigEndian.PutUint64(header[52:60], uint64(predecessor.size))
	}

	// CRC32 of header (4 bytes)
	crc := crc32.ChecksumIEEE(header[0:60])
	binary.BigEndian.PutUint32(header[60:64], crc)

	return header
}

// setPredecessor updates the successor's evidence during an explicit repair.
// Callers exclude checkpoint copies and appends. A torn
// header is rejected by its CRC.
func (s *Segment) setPredecessor(boundary *segmentBoundary) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	header := make([]byte, 64)
	if _, err := s.segmentFile.ReadAt(header, 0); err != nil {
		return err
	}
	clear(header[32:60])
	if boundary != nil {
		copy(header[32:36], boundaryMagic)
		binary.BigEndian.PutUint64(header[36:44], uint64(boundary.first))
		binary.BigEndian.PutUint64(header[44:52], uint64(boundary.last))
		binary.BigEndian.PutUint64(header[52:60], uint64(boundary.size))
	}
	binary.BigEndian.PutUint32(header[60:64], crc32.ChecksumIEEE(header[:60]))
	if _, err := s.segmentFile.WriteAt(header, 0); err != nil {
		return err
	}
	if err := s.segmentFile.Sync(); err != nil {
		return err
	}
	s.predecessor = boundary
	return nil
}

// markBoundaryDamage compares the recovered prefix with a successor's durable
// record of the closed segment. Each successor must carry boundary evidence.
func markBoundaryDamage(segments []*Segment) error {
	for i := 1; i < len(segments); i++ {
		previous, boundary := segments[i-1], segments[i].predecessor
		if boundary == nil {
			return fmt.Errorf("%w: segment %s has no predecessor boundary", ErrLogDamaged, segments[i].GetFilename())
		}
		if boundary.first != previous.firstOffset {
			continue // A segment may have been intentionally removed by retention.
		}
		if previous.lastOffset != boundary.last || previous.sizeBytes != boundary.size {
			previous.boundaryDamage = true
			previous.invalidTail = true
			previous.invalidAt = previous.sizeBytes
			previous.invalidWhy = fmt.Sprintf("a recovered boundary at offset %d and byte %d, but the next segment records offset %d and byte %d", previous.lastOffset, previous.sizeBytes, boundary.last, boundary.size)
		}
	}
	return nil
}

// AppendEvent appends a single event to the active segment. indexInterval
// controls sparse index density (one entry every N events).
func (s *Segment) AppendEvent(event *types.Event, indexInterval int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isActive {
		return s.appendEventActive(event, indexInterval)
	}
	return fmt.Errorf("cannot append to closed segment")
}

// AppendBatch appends a batch of events to the segment (thread-safe; acquires s.mu).
func (s *Segment) AppendBatch(events []*types.Event, indexInterval int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.appendBatchInternal(events, indexInterval)
}

// AppendBatchLocked appends a batch of events to the segment.
// The caller MUST already hold the segment lock (or an outer lock that
// serializes access, such as WAL.mu). This avoids double-locking on the
// hot write path.
func (s *Segment) AppendBatchLocked(events []*types.Event, indexInterval int64) error {
	return s.appendBatchInternal(events, indexInterval)
}

// AppendBatchUnsafe is a convenience wrapper that acquires s.mu before calling
// AppendBatchLocked. Prefer AppendBatchLocked when the caller already guarantees
// exclusive access.
func (s *Segment) AppendBatchUnsafe(events []*types.Event, indexInterval int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendBatchInternal(events, indexInterval)
}

// AppendPreparedBatchLocked appends a batch of pre-encoded records.
// The caller MUST hold the WAL lock (or any other outer lock that serializes
// access to the active segment). This method acquires the segment lock so that
// mmap writes/metadata remain consistent with concurrent Sync/FlushBuffer/Read
// callers. This avoids the previous double-lock deadlock because the segment
// lock is never held while waiting for the WAL lock.
func (s *Segment) AppendPreparedBatchLocked(prepared []*PreparedRecord, indexInterval int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isActive {
		return fmt.Errorf("cannot append to closed segment")
	}

	// Track position from current size (buffered writes)
	filePos := s.sizeBytes

	for _, prep := range prepared {
		// Write record to buffer
		written, err := s.writeRecord(prep.Buf)
		if err != nil {
			return fmt.Errorf("write record: %w", err)
		}

		// Update metadata
		s.lastOffset = prep.Event.Offset
		s.lastTS = prep.Event.GetScheduleTs()
		if s.firstTS == 0 {
			s.firstTS = prep.Event.GetScheduleTs()
		}
		s.times.note(prep.Event.GetScheduleTs())
		s.nextOffset++

		s.sizeBytes = filePos + written

		// Write index entry if needed (using sparse indexing)
		// Use AddEntryUnsafe to avoid nested locking
		if s.nextOffset-s.firstOffset >= s.nextIndexOffset {
			if err := s.index.AddEntryUnsafe(prep.Event.GetScheduleTs(), prep.Event.Offset, filePos); err != nil {
				return fmt.Errorf("write index: %w", err)
			}
			s.nextIndexOffset += indexInterval
			s.indexEntries++
		}

		// Advance file pos for next event in batch
		filePos += written
	}

	return nil
}

// AppendPreparedBatchUnsafe is a convenience wrapper that acquires s.mu before
// calling AppendPreparedBatchLocked. Prefer AppendPreparedBatchLocked when the
// caller already guarantees exclusive access.
func (s *Segment) AppendPreparedBatchUnsafe(prepared []*PreparedRecord, indexInterval int64) error {
	return s.AppendPreparedBatchLocked(prepared, indexInterval)
}

// appendBatchInternal is the shared batch append implementation.
// Caller must already hold s.mu (or equivalent exclusive access).
func (s *Segment) appendBatchInternal(events []*types.Event, indexInterval int64) error {
	if !s.isActive {
		return fmt.Errorf("cannot append to closed segment")
	}

	// Track position from current size (buffered writes)
	filePos := s.sizeBytes

	for _, event := range events {
		// Build event record
		record, err := s.buildEventRecord(event)
		if err != nil {
			return fmt.Errorf("build event record: %w", err)
		}

		// Write record to buffer
		written, err := s.writeRecord(record)
		if err != nil {
			return fmt.Errorf("write record: %w", err)
		}

		// Update metadata
		s.lastOffset = event.Offset
		s.lastTS = event.GetScheduleTs()
		if s.firstTS == 0 {
			s.firstTS = event.GetScheduleTs()
		}
		s.times.note(event.GetScheduleTs())
		s.nextOffset++

		s.sizeBytes = filePos + written

		// Write index entry if needed (using sparse indexing)
		// Use AddEntryUnsafe to avoid triple-lock (WAL→Segment→Index)
		if s.nextOffset-s.firstOffset >= s.nextIndexOffset {
			if err := s.index.AddEntryUnsafe(event.GetScheduleTs(), event.Offset, filePos); err != nil {
				return fmt.Errorf("write index: %w", err)
			}
			s.nextIndexOffset += indexInterval
			s.indexEntries++
		}

		// Advance file pos for next event in batch
		filePos += written
	}

	return nil
}

// appendEventActive appends to active segment (internal, caller holds lock)
func (s *Segment) appendEventActive(event *types.Event, indexInterval int64) error {
	// Get current buffered position (approximation - will be exact after flush)
	filePos := s.sizeBytes

	// Build event record
	record, err := s.buildEventRecord(event)
	if err != nil {
		return fmt.Errorf("build event record: %w", err)
	}

	// Write record to buffer (buffered I/O)
	written, err := s.writeRecord(record)
	if err != nil {
		return fmt.Errorf("write record: %w", err)
	}

	// Update metadata
	s.lastOffset = event.Offset
	s.lastTS = event.GetScheduleTs()
	if s.firstTS == 0 {
		s.firstTS = event.GetScheduleTs()
	}
	s.times.note(event.GetScheduleTs())
	s.nextOffset++
	s.sizeBytes = filePos + written

	// Write index entry if needed (using sparse indexing)
	// Use AddEntryUnsafe to avoid nested locking
	if s.nextOffset-s.firstOffset >= s.nextIndexOffset {
		if err := s.index.AddEntryUnsafe(event.GetScheduleTs(), event.Offset, filePos); err != nil {
			return fmt.Errorf("write index: %w", err)
		}
		s.nextIndexOffset += indexInterval
		s.indexEntries++
	}

	return nil
}

// AppendEventUnsafe appends an event without acquiring s.mu.
// The caller MUST guarantee exclusive access.
func (s *Segment) AppendEventUnsafe(event *types.Event, indexInterval int64) error {
	if s.isActive {
		return s.appendEventActive(event, indexInterval)
	}
	return fmt.Errorf("cannot append to closed segment")
}

// buildEventRecord builds binary event record (v2 format).
// Layout: [length 4][crc32 4][term 8][offset 8][schedule_ts 8][msgID len 2][msgID]
// [topic len 2][topic][payload len 4][payload][checksum 4][meta count 2][meta...]
func (s *Segment) buildEventRecord(event *types.Event) ([]byte, error) {
	// Calculate sizes
	msgIDLen := len(event.GetMessageId())
	topicLen := len(event.Topic)
	payloadLen := len(event.Payload)
	metaCount := len(event.Meta)

	if event.Checksum == 0 && payloadLen > 0 {
		event.Checksum = crc32.ChecksumIEEE(event.Payload)
	}

	// Calculate total size with overflow checking
	size := 4 + 4 + 8 + 8 + 8 + 2 + msgIDLen + 2 + topicLen + 4 + payloadLen + 4 + 2
	for k, v := range event.Meta {
		metaEntrySize := 2 + len(k) + 2 + len(v)
		// Check for integer overflow
		if size > (1<<63-1)-metaEntrySize {
			return nil, fmt.Errorf("event record too large: overflow in size calculation")
		}
		size += metaEntrySize
	}

	// Reuse buffer if possible
	if cap(s.recordBuf) < size {
		// Grow buffer efficiently
		s.recordBuf = make([]byte, size*2)
	}

	// Slice to required size
	record := s.recordBuf[:size]
	offset := 0

	// Length (4 bytes)
	binary.BigEndian.PutUint32(record[offset:offset+4], uint32(size))
	offset += 4

	// CRC32 (4 bytes) - skip for now, fill later
	offset += 4

	// Term (8 bytes)
	binary.BigEndian.PutUint64(record[offset:offset+8], uint64(event.GetTerm()))
	offset += 8

	// Offset (8 bytes)
	binary.BigEndian.PutUint64(record[offset:offset+8], uint64(event.Offset))
	offset += 8

	// Schedule timestamp (8 bytes)
	binary.BigEndian.PutUint64(record[offset:offset+8], uint64(event.GetScheduleTs()))
	offset += 8

	// Message ID length (2 bytes)
	binary.BigEndian.PutUint16(record[offset:offset+2], uint16(msgIDLen))
	offset += 2

	// Message ID (N bytes)
	copy(record[offset:offset+msgIDLen], event.GetMessageId())
	offset += msgIDLen

	// Topic length (2 bytes)
	binary.BigEndian.PutUint16(record[offset:offset+2], uint16(topicLen))
	offset += 2

	// Topic (N bytes)
	copy(record[offset:offset+topicLen], event.Topic)
	offset += topicLen

	// Payload length (4 bytes)
	binary.BigEndian.PutUint32(record[offset:offset+4], uint32(payloadLen))
	offset += 4

	// Payload (N bytes)
	copy(record[offset:offset+payloadLen], event.Payload)
	offset += payloadLen

	// Payload checksum (4 bytes)
	binary.BigEndian.PutUint32(record[offset:offset+4], event.Checksum)
	offset += 4

	// Meta count (2 bytes)
	binary.BigEndian.PutUint16(record[offset:offset+2], uint16(metaCount))
	offset += 2

	// Meta entries
	for k, v := range event.Meta {
		// Key length (2 bytes)
		binary.BigEndian.PutUint16(record[offset:offset+2], uint16(len(k)))
		offset += 2

		// Key (N bytes)
		copy(record[offset:offset+len(k)], k)
		offset += len(k)

		// Value length (2 bytes)
		binary.BigEndian.PutUint16(record[offset:offset+2], uint16(len(v)))
		offset += 2

		// Value (N bytes)
		copy(record[offset:offset+len(v)], v)
		offset += len(v)
	}

	// Calculate and write CRC32 (covers term, offset, payload, checksum, meta)
	crc := crc32.ChecksumIEEE(record[8:])
	binary.BigEndian.PutUint32(record[4:8], crc)

	return record, nil
}

// parseEventRecordWithoutLength parses binary event record that doesn't include length prefix.
// The v2 record layout is: [crc32 4][term 8][offset 8][schedule_ts 8][msgID len 2][msgID]
// [topic len 2][topic][payload len 4][payload][checksum 4][meta count 2][meta...]
func parseEventRecordWithoutLength(record []byte) (*types.Event, error) {
	if len(record) < 4+8+8+8 {
		return nil, fmt.Errorf("record too short")
	}

	// Validate CRC32 checksum
	storedCRC := binary.BigEndian.Uint32(record[0:4])
	computedCRC := crc32.ChecksumIEEE(record[4:])
	if storedCRC != computedCRC {
		return nil, fmt.Errorf("CRC32 mismatch: stored=0x%08x computed=0x%08x (data corruption detected)", storedCRC, computedCRC)
	}

	offset := 4 // Skip CRC32 (4 bytes), length was already read separately

	// Term (8 bytes)
	term := int64(binary.BigEndian.Uint64(record[offset : offset+8]))
	offset += 8

	// Offset (8 bytes)
	eventOffset := int64(binary.BigEndian.Uint64(record[offset : offset+8]))
	offset += 8

	// Schedule timestamp (8 bytes)
	scheduleTs := int64(binary.BigEndian.Uint64(record[offset : offset+8]))
	offset += 8

	// Message ID length (2 bytes)
	msgIDLen := int(binary.BigEndian.Uint16(record[offset : offset+2]))
	offset += 2

	// Message ID (N bytes)
	if offset+msgIDLen > len(record) {
		return nil, fmt.Errorf("record bounds exceeded for message ID")
	}
	messageID := string(record[offset : offset+msgIDLen])
	offset += msgIDLen

	// Topic length (2 bytes)
	topicLen := int(binary.BigEndian.Uint16(record[offset : offset+2]))
	offset += 2

	// Topic (N bytes)
	if offset+topicLen > len(record) {
		return nil, fmt.Errorf("record bounds exceeded for topic")
	}
	topic := string(record[offset : offset+topicLen])
	offset += topicLen

	// Payload length (4 bytes)
	payloadLen := int(binary.BigEndian.Uint32(record[offset : offset+4]))
	offset += 4

	// Payload (N bytes)
	if offset+payloadLen > len(record) {
		return nil, fmt.Errorf("record bounds exceeded for payload")
	}
	payload := make([]byte, payloadLen)
	copy(payload, record[offset:offset+payloadLen])
	offset += payloadLen

	// Payload checksum (4 bytes)
	if offset+4 > len(record) {
		return nil, fmt.Errorf("record bounds exceeded for checksum")
	}
	checksum := binary.BigEndian.Uint32(record[offset : offset+4])
	offset += 4

	// Meta count (2 bytes)
	if offset+2 > len(record) {
		return nil, fmt.Errorf("record bounds exceeded for meta count")
	}
	metaCount := int(binary.BigEndian.Uint16(record[offset : offset+2]))
	offset += 2

	// Meta entries
	meta := make(map[string]string)
	for i := 0; i < metaCount; i++ {
		// Key length (2 bytes)
		keyLen := int(binary.BigEndian.Uint16(record[offset : offset+2]))
		offset += 2

		// Key (N bytes)
		if offset+keyLen > len(record) {
			return nil, fmt.Errorf("record bounds exceeded for meta key at index %d", i)
		}
		key := string(record[offset : offset+keyLen])
		offset += keyLen

		// Value length (2 bytes)
		valLen := int(binary.BigEndian.Uint16(record[offset : offset+2]))
		offset += 2

		// Value (N bytes)
		if offset+valLen > len(record) {
			return nil, fmt.Errorf("record bounds exceeded for meta value at index %d", i)
		}
		value := string(record[offset : offset+valLen])
		offset += valLen

		meta[key] = value
	}

	return &types.Event{
		MessageId:   messageID,
		ScheduleTs:  scheduleTs,
		Payload:     payload,
		Topic:       topic,
		Meta:        meta,
		Offset:      eventOffset,
		PartitionId: 0,
		Term:        term,
		Checksum:    checksum,
	}, nil
}

// writeRecord writes record to segment using mmap if available, otherwise bufio.Writer.
// If encryption is enabled, the record payload is encrypted with AES-256-GCM.
// Returns the actual number of bytes written to the segment file.
func (s *Segment) writeRecord(record []byte) (int64, error) {
	// Fast path: use mmap for zero-copy writes if available
	if s.mmapData != nil && s.cipher == nil {
		return s.writeRecordMmap(record)
	}

	// Slow path: use bufio.Writer (for encrypted or non-mmap segments)
	return s.writeRecordBuffered(record)
}

// writeRecordMmap writes directly to memory-mapped file (zero-copy, zero-syscall).
func (s *Segment) writeRecordMmap(record []byte) (int64, error) {
	recordLen := int64(len(record))
	if recordLen == 0 {
		return 0, nil
	}

	// Grow the mapping if the record would not fit. Use len(s.mmapData) — the
	// actual mapped length — as the authoritative bound rather than s.mmapSize,
	// so a stale/oversized mmapSize can never let the copy below run past the end
	// of the real buffer. Ensure the new size is large enough for THIS record even
	// when a single record exceeds the current size (doubling alone can fall short).
	if s.mmapWritePos+recordLen > int64(len(s.mmapData)) {
		needed := s.mmapWritePos + recordLen
		newSize := s.mmapSize * 2
		if newSize < needed {
			newSize = needed
		}
		if err := s.remapMmap(newSize); err != nil {
			return 0, fmt.Errorf("remap mmap: %w", err)
		}
	}

	// Copy directly to mmap (single memcpy, no syscall)
	copy(s.mmapData[s.mmapWritePos:], record)
	s.mmapWritePos += recordLen
	s.sizeBytes = s.mmapWritePos

	return recordLen, nil
}

// segmentWriteBufSize is the size of the buffer a segment writes through when
// it does not write to its mapping.
const segmentWriteBufSize = 4 << 20

// bufferedWriter returns that buffer, and makes it on first use. Only a segment
// that is appended to without a mapping needs one: an encrypted segment, or one
// whose file could not be mapped. Every segment used to get one when it was
// created or opened, so a node held 4 MiB for each segment of its logs from the
// moment it started, whether or not the segment would ever be written again.
func (s *Segment) bufferedWriter() *bufio.Writer {
	if s.writer == nil {
		s.writer = bufio.NewWriterSize(s.segmentFile, segmentWriteBufSize)
	}
	return s.writer
}

// flushWriter writes out what the buffer holds, when there is a buffer.
func (s *Segment) flushWriter() error {
	if s.writer == nil {
		return nil
	}
	return s.writer.Flush()
}

// writeRecordBuffered writes using bufio.Writer (for encryption or fallback).
func (s *Segment) writeRecordBuffered(record []byte) (int64, error) {
	if s.cipher != nil {
		// record includes 4-byte length prefix; encrypt payload after it.
		if len(record) < 4 {
			return 0, fmt.Errorf("record too short for encryption")
		}
		plaintext := record[4:]
		// The ciphertext starts at the byte position immediately after the length
		// prefix we are about to write. Positions are unique and monotonic within a
		// partition, so they make a safe deterministic GCM counter nonce.
		ciphertextPos := s.sizeBytes + 4
		ciphertext, err := s.cipher.EncryptRecord(plaintext, ciphertextPos)
		if err != nil {
			return 0, fmt.Errorf("encrypt: %w", err)
		}
		// Write new length prefix for encrypted payload
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(4+len(ciphertext)))
		n1, err := s.bufferedWriter().Write(length[:])
		if err != nil {
			return int64(n1), err
		}
		n2, err := s.bufferedWriter().Write(ciphertext)
		return int64(n1 + n2), err
	}
	n, err := s.bufferedWriter().Write(record)
	return int64(n), err
}

// failedWriteLocked records err, if it is the segment's first failed write-out
// or sync, and returns the first one, or nil when none has failed.
//
// A failure sticks. Linux reports a failed writeback once, to the next fsync,
// and may drop the pages it could not write; an fsync after that succeeds
// without them. A flush that started after a failed one, and found nothing
// wrong, could otherwise make an append look durable before the log learned
// of the failure. The caller must hold flushMu.
func (s *Segment) failedWriteLocked(err error) error {
	if err != nil && s.writeErr == nil {
		s.writeErr = err
	}
	return s.writeErr
}

// remapMmap remaps the file with a new size.
func (s *Segment) remapMmap(newSize int64) error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	if err := s.failedWriteLocked(nil); err != nil {
		return err
	}

	if s.mmapData != nil {
		// The mapping is written out before it goes, and a failure is the
		// append's: what the mapping held may not be on disk.
		if err := syncMmap(s.mmapData); err != nil {
			return s.failedWriteLocked(fmt.Errorf("sync mapping before remap: %w", err))
		}
		if err := munmapFile(s.mmapData); err != nil {
			return fmt.Errorf("unmap old mmap: %w", err)
		}
		s.mmapData = nil
	}

	// Extend file
	if err := preallocateFile(s.segmentFile, newSize); err != nil {
		return fmt.Errorf("preallocate for remap: %w", err)
	}

	// Remap
	mmapData, err := mmapFile(s.segmentFile, newSize)
	if err != nil {
		return fmt.Errorf("mmap after preallocate: %w", err)
	}

	s.mmapData = mmapData
	// Track the ACTUAL mapped length, not the requested newSize. They are equal
	// here, but sourcing mmapSize from len(mmapData) guarantees the two never
	// diverge — the write path relies on that invariant to bound its copies.
	s.mmapSize = int64(len(mmapData))
	return nil
}

// decryptRecord decrypts record payload if encryption is enabled.
// ciphertextPos is the byte offset of the ciphertext after the 4-byte length prefix.
func (s *Segment) decryptRecord(ciphertext []byte, ciphertextPos int64) ([]byte, error) {
	if s.cipher == nil {
		return ciphertext, nil
	}
	return s.cipher.DecryptRecord(ciphertext, ciphertextPos)
}

// ReadEvent reads the event at targetOffset, using the sparse index for seeking.
func (s *Segment) ReadEvent(targetOffset int64) (*types.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if targetOffset < s.firstOffset || (s.lastOffset >= 0 && targetOffset > s.lastOffset) {
		return nil, types.ErrOffsetOutOfRange
	}

	// Use index to find starting position
	startPos := int64(64) // Default to after header
	if s.index != nil {
		if pos, found := s.index.FindByOffset(targetOffset); found {
			startPos = pos
		}
	}

	event, err := s.readEventFrom(targetOffset, startPos)
	if err != nil && startPos > 64 {
		// The index only says where to start; it can be wrong. The record is
		// in the segment, so look from its start.
		return s.readEventFrom(targetOffset, 64)
	}
	return event, err
}

// readEventFrom reads the event at targetOffset scanning from startPos. The
// caller holds s.mu.
func (s *Segment) readEventFrom(targetOffset, startPos int64) (*types.Event, error) {
	if s.mmapData != nil && startPos < int64(len(s.mmapData)) {
		return s.readEventMmap(targetOffset, startPos)
	}
	return s.readEventFile(targetOffset, startPos)
}

// readEventMmap reads event from memory mapped data
func (s *Segment) readEventMmap(targetOffset int64, startPos int64) (*types.Event, error) {
	pos := int(startPos)
	data := s.mmapData

	for pos < len(data) {
		// Read length
		if pos+4 > len(data) {
			break
		}
		length := int64(binary.BigEndian.Uint32(data[pos : pos+4]))
		if length <= 4 || length > MaxRecordLength {
			break
		}

		// Check if full record is available
		if pos+int(length) > len(data) {
			break
		}

		// Parse event
		// Record data starts after length (4 bytes).
		// recordData includes CRC + data.
		recordData := data[pos+4 : pos+int(length)]

		decrypted, err := s.decryptRecord(recordData, int64(pos+4))
		if err != nil {
			return nil, fmt.Errorf("decrypt: %w", err)
		}

		if len(decrypted) > 20 {
			offsetVal := int64(binary.BigEndian.Uint64(decrypted[12:20]))
			if offsetVal > targetOffset {
				return nil, fmt.Errorf("event not found (passed target)")
			}
			if offsetVal < targetOffset {
				pos += int(length)
				continue
			}
		}

		event, err := parseEventRecordWithoutLength(decrypted)
		if err != nil {
			return nil, fmt.Errorf("parse event: %w", err)
		}

		if event.Offset == targetOffset {
			return event, nil
		}

		pos += int(length)
	}

	// If not found in mmap (maybe partial mmap), fall back to file
	return s.readEventFile(targetOffset, int64(pos))
}

// readEventFile reads event from file using ReadAt (pread, no file handle allocation)
func (s *Segment) readEventFile(targetOffset int64, startPos int64) (*types.Event, error) {
	lengthBytes := make([]byte, 4)
	recordBuf := make([]byte, 0, 4096)
	currentPos := startPos

	// Scan from index position to find target event
	for {
		// Read record length using ReadAt (thread-safe pread)
		if _, err := s.segmentFile.ReadAt(lengthBytes, currentPos); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read length: %w", err)
		}
		currentPos += 4

		length := int64(binary.BigEndian.Uint32(lengthBytes))
		if length <= 0 || length > MaxRecordLength { // Sanity check
			break
		}

		// Read full record (length includes the 4-byte length field)
		recordLen := int(length - 4)
		if cap(recordBuf) < recordLen {
			recordBuf = make([]byte, recordLen)
		}
		record := recordBuf[:recordLen]
		ciphertextPos := currentPos
		if _, err := s.segmentFile.ReadAt(record, currentPos); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read record: %w", err)
		}
		currentPos += int64(recordLen)

		// Decrypt if needed before parsing
		decrypted, err := s.decryptRecord(record, ciphertextPos)
		if err != nil {
			return nil, fmt.Errorf("decrypt: %w", err)
		}

		// Early offset check: offset is at bytes 16-24 of record (after CRC + term).
		if len(decrypted) > 20 {
			offsetVal := int64(binary.BigEndian.Uint64(decrypted[12:20]))
			if offsetVal > targetOffset {
				break // Past target
			}
			if offsetVal < targetOffset {
				continue // Skip full parse
			}
		}

		// Parse event - record doesn't include length prefix
		event, err := parseEventRecordWithoutLength(decrypted)
		if err != nil {
			return nil, fmt.Errorf("parse event: %w", err)
		}

		if event.Offset == targetOffset {
			return event, nil
		}
	}

	return nil, fmt.Errorf("event not found at offset %d", targetOffset)
}

// ReadEventsByTime reads events whose schedule timestamp is in [startTS, endTS]
// (inclusive, Unix milliseconds).
func (s *Segment) ReadEventsByTime(startTS, endTS int64) ([]*types.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*types.Event

	// Schedule timestamps need not increase with offsets. The offset index and
	// first/last appended timestamps cannot safely prune a time-range query.
	startPos := int64(64)

	scanner := newRecordScanner(s.segmentFile, startPos)

	for {
		record, ciphertextPos, ok, err := scanner.record()
		if err != nil {
			return nil, fmt.Errorf("read record: %w", err)
		}
		if !ok {
			break
		}

		decrypted, err := s.decryptRecord(record, ciphertextPos)
		if err != nil {
			return nil, fmt.Errorf("decrypt: %w", err)
		}

		event, err := parseEventRecordWithoutLength(decrypted)
		if err != nil {
			return nil, fmt.Errorf("parse event: %w", err)
		}

		// Check timestamp
		if event.GetScheduleTs() >= startTS && event.GetScheduleTs() <= endTS {
			result = append(result, event)
		}

		if event.Offset == s.lastOffset {
			break
		}
	}

	return result, nil
}

// ReadEventsByOffsetRange reads events in the inclusive offset range
// [startOffset, endOffset].
func (s *Segment) ReadEventsByOffsetRange(startOffset, endOffset int64) ([]*types.Event, error) {
	events, _, err := s.readOffsetRange(startOffset, endOffset, 0)
	return events, err
}

// readOffsetRange reads the events of an inclusive offset range. With
// maxBytes above zero it stops, after at least one event, once the records
// it has read come to that many bytes; size is what they came to.
func (s *Segment) readOffsetRange(startOffset, endOffset, maxBytes int64) (result []*types.Event, size int64, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return result, 0, nil // removed (retention, truncation) after the caller listed it
	}

	// Find starting position using index
	startPos := int64(64) // Default to after header
	if s.index != nil {
		if pos, found := s.index.FindByOffset(startOffset); found {
			startPos = pos
		}
	}
	result, size, err = s.scanOffsetRange(startPos, startOffset, endOffset, maxBytes)
	if startPos > 64 {
		// The index only says where to start; it can be wrong. A scan that
		// does not begin with the first offset of the range the segment holds
		// started in the wrong place, and is done again from the segment's
		// start. It used to return nothing, and no error.
		want := max(startOffset, s.firstOffset)
		if want <= endOffset && want <= s.lastOffset &&
			(err != nil || len(result) == 0 || result[0].Offset != want) {
			return s.scanOffsetRange(64, startOffset, endOffset, maxBytes)
		}
	}
	return result, size, err
}

// scanOffsetRange reads the events of an inclusive offset range, scanning the
// segment from startPos. The caller holds s.mu.
func (s *Segment) scanOffsetRange(startPos, startOffset, endOffset, maxBytes int64) (result []*types.Event, size int64, err error) {
	// ReadAt on the existing file handle is safe alongside appends.
	scanner := newRecordScanner(s.segmentFile, startPos)

	for {
		record, ciphertextPos, ok, err := scanner.record()
		if err != nil {
			return nil, 0, fmt.Errorf("read record: %w", err)
		}
		if !ok {
			break
		}

		decrypted, err := s.decryptRecord(record, ciphertextPos)
		if err != nil {
			return nil, 0, fmt.Errorf("decrypt: %w", err)
		}

		// Early offset check before full event parsing.
		// Offset is at bytes 12-20 of record (after 4-byte CRC + 8-byte term).
		// This avoids CRC check + string allocs for out-of-range records.
		if len(decrypted) > 20 {
			eventOffset := int64(binary.BigEndian.Uint64(decrypted[12:20]))
			if eventOffset > endOffset {
				break // Past range, done
			}
			if eventOffset < startOffset {
				continue // Before range, skip full parse
			}
		}

		event, err := parseEventRecordWithoutLength(decrypted)
		if err != nil {
			return nil, 0, fmt.Errorf("parse event: %w", err)
		}

		if event.Offset >= startOffset && event.Offset <= endOffset {
			result = append(result, event)
			size += int64(len(decrypted))
		}

		if event.Offset >= endOffset || (maxBytes > 0 && size >= maxBytes) {
			break
		}
	}

	return result, size, nil
}

// TruncateAfterOffset removes all records with offset > keepThroughOffset from
// this segment, so its new last offset becomes keepThroughOffset. It is used by
// follower log truncation when a higher-term leader's log diverges from (starts
// before) the follower's tail. Records are located by scanning from the header;
// the byte position of the first record to drop becomes the new segment size.
//
// Returns the number of records removed. If keepThroughOffset is at or past the
// segment's last offset, it is a no-op.
func (s *Segment) TruncateAfterOffset(keepThroughOffset int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, fmt.Errorf("truncate on closed segment")
	}
	if keepThroughOffset >= s.lastOffset {
		return 0, nil // nothing to drop
	}

	// Flush buffered/mmap writes so the on-disk view is complete before scanning.
	if s.mmapData != nil {
		if err := syncMmap(s.mmapData[:s.mmapWritePos]); err != nil {
			return 0, fmt.Errorf("sync mmap before truncate: %w", err)
		}
	} else if err := s.flushWriter(); err != nil {
		return 0, fmt.Errorf("flush writer before truncate: %w", err)
	}

	// Scan from the header to find the byte position of the first record whose
	// offset exceeds keepThroughOffset, and the new last-good offset.
	var cutPos int64 = -1
	var newLastOffset int64 = s.firstOffset - 1
	var newLastTS int64
	var kept timeRange // of the events that stay
	lengthBytes := make([]byte, 4)
	recordBuf := make([]byte, 0, 4096)
	currentPos := int64(64) // after header

	for currentPos < s.sizeBytes {
		recordStart := currentPos
		if _, err := s.segmentFile.ReadAt(lengthBytes, currentPos); err != nil {
			if err == io.EOF {
				break
			}
			return 0, fmt.Errorf("read length: %w", err)
		}
		length := int64(binary.BigEndian.Uint32(lengthBytes))
		if length <= 4 || length > MaxRecordLength {
			break
		}
		recordLen := int(length - 4)
		if cap(recordBuf) < recordLen {
			recordBuf = make([]byte, recordLen)
		}
		record := recordBuf[:recordLen]
		if _, err := s.segmentFile.ReadAt(record, currentPos+4); err != nil {
			return 0, fmt.Errorf("read record: %w", err)
		}
		decrypted, err := s.decryptRecord(record, currentPos+4)
		if err != nil {
			return 0, fmt.Errorf("decrypt during truncate: %w", err)
		}
		event, err := parseEventRecordWithoutLength(decrypted)
		if err != nil {
			return 0, fmt.Errorf("parse during truncate: %w", err)
		}

		if event.Offset > keepThroughOffset {
			cutPos = recordStart
			break
		}
		newLastOffset = event.Offset
		newLastTS = event.GetScheduleTs()
		kept.note(event.GetScheduleTs())
		currentPos += 4 + int64(recordLen)
	}

	if cutPos < 0 {
		return 0, nil // did not find any record to drop
	}

	// Physically truncate the file to cutPos. truncateToPosition handles the
	// munmap/remap dance and updates sizeBytes + mmapWritePos. Lock order is
	// s.mu (held) -> flushMu (taken inside), consistent with Close/closeLocked.
	if err := s.truncateToPosition(cutPos); err != nil {
		return 0, fmt.Errorf("truncate segment file to %d: %w", cutPos, err)
	}

	s.lastOffset = newLastOffset
	s.lastTS = newLastTS
	s.times = kept
	s.nextOffset = newLastOffset + 1

	// Reconcile the sparse index so it holds no entries past the new last offset.
	if s.index != nil {
		if err := s.index.Truncate(newLastOffset); err != nil {
			return 0, fmt.Errorf("truncate index to %d: %w", newLastOffset, err)
		}
	}

	// Return value is advisory; the WAL computes the authoritative removed count
	// from its offset counters. Report 1 to signal that a truncation occurred.
	return 1, nil
}

// scan scans segment to recover metadata and handles tail corruption
// by truncating any half-written or corrupt frames at the end of the file.
func (s *Segment) scan() error {
	// Scan segment file to find last offset and timestamp
	file := s.segmentFile

	// Seek to start (after header)
	if _, err := file.Seek(64, io.SeekStart); err != nil {
		return fmt.Errorf("seek to start: %w", err)
	}

	s.lastOffset = -1
	s.lastTS = 0
	s.times = timeRange{}

	// Capture the on-disk size before the scan loop mutates s.sizeBytes (line
	// below: s.sizeBytes = lastGoodPos on each valid record). The truncation
	// guard must compare against the original file size, otherwise a corrupt
	// tail that lies within the preallocated region never triggers truncation
	// (lastGoodPos == s.sizeBytes after the loop).
	origSizeBytes := s.sizeBytes

	var lastGoodPos int64 = 64 // Track position of last valid record end
	var recordStartPos int64 = 64
	lengthBytes := make([]byte, 4)
	recordBuf := make([]byte, 0, 4096)
	// invalid is why reading stopped before the end of the file, when it was
	// for anything but reaching space that was never written.
	invalid := ""
	// Positions are tracked in lastGoodPos, and OpenSegment repositions the file
	// handle after the scan, so reading ahead here is safe.
	reader := bufio.NewReaderSize(file, 1<<20)

	// Read through all records
	for {
		recordStartPos = lastGoodPos

		// Read record length
		if _, err := io.ReadFull(reader, lengthBytes); err != nil {
			if err == io.EOF {
				// Clean end of file
				break
			}
			if err == io.ErrUnexpectedEOF {
				invalid = "the file ends inside a record length"
				break
			}
			return fmt.Errorf("read length: %w", err)
		}

		length := int64(binary.BigEndian.Uint32(lengthBytes))
		if length == 0 {
			// Space that was never written, which a preallocated file ends
			// with, unless something follows it.
			if follows, err := dataFollows(reader); err != nil {
				return fmt.Errorf("read after the last record: %w", err)
			} else if follows {
				invalid = "a record length of 0 with data after it"
			}
			break
		}
		if length <= 4 || length > MaxRecordLength { // Sanity check: must be > 4 bytes, max 10MB record
			invalid = fmt.Sprintf("a record length of %d", length)
			break
		}

		// Read remainder of record (length includes the 4-byte length field itself)
		recordLen := int(length - 4)
		if cap(recordBuf) < recordLen {
			recordBuf = make([]byte, recordLen)
		}
		recordData := recordBuf[:recordLen]
		if _, err := io.ReadFull(reader, recordData); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				invalid = fmt.Sprintf("a record of %d bytes that the file ends inside of", length)
				break
			}
			return fmt.Errorf("read record: %w", err)
		}

		// Decrypt if needed, then parse event to get offset and timestamp
		decrypted, err := s.decryptRecord(recordData, lastGoodPos+4)
		if err != nil {
			torn, tornErr := s.tornRecord(recordData, lastGoodPos > 64, reader)
			if tornErr != nil {
				return fmt.Errorf("read after the record at %d: %w", lastGoodPos, tornErr)
			}
			if !torn {
				return fmt.Errorf("decrypt existing record at %d (wrong key or corrupt data): %w", lastGoodPos, err)
			}
			invalid = fmt.Sprintf("a record of %d bytes that was not completely written", length)
			break
		}
		event, err := parseEventRecordWithoutLength(decrypted)
		if err != nil {
			// CRC mismatch or parse error
			invalid = fmt.Sprintf("a record that does not read (%v)", err)
			break
		}

		// Valid record - update last good position
		s.lastOffset = event.Offset
		s.lastTS = event.GetScheduleTs()
		s.times.note(event.GetScheduleTs())
		lastGoodPos = recordStartPos + 4 + int64(len(recordData)) // After length + recordData
	}

	// Position the logical size at the real data end. OpenSegment then sets the
	// mmap write cursor (mmapWritePos) to this value. This is the actual corruption
	// fix: appends land immediately after the last valid record and reads stop at
	// lastOffset, so a preallocated or corrupt tail past lastGoodPos is never
	// written into or read. Previously the scan loop advanced s.sizeBytes to
	// lastGoodPos, which left mmapWritePos at the preallocated EOF — so the first
	// append after reopen wrote ~prealloc-size bytes past the data, corrupting
	// range reads on every restart.
	// An indexed, independently verified record beyond the scan boundary proves
	// this is not simply an interrupted final write or preallocated space. Check
	// before truncating the advisory index, so repeated opens retain the proof.
	if s.index != nil {
		offset, err := s.indexedRecordAfter(lastGoodPos, origSizeBytes)
		if err != nil {
			return err
		}
		s.validAfterTail = offset
		if offset >= 0 && invalid == "" {
			invalid = "zero-filled space before a later valid indexed record"
		}
	}
	realDataEnd := lastGoodPos
	s.sizeBytes = realDataEnd
	s.invalidTail, s.invalidAt, s.invalidWhy = invalid != "", lastGoodPos, invalid

	// Do NOT physically truncate the file/mmap here. Retaining the preallocated
	// tail keeps the segment's mmap at its full size so continued appends have
	// write headroom and never trigger a remap-on-grow. Trimming on reopen shrank
	// the mmap and forced that remap path, which on Windows could desynchronize the
	// mmap size bookkeeping from the actual mapped length and panic on the next
	// write (mmapWritePos past len(mmapData)). Correctness does not need the trim —
	// the write cursor is already at the real data end and reads are bounded by
	// lastOffset. Deliberate truncation (follower realign, retention) still goes
	// through WAL.TruncateToOffset / truncateToPosition.
	if realDataEnd < origSizeBytes && s.index != nil && s.validAfterTail < 0 {
		// Reconcile the sparse index so it holds no entries past the real data end
		// (a genuine corrupt tail could otherwise leave index entries pointing past
		// lastOffset). s.lastOffset is the last valid event offset.
		if err := s.index.Truncate(s.lastOffset); err != nil {
			return fmt.Errorf("truncate index to offset %d: %w", s.lastOffset, err)
		}
	}

	// If we couldn't find any events, start from firstOffset
	if s.lastOffset < s.firstOffset {
		s.lastOffset = s.firstOffset - 1
	}

	return nil
}

// indexedRecordAfter uses the index only as a hint. A record is evidence of
// damage only if its length, checksum (and authentication when encrypted),
// and offset all verify. Stale or missing index entries are not evidence.
func (s *Segment) indexedRecordAfter(position, fileSize int64) (int64, error) {
	for _, entry := range s.index.entriesAfter(s.lastOffset) {
		if entry.FilePosition <= position || entry.FilePosition > fileSize-4 {
			continue
		}
		var lengthBytes [4]byte
		if _, err := s.segmentFile.ReadAt(lengthBytes[:], entry.FilePosition); err != nil {
			return -1, fmt.Errorf("read indexed record length: %w", err)
		}
		length := int64(binary.BigEndian.Uint32(lengthBytes[:]))
		if length <= 4 || length > MaxRecordLength || length > fileSize-entry.FilePosition {
			continue
		}
		data := make([]byte, int(length-4))
		if _, err := s.segmentFile.ReadAt(data, entry.FilePosition+4); err != nil {
			return -1, fmt.Errorf("read indexed record: %w", err)
		}
		plain, err := s.decryptRecord(data, entry.FilePosition+4)
		if err != nil {
			continue
		}
		event, err := parseEventRecordWithoutLength(plain)
		if err == nil && event.Offset == entry.Offset {
			return event.Offset, nil
		}
	}
	return -1, nil
}

// dataFollows reports whether anything but zero bytes comes next in r. It
// looks at the next 64 KiB: enough to tell space that was never written from
// a hole punched into written data, without reading the whole of a
// preallocated file every time a log is opened.
func dataFollows(r io.Reader) (bool, error) {
	window := make([]byte, 64<<10)
	n, err := io.ReadFull(r, window)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return false, err
	}
	for _, b := range window[:n] {
		if b != 0 {
			return true, nil
		}
	}
	return false, nil
}

// tornRecord reports whether an encrypted record that does not decrypt is what
// an interrupted write leaves: nothing is written after it, and it ends in
// bytes that were never written. A record is written front to back and ends
// with its authentication tag, so the zeros of preallocated space are still
// there at its end until the write completes. An encrypted segment is written
// through a buffer that may put out part of a record and keep the rest until
// the next flush, so a crash can leave one.
//
// A wrong key fails every record, the first included. After a record of the
// same segment has decrypted, the key is known to be right and an unwritten
// last byte is enough; the first record of a segment must have its whole tag
// unwritten, which a written tag is with negligible probability. Anything else
// is reported as it was: a wrong key or damage. A torn record is then handled
// like a plaintext record whose checksum fails (see WAL.judgeInvalidTails):
// cut off at the end of the log, damage anywhere before it.
func (s *Segment) tornRecord(record []byte, afterValid bool, rest io.Reader) (bool, error) {
	if s.cipher == nil || len(record) == 0 {
		return false, nil
	}
	if follows, err := dataFollows(rest); err != nil || follows {
		return false, err
	}
	unwritten := 1
	if !afterValid {
		unwritten = s.cipher.aead.Overhead()
	}
	if len(record) < unwritten {
		unwritten = len(record)
	}
	for _, b := range record[len(record)-unwritten:] {
		if b != 0 {
			return false, nil
		}
	}
	return true, nil
}

// InvalidTail reports whether the bytes after the segment's last valid record
// are something other than empty space, where they begin and what was found.
func (s *Segment) InvalidTail() (invalid bool, position int64, found string) {
	return s.invalidTail, s.invalidAt, s.invalidWhy
}

// clearInvalidTail overwrites everything after the segment's last valid record
// with zeros, which is what never-written space looks like.
//
// New records are written over an interrupted write from its start, but they
// need not reach its end. Whatever stuck out would still be there when the
// segment is full and closed, and the next time the log is opened it would be
// found in a closed segment, where bytes that are not records mean damage.
func (s *Segment) clearInvalidTail() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.invalidTail {
		return nil
	}
	info, err := s.segmentFile.Stat()
	if err != nil {
		return fmt.Errorf("stat segment: %w", err)
	}
	zeros := make([]byte, 1<<20)
	for position := s.invalidAt; position < info.Size(); position += int64(len(zeros)) {
		chunk := zeros[:min(int64(len(zeros)), info.Size()-position)]
		if _, err := s.segmentFile.WriteAt(chunk, position); err != nil {
			return fmt.Errorf("clear segment from byte %d: %w", s.invalidAt, err)
		}
	}
	if err := s.segmentFile.Sync(); err != nil {
		return fmt.Errorf("sync cleared segment: %w", err)
	}
	s.invalidTail, s.invalidWhy = false, ""
	return nil
}

// truncateToPosition truncates the segment file to the given position
func (s *Segment) truncateToPosition(pos int64) error {
	if pos >= s.sizeBytes {
		return nil // Nothing to truncate
	}

	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	// Sync before truncating to ensure data integrity
	if err := s.segmentFile.Sync(); err != nil {
		return fmt.Errorf("sync before truncate: %w", err)
	}

	// Windows cannot truncate a file while it has an active memory mapping.
	// Unmap before truncating, then remap to the new size so reads/writes can
	// continue. Do this under flushMu to serialize with flush/sync/remap.
	hadMmap := s.mmapData != nil
	if hadMmap {
		if err := munmapFile(s.mmapData); err != nil {
			return fmt.Errorf("unmap before truncate: %w", err)
		}
		s.mmapData = nil
	}

	// Truncate the file. If this fails (e.g. Windows refuses to truncate a file
	// that another handle still has memory-mapped), remap at the ORIGINAL size so
	// the segment remains fully usable, and surface the error to the caller. The
	// caller in scan() treats trim failure as non-fatal because sizeBytes and
	// mmapWritePos are already positioned at the real data end — physical
	// truncation only reclaims the unused preallocated/corrupt tail on disk.
	if err := s.segmentFile.Truncate(pos); err != nil {
		if hadMmap {
			if remapData, remapErr := mmapFile(s.segmentFile, s.mmapSize); remapErr == nil {
				s.mmapData = remapData
			}
		}
		return fmt.Errorf("truncate to %d: %w", pos, err)
	}

	// Remap if the segment was memory-mapped.
	if hadMmap {
		mmapData, err := mmapFile(s.segmentFile, pos)
		if err != nil {
			return fmt.Errorf("remap after truncate: %w", err)
		}
		s.mmapData = mmapData
		s.mmapSize = pos
		s.mmapWritePos = pos
	}
	if s.mmapFlushedPos > pos {
		s.mmapFlushedPos = pos
	}

	// Sync again to ensure truncation is persisted
	if err := s.segmentFile.Sync(); err != nil {
		return fmt.Errorf("sync after truncate: %w", err)
	}

	s.sizeBytes = pos
	return nil
}

// FlushBuffer flushes the buffered writer without syncing to disk.
// For mmap segments, this syncs the mmap view to disk.
// Safe to call on a closed segment — returns nil without action.
func (s *Segment) FlushBuffer() error {
	s.mu.RLock()
	closed := s.closed
	mapped := s.mmapData != nil
	pos := s.mmapWritePos
	s.mu.RUnlock()
	if closed {
		return nil
	}
	// If using mmap, sync mmap to disk. s.mu is released before waiting for
	// flushMu: a flush queued behind an in-flight sync while still holding the
	// read lock would park the next appender (a writer on s.mu) for the rest of
	// that sync. mmapData and closed only change with flushMu held, so they are
	// re-read under it.
	if mapped {
		s.flushMu.Lock()
		defer s.flushMu.Unlock()
		if s.closed {
			return nil
		}
		if err := s.failedWriteLocked(nil); err != nil {
			return err
		}
		if s.mmapData == nil {
			return nil // deactivated meanwhile, which flushes before unmapping
		}
		return s.failedWriteLocked(s.syncMmapTailLocked(pos))
	}
	// Buffered writer is not thread-safe, so flush it under the exclusive lock.
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	if s.closed {
		return nil
	}
	return s.failedWriteLocked(s.flushWriter())
}

// Sync persists the segment file to disk (fdatasync).
// For mmap segments, this flushes the mmap view.
// Safe to call on a closed segment — returns nil without action.
func (s *Segment) Sync() error {
	s.mu.RLock()
	closed := s.closed
	pos := s.mmapWritePos
	s.mu.RUnlock()
	if closed {
		return nil
	}
	// As in FlushBuffer, wait for flushMu without holding s.mu so appends can
	// continue while another sync is in flight, then re-read the state that
	// only changes under flushMu.
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	if s.closed {
		return nil
	}
	if err := s.failedWriteLocked(nil); err != nil {
		return err
	}
	if s.mmapData != nil {
		if err := s.syncMmapTailLocked(pos); err != nil {
			return s.failedWriteLocked(err)
		}
	}
	return s.failedWriteLocked(s.segmentFile.Sync())
}

// mmapPageSize aligns partial flushes: msync rejects unaligned addresses.
var mmapPageSize = int64(os.Getpagesize())

// syncMmapTailLocked flushes the mapped bytes written since the last flush, up
// to pos. Records are append-only, so the already-flushed prefix cannot have
// changed; re-walking it on every flush costs time proportional to the whole
// segment. The caller must hold flushMu and have checked mmapData != nil.
func (s *Segment) syncMmapTailLocked(pos int64) error {
	if pos > int64(len(s.mmapData)) {
		pos = int64(len(s.mmapData))
	}
	if pos <= s.mmapFlushedPos {
		return nil
	}
	start := s.mmapFlushedPos - s.mmapFlushedPos%mmapPageSize
	if err := syncMmap(s.mmapData[start:pos]); err != nil {
		return err
	}
	s.mmapFlushedPos = pos
	return nil
}

// Flush flushes pending writes and syncs to disk.
// This is expensive; prefer FlushBuffer for routine flushing.
func (s *Segment) Flush() error {
	if err := s.FlushBuffer(); err != nil {
		return err
	}
	return s.Sync()
}

// Close closes segment and returns all accumulated close/sync errors.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	return s.closeLocked()
}

// Deactivate releases the write-side resources of a rotated (now inactive)
// segment — it flushes and syncs pending writes and unmaps the mmap — while
// KEEPING the read file handle and sparse index open so historical reads
// (Replay, follower catch-up, cross-region fetch) continue to work.
//
// Previously rotation called Close(), which shut the file handle; every read of
// a rotated segment then failed with os.ErrClosed. Reads use segmentFile.ReadAt
// (never the mmap), so dropping the mmap is safe. The segment remains fully
// readable until retention deletes it via Delete()/Close().
func (s *Segment) Deactivate() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	if s.closed {
		return nil
	}

	var errs []error

	// Flush any buffered (e.g. encrypted) records still in the writer. The
	// segment takes no more appends, so the buffer goes with them.
	if err := s.flushWriter(); err != nil {
		errs = append(errs, fmt.Errorf("flush writer: %w", err))
	} else {
		s.writer = nil
	}

	// Sync and unmap the mmap; reads do not depend on it.
	if s.mmapData != nil {
		if err := syncMmap(s.mmapData[:s.mmapWritePos]); err != nil {
			errs = append(errs, fmt.Errorf("sync mmap: %w", err))
		}
		if err := munmapFile(s.mmapData); err != nil {
			errs = append(errs, fmt.Errorf("unmap: %w", err))
		}
		s.mmapData = nil
	}

	// Persist the file so a reader opening it fresh sees all data.
	if err := s.segmentFile.Sync(); err != nil {
		errs = append(errs, fmt.Errorf("sync file: %w", err))
	}

	s.isActive = false
	// NOTE: segmentFile and index intentionally stay open for reads.
	return s.failedWriteLocked(errors.Join(errs...))
}

// closeLocked performs the actual close logic. Caller must hold s.mu.
func (s *Segment) closeLocked() error {
	if s.closed {
		return nil
	}
	s.closed = true

	var errs []error

	// Always flush the buffered writer first. Even when mmap is active, the
	// writer may contain data (e.g., encrypted records or headers written
	// before the mmap path was chosen) and must be persisted before unmapping.
	if err := s.flushWriter(); err != nil {
		errs = append(errs, fmt.Errorf("flush writer: %w", err))
	}

	// If using mmap, sync and unmap
	if s.mmapData != nil {
		if err := syncMmap(s.mmapData[:s.mmapWritePos]); err != nil {
			errs = append(errs, fmt.Errorf("sync mmap: %w", err))
		}
		if err := munmapFile(s.mmapData); err != nil {
			errs = append(errs, fmt.Errorf("unmap: %w", err))
		}
		s.mmapData = nil
	}

	if err := s.segmentFile.Sync(); err != nil {
		errs = append(errs, fmt.Errorf("sync file: %w", err))
	}

	if s.index != nil {
		if err := s.index.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close index: %w", err))
		}
	}
	if err := s.segmentFile.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close segment file: %w", err))
	}
	return errors.Join(errs...)
}

// Delete permanently removes the segment and its index files from disk after
// closing open handles.
func (s *Segment) Delete() error {
	s.deleted.Store(true)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	var errs []error

	// Close files first if not already closed.
	if err := s.closeLocked(); err != nil {
		errs = append(errs, fmt.Errorf("close segment before delete: %w", err))
	}

	// Delete segment file
	filePath := filepath.Join(s.dataDir, "segments", s.filename)
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		errs = append(errs, fmt.Errorf("delete segment file: %w", err))
	}

	// Delete index file
	indexFilePath := filepath.Join(s.dataDir, "index", s.indexFilename)
	if err := os.Remove(indexFilePath); err != nil && !os.IsNotExist(err) {
		errs = append(errs, fmt.Errorf("delete index file: %w", err))
	}

	return errors.Join(errs...)
}

// IsFull reports whether the segment's logical size is at or above maxSize bytes.
func (s *Segment) IsFull(maxSize int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sizeBytes >= maxSize
}

// GetSize returns the logical segment size in bytes (header plus records).
func (s *Segment) GetSize() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sizeBytes
}

// GetFilename returns the segment file name (e.g. "00000000000000000012.log").
func (s *Segment) GetFilename() string {
	return s.filename
}

// GetFirstOffset returns the first event offset stored in this segment.
func (s *Segment) GetFirstOffset() int64 {
	return s.firstOffset
}

// GetLastOffset returns the last event offset in this segment, or -1 if empty.
func (s *Segment) GetLastOffset() int64 {
	return s.lastOffset
}

// timeRange is the least and the greatest schedule timestamp (Unix ms) of a
// set of events.
type timeRange struct {
	min, max int64
	has      bool // false while the set is empty
}

// note widens the range to take in ts.
func (r *timeRange) note(ts int64) {
	if !r.has {
		r.min, r.max, r.has = ts, ts, true
		return
	}
	r.min, r.max = min(r.min, ts), max(r.max, ts)
}

// MayHoldTime reports whether the segment can hold an event scheduled in
// [startTS, endTS]. Events are not in time order in the log, so the first and
// the last event's times say nothing about the ones between them; this compares
// with the least and the greatest. A segment it says no for holds none.
func (s *Segment) MayHoldTime(startTS, endTS int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.times.has && s.times.min <= endTS && s.times.max >= startTS
}

// GetFirstTS returns the schedule timestamp of the first event (Unix ms), or 0.
func (s *Segment) GetFirstTS() int64 {
	return s.firstTS
}

// GetLastTS returns the schedule timestamp of the last event (Unix ms).
func (s *Segment) GetLastTS() int64 {
	return s.lastTS
}

// IsActive reports whether this segment still accepts appends.
func (s *Segment) IsActive() bool {
	return s.isActive
}

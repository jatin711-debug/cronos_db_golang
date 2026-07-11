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
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// Segment represents a WAL segment file
type Segment struct {
	mu              sync.RWMutex
	segmentFile     *os.File
	writer          *bufio.Writer
	reader          io.ReaderAt
	mmapData        []byte // Memory mapped data
	mmapWritePos    int64  // Current write position in mmap
	mmapSize        int64  // Total mmap size
	firstOffset     int64
	lastOffset      int64
	firstTS         int64
	lastTS          int64
	nextOffset      int64
	indexEntries    int64
	nextIndexOffset int64
	sizeBytes       int64
	createdTS       int64
	isActive        bool
	closed          bool // Set to true after Close()
	dataDir         string
	filename        string
	indexFilename   string
	index           *Index // sparse index for fast seeking
	recordBuf       []byte // Reusable buffer for serialization
	cipher          *SegmentCipher
}

// NewSegment creates a new segment
func NewSegment(dataDir string, firstOffset int64, isActive bool, cipher *SegmentCipher) (*Segment, error) {
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
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("open segment file: %w", err)
	}

	// Determine whether the file is newly created *before* extending it. Pre-allocation
	// sets the size to 1GB, so we cannot rely on stat.Size() == 0 after that step.
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("stat file: %w", err)
	}
	isNewFile := stat.Size() == 0

	// Pre-allocate segment file for zero-allocation writes
	const preallocSize = 1024 * 1024 * 1024 // 1GB pre-allocation
	if err := preallocateFile(file, preallocSize); err != nil {
		// Pre-allocation is optional — log warning but continue
		log.Printf("[SEGMENT] Pre-allocation failed for %s: %v (continuing without pre-allocation)", filename, err)
	}

	// Get file stats after pre-allocation
	stat, err = file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("stat file after preallocate: %w", err)
	}

	createdTS := time.Now().UnixMilli()
	dataEnd := stat.Size()

	// For newly created files, write the header directly to the file before any
	// mmap or buffered writer is set up. This guarantees the header is at offset 0
	// and leaves the file position at the end of the header for subsequent writes.
	if isNewFile {
		if _, err := file.Write(buildSegmentHeader(firstOffset, createdTS)); err != nil {
			file.Close()
			return nil, fmt.Errorf("write header: %w", err)
		}
		if err := file.Sync(); err != nil {
			file.Close()
			return nil, fmt.Errorf("sync header: %w", err)
		}
		dataEnd = 64
	}

	// Create index
	index, err := NewIndex(dataDir, firstOffset)
	if err != nil {
		file.Close()
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
		writer:          bufio.NewWriterSize(file, 4*1024*1024), // 4MB buffer for high throughput
		reader:          file,                                   // io.ReaderAt doesn't have buffered option
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
	}

	return segment, nil
}

// OpenSegment opens an existing segment
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
		segmentFile:   file,
		writer:        bufio.NewWriterSize(file, 4*1024*1024), // 4MB buffer for high throughput
		reader:        file,
		mmapData:      mmapData,
		mmapWritePos:  stat.Size(), // Start writing at end of existing data
		mmapSize:      mmapSize,
		firstOffset:   firstOffset,
		createdTS:     time.Now().UnixMilli(),
		isActive:      true, // Opened segments are active by default
		sizeBytes:     stat.Size(),
		dataDir:       dataDir,
		filename:      filename,
		indexFilename: fmt.Sprintf("%020d.index", firstOffset),
		index:         index,
		recordBuf:     make([]byte, 0, 4096),
		cipher:        cipher,
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
	if header[7] != 1 {
		return fmt.Errorf("unsupported segment version: %d", header[7])
	}
	stored := binary.BigEndian.Uint32(header[60:64])
	computed := crc32.ChecksumIEEE(header[0:60])
	if stored != computed {
		return fmt.Errorf("header CRC mismatch: stored=%08x computed=%08x", stored, computed)
	}
	return nil
}

// buildSegmentHeader returns the 64-byte segment header with magic, version,
// first offset, created timestamp, and CRC32.
func buildSegmentHeader(firstOffset, createdTS int64) []byte {
	header := make([]byte, 64)

	// Magic number "CRNOS1" (7 bytes)
	copy(header[0:7], []byte("CRNOS1"))

	// Version (1 byte)
	header[7] = 1

	// First offset (8 bytes)
	binary.BigEndian.PutUint64(header[8:16], uint64(firstOffset))

	// Created timestamp (8 bytes)
	binary.BigEndian.PutUint64(header[24:32], uint64(createdTS))

	// Reserved (32 bytes for future use)
	// ...

	// CRC32 of header (4 bytes)
	crc := crc32.ChecksumIEEE(header[0:60])
	binary.BigEndian.PutUint32(header[60:64], crc)

	return header
}

// AppendEvent appends an event to the segment
func (s *Segment) AppendEvent(event *types.Event, indexInterval int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isActive {
		return s.appendEventActive(event, indexInterval)
	}
	return fmt.Errorf("cannot append to closed segment")
}

// AppendBatch appends a batch of events (thread-safe)
func (s *Segment) AppendBatch(events []*types.Event, indexInterval int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.appendBatchInternal(events, indexInterval)
}

// AppendBatchUnsafe appends a batch of events without acquiring s.mu.
// The caller MUST guarantee exclusive access (e.g. WAL.mu is held).
// This eliminates double-locking in the WAL→Segment write path.
func (s *Segment) AppendBatchUnsafe(events []*types.Event, indexInterval int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendBatchInternal(events, indexInterval)
}

// AppendPreparedBatchUnsafe appends a batch of pre-encoded records without acquiring s.mu.
// The caller MUST guarantee exclusive access (e.g. WAL.mu is held).
func (s *Segment) AppendPreparedBatchUnsafe(prepared []*PreparedRecord, indexInterval int64) error {
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

// appendBatchInternal is the shared batch append implementation
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
	recordLen := len(record)
	if recordLen == 0 {
		return 0, nil
	}

	// Check if we have space in mmap
	if s.mmapWritePos+int64(recordLen) > s.mmapSize {
		// Remap with larger size
		if err := s.remapMmap(s.mmapSize * 2); err != nil {
			return 0, fmt.Errorf("remap mmap: %w", err)
		}
	}

	// Copy directly to mmap (single memcpy, no syscall)
	copy(s.mmapData[s.mmapWritePos:], record)
	s.mmapWritePos += int64(recordLen)
	s.sizeBytes = s.mmapWritePos

	return int64(recordLen), nil
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
		n1, err := s.writer.Write(length[:])
		if err != nil {
			return int64(n1), err
		}
		n2, err := s.writer.Write(ciphertext)
		return int64(n1 + n2), err
	}
	n, err := s.writer.Write(record)
	return int64(n), err
}

// remapMmap remaps the file with a new size.
func (s *Segment) remapMmap(newSize int64) error {
	if s.mmapData != nil {
		if err := syncMmap(s.mmapData); err != nil {
			log.Printf("[SEGMENT] mmap sync before remap failed: %v", err)
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
	s.mmapSize = newSize
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

// ReadEvent reads event at offset using index for fast seeking
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

	// Try to read from mmap first
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
		if length <= 4 || length > 10*1024*1024 {
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
		if length <= 0 || length > 10*1024*1024 { // Sanity check
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

// ReadEventsByTime reads events in the given timestamp range [startTS, endTS]
func (s *Segment) ReadEventsByTime(startTS, endTS int64) ([]*types.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*types.Event

	if s.lastTS > 0 && s.firstTS > endTS {
		return result, nil // Segment is completely after the range
	}
	if s.lastTS > 0 && s.lastTS < startTS {
		return result, nil // Segment is completely before the range
	}

	// Find starting position using index
	startPos := int64(64) // Default to after header
	if s.index != nil {
		if pos, found := s.index.FindByTimestamp(startTS); found {
			startPos = pos
		}
	}

	// Use ReadAt on existing file handle instead of opening new file
	lengthBytes := make([]byte, 4)
	recordBuf := make([]byte, 0, 4096)
	currentPos := startPos

	for {
		// Read record length using ReadAt
		if _, err := s.segmentFile.ReadAt(lengthBytes, currentPos); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read length: %w", err)
		}
		currentPos += 4

		length := int64(binary.BigEndian.Uint32(lengthBytes))
		if length <= 0 || length > 10*1024*1024 {
			break
		}

		// Read full record
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

// ReadEventsByOffsetRange reads events in the given offset range [startOffset, endOffset]
func (s *Segment) ReadEventsByOffsetRange(startOffset, endOffset int64) ([]*types.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*types.Event

	// Find starting position using index
	startPos := int64(64) // Default to after header
	if s.index != nil {
		if pos, found := s.index.FindByOffset(startOffset); found {
			startPos = pos
		}
	}

	// Use ReadAt on existing file handle (pread is thread-safe)
	lengthBytes := make([]byte, 4)
	recordBuf := make([]byte, 0, 4096)
	currentPos := startPos

	for {
		// Read record length using ReadAt
		if _, err := s.segmentFile.ReadAt(lengthBytes, currentPos); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read length: %w", err)
		}
		currentPos += 4

		length := int64(binary.BigEndian.Uint32(lengthBytes))
		if length <= 0 || length > 10*1024*1024 {
			break
		}

		// Read full record
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

		decrypted, err := s.decryptRecord(record, ciphertextPos)
		if err != nil {
			return nil, fmt.Errorf("decrypt: %w", err)
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
			return nil, fmt.Errorf("parse event: %w", err)
		}

		if event.Offset >= startOffset && event.Offset <= endOffset {
			result = append(result, event)
		}

		if event.Offset >= endOffset {
			break
		}
	}

	return result, nil
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

	// Read through all records
	for {
		recordStartPos = lastGoodPos

		// Read record length
		if _, err := io.ReadFull(file, lengthBytes); err != nil {
			if err == io.EOF {
				// Clean end of file
				break
			}
			if err == io.ErrUnexpectedEOF {
				// Truncated file - corrupt tail detected
				log.Printf("[SEGMENT] Truncated tail detected at position %d, truncating file", lastGoodPos)
				break
			}
			return fmt.Errorf("read length: %w", err)
		}

		length := int64(binary.BigEndian.Uint32(lengthBytes))
		if length <= 4 || length > 10*1024*1024 { // Sanity check: must be > 4 bytes, max 10MB record
			log.Printf("[SEGMENT] Invalid record length %d at position %d, treating as corrupt tail", length, lastGoodPos)
			break
		}

		// Read remainder of record (length includes the 4-byte length field itself)
		recordLen := int(length - 4)
		if cap(recordBuf) < recordLen {
			recordBuf = make([]byte, recordLen)
		}
		recordData := recordBuf[:recordLen]
		if _, err := io.ReadFull(file, recordData); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// Truncated or incomplete file - corrupt tail detected
				log.Printf("[SEGMENT] Incomplete record at position %d (expected %d bytes), truncating", lastGoodPos, length)
				break
			}
			return fmt.Errorf("read record: %w", err)
		}

		// Decrypt if needed, then parse event to get offset and timestamp
		decrypted, err := s.decryptRecord(recordData, lastGoodPos+4)
		if err != nil {
			log.Printf("[SEGMENT] Decrypt failed at position %d: %v, truncating file", lastGoodPos, err)
			break
		}
		event, err := parseEventRecordWithoutLength(decrypted)
		if err != nil {
			// CRC mismatch or parse error - corrupt frame detected
			log.Printf("[SEGMENT] Corrupt frame at position %d: %v, truncating file", lastGoodPos, err)
			break
		}

		// Valid record - update last good position
		s.lastOffset = event.Offset
		s.lastTS = event.GetScheduleTs()
		lastGoodPos = recordStartPos + 4 + int64(len(recordData)) // After length + recordData

		// Update segment size to reflect truncated state
		s.sizeBytes = lastGoodPos
	}

	// If we found corrupt data at the tail, truncate the file. Compare against
	// the original on-disk size (origSizeBytes), not s.sizeBytes which the loop
	// above already advanced to lastGoodPos.
	if lastGoodPos < origSizeBytes {
		if err := s.truncateToPosition(lastGoodPos); err != nil {
			return fmt.Errorf("truncate corrupt tail at %d: %w", lastGoodPos, err)
		}
		log.Printf("[SEGMENT] Truncated %d bytes of corrupt tail", origSizeBytes-lastGoodPos)

		// Reconcile the sparse index with the truncated segment. Without this,
		// the index may still contain entries whose FilePosition points past
		// the new (shorter) segment EOF, causing later reads to dereference
		// uninitialized bytes. s.lastOffset is the last valid event offset.
		if s.index != nil {
			if err := s.index.Truncate(s.lastOffset); err != nil {
				return fmt.Errorf("truncate index to offset %d: %w", s.lastOffset, err)
			}
		}
	}

	// If we couldn't find any events, start from firstOffset
	if s.lastOffset < s.firstOffset {
		s.lastOffset = s.firstOffset - 1
	}

	return nil
}

// truncateToPosition truncates the segment file to the given position
func (s *Segment) truncateToPosition(pos int64) error {
	if pos >= s.sizeBytes {
		return nil // Nothing to truncate
	}

	// Sync before truncating to ensure data integrity
	if err := s.segmentFile.Sync(); err != nil {
		return fmt.Errorf("sync before truncate: %w", err)
	}

	// Truncate the file
	if err := s.segmentFile.Truncate(pos); err != nil {
		return fmt.Errorf("truncate to %d: %w", pos, err)
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
	defer s.mu.RUnlock()
	if s.closed {
		return nil
	}
	// If using mmap, sync mmap to disk
	if s.mmapData != nil {
		return syncMmap(s.mmapData[:s.mmapWritePos])
	}
	return s.writer.Flush()
}

// Sync persists the segment file to disk (fdatasync).
// For mmap segments, this flushes the mmap view.
// Safe to call on a closed segment — returns nil without action.
func (s *Segment) Sync() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil
	}
	// If using mmap, sync mmap to disk
	if s.mmapData != nil {
		if err := syncMmap(s.mmapData[:s.mmapWritePos]); err != nil {
			return err
		}
		return s.segmentFile.Sync() // Also sync the file descriptor
	}
	return s.segmentFile.Sync()
}

// Flush flushes pending writes and syncs to disk.
// This is expensive; prefer FlushBuffer for routine flushing.
func (s *Segment) Flush() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil
	}
	if s.mmapData != nil {
		if err := syncMmap(s.mmapData[:s.mmapWritePos]); err != nil {
			return err
		}
		return s.segmentFile.Sync()
	}
	if err := s.writer.Flush(); err != nil {
		return err
	}
	return s.segmentFile.Sync()
}

// Close closes segment and returns all accumulated close/sync errors.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closeLocked()
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
	if err := s.writer.Flush(); err != nil {
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

// Delete permanently removes the segment and its index files from disk
func (s *Segment) Delete() error {
	s.mu.Lock()
	defer s.mu.Unlock()

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

// IsFull checks if segment is full
func (s *Segment) IsFull(maxSize int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sizeBytes >= maxSize
}

// GetSize returns segment size in bytes
func (s *Segment) GetSize() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sizeBytes
}

// GetFilename returns filename
func (s *Segment) GetFilename() string {
	return s.filename
}

// GetFirstOffset returns first offset
func (s *Segment) GetFirstOffset() int64 {
	return s.firstOffset
}

// GetLastOffset returns last offset
func (s *Segment) GetLastOffset() int64 {
	return s.lastOffset
}

// GetFirstTS returns first timestamp
func (s *Segment) GetFirstTS() int64 {
	return s.firstTS
}

// GetLastTS returns last timestamp
func (s *Segment) GetLastTS() int64 {
	return s.lastTS
}

// IsActive returns active status
func (s *Segment) IsActive() bool {
	return s.isActive
}

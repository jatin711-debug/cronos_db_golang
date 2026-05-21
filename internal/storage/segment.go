package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"cronos_db/pkg/types"
)

// Segment represents a WAL segment file
type Segment struct {
	mu              sync.RWMutex
	segmentFile     *os.File
	writer          *bufio.Writer
	reader          io.ReaderAt
	mmapData        []byte // Memory mapped data
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
}

// NewSegment creates a new segment
func NewSegment(dataDir string, firstOffset int64, isActive bool) (*Segment, error) {
	filename := fmt.Sprintf("%020d.log", firstOffset)
	filePath := filepath.Join(dataDir, "segments", filename)

	// Create segments directory if it doesn't exist
	segmentsDir := filepath.Join(dataDir, "segments")
	if err := os.MkdirAll(segmentsDir, 0755); err != nil {
		return nil, fmt.Errorf("create segments dir: %w", err)
	}

	// Open or create segment file
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open segment file: %w", err)
	}

	// Create index
	index, err := NewIndex(dataDir, firstOffset)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("create index: %w", err)
	}

	// Try to mmap the file for reading
	var mmapData []byte
	stat, _ := file.Stat()
	if stat.Size() > 0 {
		mmapData, _ = mmapFile(file, stat.Size())
		// We ignore mmap errors and fall back to file reading (mmapData will be nil if error)
	}

	segment := &Segment{
		segmentFile:     file,
		writer:          bufio.NewWriterSize(file, 1024*1024), // 1MB buffer
		reader:          file,                                 // io.ReaderAt doesn't have buffered option
		mmapData:        mmapData,
		firstOffset:     firstOffset,
		nextOffset:      firstOffset,
		createdTS:       time.Now().UnixMilli(),
		isActive:        isActive,
		dataDir:         dataDir,
		filename:        filename,
		indexFilename:   fmt.Sprintf("%020d.index", firstOffset),
		nextIndexOffset: 0,
		indexEntries:    0,
		index:           index,
		recordBuf:       make([]byte, 0, 4096), // Pre-allocate 4KB
	}

	// Write header if new file
	stat, _ = file.Stat()
	if stat.Size() == 0 {
		if err := segment.writeHeader(); err != nil {
			file.Close()
			index.Close()
			return nil, fmt.Errorf("write header: %w", err)
		}
	}

	return segment, nil
}

// OpenSegment opens an existing segment
func OpenSegment(dataDir string, filename string) (*Segment, error) {
	filePath := filepath.Join(dataDir, "segments", filename)
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_APPEND, 0644)
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

	// Try to mmap the file
	var mmapData []byte
	if stat.Size() > 0 {
		mmapData, _ = mmapFile(file, stat.Size())
		// Fallback to file reading if mmap fails or unsupported
	}

	segment := &Segment{
		segmentFile:   file,
		writer:        bufio.NewWriterSize(file, 1024*1024),
		reader:        file,
		mmapData:      mmapData,
		firstOffset:   firstOffset,
		createdTS:     time.Now().UnixMilli(),
		isActive:      true, // Opened segments are active by default
		sizeBytes:     stat.Size(),
		dataDir:       dataDir,
		filename:      filename,
		indexFilename: fmt.Sprintf("%020d.index", firstOffset),
		index:         index,
		recordBuf:     make([]byte, 0, 4096),
	}

	// Scan segment to get metadata
	if err := segment.scan(); err != nil {
		file.Close()
		index.Close()
		return nil, fmt.Errorf("scan segment: %w", err)
	}

	// Set nextOffset based on lastOffset found during scan
	segment.nextOffset = segment.lastOffset + 1

	return segment, nil
}

// writeHeader writes segment file header
func (s *Segment) writeHeader() error {
	header := make([]byte, 64)

	// Magic number "CRNOS1" (7 bytes)
	copy(header[0:7], []byte("CRNOS1"))

	// Version (1 byte)
	header[7] = 1

	// First offset (8 bytes)
	binary.BigEndian.PutUint64(header[8:16], uint64(s.firstOffset))

	// Created timestamp (8 bytes)
	binary.BigEndian.PutUint64(header[24:32], uint64(s.createdTS))

	// Reserved (32 bytes for future use)
	// ...

	// CRC32 of header (4 bytes)
	crc := crc32.ChecksumIEEE(header[0:60])
	binary.BigEndian.PutUint32(header[60:64], crc)

	_, err := s.writer.Write(header)
	if err == nil {
		s.sizeBytes += 64
	}
	return err
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
	return s.appendBatchInternal(events, indexInterval)
}

// AppendPreparedBatchUnsafe appends a batch of pre-encoded records without acquiring s.mu.
// The caller MUST guarantee exclusive access (e.g. WAL.mu is held).
func (s *Segment) AppendPreparedBatchUnsafe(prepared []*PreparedRecord, indexInterval int64) error {
	if !s.isActive {
		return fmt.Errorf("cannot append to closed segment")
	}

	// Track position from current size (buffered writes)
	filePos := s.sizeBytes

	for _, prep := range prepared {
		// Write record to buffer
		if err := s.writeRecord(prep.Buf); err != nil {
			return fmt.Errorf("write record: %w", err)
		}

		// Update metadata
		s.lastOffset = prep.Event.Offset
		s.lastTS = prep.Event.GetScheduleTs()
		if s.firstTS == 0 {
			s.firstTS = prep.Event.GetScheduleTs()
		}
		s.nextOffset++

		recordLen := int64(len(prep.Buf))
		s.sizeBytes = filePos + recordLen

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
		filePos += recordLen
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
		if err := s.writeRecord(record); err != nil {
			return fmt.Errorf("write record: %w", err)
		}

		// Update metadata
		s.lastOffset = event.Offset
		s.lastTS = event.GetScheduleTs()
		if s.firstTS == 0 {
			s.firstTS = event.GetScheduleTs()
		}
		s.nextOffset++

		recordLen := int64(len(record))
		s.sizeBytes = filePos + recordLen

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
		filePos += recordLen
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
	if err := s.writeRecord(record); err != nil {
		return fmt.Errorf("write record: %w", err)
	}

	// Update metadata
	s.lastOffset = event.Offset
	s.lastTS = event.GetScheduleTs()
	if s.firstTS == 0 {
		s.firstTS = event.GetScheduleTs()
	}
	s.nextOffset++
	s.sizeBytes = filePos + int64(len(record))

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

// buildEventRecord builds binary event record
func (s *Segment) buildEventRecord(event *types.Event) ([]byte, error) {
	// Calculate sizes
	msgIDLen := len(event.GetMessageId())
	topicLen := len(event.Topic)
	payloadLen := len(event.Payload)
	metaCount := len(event.Meta)

	// Calculate total size with overflow checking
	size := 4 + 4 + 8 + 8 + 2 + msgIDLen + 2 + topicLen + 4 + payloadLen + 2
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

	// Calculate and write CRC32
	crc := crc32.ChecksumIEEE(record[8:])
	binary.BigEndian.PutUint32(record[4:8], crc)

	return record, nil
}

// parseEventRecordWithoutLength parses binary event record that doesn't include length prefix
// The record starts with CRC32 (4 bytes) followed by event data
func parseEventRecordWithoutLength(record []byte) (*types.Event, error) {
	if len(record) < 4 {
		return nil, fmt.Errorf("record too short")
	}

	// Validate CRC32 checksum
	storedCRC := binary.BigEndian.Uint32(record[0:4])
	computedCRC := crc32.ChecksumIEEE(record[4:])
	if storedCRC != computedCRC {
		return nil, fmt.Errorf("CRC32 mismatch: stored=0x%08x computed=0x%08x (data corruption detected)", storedCRC, computedCRC)
	}

	offset := 4 // Skip CRC32 (4 bytes), length was already read separately

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

	// Meta count (2 bytes)
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
	}, nil
}

// writeRecord writes record to segment
func (s *Segment) writeRecord(record []byte) error {
	if _, err := s.writer.Write(record); err != nil {
		return err
	}
	return nil
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

		// Optimization: Check offset before parsing full event?
		// Offset is at bytes 8-16 of recordData (after 4 byte CRC + 4 byte Length which is not in recordData)
		// Wait, recordData here excludes the length prefix.
		// Structure: [Length 4][CRC 4][Offset 8]...
		// So in recordData (which is record[4:] from writer POV):
		// [0-3] is CRC
		// [4-11] is Offset

		if len(recordData) > 12 {
			offsetVal := int64(binary.BigEndian.Uint64(recordData[4:12]))
			if offsetVal > targetOffset {
				return nil, fmt.Errorf("event not found (passed target)")
			}
			if offsetVal < targetOffset {
				pos += int(length)
				continue
			}
		}

		event, err := parseEventRecordWithoutLength(recordData)
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
		if _, err := s.segmentFile.ReadAt(record, currentPos); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read record: %w", err)
		}
		currentPos += int64(recordLen)

		// Early offset check: offset is at bytes 4-12 of record (after CRC)
		if len(record) > 12 {
			offsetVal := int64(binary.BigEndian.Uint64(record[4:12]))
			if offsetVal > targetOffset {
				break // Past target
			}
			if offsetVal < targetOffset {
				continue // Skip full parse
			}
		}

		// Parse event - record doesn't include length prefix
		event, err := parseEventRecordWithoutLength(record)
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
		if _, err := s.segmentFile.ReadAt(record, currentPos); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read record: %w", err)
		}
		currentPos += int64(recordLen)

		event, err := parseEventRecordWithoutLength(record)
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
		if _, err := s.segmentFile.ReadAt(record, currentPos); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read record: %w", err)
		}
		currentPos += int64(recordLen)

		// Early offset check before full event parsing.
		// Offset is at bytes 4-12 of record (after 4-byte CRC).
		// This avoids CRC check + string allocs for out-of-range records.
		if len(record) > 12 {
			eventOffset := int64(binary.BigEndian.Uint64(record[4:12]))
			if eventOffset > endOffset {
				break // Past range, done
			}
			if eventOffset < startOffset {
				continue // Before range, skip full parse
			}
		}

		event, err := parseEventRecordWithoutLength(record)
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

		// Parse event to get offset and timestamp (record doesn't include length prefix)
		event, err := parseEventRecordWithoutLength(recordData)
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

	// If we found corrupt data at the tail, truncate the file
	if lastGoodPos < s.sizeBytes {
		if err := s.truncateToPosition(lastGoodPos); err != nil {
			log.Printf("[SEGMENT] Warning: failed to truncate corrupt tail: %v", err)
		} else {
			log.Printf("[SEGMENT] Truncated %d bytes of corrupt tail", s.sizeBytes-lastGoodPos)
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
// Use this for frequent, low-cost flush operations.
// Safe to call on a closed segment — returns nil without action.
func (s *Segment) FlushBuffer() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil
	}
	return s.writer.Flush()
}

// Sync persists the segment file to disk (fdatasync).
// Safe to call on a closed segment — returns nil without action.
func (s *Segment) Sync() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil
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
	if err := s.writer.Flush(); err != nil {
		return err
	}
	return s.segmentFile.Sync()
}

// Close closes segment
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	if err := s.writer.Flush(); err != nil {
		return err
	}
	if err := s.segmentFile.Sync(); err != nil {
		return err
	}

	// Unmap memory
	if s.mmapData != nil {
		munmapFile(s.mmapData)
		s.mmapData = nil
	}

	if s.index != nil {
		if err := s.index.Close(); err != nil {
			return err
		}
	}
	return s.segmentFile.Close()
}

// Delete permanently removes the segment and its index files from disk
func (s *Segment) Delete() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close files first if they are open
	if s.segmentFile != nil {
		_ = s.segmentFile.Close()
	}

	if s.mmapData != nil {
		munmapFile(s.mmapData)
		s.mmapData = nil
	}

	if s.index != nil {
		_ = s.index.Close()
	}

	// Delete segment file
	filePath := filepath.Join(s.dataDir, "segments", s.filename)
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete segment file: %w", err)
	}

	// Delete index file
	indexFilePath := filepath.Join(s.dataDir, "index", s.indexFilename)
	if err := os.Remove(indexFilePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete index file: %w", err)
	}

	return nil
}

// IsFull checks if segment is full
func (s *Segment) IsFull(maxSize int64) bool {
	return s.sizeBytes >= maxSize
}

// GetSize returns segment size in bytes
func (s *Segment) GetSize() int64 {
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

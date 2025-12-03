package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
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
	dataDir         string
	filename        string
	indexFilename   string
	index           *Index // sparse index for fast seeking
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

// AppendBatch appends a batch of events
func (s *Segment) AppendBatch(events []*types.Event, indexInterval int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isActive {
		return fmt.Errorf("cannot append to closed segment")
	}

	// Track position from current size (buffered writes)
	filePos := s.sizeBytes

	for _, event := range events {
		// Build event record
		record := s.buildEventRecord(event)

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
		if s.nextOffset-s.firstOffset >= s.nextIndexOffset {
			if err := s.index.AddEntry(event.GetScheduleTs(), event.Offset, filePos); err != nil {
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

// appendEventActive appends to active segment
func (s *Segment) appendEventActive(event *types.Event, indexInterval int64) error {
	// Get current buffered position (approximation - will be exact after flush)
	filePos := s.sizeBytes

	// Build event record
	record := s.buildEventRecord(event)

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
	if s.nextOffset-s.firstOffset >= s.nextIndexOffset {
		if err := s.index.AddEntry(event.GetScheduleTs(), event.Offset, filePos); err != nil {
			return fmt.Errorf("write index: %w", err)
		}
		s.nextIndexOffset += indexInterval
		s.indexEntries++
	}

	return nil
}

// buildEventRecord builds binary event record
func (s *Segment) buildEventRecord(event *types.Event) []byte {
	// Calculate sizes
	msgIDLen := len(event.GetMessageId())
	topicLen := len(event.Topic)
	payloadLen := len(event.Payload)
	metaCount := len(event.Meta)

	// Calculate total size
	size := 4 + 4 + 8 + 8 + 2 + msgIDLen + 2 + topicLen + 4 + payloadLen + 2
	for k, v := range event.Meta {
		size += 2 + len(k) + 2 + len(v) // keylen + key + valuelen + value
	}

	record := make([]byte, size)
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

	return record
}

// parseEventRecordWithoutLength parses binary event record that doesn't include length prefix
// The record starts with CRC32 (4 bytes) followed by event data
func parseEventRecordWithoutLength(record []byte) (*types.Event, error) {
	offset := 4 // Skip CRC32 (4 bytes), length was already read separately

	if len(record) < 4 {
		return nil, fmt.Errorf("record too short")
	}

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

// readEventFile reads event from file
func (s *Segment) readEventFile(targetOffset int64, startPos int64) (*types.Event, error) {
	// Open a separate file handle for reading to avoid conflicts with writer
	filePath := filepath.Join(s.dataDir, "segments", s.filename)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open for read: %w", err)
	}
	defer file.Close()

	// Seek to starting position
	if _, err := file.Seek(startPos, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek: %w", err)
	}

	// Scan from index position to find target event
	for {
		// Read record length
		lengthBytes := make([]byte, 4)
		if _, err := io.ReadFull(file, lengthBytes); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read length: %w", err)
		}

		length := int64(binary.BigEndian.Uint32(lengthBytes))
		if length <= 0 || length > 10*1024*1024 { // Sanity check
			break
		}

		// Read full record (length includes the 4-byte length field)
		record := make([]byte, length-4)
		if _, err := io.ReadFull(file, record); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read record: %w", err)
		}

		// Parse event - record doesn't include length prefix
		event, err := parseEventRecordWithoutLength(record)
		if err != nil {
			return nil, fmt.Errorf("parse event: %w", err)
		}

		if event.Offset == targetOffset {
			return event, nil
		}

		// If we've gone past the target, stop
		if event.Offset > targetOffset {
			break
		}
	}

	return nil, fmt.Errorf("event not found at offset %d", targetOffset)
}

// scan scans segment to recover metadata
func (s *Segment) scan() error {
	// Scan segment file to find last offset and timestamp
	file := s.segmentFile

	// Seek to start (after header)
	if _, err := file.Seek(64, io.SeekStart); err != nil {
		return fmt.Errorf("seek to start: %w", err)
	}

	s.lastOffset = -1
	s.lastTS = 0

	// Read through all records
	for {
		// Read record length
		lengthBytes := make([]byte, 4)
		if _, err := file.Read(lengthBytes); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// End of file - no more records
				break
			}
			return fmt.Errorf("read length: %w", err)
		}

		length := int64(binary.BigEndian.Uint32(lengthBytes))
		if length <= 4 || length > 10*1024*1024 { // Sanity check: must be > 4 bytes, max 10MB record
			break
		}

		// Read remainder of record (length includes the 4-byte length field itself)
		recordData := make([]byte, length-4)
		if _, err := io.ReadFull(file, recordData); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// Truncated or incomplete file - stop scanning
				// This is normal after a crash or incomplete write
				break
			}
			return fmt.Errorf("read record: %w", err)
		}

		// Parse event to get offset and timestamp (record doesn't include length prefix)
		event, err := parseEventRecordWithoutLength(recordData)
		if err != nil {
			return fmt.Errorf("parse event: %w", err)
		}

		s.lastOffset = event.Offset
		s.lastTS = event.GetScheduleTs()
	}

	// If we couldn't find any events, start from firstOffset
	if s.lastOffset < s.firstOffset {
		s.lastOffset = s.firstOffset - 1
	}

	return nil
}

// Flush flushes pending writes
func (s *Segment) Flush() error {
	if err := s.writer.Flush(); err != nil {
		return err
	}
	return s.segmentFile.Sync()
}

// Close closes segment
func (s *Segment) Close() error {
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

// Sync syncs segment to disk
func (s *Segment) Sync() error {
	return s.segmentFile.Sync()
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

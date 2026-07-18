package delivery

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const (
	dlqMagic        = "CRNDLQ1"
	dlqHeaderSize   = 64
	dlqSegmentSize  = 64 * 1024 * 1024 // 64MB per segment
	dlqWriteBufSize = 4 * 1024 * 1024  // 4MB write buffer
)

// DLQSegmentWriter writes DLQ entries to append-only rotating segment files.
// Records are length-prefixed with CRC32; segments rotate at ~64MB.
type DLQSegmentWriter struct {
	mu         sync.Mutex
	dataDir    string
	activeFile *os.File
	writer     *bufio.Writer
	size       int64 // current active segment size in bytes
	seqNum     int64 // active segment sequence number
}

// NewDLQSegmentWriter creates a writer under dataDir and opens the next segment.
func NewDLQSegmentWriter(dataDir string) (*DLQSegmentWriter, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create dlq dir: %w", err)
	}

	sw := &DLQSegmentWriter{
		dataDir: dataDir,
	}

	if err := sw.openActiveSegment(); err != nil {
		return nil, err
	}

	return sw, nil
}

// openActiveSegment opens or creates the active segment file.
func (sw *DLQSegmentWriter) openActiveSegment() error {
	// Find the highest sequence number
	entries, err := os.ReadDir(sw.dataDir)
	if err != nil {
		return fmt.Errorf("read dlq dir: %w", err)
	}

	var maxSeq int64 = -1
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".dlq") {
			continue
		}
		seqStr := strings.TrimSuffix(entry.Name(), ".dlq")
		if seq, err := strconv.ParseInt(seqStr, 10, 64); err == nil && seq > maxSeq {
			maxSeq = seq
		}
	}

	sw.seqNum = maxSeq + 1
	filename := filepath.Join(sw.dataDir, fmt.Sprintf("%020d.dlq", sw.seqNum))

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open dlq segment: %w", err)
	}

	// Write header if new file
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return fmt.Errorf("stat dlq segment: %w", err)
	}

	if info.Size() == 0 {
		if err := sw.writeHeader(file); err != nil {
			file.Close()
			return fmt.Errorf("write dlq header: %w", err)
		}
	}

	sw.activeFile = file
	sw.writer = bufio.NewWriterSize(file, dlqWriteBufSize)
	sw.size = info.Size()
	return nil
}

// writeHeader writes the 64-byte segment header.
func (sw *DLQSegmentWriter) writeHeader(file *os.File) error {
	header := make([]byte, dlqHeaderSize)
	copy(header[0:7], dlqMagic)
	header[7] = 1 // version
	// Bytes 8-63 reserved, zero-filled
	// CRC32 of header at bytes 60-63 (but we'll skip for simplicity)
	if _, err := file.Write(header); err != nil {
		return err
	}
	return file.Sync()
}

// WriteEntry appends a DLQ entry to the active segment.
func (sw *DLQSegmentWriter) WriteEntry(data []byte) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	// Check rotation
	if sw.size > dlqSegmentSize {
		if err := sw.rotate(); err != nil {
			return fmt.Errorf("rotate dlq segment: %w", err)
		}
	}

	// Record format: [length:4][crc32:4][data:N]
	length := 8 + len(data)
	record := make([]byte, 4+length)
	binary.BigEndian.PutUint32(record[0:4], uint32(length))
	binary.BigEndian.PutUint32(record[4:8], crc32.ChecksumIEEE(data))
	copy(record[8:], data)

	if _, err := sw.writer.Write(record); err != nil {
		return fmt.Errorf("write dlq entry: %w", err)
	}
	if err := sw.writer.Flush(); err != nil {
		return fmt.Errorf("flush dlq entry: %w", err)
	}
	if err := sw.activeFile.Sync(); err != nil {
		return fmt.Errorf("sync dlq entry: %w", err)
	}

	sw.size += int64(len(record))
	return nil
}

// rotate closes the current segment and opens a new one.
func (sw *DLQSegmentWriter) rotate() error {
	if err := sw.writer.Flush(); err != nil {
		return err
	}
	if err := sw.activeFile.Sync(); err != nil {
		return err
	}
	if err := sw.activeFile.Close(); err != nil {
		return err
	}
	return sw.openActiveSegment()
}

// Close flushes and closes the active segment.
func (sw *DLQSegmentWriter) Close() error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	if sw.writer != nil {
		if err := sw.writer.Flush(); err != nil {
			return err
		}
	}
	if sw.activeFile != nil {
		if err := sw.activeFile.Sync(); err != nil {
			return err
		}
		return sw.activeFile.Close()
	}
	return nil
}

// Scan reads all valid entries from all segment files in the DLQ directory.
func (sw *DLQSegmentWriter) Scan() ([][]byte, error) {
	sw.mu.Lock()
	// Flush active writer so we can read the file
	if sw.writer != nil {
		sw.writer.Flush()
	}
	sw.mu.Unlock()

	return scanEntries(sw.dataDir)
}

// scanEntries reads all valid entries from DLQ segment files (lock-free).
func scanEntries(dataDir string) ([][]byte, error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, fmt.Errorf("read dlq dir: %w", err)
	}

	var results [][]byte
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".dlq") {
			continue
		}

		path := filepath.Join(dataDir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue // Skip unreadable files
		}

		// Skip header
		if len(data) < dlqHeaderSize {
			continue
		}
		offset := dlqHeaderSize

		for offset < len(data) {
			if offset+4 > len(data) {
				break
			}
			length := int(binary.BigEndian.Uint32(data[offset : offset+4]))
			if length < 8 || offset+4+length > len(data) {
				break // Truncated or corrupt
			}

			storedCRC := binary.BigEndian.Uint32(data[offset+4 : offset+8])
			entryData := data[offset+8 : offset+4+length]
			if crc32.ChecksumIEEE(entryData) == storedCRC {
				results = append(results, entryData)
			}

			offset += 4 + length
		}
	}

	return results, nil
}

// GetSegmentCount returns the number of segment files.
func (sw *DLQSegmentWriter) GetSegmentCount() int {
	entries, err := os.ReadDir(sw.dataDir)
	if err != nil {
		return 0
	}
	count := 0
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".dlq") {
			count++
		}
	}
	return count
}

// Compact rewrites all segments into a single compacted segment, removing duplicates.
// This should be called during low-traffic periods.
func (sw *DLQSegmentWriter) Compact(keep func(data []byte) bool) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	// Flush current writer
	if sw.writer != nil {
		sw.writer.Flush()
	}

	// Close active file BEFORE reading so Windows can read/delete it
	if sw.activeFile != nil {
		sw.activeFile.Close()
		sw.activeFile = nil
	}
	// Reset writer so it won't be used while we rebuild
	sw.writer = nil

	// Read all entries (lock-free — we already hold the lock)
	allData, err := scanEntries(sw.dataDir)
	if err != nil {
		return err
	}

	// Remove old segments
	entries, _ := os.ReadDir(sw.dataDir)
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".dlq") {
			os.Remove(filepath.Join(sw.dataDir, entry.Name()))
		}
	}

	// Reset and write compacted data
	sw.seqNum = 0
	sw.size = 0
	filename := filepath.Join(sw.dataDir, fmt.Sprintf("%020d.dlq", sw.seqNum))
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("create compacted dlq: %w", err)
	}

	if err := sw.writeHeader(file); err != nil {
		file.Close()
		return err
	}

	writer := bufio.NewWriterSize(file, dlqWriteBufSize)
	for _, data := range allData {
		if keep != nil && !keep(data) {
			continue
		}
		length := 8 + len(data)
		record := make([]byte, 4+length)
		binary.BigEndian.PutUint32(record[0:4], uint32(length))
		binary.BigEndian.PutUint32(record[4:8], crc32.ChecksumIEEE(data))
		copy(record[8:], data)
		writer.Write(record)
		sw.size += int64(len(record))
	}

	writer.Flush()
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync compacted dlq: %w", err)
	}
	sw.activeFile = file
	sw.writer = writer
	return nil
}

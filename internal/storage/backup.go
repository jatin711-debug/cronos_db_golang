package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
)

// BackupCheckpoint tracks the last successfully backed-up segment starting offset
// so subsequent BackupWAL calls can skip already-copied closed segments.
type BackupCheckpoint struct {
	// LastOffset is the highest segment starting offset that has been backed up.
	// Segments with starting offset <= LastOffset are skipped on the next run.
	LastOffset int64 `json:"last_offset"`
	// Timestamp is when this checkpoint was written (UTC).
	Timestamp time.Time `json:"timestamp"`
}

// BackupWAL creates an incremental backup of WAL segments and their sparse
// indexes. walDir is the partition data directory (containing "segments" and
// "index" subdirectories). The active segment is skipped to avoid backing up a
// partially-written file. Segments whose starting offset is <= the previous
// checkpoint are also skipped.
func BackupWAL(walDir string, destDir string) error {
	segmentsDir := filepath.Join(walDir, "segments")
	indexDir := filepath.Join(walDir, "index")
	destSegmentsDir := filepath.Join(destDir, "segments")
	destIndexDir := filepath.Join(destDir, "index")

	if err := os.MkdirAll(destSegmentsDir, 0755); err != nil {
		return fmt.Errorf("create backup segments dir: %w", err)
	}
	if err := os.MkdirAll(destIndexDir, 0755); err != nil {
		return fmt.Errorf("create backup index dir: %w", err)
	}

	// Load previous checkpoint to determine incremental boundary
	ckpt, err := loadBackupCheckpoint(destDir)
	if err != nil {
		return fmt.Errorf("load backup checkpoint: %w", err)
	}

	segmentEntries, err := os.ReadDir(segmentsDir)
	if err != nil {
		return fmt.Errorf("read segments dir: %w", err)
	}

	// Identify active segment (highest starting offset among .log files)
	var activeSegment string
	var maxOffset int64 = -1
	for _, entry := range segmentEntries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}
		offset, err := parseSegmentOffset(entry.Name())
		if err != nil {
			continue
		}
		if offset > maxOffset {
			maxOffset = offset
			activeSegment = entry.Name()
		}
	}

	var maxBackedUpOffset int64 = ckpt.LastOffset
	backedUpSegments := 0
	copiedOffsets := make(map[int64]bool)

	for _, entry := range segmentEntries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}
		name := entry.Name()
		if name == activeSegment {
			continue
		}

		offset, err := parseSegmentOffset(name)
		if err == nil && offset <= ckpt.LastOffset {
			continue // Already backed up
		}
		if offset > maxBackedUpOffset {
			maxBackedUpOffset = offset
		}

		src := filepath.Join(segmentsDir, name)
		dst := filepath.Join(destSegmentsDir, name)
		if err := copyFile(src, dst); err != nil {
			return fmt.Errorf("copy segment %s: %w", name, err)
		}
		backedUpSegments++
		copiedOffsets[offset] = true
	}

	// Copy the corresponding index files for every segment we backed up.
	indexEntries, err := os.ReadDir(indexDir)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read index dir: %w", err)
	}
	backedUpIndexes := 0
	for _, entry := range indexEntries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".index") {
			continue
		}
		offset, err := parseSegmentOffset(strings.TrimSuffix(entry.Name(), ".index"))
		if err != nil {
			continue
		}
		if !copiedOffsets[offset] {
			continue
		}

		src := filepath.Join(indexDir, entry.Name())
		dst := filepath.Join(destIndexDir, entry.Name())
		if err := copyFile(src, dst); err != nil {
			return fmt.Errorf("copy index %s: %w", entry.Name(), err)
		}
		backedUpIndexes++
	}

	// The active segment is excluded from this incremental backup, but its
	// header may be the only durable boundary of the last closed segment.
	// Preserve that header as an empty successor; a later incremental backup
	// replaces it with the full segment after rotation. No active records are
	// included, so the original backup contract is unchanged.
	if activeSegment != "" && maxBackedUpOffset >= 0 {
		file, err := os.Open(filepath.Join(segmentsDir, activeSegment))
		if err != nil {
			return fmt.Errorf("open active segment header: %w", err)
		}
		header := make([]byte, 64)
		_, readErr := io.ReadFull(file, header)
		closeErr := file.Close()
		if readErr != nil {
			return fmt.Errorf("read active segment header: %w", readErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close active segment header: %w", closeErr)
		}
		if string(header[:6]) != "CRNOS1" || header[7] != SegmentHeaderVersion || binary.BigEndian.Uint32(header[60:64]) != crc32.ChecksumIEEE(header[:60]) {
			return fmt.Errorf("invalid active segment header %s", activeSegment)
		}
		if string(header[32:36]) == boundaryMagic && int64(binary.BigEndian.Uint64(header[36:44])) == maxBackedUpOffset {
			if err := utils.AtomicWriteFile(filepath.Join(destSegmentsDir, activeSegment), header, 0644); err != nil {
				return fmt.Errorf("copy active segment boundary: %w", err)
			}
		}
	}

	// Write backup manifest atomically with fsync.
	manifest := fmt.Sprintf("backup_time=%s\nsegments=%d\nindexes=%d\nlast_offset=%d\n",
		time.Now().UTC().Format(time.RFC3339), backedUpSegments, backedUpIndexes, maxBackedUpOffset)
	manifestPath := filepath.Join(destDir, "backup.manifest")
	if err := utils.AtomicWriteFile(manifestPath, []byte(manifest), 0644); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	// Atomically update checkpoint with fsync.
	newCkpt := BackupCheckpoint{LastOffset: maxBackedUpOffset, Timestamp: time.Now().UTC()}
	if err := saveBackupCheckpoint(destDir, newCkpt); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	return nil
}

// parseSegmentOffset extracts the starting offset from a segment filename
// e.g. "00000000000000000012.log" or "00000000000000000012.index" -> 12
func parseSegmentOffset(name string) (int64, error) {
	base := strings.TrimSuffix(name, ".log")
	base = strings.TrimSuffix(base, ".index")
	return strconv.ParseInt(base, 10, 64)
}

func loadBackupCheckpoint(destDir string) (BackupCheckpoint, error) {
	path := filepath.Join(destDir, "backup_checkpoint.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return BackupCheckpoint{LastOffset: -1}, nil
		}
		return BackupCheckpoint{}, err
	}
	var ckpt BackupCheckpoint
	if err := json.Unmarshal(data, &ckpt); err != nil {
		return BackupCheckpoint{}, err
	}
	return ckpt, nil
}

func saveBackupCheckpoint(destDir string, ckpt BackupCheckpoint) error {
	path := filepath.Join(destDir, "backup_checkpoint.json")
	data, err := json.Marshal(ckpt)
	if err != nil {
		return err
	}
	return utils.AtomicWriteFile(path, data, 0644)
}

// RestoreWAL restores WAL segments and their indexes from a backup directory.
// The backup directory is expected to contain "segments" and "index" subdirectories.
func RestoreWAL(backupDir string, walDir string) error {
	backupSegmentsDir := filepath.Join(backupDir, "segments")
	backupIndexDir := filepath.Join(backupDir, "index")
	segmentsDir := filepath.Join(walDir, "segments")
	indexDir := filepath.Join(walDir, "index")
	if err := validateBackupSegmentVersions(backupSegmentsDir); err != nil {
		return err
	}

	if err := os.MkdirAll(segmentsDir, 0755); err != nil {
		return fmt.Errorf("create wal segments dir: %w", err)
	}
	if err := os.MkdirAll(indexDir, 0755); err != nil {
		return fmt.Errorf("create wal index dir: %w", err)
	}

	entries, err := os.ReadDir(backupSegmentsDir)
	if err != nil {
		return fmt.Errorf("read backup segments dir: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		src := filepath.Join(backupSegmentsDir, entry.Name())
		dst := filepath.Join(segmentsDir, entry.Name())
		if err := copyFile(src, dst); err != nil {
			return fmt.Errorf("copy segment %s: %w", entry.Name(), err)
		}
	}

	indexEntries, err := os.ReadDir(backupIndexDir)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read backup index dir: %w", err)
	}
	for _, entry := range indexEntries {
		if entry.IsDir() {
			continue
		}
		src := filepath.Join(backupIndexDir, entry.Name())
		dst := filepath.Join(indexDir, entry.Name())
		if err := copyFile(src, dst); err != nil {
			return fmt.Errorf("copy index %s: %w", entry.Name(), err)
		}
	}

	return nil
}

// validateBackupSegmentVersions checks headers without opening a WAL or
// modifying its index. Restore must reject unsupported data before publishing it.
func validateBackupSegmentVersions(segmentsDir string) error {
	entries, err := os.ReadDir(segmentsDir)
	if err != nil {
		return fmt.Errorf("read backup segments: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".log" {
			continue
		}
		file, err := os.Open(filepath.Join(segmentsDir, entry.Name()))
		if err != nil {
			return err
		}
		var header [64]byte
		_, readErr := io.ReadFull(file, header[:])
		closeErr := file.Close()
		if readErr != nil {
			return fmt.Errorf("read segment %s header: %w", entry.Name(), readErr)
		}
		if closeErr != nil {
			return closeErr
		}
		if string(header[:6]) != "CRNOS1" || binary.BigEndian.Uint32(header[60:]) != crc32.ChecksumIEEE(header[:60]) {
			return fmt.Errorf("invalid backup segment header: %s", entry.Name())
		}
		if header[7] != SegmentHeaderVersion {
			return fmt.Errorf("unsupported segment version: %d in %s (this build requires version %d; older WAL data is not migrated)", header[7], entry.Name(), SegmentHeaderVersion)
		}
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	out, err := os.Create(dst)
	if err != nil {
		return err
	}

	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return nil
}

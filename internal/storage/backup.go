package storage

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// BackupCheckpoint tracks the last successfully backed-up offset.
type BackupCheckpoint struct {
	LastOffset int64     `json:"last_offset"`
	Timestamp  time.Time `json:"timestamp"`
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

	// Write backup manifest
	manifest := fmt.Sprintf("backup_time=%s\nsegments=%d\nindexes=%d\nlast_offset=%d\n",
		time.Now().UTC().Format(time.RFC3339), backedUpSegments, backedUpIndexes, maxBackedUpOffset)
	manifestPath := filepath.Join(destDir, "backup.manifest")
	if err := os.WriteFile(manifestPath, []byte(manifest), 0644); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	// Atomically update checkpoint
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
			return BackupCheckpoint{}, nil
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
	tmpPath := path + ".tmp"
	data, err := json.Marshal(ckpt)
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

// RestoreWAL restores WAL segments and their indexes from a backup directory.
// The backup directory is expected to contain "segments" and "index" subdirectories.
func RestoreWAL(backupDir string, walDir string) error {
	backupSegmentsDir := filepath.Join(backupDir, "segments")
	backupIndexDir := filepath.Join(backupDir, "index")
	segmentsDir := filepath.Join(walDir, "segments")
	indexDir := filepath.Join(walDir, "index")

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
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Close()
}

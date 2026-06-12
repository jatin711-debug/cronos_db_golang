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

// BackupWAL creates an incremental backup of WAL segments whose last offset
// is greater than the previous checkpoint. It skips the active segment.
func BackupWAL(walDir string, destDir string) error {
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("create backup dir: %w", err)
	}

	// Load previous checkpoint to determine incremental boundary
	ckpt, err := loadBackupCheckpoint(destDir)
	if err != nil {
		return fmt.Errorf("load backup checkpoint: %w", err)
	}

	entries, err := os.ReadDir(walDir)
	if err != nil {
		return fmt.Errorf("read wal dir: %w", err)
	}

	// Identify active segment (highest offset among .log files)
	var activeSegment string
	var maxOffset int64 = -1
	for _, entry := range entries {
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
	backedUp := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		// Skip active segment
		if name == activeSegment {
			continue
		}

		// For segment files, only copy if newer than last checkpoint
		if strings.HasSuffix(name, ".log") {
			offset, err := parseSegmentOffset(name)
			if err == nil && offset <= ckpt.LastOffset {
				continue // Already backed up
			}
			if offset > maxBackedUpOffset {
				maxBackedUpOffset = offset
			}
		}

		src := filepath.Join(walDir, name)
		dst := filepath.Join(destDir, name)

		if err := copyFile(src, dst); err != nil {
			return fmt.Errorf("copy %s: %w", name, err)
		}
		backedUp++
	}

	// Write backup manifest
	manifest := fmt.Sprintf("backup_time=%s\nsegments=%d\nlast_offset=%d\n", time.Now().UTC().Format(time.RFC3339), backedUp, maxBackedUpOffset)
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
// e.g. "00000000000000000012.log" -> 12
func parseSegmentOffset(name string) (int64, error) {
	base := strings.TrimSuffix(name, ".log")
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

// RestoreWAL restores WAL segments from a backup directory.
func RestoreWAL(backupDir string, walDir string) error {
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return fmt.Errorf("create wal dir: %w", err)
	}

	entries, err := os.ReadDir(backupDir)
	if err != nil {
		return fmt.Errorf("read backup dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == "backup.manifest" || name == "backup_checkpoint.json" {
			continue
		}

		src := filepath.Join(backupDir, name)
		dst := filepath.Join(walDir, name)

		if err := copyFile(src, dst); err != nil {
			return fmt.Errorf("copy %s: %w", name, err)
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

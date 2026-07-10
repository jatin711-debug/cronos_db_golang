package utils

import (
	"fmt"
	"os"
	"path/filepath"
)

// AtomicWriteFile writes data to path atomically using a temporary file,
// fsync, rename, and parent-directory fsync. This protects against torn
// writes on crash or power loss.
//
// The temp file is created in the same directory as the target so that the
// rename is always within the same filesystem.
func AtomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if dir == "" {
		dir = "."
	}

	tmpPath := path + ".tmp"
	tmp, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return fmt.Errorf("atomic write: create temp file: %w", err)
	}

	writeErr := func() error {
		if _, err := tmp.Write(data); err != nil {
			return fmt.Errorf("atomic write: write temp file: %w", err)
		}
		if err := tmp.Sync(); err != nil {
			return fmt.Errorf("atomic write: fsync temp file: %w", err)
		}
		if err := tmp.Close(); err != nil {
			return fmt.Errorf("atomic write: close temp file: %w", err)
		}
		if err := os.Rename(tmpPath, path); err != nil {
			return fmt.Errorf("atomic write: rename temp file: %w", err)
		}
		// Fsync the parent directory so the rename is durable on POSIX.
		if dirFile, err := os.Open(dir); err == nil {
			_ = dirFile.Sync()
			_ = dirFile.Close()
		}
		return nil
	}()

	if writeErr != nil {
		// Best-effort cleanup of the temp file. Ignore errors because the file
		// may have been renamed already or be locked on Windows.
		_ = os.Remove(tmpPath)
		_ = tmp.Close()
		return writeErr
	}

	return nil
}

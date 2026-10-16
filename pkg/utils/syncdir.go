package utils

import (
	"os"
	"runtime"
)

// SyncDirectory makes file creation/rename durable on POSIX. Windows does not
// support fsync on directory handles through os.File; file writes are synced.
func SyncDirectory(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

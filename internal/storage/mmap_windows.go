//go:build windows

package storage

import (
	"os"
)

// mmapFile on Windows returns nil - we fall back to regular file I/O
// Windows does support memory mapping via CreateFileMapping/MapViewOfFile
// but for simplicity, we skip it and use file-based reading instead
func mmapFile(file *os.File, size int64) ([]byte, error) {
	// Return nil to signal mmap is not available
	// The segment code will fall back to file-based reading
	return nil, nil
}

// munmapFile on Windows is a no-op since we don't mmap
func munmapFile(data []byte) error {
	return nil
}

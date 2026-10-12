//go:build !windows && !linux

package storage

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// preallocateFile gives a file its full length on the Unix systems that have
// no fallocate, macOS among them. The length is what a segment maps; the
// blocks are allocated as they are written. The Linux build reserves them.
func preallocateFile(file *os.File, size int64) error {
	if size <= 0 {
		return nil
	}
	if err := unix.Ftruncate(int(file.Fd()), size); err != nil {
		return fmt.Errorf("ftruncate: %w", err)
	}
	return nil
}

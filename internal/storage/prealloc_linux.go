//go:build linux

package storage

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// preallocateFile pre-allocates disk space for a file on Linux.
//
// fallocate reserves the blocks up front so that later writes don't stall on
// block allocation or fragment the file. The mode 0 keeps the space allocated
// but does not zero it (fallocate is fast and sparse-friendly on modern
// filesystems). Errors that indicate the filesystem does not support
// preallocation (e.g. EINVAL, ENOSYS, EOPNOTSUPP) are tolerated so that the
// segment can still be used on exotic filesystems; in that case the file grows
// lazily like before.
func preallocateFile(file *os.File, size int64) error {
	if size <= 0 {
		return nil
	}
	err := unix.Fallocate(int(file.Fd()), 0, 0, size)
	if err == nil {
		return nil
	}
	switch err {
	case syscall.EINVAL, syscall.ENOSYS, syscall.EOPNOTSUPP, syscall.EPERM:
		// Not supported by this filesystem/kernel (e.g. some network FS, or
		// certain mounted volumes). Fall back to ftruncate so the file at
		// least has the right length; the writes will allocate blocks lazily.
		if te := unix.Ftruncate(int(file.Fd()), size); te != nil {
			return fmt.Errorf("fallocate (unsupported) and ftruncate also failed: %w", te)
		}
		return nil
	default:
		return fmt.Errorf("fallocate: %w", err)
	}
}

// releaseBeyondEnd frees the blocks a filesystem holds past the end of a
// file. When a write extends a file, XFS reserves about as much again past
// its end, and it keeps that reservation while the file is open: its
// background trimming skips files that were ever fallocate'd, as segments
// are, and closing one does not free it either. A segment's last batch runs a
// little past its preallocated length, and a sealed segment stays open for
// reads, so every segment kept about twice its size in hidden blocks for as
// long as the log kept it: 64 MiB for each 32 MiB segment. Truncating to the
// current length frees them and changes nothing a reader can see.
func releaseBeyondEnd(file *os.File) error {
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	return file.Truncate(stat.Size())
}

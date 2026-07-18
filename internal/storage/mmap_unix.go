//go:build !windows

package storage

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// mmapFile memory-maps a file for reading and writing on Unix.
//
// The mapping is MAP_SHARED with PROT_READ|PROT_WRITE so that writes performed
// through the returned slice (e.g. writeRecordMmap) are reflected in the
// backing file. A read-only mapping would SIGSEGV on the first write through
// the slice, which is why this is not PROT_READ only.
func mmapFile(file *os.File, size int64) ([]byte, error) {
	if size <= 0 {
		return nil, nil
	}
	data, err := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap: %w", err)
	}
	return data, nil
}

// munmapFile unmaps previously mapped memory.
func munmapFile(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return syscall.Munmap(data)
}

// preallocateFile pre-allocates disk space for a file on Unix.
//
// fallocate (Linux) / posix_fallocate (Darwin/BSD) reserve the blocks up front
// so that later writes don't stall on block allocation or fragment the file.
// On Linux the mode 0 keeps the space allocated but does not zero it (fallocate
// is fast and sparse-friendly on modern filesystems). Errors that indicate the
// filesystem does not support preallocation (e.g. EINVAL, ENOSYS, EOPNOTSUPP)
// are tolerated so that the segment can still be used on exotic filesystems;
// in that case the file grows lazily like before.
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

// syncMmap flushes modified pages of the mmap region to disk. MS_SYNC requests
// a synchronous flush (the call blocks until the writes are durable to the
// file); MS_INVALIDATE discards any cached copies that are identical to the
// synced pages so subsequent reads pick up the on-disk state.
func syncMmap(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if err := unix.Msync(data, unix.MS_SYNC|unix.MS_INVALIDATE); err != nil {
		return fmt.Errorf("msync: %w", err)
	}
	return nil
}

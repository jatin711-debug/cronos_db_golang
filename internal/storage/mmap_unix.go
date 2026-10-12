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

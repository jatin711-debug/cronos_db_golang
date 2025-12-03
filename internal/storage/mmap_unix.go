//go:build !windows

package storage

import (
	"os"
	"syscall"
)

// mmapFile memory maps a file for reading
func mmapFile(file *os.File, size int64) ([]byte, error) {
	if size <= 0 {
		return nil, nil
	}
	data, err := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// munmapFile unmaps previously mapped memory
func munmapFile(data []byte) error {
	if data == nil {
		return nil
	}
	return syscall.Munmap(data)
}

//go:build windows

package storage

import (
	"fmt"
	"os"
	"reflect"
	"unsafe"

	"golang.org/x/sys/windows"
)

// mmapFile memory-maps a file for reading and writing on Windows via
// CreateFileMapping and MapViewOfFile.
func mmapFile(file *os.File, size int64) ([]byte, error) {
	if size <= 0 {
		return nil, fmt.Errorf("invalid mmap size: %d", size)
	}
	length := int(size)
	if int64(length) != size {
		return nil, fmt.Errorf("mmap size exceeds platform int range: %d", size)
	}

	// Get file handle
	handle := windows.Handle(file.Fd())

	// Create file mapping
	// PAGE_READWRITE allows us to write through the mmap
	mapping, err := windows.CreateFileMapping(handle, nil, windows.PAGE_READWRITE, 0, 0, nil)
	if err != nil {
		return nil, fmt.Errorf("CreateFileMapping: %w", err)
	}
	defer windows.CloseHandle(mapping)

	// Map view of file
	// FILE_MAP_WRITE allows writes, FILE_MAP_READ allows reads
	addr, err := windows.MapViewOfFile(mapping, windows.FILE_MAP_WRITE|windows.FILE_MAP_READ, 0, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("MapViewOfFile: %w", err)
	}

	// Convert to byte slice
	// This is safe because the memory is backed by the file
	var data []byte
	header := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	header.Data = addr
	header.Len = length
	header.Cap = length

	return data, nil
}

// munmapFile unmaps a previously mapped file view on Windows.
func munmapFile(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	// Get the address from the slice header
	addr := uintptr(unsafe.Pointer(&data[0]))

	if err := windows.UnmapViewOfFile(addr); err != nil {
		return fmt.Errorf("UnmapViewOfFile: %w", err)
	}

	return nil
}

// preallocateFile pre-allocates disk space for a file on Windows.
// This prevents fragmentation and ensures the file can grow without allocation overhead.
func preallocateFile(file *os.File, size int64) error {
	if size <= 0 {
		return nil
	}

	// Use SetFileValidData to pre-allocate without zeroing (requires admin on some Windows versions)
	// Fallback to SetEndOfFile which zeros the space
	handle := windows.Handle(file.Fd())

	// Move file pointer to desired size using high/low 32-bit parts
	sizeHigh := int32(size >> 32)
	sizeLow := int32(size & 0xFFFFFFFF)

	newPos, err := windows.SetFilePointer(handle, sizeLow, &sizeHigh, windows.FILE_BEGIN)
	if err != nil {
		return fmt.Errorf("SetFilePointer: %w", err)
	}
	_ = newPos

	// Set end of file (this allocates space)
	if err := windows.SetEndOfFile(handle); err != nil {
		return fmt.Errorf("SetEndOfFile: %w", err)
	}

	// Move pointer back to beginning
	zeroHigh := int32(0)
	_, err = windows.SetFilePointer(handle, 0, &zeroHigh, windows.FILE_BEGIN)
	if err != nil {
		return fmt.Errorf("SetFilePointer reset: %w", err)
	}

	return nil
}

// syncMmap flushes mmap data to disk on Windows.
func syncMmap(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	addr := uintptr(unsafe.Pointer(&data[0]))
	size := uintptr(len(data))

	if err := windows.FlushViewOfFile(addr, size); err != nil {
		return fmt.Errorf("FlushViewOfFile: %w", err)
	}

	return nil
}

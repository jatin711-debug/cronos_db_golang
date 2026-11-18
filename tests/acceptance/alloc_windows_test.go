//go:build acceptance && windows

package acceptance

import "io/fs"

// allocatedBytes is a file's length on Windows, which reports no blocks.
func allocatedBytes(info fs.FileInfo) int64 { return info.Size() }

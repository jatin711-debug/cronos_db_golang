package storage

import (
	"encoding/binary"
	"io"
)

// recordScanBlock is the read-ahead size used when scanning a segment.
const recordScanBlock = 256 << 10

// recordScanner reads consecutive length-prefixed records from a segment file.
// It reads the file in blocks: one ReadAt covers hundreds of records, where
// reading the length and the body separately cost two system calls per record
// and dominated every range read, replay, and catch-up.
type recordScanner struct {
	file io.ReaderAt
	buf  []byte // buf[head:tail] holds bytes read from the file but not yet consumed
	head int
	tail int
	next int64 // file offset of the first byte not yet read into buf
	eof  bool
}

func newRecordScanner(file io.ReaderAt, pos int64) *recordScanner {
	return &recordScanner{file: file, buf: make([]byte, recordScanBlock), next: pos}
}

// fill buffers at least n unconsumed bytes. It reports false when the file
// ends first.
func (r *recordScanner) fill(n int) (bool, error) {
	for r.tail-r.head < n {
		if r.eof {
			return false, nil
		}
		if r.head > 0 {
			r.tail = copy(r.buf, r.buf[r.head:r.tail])
			r.head = 0
		}
		if n > len(r.buf) {
			grown := make([]byte, n+recordScanBlock)
			copy(grown, r.buf[:r.tail])
			r.buf = grown
		}
		read, err := r.file.ReadAt(r.buf[r.tail:], r.next)
		r.tail += read
		r.next += int64(read)
		if err == io.EOF || (err == nil && read == 0) {
			r.eof = true
		} else if err != nil {
			return false, err
		}
	}
	return true, nil
}

// record returns the next record without its 4-byte length prefix, and the
// file position where that body starts. The slice is only valid until the next
// call. ok is false at the end of the data: end of file, a truncated record,
// or the zero/invalid length prefix of a preallocated or corrupt tail.
func (r *recordScanner) record() (body []byte, bodyPos int64, ok bool, err error) {
	if have, err := r.fill(4); err != nil || !have {
		return nil, 0, false, err
	}
	length := int64(binary.BigEndian.Uint32(r.buf[r.head:]))
	if length <= 4 || length > 10*1024*1024 {
		return nil, 0, false, nil
	}
	if have, err := r.fill(int(length)); err != nil || !have {
		return nil, 0, false, err
	}
	bodyPos = r.next - int64(r.tail-r.head) + 4
	body = r.buf[r.head+4 : r.head+int(length)]
	r.head += int(length)
	return body, bodyPos, true, nil
}

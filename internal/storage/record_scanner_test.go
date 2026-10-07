package storage

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// frame builds one on-disk record: a 4-byte length that counts itself,
// followed by the body.
func frame(body []byte) []byte {
	out := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(out, uint32(4+len(body)))
	copy(out[4:], body)
	return out
}

func body(size int, fill byte) []byte {
	return bytes.Repeat([]byte{fill}, size)
}

// The scanner must return every record exactly as two reads per record would,
// including records that straddle a block boundary and records larger than a
// block, and report where each body starts in the file.
func TestRecordScanner_ReadsAcrossBlockBoundaries(t *testing.T) {
	bodies := [][]byte{
		body(10, 'a'),
		body(recordScanBlock-40, 'b'), // ends just short of the first block
		body(100, 'c'),                // straddles the boundary
		body(3*recordScanBlock, 'd'),  // larger than the read-ahead block
		body(1, 'e'),
	}
	const start = 64
	file := make([]byte, start)
	positions := make([]int64, len(bodies))
	for i, b := range bodies {
		positions[i] = int64(len(file)) + 4
		file = append(file, frame(b)...)
	}
	file = append(file, make([]byte, 4096)...) // preallocated zero tail

	scanner := newRecordScanner(bytes.NewReader(file), start)
	for i, want := range bodies {
		got, pos, ok, err := scanner.record()
		if err != nil || !ok {
			t.Fatalf("record %d: ok=%v err=%v", i, ok, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("record %d: body differs (len %d, want %d)", i, len(got), len(want))
		}
		if pos != positions[i] {
			t.Fatalf("record %d: body position %d, want %d", i, pos, positions[i])
		}
	}
	if _, _, ok, err := scanner.record(); ok || err != nil {
		t.Fatalf("zero tail must end the scan cleanly: ok=%v err=%v", ok, err)
	}
}

// A record cut short by the end of the file, and a file that ends exactly
// after a record, both end the scan without an error.
func TestRecordScanner_StopsAtTruncatedTail(t *testing.T) {
	complete := frame(body(50, 'x'))
	cut := frame(body(500, 'y'))[:200]

	for name, file := range map[string][]byte{
		"truncated record": append(append([]byte{}, complete...), cut...),
		"partial length":   append(append([]byte{}, complete...), 0, 0),
		"clean end":        complete,
	} {
		scanner := newRecordScanner(bytes.NewReader(file), 0)
		got, _, ok, err := scanner.record()
		if err != nil || !ok || len(got) != 50 {
			t.Fatalf("%s: first record ok=%v err=%v len=%d", name, ok, err, len(got))
		}
		if _, _, ok, err := scanner.record(); ok || err != nil {
			t.Fatalf("%s: scan should end cleanly, got ok=%v err=%v", name, ok, err)
		}
	}
}

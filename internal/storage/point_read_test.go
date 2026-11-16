package storage

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// Every entry of a log reads back by its offset, in a segment being written and
// in sealed ones, encrypted or not, with records of mixed sizes, an index
// entry only every so many records, and across reopening. A point read skips
// to its record by counting records from the nearest index entry instead of
// reading each one on the way; this checks it finds the right one.
func TestWAL_PointReadsFindEveryEntry(t *testing.T) {
	key := bytes.Repeat([]byte{9}, 32)
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("encrypted=%v", encrypted), func(t *testing.T) {
			var cipher *SegmentCipher
			if encrypted {
				var err error
				if cipher, err = NewSegmentCipher(key, 0); err != nil {
					t.Fatal(err)
				}
			}
			dir := t.TempDir()
			open := func() *WAL {
				w, err := NewWAL(dir, 0, &WALConfig{SegmentSizeBytes: 256 << 10, IndexInterval: 100, FsyncMode: "batch"}, cipher)
				if err != nil {
					t.Fatal(err)
				}
				return w
			}
			w := open()
			const total = 2500
			for start := 0; start < total; start += 50 {
				batch := make([]*types.Event, 0, 50)
				for i := start; i < start+50; i++ {
					payload := []byte("small")
					if i%37 == 5 {
						payload = bytes.Repeat([]byte{byte(i)}, 9000)
					}
					batch = append(batch, &types.Event{MessageId: fmt.Sprintf("m-%d", i), Topic: "orders", Payload: payload, ScheduleTs: int64(i)})
				}
				if err := w.AppendBatch(batch); err != nil {
					t.Fatal(err)
				}
			}
			check := func(w *WAL) {
				t.Helper()
				for offset := int64(0); offset < total; offset++ {
					event, err := w.ReadEvent(offset)
					if err != nil {
						t.Fatalf("read offset %d: %v", offset, err)
					}
					if want := fmt.Sprintf("m-%d", offset); event.Offset != offset || event.MessageId != want {
						t.Fatalf("read offset %d: got offset %d, id %s; want %s", offset, event.Offset, event.MessageId, want)
					}
				}
				if _, err := w.ReadEvent(total); err == nil {
					t.Fatal("read an offset beyond the end of the log")
				}
			}
			check(w)
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			reopened := open()
			defer reopened.Close()
			check(reopened)
		})
	}
}

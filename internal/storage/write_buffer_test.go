package storage

import (
	"fmt"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// writeBuffers counts the segments of the log that hold a write buffer.
func writeBuffers(w *WAL) (held, segments int) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	for _, segment := range w.segments {
		segment.mu.RLock()
		if segment.writer != nil {
			held++
		}
		segment.mu.RUnlock()
	}
	return held, len(w.segments)
}

// appendSome appends count events of 100 bytes, which is several segments of
// the 1 KiB the tests below give a segment.
func appendSome(t *testing.T, w *WAL, from, count int) {
	t.Helper()
	for i := from; i < from+count; i++ {
		event := &types.Event{MessageId: fmt.Sprintf("msg-%04d", i), Topic: "t", Payload: make([]byte, 100)}
		if err := w.AppendEvent(event); err != nil {
			t.Fatalf("AppendEvent failed: %v", err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
}

// TestSegments_HoldNoWriteBufferTheyDoNotWriteThrough checks what a log holds
// in write buffers: none where the segments are written through their mapping,
// and one, for the segment being appended to, where they are encrypted. A log
// that has just been opened holds none either way. Each buffer is 4 MiB, and
// every segment used to get one when it was created or opened, so a node held
// 4 MiB for each segment of its logs from the moment it started.
func TestSegments_HoldNoWriteBufferTheyDoNotWriteThrough(t *testing.T) {
	config := &WALConfig{SegmentSizeBytes: 1024, IndexInterval: 1, FsyncMode: "batch", FlushIntervalMS: 10}
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("encrypted=%v", encrypted), func(t *testing.T) {
			dir := t.TempDir()
			open := func() *WAL {
				t.Helper()
				var cipher *SegmentCipher
				if encrypted {
					var err error
					if cipher, err = NewSegmentCipher(key, 0); err != nil {
						t.Fatalf("NewSegmentCipher failed: %v", err)
					}
				}
				w, err := NewWAL(dir, 0, config, cipher)
				if err != nil {
					t.Fatalf("NewWAL failed: %v", err)
				}
				return w
			}
			// An encrypted segment is written through a buffer, so the one being
			// appended to holds one. The others have been closed for appends.
			whileWriting := 0
			if encrypted {
				whileWriting = 1
			}

			w := open()
			appendSome(t, w, 0, 60)
			held, segments := writeBuffers(w)
			if segments < 4 {
				t.Fatalf("the log has %d segments; the test needs several", segments)
			}
			if held > whileWriting {
				t.Fatalf("%d of %d segments hold a write buffer after appends; want at most %d", held, segments, whileWriting)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close failed: %v", err)
			}

			w = open()
			defer w.Close()
			held, segments = writeBuffers(w)
			if segments < 4 {
				t.Fatalf("the reopened log has %d segments; the test needs several", segments)
			}
			if held != 0 {
				t.Fatalf("%d of %d segments hold a write buffer after the log was opened; want none", held, segments)
			}

			// The log still takes appends, and gives back what it was given.
			appendSome(t, w, 60, 20)
			if held, segments = writeBuffers(w); held > whileWriting {
				t.Fatalf("%d of %d segments hold a write buffer after more appends; want at most %d", held, segments, whileWriting)
			}
			events, err := w.ReadEvents(0, w.GetLastOffset())
			if err != nil {
				t.Fatalf("ReadEvents failed: %v", err)
			}
			if len(events) != 80 {
				t.Fatalf("read %d events; want 80", len(events))
			}
			for i, event := range events {
				if want := fmt.Sprintf("msg-%04d", i); event.GetMessageId() != want {
					t.Fatalf("event %d is %s; want %s", i, event.GetMessageId(), want)
				}
			}
		})
	}
}

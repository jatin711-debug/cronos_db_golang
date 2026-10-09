package storage

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// A read of the log is bounded by the bytes it holds, not by a count of
// events: an event may be megabytes. A walk that continues after the last
// event of each read sees every entry once, over the ends of segments too,
// and an event larger than the bound comes back on its own.
func TestWAL_ReadEventsWithinBoundsABatchByItsSize(t *testing.T) {
	wal, err := NewWAL(t.TempDir(), 0, &WALConfig{SegmentSizeBytes: 16 * 1024, IndexInterval: 4, FsyncMode: "batch", FlushIntervalMS: 100}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	const small, large = 1024, 40 * 1024
	sizes := make([]int, 60)
	for i := range sizes {
		sizes[i] = small
	}
	sizes[25] = large // larger than any bound used below, and than a segment
	for i, size := range sizes {
		event := &types.Event{MessageId: fmt.Sprintf("m%d", i), Topic: "t", ScheduleTs: 1, Payload: bytes.Repeat([]byte{byte(i)}, size)}
		if err := wal.AppendEvent(event); err != nil {
			t.Fatal(err)
		}
	}
	if err := wal.Flush(); err != nil {
		t.Fatal(err)
	}
	last := wal.GetLastOffset()
	if last != int64(len(sizes)-1) {
		t.Fatalf("last offset %d, want %d", last, len(sizes)-1)
	}

	const bound = 4 * small
	next, reads := int64(0), 0
	for next <= last {
		events, err := wal.ReadEventsWithin(next, last, bound)
		if err != nil {
			t.Fatal(err)
		}
		if len(events) == 0 {
			t.Fatalf("a read from offset %d returned nothing with the log ending at %d", next, last)
		}
		reads++
		payload := 0
		for _, event := range events {
			if event.Offset != next {
				t.Fatalf("read returned offset %d where %d was due", event.Offset, next)
			}
			if len(event.Payload) != sizes[event.Offset] || event.Payload[0] != byte(event.Offset) {
				t.Fatalf("offset %d came back with another event's payload", event.Offset)
			}
			payload += len(event.Payload)
			next++
		}
		// It stops with the event that takes it to the bound, so a read holds
		// less than the bound and one event more.
		if without := payload - len(events[len(events)-1].Payload); without >= bound {
			t.Fatalf("a read of %d events held %d bytes before its last event, with a bound of %d", len(events), without, bound)
		}
		if events[0].Offset == 25 && len(events) != 1 {
			t.Fatalf("the event larger than the bound came back with %d others", len(events)-1)
		}
	}
	if reads < len(sizes)/5 {
		t.Fatalf("the log was read in %d reads; a bound of %d bytes allows about four events a read", reads, bound)
	}

	// The end of the range still counts, and no bound means the whole range.
	if events, err := wal.ReadEventsWithin(10, 12, 1<<30); err != nil || len(events) != 3 {
		t.Fatalf("read of offsets 10 to 12 under a large bound: %d events, %v", len(events), err)
	}
	if events, err := wal.ReadEventsWithin(0, last, 0); err != nil || len(events) != len(sizes) {
		t.Fatalf("read without a bound: %d events, %v; want %d", len(events), err, len(sizes))
	}
	if events, err := wal.ReadEvents(0, last); err != nil || len(events) != len(sizes) {
		t.Fatalf("ReadEvents: %d events, %v; want %d", len(events), err, len(sizes))
	}
}

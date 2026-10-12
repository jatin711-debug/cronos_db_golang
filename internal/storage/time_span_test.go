package storage

import (
	"fmt"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

const (
	spanHour = int64(3_600_000)
	spanBase = int64(1_700_000_000_000)
)

// appendEras appends five hours' worth of events, an hour after another, with
// the events of an hour in no order of time, and then one event scheduled in
// the first hour. Each hour fills several segments of the 1 KiB the tests give
// a segment, and the last segment holds the earliest time and nearly the latest.
func appendEras(t *testing.T, w *WAL) {
	t.Helper()
	id := 0
	add := func(ts int64) {
		event := &types.Event{MessageId: fmt.Sprintf("msg-%04d", id), Topic: "t", ScheduleTs: ts, Payload: make([]byte, 100)}
		id++
		if err := w.AppendEvent(event); err != nil {
			t.Fatalf("AppendEvent failed: %v", err)
		}
	}
	for era := int64(0); era < 5; era++ {
		for i := int64(0); i < 30; i++ {
			add(spanBase + era*spanHour + ((i*7)%30)*60_000)
		}
	}
	add(spanBase + 1)
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
}

// spanWindows are windows of time to ask about: each hour, two hours together,
// everything, nothing before, nothing after, and a gap between two events.
func spanWindows() [][2]int64 {
	windows := [][2]int64{
		{spanBase, spanBase + 5*spanHour},
		{spanBase + spanHour, spanBase + 3*spanHour - 1},
		{spanBase - 2*spanHour, spanBase - 1},
		{spanBase + 9*spanHour, spanBase + 10*spanHour},
		{spanBase + 2*spanHour + 30_000, spanBase + 2*spanHour + 40_000},
	}
	for era := int64(0); era < 5; era++ {
		windows = append(windows, [2]int64{spanBase + era*spanHour, spanBase + (era+1)*spanHour - 1})
	}
	return windows
}

// checkTimeSpans compares what the log says about each window with what its
// events say. A segment may hold the window exactly when the window meets the
// range from its least to its greatest time, and the spans the log gives cover
// those segments and no others.
func checkTimeSpans(t *testing.T, w *WAL, when string) {
	t.Helper()
	last := w.GetLastOffset()
	all, err := w.ReadEvents(0, last)
	if err != nil {
		t.Fatalf("%s: ReadEvents failed: %v", when, err)
	}
	byOffset := make(map[int64]*types.Event, len(all))
	for _, event := range all {
		byOffset[event.Offset] = event
	}

	w.mu.RLock()
	segments := append([]*Segment(nil), w.segments...)
	w.mu.RUnlock()

	skippedSome := false
	for _, window := range spanWindows() {
		start, end := window[0], window[1]
		// What the events say: the offsets in segments whose range of times
		// meets the window.
		want := make(map[int64]bool)
		for _, segment := range segments {
			var times timeRange
			for offset := segment.GetFirstOffset(); offset <= segment.GetLastOffset(); offset++ {
				if event, held := byOffset[offset]; held {
					times.note(event.GetScheduleTs())
				}
			}
			meets := times.has && times.min <= end && times.max >= start
			if got := segment.MayHoldTime(start, end); got != meets {
				t.Fatalf("%s: segment %s says %v for the window %d-%d; its events are scheduled from %d to %d",
					when, segment.GetFilename(), got, start, end, times.min, times.max)
			}
			if !meets {
				continue
			}
			for offset := segment.GetFirstOffset(); offset <= segment.GetLastOffset(); offset++ {
				want[offset] = true
			}
		}

		got := make(map[int64]bool)
		for from := int64(0); from <= last; {
			first, spanEnd, ok := w.TimeSpan(from, last, start, end)
			if !ok {
				break
			}
			if first < from || spanEnd < first || spanEnd > last {
				t.Fatalf("%s: TimeSpan(%d, %d) gave %d-%d", when, from, last, first, spanEnd)
			}
			for offset := first; offset <= spanEnd; offset++ {
				got[offset] = true
			}
			from = spanEnd + 1
		}
		if len(got) != len(want) {
			t.Fatalf("%s: window %d-%d: the spans cover %d offsets; the segments that can hold it have %d", when, start, end, len(got), len(want))
		}
		for offset := range want {
			if !got[offset] {
				t.Fatalf("%s: window %d-%d: offset %d is in a segment that can hold it and in no span", when, start, end, offset)
			}
		}
		// Whatever the spans leave out, no event in the window is among it.
		for _, event := range all {
			if ts := event.GetScheduleTs(); ts >= start && ts <= end && !got[event.Offset] {
				t.Fatalf("%s: window %d-%d: the event at offset %d is scheduled at %d and in no span", when, start, end, event.Offset, ts)
			}
		}
		if len(got) > 0 && len(got) < len(all) {
			skippedSome = true
		}
	}
	if !skippedSome {
		t.Fatalf("%s: no window was answered with less than the whole log", when)
	}
}

// TestWAL_TimeSpan_NamesTheSegmentsThatCanHoldAWindow checks the spans of
// offsets the log gives for a window of time against the events it holds: as
// it was written, after it is opened again, and after its end is cut off.
func TestWAL_TimeSpan_NamesTheSegmentsThatCanHoldAWindow(t *testing.T) {
	config := &WALConfig{SegmentSizeBytes: 1024, IndexInterval: 1, FsyncMode: "batch", FlushIntervalMS: 10}
	dir := t.TempDir()

	w, err := NewWAL(dir, 0, config, nil)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	appendEras(t, w)
	w.mu.RLock()
	segments := len(w.segments)
	w.mu.RUnlock()
	if segments < 10 {
		t.Fatalf("the log has %d segments; the test needs many", segments)
	}
	checkTimeSpans(t, w, "as written")
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	w, err = NewWAL(dir, 0, config, nil)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer w.Close()
	checkTimeSpans(t, w, "opened again")

	// Cut the end off inside a segment: what that segment holds changes, and
	// with it the times it can answer for.
	cut := w.GetLastOffset() - 3
	if _, err := w.TruncateToOffset(cut); err != nil {
		t.Fatalf("TruncateToOffset failed: %v", err)
	}
	if got := w.GetLastOffset(); got != cut-1 {
		t.Fatalf("the log ends at %d after it was cut at %d", got, cut)
	}
	checkTimeSpans(t, w, "cut")
}

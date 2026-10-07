package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// oneEventSegments returns a log in which every event fills a segment, holding
// count events at offsets 0 to count-1.
func oneEventSegments(t *testing.T, dir string, count int) *WAL {
	t.Helper()
	w, err := NewWAL(dir, 0, &WALConfig{SegmentSizeBytes: 1024, IndexInterval: 1, FsyncMode: "batch", FlushIntervalMS: 10}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		event := &types.Event{MessageId: fmt.Sprintf("event-%d", i), Topic: "orders", Payload: make([]byte, 2048), ScheduleTs: 1}
		if err := w.AppendEvent(event); err != nil {
			t.Fatal(err)
		}
	}
	return w
}

func offsetsOf(t *testing.T, w *WAL) []int64 {
	t.Helper()
	events, err := w.ReadEvents(0, w.GetLastOffset())
	if err != nil {
		t.Fatal(err)
	}
	offsets := make([]int64, len(events))
	for i, event := range events {
		offsets[i] = event.Offset
	}
	return offsets
}

// Pruning a replicated log removes its oldest segments only and stops at the
// first one that has to stay: the log must remain one unbroken range of
// offsets. Without that restriction every finished segment goes, wherever it
// is.
func TestPrune_PrefixOnlyStopsAtTheFirstSegmentThatStays(t *testing.T) {
	stays := func(offset int64) func(*types.Event) bool {
		return func(event *types.Event) bool { return event.Offset != offset }
	}

	w := oneEventSegments(t, t.TempDir(), 6)
	defer w.Close()
	deleted, err := w.Prune(context.Background(), PruneOptions{AllCompleted: true, PrefixOnly: true}, stays(2))
	if err != nil || deleted != 2 {
		t.Fatalf("removed %d segments (%v), want the two before the entry that stays", deleted, err)
	}
	if got, want := fmt.Sprint(offsetsOf(t, w)), "[2 3 4 5]"; got != want {
		t.Fatalf("log holds offsets %s, want %s", got, want)
	}
	if first := w.GetFirstOffset(); first != 2 {
		t.Fatalf("log starts at offset %d, want 2", first)
	}

	anywhere := oneEventSegments(t, t.TempDir(), 6)
	defer anywhere.Close()
	deleted, err = anywhere.Prune(context.Background(), PruneOptions{AllCompleted: true}, stays(2))
	if err != nil || deleted != 5 {
		t.Fatalf("without the restriction %d segments were removed (%v), want every finished one", deleted, err)
	}
	if got, want := fmt.Sprint(offsetsOf(t, anywhere)), "[2]"; got != want {
		t.Fatalf("log holds offsets %s, want %s", got, want)
	}
}

// A replica follows the start of its leader's log by dropping the segments
// that lie wholly below it.
func TestWAL_DropBelowRemovesWholeSegments(t *testing.T) {
	w := oneEventSegments(t, t.TempDir(), 5)
	defer w.Close()

	dropped, err := w.DropBelow(3)
	if err != nil || dropped != 3 {
		t.Fatalf("dropped %d segments (%v), want 3", dropped, err)
	}
	if got, want := fmt.Sprint(offsetsOf(t, w)), "[3 4]"; got != want {
		t.Fatalf("log holds offsets %s, want %s", got, want)
	}
	// Past the end of the log, everything closed goes and the log still ends
	// where it did.
	if _, err := w.DropBelow(100); err != nil {
		t.Fatal(err)
	}
	if next := w.GetNextOffset(); next != 5 {
		t.Fatalf("next offset is %d after dropping, want 5", next)
	}
	if err := w.AppendEvent(&types.Event{MessageId: "next", Topic: "orders", Payload: []byte("x"), ScheduleTs: 1}); err != nil {
		t.Fatal(err)
	}
	if last := w.GetLastOffset(); last != 5 {
		t.Fatalf("the next event got offset %d, want 5", last)
	}
}

// A replica whose log ends before its leader's begins restarts its log at the
// leader's start. The next entry gets that offset, also after a restart.
func TestWAL_ResetToRestartsTheLogAtAnOffset(t *testing.T) {
	dir := t.TempDir()
	w := oneEventSegments(t, dir, 3)

	if err := w.ResetTo(40); err != nil {
		t.Fatal(err)
	}
	if first, next, last := w.GetFirstOffset(), w.GetNextOffset(), w.GetLastOffset(); first != 40 || next != 40 || last != 39 {
		t.Fatalf("after the reset the log starts at %d, ends at %d and continues at %d; want 40, 39, 40", first, last, next)
	}
	if events, err := w.ReadEvents(0, 100); err != nil || len(events) != 0 {
		t.Fatalf("the emptied log still returns %d events (%v)", len(events), err)
	}
	replicated := []*types.Event{
		{MessageId: "a", Topic: "orders", Payload: []byte("a"), ScheduleTs: 1, Offset: 40, Term: 3},
		{MessageId: "b", Topic: "orders", Payload: []byte("b"), ScheduleTs: 1, Offset: 41, Term: 3},
	}
	if err := w.AppendReplicatedBatch(replicated); err != nil {
		t.Fatalf("append at the new start: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewWAL(dir, 0, &WALConfig{SegmentSizeBytes: 1024, IndexInterval: 1, FsyncMode: "batch", FlushIntervalMS: 10}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, want := fmt.Sprint(offsetsOf(t, reopened)), "[40 41]"; got != want {
		t.Fatalf("after reopening the log holds offsets %s, want %s", got, want)
	}
	if first, next := reopened.GetFirstOffset(), reopened.GetNextOffset(); first != 40 || next != 42 {
		t.Fatalf("after reopening the log starts at %d and continues at %d, want 40 and 42", first, next)
	}

	// A log reset while empty, and reopened before anything is appended.
	emptyDir := t.TempDir()
	empty := oneEventSegments(t, emptyDir, 0)
	if err := empty.ResetTo(7); err != nil {
		t.Fatal(err)
	}
	if err := empty.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := NewWAL(emptyDir, 0, &WALConfig{SegmentSizeBytes: 1024, IndexInterval: 1, FsyncMode: "batch", FlushIntervalMS: 10}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if first, next := again.GetFirstOffset(), again.GetNextOffset(); first != 7 || next != 7 {
		t.Fatalf("an empty log reset to 7 reopens starting at %d and continuing at %d", first, next)
	}
}

// Reading a range while its segments are pruned returns what is still in the
// log; it is not an error that entries left it.
func TestWAL_ReadSkipsSegmentsPrunedSinceItLooked(t *testing.T) {
	w := oneEventSegments(t, t.TempDir(), 4)
	defer w.Close()
	pruned := w.GetSegments()[1]
	if removed, err := w.unlinkSegment(pruned); err != nil || !removed {
		t.Fatalf("unlink: %v %v", removed, err)
	}
	if !pruned.deleted.Load() {
		t.Fatal("a removed segment is not marked as gone")
	}
	if got, want := fmt.Sprint(offsetsOf(t, w)), "[0 2 3]"; got != want {
		t.Fatalf("log holds offsets %s, want %s", got, want)
	}
}

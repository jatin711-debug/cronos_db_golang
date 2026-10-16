package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func openOutcomeWAL(t *testing.T, dir string, cipher *SegmentCipher) *WAL {
	t.Helper()
	w, err := NewWAL(dir, 0, &WALConfig{SegmentSizeBytes: 1 << 20, IndexInterval: 1, FsyncMode: "batch"}, cipher)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func outcomeEvent(id string) *types.Event {
	return &types.Event{MessageId: id, Topic: "orders", Payload: []byte("payload"), ScheduleTs: 1}
}

// sizedEvent returns an event whose record is exactly size bytes before
// encryption, made up with metadata.
func sizedEvent(t *testing.T, id string, size int) *types.Event {
	t.Helper()
	e := outcomeEvent(id)
	e.Meta = map[string]string{}
	for i := 0; ; i++ {
		key := fmt.Sprintf("k%04d", i)
		left := size - EventRecordSize(e) - 4 - len(key)
		if left < 0 {
			t.Fatalf("cannot make an event of %d bytes", size)
		}
		if left <= 60000 {
			e.Meta[key] = strings.Repeat("v", left)
			break
		}
		e.Meta[key] = strings.Repeat("v", 60000)
	}
	if got := EventRecordSize(e); got != size {
		t.Fatalf("made an event of %d bytes, want %d", got, size)
	}
	return e
}

// Every event a log takes can be read back, after reopening too, with and
// without encryption; one byte more is refused before anything is written.
func TestAppend_RecordSizeLimit(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
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
			w := openOutcomeWAL(t, dir, cipher)

			tooLarge := sizedEvent(t, "too-large", MaxEventRecordSize+1)
			err := w.AppendEvent(tooLarge)
			if !errors.Is(err, ErrRecordTooLarge) || EventsAppended(err) {
				t.Fatalf("append of %d bytes: %v", MaxEventRecordSize+1, err)
			}
			if err := CheckEventSize(tooLarge); !errors.Is(err, ErrRecordTooLarge) {
				t.Fatalf("CheckEventSize: %v", err)
			}
			if n := w.GetNextOffset(); n != 0 {
				t.Fatalf("refused append moved the next offset to %d", n)
			}

			largest := sizedEvent(t, "largest", MaxEventRecordSize)
			if err := CheckEventSize(largest); err != nil {
				t.Fatal(err)
			}
			if err := w.AppendBatch([]*types.Event{largest, outcomeEvent("after")}); err != nil {
				t.Fatal(err)
			}
			check := func(w *WAL) {
				t.Helper()
				got, err := w.ReadEvents(0, 1)
				if err != nil || len(got) != 2 || got[0].MessageId != "largest" || got[1].MessageId != "after" {
					t.Fatalf("read back: %d events, err %v", len(got), err)
				}
				if len(got[0].Meta) != len(largest.Meta) {
					t.Fatalf("read back %d metadata entries, want %d", len(got[0].Meta), len(largest.Meta))
				}
			}
			check(w)
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			reopened := openOutcomeWAL(t, dir, cipher)
			defer reopened.Close()
			check(reopened)
		})
	}
}

// The term of an entry a log appends for itself is the log's own: terms say
// which leader wrote an entry, and are not for whoever sent the event to choose.
func TestAppend_TermIsTheLogs(t *testing.T) {
	w := openOutcomeWAL(t, t.TempDir(), nil)
	defer w.Close()
	w.SetCurrentTerm(7)

	forged := outcomeEvent("forged")
	forged.Term = 999999
	if err := w.AppendEvent(forged); err != nil {
		t.Fatal(err)
	}
	batch := []*types.Event{outcomeEvent("batch-0"), outcomeEvent("batch-1")}
	batch[0].Term, batch[1].Term = -5, 3
	if err := w.AppendBatch(batch); err != nil {
		t.Fatal(err)
	}
	got, err := w.ReadEvents(0, 2)
	if err != nil || len(got) != 3 {
		t.Fatalf("read: %d events, %v", len(got), err)
	}
	for _, e := range got {
		if e.Term != 7 {
			t.Fatalf("%s stored under term %d, want the log's 7", e.MessageId, e.Term)
		}
	}
	if forged.Term != 7 {
		t.Fatalf("event given back with term %d, want 7", forged.Term)
	}

	// A follower stores what its leader sent.
	replicated := outcomeEvent("replicated")
	replicated.Offset, replicated.Term = 3, 9
	if err := w.AppendReplicatedBatch([]*types.Event{replicated}); err != nil {
		t.Fatal(err)
	}
	if term, err := w.GetTermForOffset(3); err != nil || term != 9 {
		t.Fatalf("replicated entry stored under term %d (%v), want 9", term, err)
	}
}

// A segment that cannot be started does not fail the append that filled the
// one before: that append is complete. The next one starts the segment.
func TestAppend_RotationFailureIsNotTheAppends(t *testing.T) {
	dir := t.TempDir()
	w := openOutcomeWAL(t, dir, nil)
	defer w.Close()

	blocked := filepath.Join(dir, "segments", fmt.Sprintf("%020d.log", 1))
	if err := os.Mkdir(blocked, 0o755); err != nil {
		t.Fatal(err)
	}
	full := outcomeEvent("fills-the-segment")
	full.Payload = make([]byte, 1<<20)
	if err := w.AppendEvent(full); err != nil {
		t.Fatalf("append that filled the segment: %v", err)
	}
	if err := w.Failure(); err != nil {
		t.Fatalf("a failed rotation failed the log: %v", err)
	}
	if got := len(w.GetSegments()); got != 1 {
		t.Fatalf("%d segments, want 1 while the next cannot be made", got)
	}

	if err := os.Remove(blocked); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendEvent(outcomeEvent("next")); err != nil {
		t.Fatal(err)
	}
	if got := len(w.GetSegments()); got != 2 {
		t.Fatalf("%d segments after the next append, want 2", got)
	}
	got, err := w.ReadEvents(0, 1)
	if err != nil || len(got) != 2 {
		t.Fatalf("read: %d events, %v", len(got), err)
	}
}

// After a sync fails, the append that it covered is reported as in the log
// but not durable, and the log takes nothing more: no later sync can be
// trusted for the pages the failed one may have dropped.
func TestAppend_SyncFailureStopsTheLog(t *testing.T) {
	dir := t.TempDir()
	w := openOutcomeWAL(t, dir, nil)
	defer w.Close()

	if err := w.AppendEvent(outcomeEvent("before")); err != nil {
		t.Fatal(err)
	}
	// Close the file under the segment, so its next fsync fails.
	if err := w.GetActiveSegment().segmentFile.Close(); err != nil {
		t.Fatal(err)
	}

	err := w.AppendEvent(outcomeEvent("during"))
	if !EventsAppended(err) || !errors.Is(err, ErrLogFailed) {
		t.Fatalf("append whose sync failed: %v (appended=%v)", err, EventsAppended(err))
	}
	select {
	case <-LogFailed():
	default:
		t.Fatal("LogFailed was not closed")
	}

	next := w.GetNextOffset()
	for name, appendFn := range map[string]func() error{
		"local": func() error { return w.AppendEvent(outcomeEvent("after")) },
		"replicated": func() error {
			e := outcomeEvent("replicated")
			e.Offset, e.Term = next, 1
			return w.AppendReplicatedBatch([]*types.Event{e})
		},
	} {
		err := appendFn()
		if !errors.Is(err, ErrLogFailed) || EventsAppended(err) {
			t.Fatalf("%s append after the failure: %v", name, err)
		}
	}
	if got := w.GetNextOffset(); got != next {
		t.Fatalf("a refused append moved the next offset from %d to %d", next, got)
	}
	if err := w.Flush(); !errors.Is(err, ErrLogFailed) {
		t.Fatalf("Flush of a failed log: %v", err)
	}
}

// A writer queued behind one that failed while writing does not write either:
// its offsets would come after a hole.
func TestAppend_NothingIsWrittenBehindAFailure(t *testing.T) {
	w := openOutcomeWAL(t, t.TempDir(), nil)
	defer w.Close()
	if err := w.AppendEvent(outcomeEvent("first")); err != nil {
		t.Fatal(err)
	}
	_ = w.Fail(errors.New("injected"))
	err := w.AppendEvent(outcomeEvent("second"))
	if !errors.Is(err, ErrLogFailed) || EventsAppended(err) {
		t.Fatalf("append after Fail: %v", err)
	}
	if last := w.GetLastOffset(); last != 0 {
		t.Fatalf("last offset %d, want 0", last)
	}
	// Reads go on.
	if got, err := w.ReadEvents(0, 0); err != nil || len(got) != 1 || got[0].MessageId != "first" {
		t.Fatalf("read after the failure: %v %v", got, err)
	}
}

// A segment's failed sync sticks. Linux reports a failed writeback to one
// fsync and may drop the pages; the next fsync then succeeds without them. A
// flush that comes after the failure, before the log has learned of it, must
// not make an append look durable.
func TestAppend_FailedSyncSticksToTheSegment(t *testing.T) {
	w := openOutcomeWAL(t, t.TempDir(), nil)
	defer w.Close()
	if err := w.AppendEvent(outcomeEvent("before")); err != nil {
		t.Fatal(err)
	}
	seg := w.GetActiveSegment()
	path := seg.segmentFile.Name()
	if err := seg.segmentFile.Close(); err != nil {
		t.Fatal(err)
	}
	// A background sync fails, and is not yet the log's failure.
	if err := seg.Sync(); err == nil {
		t.Fatal("sync of a closed file succeeded")
	}
	if err := w.Failure(); err != nil {
		t.Fatalf("log failed before an append saw the failure: %v", err)
	}
	// From here on an fsync succeeds, as one after a reported failure does.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	seg.segmentFile = f

	err = w.AppendEvent(outcomeEvent("after"))
	if !EventsAppended(err) || !errors.Is(err, ErrLogFailed) {
		t.Fatalf("append synced after a failed sync: %v (appended=%v)", err, EventsAppended(err))
	}
}

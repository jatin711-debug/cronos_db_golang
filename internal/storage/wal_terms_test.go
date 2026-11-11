package storage

import (
	"fmt"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// termsMatchDisk requires the term the WAL answers for every entry of its log
// to be the one stored in the entry's record, and, where want names one, that
// term.
func termsMatchDisk(t *testing.T, w *WAL, want map[int64]int64) {
	t.Helper()
	first, last := w.GetFirstOffset(), w.GetLastOffset()
	for offset := first; offset <= last; offset++ {
		got, err := w.GetTermForOffset(offset)
		if err != nil {
			t.Fatalf("term of offset %d: %v", offset, err)
		}
		event, err := w.ReadEvent(offset)
		if err != nil {
			t.Fatalf("read offset %d: %v", offset, err)
		}
		if got != event.GetTerm() {
			t.Fatalf("offset %d: the WAL answers term %d and its record holds %d", offset, got, event.GetTerm())
		}
		if term, ok := want[offset]; ok && got != term {
			t.Fatalf("offset %d: term %d, want %d", offset, got, term)
		}
	}
}

func appendInTerm(t *testing.T, w *WAL, term int64, count int, want map[int64]int64) {
	t.Helper()
	w.SetCurrentTerm(term)
	for i := 0; i < count; i++ {
		event := &types.Event{MessageId: fmt.Sprintf("term-%d-%d", term, i), ScheduleTs: 1000, Payload: make([]byte, 200), Topic: "terms"}
		if err := w.AppendEvent(event); err != nil {
			t.Fatalf("append in term %d: %v", term, err)
		}
		want[event.Offset] = term
	}
}

// The terms the WAL remembers are the terms on disk through everything that
// changes which entries a log holds: appends in new terms across segment
// rotations, a follower's appends, truncation of a divergent tail and appends
// over it, removal of old segments, reopening, and a reset.
func TestWALAnswersTheTermsOnDisk(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
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
			config := &WALConfig{SegmentSizeBytes: 1024, IndexInterval: 1000, FsyncMode: "every_event"}
			w, err := NewWAL(dir, 0, config, cipher)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = w.Close() })
			want := map[int64]int64{}
			appendInTerm(t, w, 1, 12, want)
			appendInTerm(t, w, 2, 7, want)
			appendInTerm(t, w, 5, 9, want)
			if len(w.GetSegments()) < 3 {
				t.Fatalf("the test needs entries in several segments; it has %d", len(w.GetSegments()))
			}
			termsMatchDisk(t, w, want)

			// A divergent tail is cut and the leader's entries take its place.
			cut := int64(15)
			if _, err := w.TruncateToOffset(cut); err != nil {
				t.Fatal(err)
			}
			for offset := range want {
				if offset >= cut {
					delete(want, offset)
				}
			}
			if _, err := w.GetTermForOffset(cut); err == nil {
				t.Fatalf("offset %d was cut and still has a term", cut)
			}
			var replicated []*types.Event
			for i := 0; i < 6; i++ {
				offset := cut + int64(i)
				replicated = append(replicated, &types.Event{MessageId: fmt.Sprintf("leader-%d", i), ScheduleTs: 1000, Payload: make([]byte, 200), Topic: "terms", Offset: offset, Term: 7})
				want[offset] = 7
			}
			if err := w.AppendReplicatedBatch(replicated); err != nil {
				t.Fatal(err)
			}
			appendInTerm(t, w, 8, 5, want)
			termsMatchDisk(t, w, want)

			// Old segments are removed: their entries have no term any more.
			removed, err := w.CompactByOffset(10)
			if err != nil {
				t.Fatal(err)
			}
			if removed == 0 {
				t.Fatal("no segment was removed")
			}
			if first := w.GetFirstOffset(); first > 0 {
				if _, err := w.GetTermForOffset(first - 1); err == nil {
					t.Fatalf("offset %d was removed and still has a term", first-1)
				}
			}
			termsMatchDisk(t, w, want)

			// Reopened, the WAL remembers nothing and reads the terms from disk.
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if w, err = NewWAL(dir, 0, config, cipher); err != nil {
				t.Fatal(err)
			}
			termsMatchDisk(t, w, want)
			appendInTerm(t, w, 9, 3, want)
			termsMatchDisk(t, w, want)

			// A reset empties the log; it starts again at a later offset.
			if err := w.ResetTo(200); err != nil {
				t.Fatal(err)
			}
			if _, err := w.GetTermForOffset(20); err == nil {
				t.Fatal("an entry of the log before the reset still has a term")
			}
			appendInTerm(t, w, 10, 4, want)
			termsMatchDisk(t, w, want)
		})
	}
}

// The entry before the next append is the one replication asks about on every
// batch. Its term is answered from memory once the WAL has appended it, on the
// path of a leader and of a follower alike.
func TestWALRemembersTheTermsOfWhatItAppended(t *testing.T) {
	w, err := NewWAL(t.TempDir(), 0, &WALConfig{SegmentSizeBytes: 1 << 20, IndexInterval: 1000, FsyncMode: "batch"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	want := map[int64]int64{}
	appendInTerm(t, w, 3, 50, want)
	if term, ok := w.rememberedTerm(w.GetLastOffset()); !ok || term != 3 {
		t.Fatalf("the last appended entry's term is not remembered: %d, %v", term, ok)
	}
	next := w.GetNextOffset()
	if err := w.AppendReplicatedBatch([]*types.Event{{MessageId: "replicated", ScheduleTs: 1000, Payload: []byte("p"), Topic: "terms", Offset: next, Term: 4}}); err != nil {
		t.Fatal(err)
	}
	if term, ok := w.rememberedTerm(next); !ok || term != 4 {
		t.Fatalf("the replicated entry's term is not remembered: %d, %v", term, ok)
	}
	if term, ok := w.rememberedTerm(0); !ok || term != 3 {
		t.Fatalf("the first entry's term is not remembered: %d, %v", term, ok)
	}
}

// More terms than the WAL keeps runs for: the oldest are forgotten and read
// from disk, and every answer is still the term on disk.
func TestWALForgetsOldTermRuns(t *testing.T) {
	w, err := NewWAL(t.TempDir(), 0, &WALConfig{SegmentSizeBytes: 1 << 20, IndexInterval: 1000, FsyncMode: "batch"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	want := map[int64]int64{}
	for term := int64(1); term <= 2*maxTermRuns+5; term++ {
		appendInTerm(t, w, term, 1, want)
	}
	w.mu.RLock()
	runs := len(w.terms)
	w.mu.RUnlock()
	if runs > maxTermRuns {
		t.Fatalf("the WAL keeps %d term runs; the bound is %d", runs, maxTermRuns)
	}
	termsMatchDisk(t, w, want)
}

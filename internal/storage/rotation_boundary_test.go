package storage

import (
	"testing"
)

// Right after a rotation the active segment is empty. The WAL as a whole still
// ends at the last event of the previous segment, and it must say so: this is
// what replication positions, elections, catch-up, timer replay and the
// reopened WAL's next offset are all derived from.
func TestWALLastOffsetAcrossAnEmptyActiveSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, 0, checkpointTestConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	// Append until an append leaves a fresh, empty active segment behind.
	events := 0
	for {
		appendTagged(t, w, "log", 1)
		events++
		if active := w.GetActiveSegment(); len(w.GetSegments()) > 2 && active.GetLastOffset() < active.GetFirstOffset() {
			break
		}
		if events > 1000 {
			t.Fatal("no rotation left an empty active segment")
		}
	}
	want := int64(events - 1)
	if got := w.GetLastOffset(); got != want {
		t.Fatalf("GetLastOffset = %d just after a rotation, want %d", got, want)
	}
	if got := w.GetNextOffset(); got != want+1 {
		t.Fatalf("GetNextOffset = %d just after a rotation, want %d", got, want+1)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// A restart at that moment must resume where the log ends.
	w, err = NewWAL(dir, 0, checkpointTestConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if got := w.GetLastOffset(); got != want {
		t.Fatalf("GetLastOffset = %d after reopening, want %d", got, want)
	}
	if got := w.GetNextOffset(); got != want+1 {
		t.Fatalf("GetNextOffset = %d after reopening, want %d: new events would reuse offsets", got, want+1)
	}
	appendTagged(t, w, "log", 1)
	if tag, count := generation(t, w); tag != "log" || count != events+1 {
		t.Fatalf("log after reopening and appending: %d %q entries, want %d", count, tag, events+1)
	}
}

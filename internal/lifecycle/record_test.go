package lifecycle

import (
	"context"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestIndexRebuildDropsStaleGenerationAndPreservesLatestRoot(t *testing.T) {
	dir := t.TempDir()
	wal, err := storage.NewWAL(dir, 7, &storage.WALConfig{SegmentSizeBytes: 1024, IndexInterval: 1, FsyncMode: "every_event"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	index, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	original := &types.Event{MessageId: "m", Topic: "a", ScheduleTs: 100, Payload: []byte("p")}
	if err = wal.AppendEvent(original); err != nil {
		t.Fatal(err)
	}
	root := Reference(original)
	rescheduled, err := Replacement(original, Reschedule, "command", 200)
	if err != nil {
		t.Fatal(err)
	}
	if err = wal.AppendEvent(rescheduled); err != nil {
		t.Fatal(err)
	}
	if err = index.Rebuild(context.Background(), wal); err != nil {
		t.Fatal(err)
	}
	if active, err := index.Active(original); err != nil || active {
		t.Fatalf("original remained active: %v %v", active, err)
	}
	// A replacement record is sufficient even when the log's prefix is gone.
	copy, err := Replacement(rescheduled, Relocate, "", 200)
	if err != nil {
		t.Fatal(err)
	}
	if err = wal.AppendEvent(copy); err != nil {
		t.Fatal(err)
	}
	if err = index.Apply(copy); err != nil {
		t.Fatal(err)
	}
	if err = index.DropBefore(copy.Offset); err != nil {
		t.Fatal(err)
	}
	if at, found, err := index.Latest(root); err != nil || !found || at != copy.Offset {
		t.Fatalf("GC removed current head: %d %v %v", at, found, err)
	}
	// Rebuilding after a suffix cut must not retain the canceled/superseded
	// state of the former generation, even when physical offsets are reused.
	if _, err = wal.TruncateToOffset(rescheduled.Offset); err != nil {
		t.Fatal(err)
	}
	if err = index.Rebuild(context.Background(), wal); err != nil {
		t.Fatal(err)
	}
	if active, err := index.Active(original); err != nil || !active {
		t.Fatalf("stale index survived suffix cut: %v %v", active, err)
	}
}

func TestDecodeRejectsMalformedAndUnknownInternalRecords(t *testing.T) {
	for _, meta := range []map[string]string{{Key: "{"}, {Key: `{"version":2}`}, {Prefix + "other": "x"}, {Key: `{"version":1,"kind":"cancel"}`}} {
		if _, err := Decode(&types.Event{Meta: meta}); err == nil {
			t.Fatalf("accepted malformed internal record: %v", meta)
		}
	}
}

func TestReplacementRefusesUnsupportedLegacyBodiesBeforeAppend(t *testing.T) {
	for _, e := range []*types.Event{{MessageId: "", Topic: "a", Payload: []byte("p"), ScheduleTs: 100}, {MessageId: "m", Topic: "a", ScheduleTs: 100}} {
		if _, err := Replacement(e, Relocate, "", 100); err == nil {
			t.Fatalf("constructed an unreadable replacement from %v", e)
		}
	}
	// Empty topics were accepted by the existing publish API and remain part
	// of an event's identity; cancellation must support those retained events.
	e := &types.Event{MessageId: "legacy", Payload: []byte("p"), ScheduleTs: 100}
	next, err := Replacement(e, Cancel, "cancel", 0)
	if err != nil {
		t.Fatal(err)
	}
	next.Offset = 1
	if _, err = Decode(next); err != nil {
		t.Fatalf("legacy empty-topic cancellation cannot recover: %v", err)
	}
}

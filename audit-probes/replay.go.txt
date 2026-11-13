package replay

import (
	"context"
	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"testing"
)

func TestAuditReplayFiltersAuthorizedTopic(t *testing.T) {
	w, err := storage.NewWAL(t.TempDir(), 0, &storage.WALConfig{SegmentSizeBytes: 1 << 20, IndexInterval: 1, FsyncMode: "batch", FlushIntervalMS: 100}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for i, topic := range []string{"allowed", "secret"} {
		if err := w.AppendEvent(&types.Event{MessageId: topic, Topic: topic, Payload: []byte("payload"), ScheduleTs: int64(i + 1)}); err != nil {
			t.Fatal(err)
		}
	}
	out := make(chan *ReplayEvent, 10)
	if err := NewReplayEngine(w).ReplayStream(context.Background(), &ReplayRequest{Topic: "allowed", StartOffset: 0, Count: 10}, out); err != nil {
		t.Fatal(err)
	}
	for ev := range out {
		if ev.Event.Topic != "allowed" {
			t.Fatalf("replay leaked topic %q to request for allowed", ev.Event.Topic)
		}
	}
}

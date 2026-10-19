package replay

import (
	"context"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/internal/logcontrol"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestReplaySkipsTermStartRecordsAcrossChunkBoundaries(t *testing.T) {
	w := openReplayWAL(t)
	w.SetCurrentTerm(1)
	for _, e := range []*types.Event{logcontrol.NewTermStart(0), {MessageId: "user", Topic: "orders", ScheduleTs: 1, Payload: []byte("p")}, logcontrol.NewTermStart(0)} {
		if err := w.AppendEvent(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	engine := NewReplayEngine(w)
	engine.chunkBytes = 1
	got, err := engine.ReplayByOffset(context.Background(), 0, 3)
	if err != nil || len(got) != 1 || got[0].MessageId != "user" || got[0].Offset != 1 {
		t.Fatalf("replayed controls or changed physical offsets: %v %v", got, err)
	}
}

package replay

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// openReplayWAL opens an empty WAL for a test. It is closed when the test ends.
func openReplayWAL(t *testing.T) *storage.WAL {
	t.Helper()
	wal, err := storage.NewWAL(t.TempDir(), 0, &storage.WALConfig{SegmentSizeBytes: 64 << 20, IndexInterval: 100, FsyncMode: "batch", FlushIntervalMS: 100}, nil)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	t.Cleanup(func() { wal.Close() })
	return wal
}

// appendMixed writes n events over two topics. Their schedule timestamps are not
// in offset order, some fall outside the window the tests replay, and the
// payloads vary in size, so a read of the log stops at different places.
func appendMixed(t *testing.T, wal *storage.WAL, n int, now int64) {
	t.Helper()
	for i := 0; i < n; i++ {
		topic := "orders"
		if i%3 == 0 {
			topic = "billing"
		}
		ts := now - int64((i*7)%13)*100
		payload := make([]byte, 1+(i*37)%5000)
		event := &types.Event{MessageId: fmt.Sprintf("msg-%03d", i), Topic: topic, ScheduleTs: ts, Payload: payload}
		if err := wal.AppendEvent(event); err != nil {
			t.Fatalf("AppendEvent failed: %v", err)
		}
	}
	wal.Flush()
}

// streamAll runs ReplayStream to the end and returns what it sent, each event
// as "replay offset:message id".
func streamAll(t *testing.T, engine *ReplayEngine, req *ReplayRequest) []string {
	t.Helper()
	ch := make(chan *ReplayEvent, 1000)
	if err := engine.ReplayStream(context.Background(), req, ch); err != nil {
		t.Fatalf("ReplayStream failed: %v", err)
	}
	var got []string
	for ev := range ch {
		got = append(got, fmt.Sprintf("%d:%s", ev.ReplayOffset, ev.Event.GetMessageId()))
	}
	return got
}

// wantStream is what a replay of covered should send: the events whose topic
// matches, each numbered by its place among covered, the topic filter aside.
func wantStream(covered []*types.Event, topic string) []string {
	var want []string
	for i, event := range covered {
		if topic != "" && event.GetTopic() != topic {
			continue
		}
		want = append(want, fmt.Sprintf("%d:%s", i, event.GetMessageId()))
	}
	return want
}

// TestReplayStream_ChunkBoundariesDoNotChangeTheStream replays the same windows
// with one event to a read, a few to a read, and the default chunk, and checks
// each stream against the events the window covers.
func TestReplayStream_ChunkBoundariesDoNotChangeTheStream(t *testing.T) {
	wal := openReplayWAL(t)
	now := time.Now().UnixMilli()
	appendMixed(t, wal, 40, now)

	all, err := wal.ReadEvents(0, wal.GetLastOffset())
	if err != nil {
		t.Fatalf("ReadEvents failed: %v", err)
	}
	var inWindow []*types.Event
	for _, event := range all {
		if ts := event.GetScheduleTs(); ts >= now-1000 && ts <= now {
			inWindow = append(inWindow, event)
		}
	}

	windows := []struct {
		name    string
		req     *ReplayRequest
		covered []*types.Event
	}{
		{"offsets, every event", &ReplayRequest{StartOffset: 0, Count: 40}, all},
		{"offsets, 20 from offset 5", &ReplayRequest{StartOffset: 5, Count: 20}, all[5:25]},
		{"offsets, one topic", &ReplayRequest{Topic: "orders", StartOffset: 0, Count: 40}, all},
		{"time window", &ReplayRequest{StartTS: now - 1000, EndTS: now}, inWindow},
		{"time window, one topic", &ReplayRequest{Topic: "billing", StartTS: now - 1000, EndTS: now}, inWindow},
	}
	for _, chunk := range []int64{1, 3000, replayChunkBytes} {
		engine := NewReplayEngine(wal)
		engine.chunkBytes = chunk
		for _, w := range windows {
			want := wantStream(w.covered, w.req.Topic)
			got := streamAll(t, engine, w.req)
			if !slices.Equal(got, want) {
				t.Fatalf("chunk %d, %s:\n got %v\nwant %v", chunk, w.name, got, want)
			}
		}
	}
}

// TestReplayCursor_ReadStopsAtTheByteBound checks that a read of the log takes
// the events that reach the byte bound and no more, so a replay of large events
// holds a chunk of the log and not the range.
func TestReplayCursor_ReadStopsAtTheByteBound(t *testing.T) {
	wal := openReplayWAL(t)
	payload := make([]byte, 1<<20)
	for i := 0; i < 12; i++ {
		if err := wal.AppendEvent(&types.Event{MessageId: fmt.Sprintf("big-%02d", i), Payload: payload}); err != nil {
			t.Fatalf("AppendEvent failed: %v", err)
		}
	}
	wal.Flush()

	engine := NewReplayEngine(wal)
	cursor := engine.offsetCursor(0, 100)
	if _, err := cursor.Next(context.Background()); err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	// The first read stops once it has replayChunkBytes (4 MiB) of one-MiB events,
	// which is four of them. The cursor has the first and holds the other three.
	if read := len(cursor.chunk) + 1; read < 2 || read > 5 {
		t.Fatalf("first read took %d events of 1 MiB under a %d byte bound; want 2 to 5", read, replayChunkBytes)
	}
}

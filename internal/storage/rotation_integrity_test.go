package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestMaintenanceConcurrentRotationPreservesEveryAcceptedOffset(t *testing.T) {
	dir := t.TempDir()
	cfg := &WALConfig{SegmentSizeBytes: 512, IndexInterval: 1, FsyncMode: "periodic", FlushIntervalMS: 10}
	w, err := NewWAL(dir, 0, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { w.Close() }()
	const workers, perWorker = 4, 30
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				if err := w.AppendEvent(&types.Event{MessageId: fmt.Sprintf("%d-%d", id, i), Topic: "a", Payload: make([]byte, 64), ScheduleTs: 1}); err != nil {
					errs <- err
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for pass := 0; pass < 2; pass++ {
		for _, seg := range w.GetSegments() {
			if seg.GetLastOffset() < seg.GetFirstOffset() {
				continue
			}
			ok, err := seg.allEventsMatch(t.Context(), func(*types.Event) bool { return true })
			if err != nil || !ok {
				t.Fatalf("segment integrity pass %d: %s %v", pass, seg.GetFilename(), err)
			}
		}
		events, err := w.ReadEvents(0, workers*perWorker-1)
		if err != nil || len(events) != workers*perWorker {
			t.Fatalf("accepted events lost (pass %d): count=%d err=%v", pass, len(events), err)
		}
		seen := make(map[string]bool)
		for i, event := range events {
			if event.Offset != int64(i) || seen[event.MessageId] {
				t.Fatalf("offset gap/duplicate: index=%d event=%+v", i, event)
			}
			seen[event.MessageId] = true
		}
		if pass == 0 {
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			w, err = NewWAL(dir, 0, cfg, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

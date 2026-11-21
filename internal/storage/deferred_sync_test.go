package storage

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func deferredTestEvents(tag string, n int) []*types.Event {
	events := make([]*types.Event, n)
	for i := range events {
		events[i] = &types.Event{MessageId: fmt.Sprintf("%s-%d", tag, i), Topic: "orders", Payload: []byte("payload"), ScheduleTs: 1}
	}
	return events
}

// deferredTestWALs runs a test on logs of both syncing modes, with a mapped
// segment (plain) and a buffered one (encrypted).
func deferredTestWALs(t *testing.T, test func(t *testing.T, w *WAL)) {
	for _, mode := range []string{"every_event", "batch"} {
		for _, encrypted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/encrypted=%v", mode, encrypted), func(t *testing.T) {
				var cipher *SegmentCipher
				if encrypted {
					var err error
					if cipher, err = NewSegmentCipher(bytes.Repeat([]byte{9}, 32), 0); err != nil {
						t.Fatal(err)
					}
				}
				w, err := NewWAL(t.TempDir(), 0, &WALConfig{SegmentSizeBytes: 1 << 20, IndexInterval: 100, FsyncMode: mode}, cipher)
				if err != nil {
					t.Fatal(err)
				}
				defer w.Close()
				test(t, w)
			})
		}
	}
}

// An append whose caller waits for the sync itself is in the log, and reads
// back, as soon as it returns; it counts as durable only once that wait has
// succeeded, and nothing after it does before it does.
func TestWAL_DeferredAppendIsDurableOnceItsSyncSucceeds(t *testing.T) {
	deferredTestWALs(t, func(t *testing.T, w *WAL) {
		durable := func(want int64, when string) {
			t.Helper()
			if got := w.DurableThrough(); got != want {
				t.Fatalf("%s: durable through %d, want %d (log ends at %d)", when, got, want, w.GetLastOffset())
			}
		}
		durable(-1, "empty log")
		if err := w.AppendBatch(deferredTestEvents("a", 2)); err != nil {
			t.Fatal(err)
		}
		durable(1, "after an append that waited for its sync")

		first, err := w.AppendBatchDeferred(deferredTestEvents("b", 2))
		if err != nil {
			t.Fatal(err)
		}
		if got := w.GetLastOffset(); got != 3 {
			t.Fatalf("log ends at %d after the deferred append, want 3", got)
		}
		durable(1, "a deferred append not waited for")
		// Written out: a reader finds it before it is durable.
		if event, err := w.ReadEvent(3); err != nil || event.MessageId != "b-1" {
			t.Fatalf("read the deferred append before its sync: %v", err)
		}
		if events, err := w.ReadEventsWithin(2, 3, 0); err != nil || len(events) != 2 {
			t.Fatalf("range read of the deferred append before its sync: %d events, %v", len(events), err)
		}

		second, err := w.AppendEventDeferred(deferredTestEvents("c", 1)[0])
		if err != nil {
			t.Fatal(err)
		}
		durable(1, "two deferred appends not waited for")
		if err := second.Wait(); err != nil {
			t.Fatal(err)
		}
		durable(1, "the later deferred append is durable, the earlier one not known to be")

		if err := w.AppendBatch(deferredTestEvents("d", 1)); err != nil {
			t.Fatal(err)
		}
		durable(1, "an append that waited for its sync, behind a deferred one not waited for")

		if err := first.Wait(); err != nil {
			t.Fatal(err)
		}
		durable(5, "every append waited for")

		// And again, in order.
		third, err := w.AppendBatchDeferred(deferredTestEvents("e", 3))
		if err != nil {
			t.Fatal(err)
		}
		durable(5, "a new deferred append")
		if err := third.Wait(); err != nil {
			t.Fatal(err)
		}
		durable(8, "waited for")
		for offset := int64(0); offset <= 8; offset++ {
			if event, err := w.ReadEvent(offset); err != nil || event.Offset != offset {
				t.Fatalf("read offset %d: %v", offset, err)
			}
		}
	})
}

// With periodic sync nothing is promised and nothing is waited for.
func TestWAL_DeferredAppendWithPeriodicSync(t *testing.T) {
	w, err := NewWAL(t.TempDir(), 0, &WALConfig{SegmentSizeBytes: 1 << 20, IndexInterval: 100, FsyncMode: "periodic", FlushIntervalMS: 10}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	pending, err := w.AppendBatchDeferred(deferredTestEvents("a", 3))
	if err != nil {
		t.Fatal(err)
	}
	if got := w.DurableThrough(); got != 2 {
		t.Fatalf("durable through %d, want the log end, 2", got)
	}
	if err := pending.Wait(); err != nil {
		t.Fatal(err)
	}
	if pending, err := w.AppendBatchDeferred(nil); err != nil || pending.Wait() != nil {
		t.Fatalf("an empty deferred append: %v", err)
	}
}

// A deferred append does not wait for the disk: while a sync is in flight,
// appends go on reaching the log, and they are made durable together by a
// sync that starts after them. That is what lets every publish that arrives
// during one sync share the next.
func TestWAL_DeferredAppendsDoNotWaitForASyncInFlight(t *testing.T) {
	deferredTestWALs(t, func(t *testing.T, w *WAL) {
		if err := w.AppendBatch(deferredTestEvents("a", 1)); err != nil {
			t.Fatal(err)
		}
		seg := w.GetActiveSegment()

		// A sync in flight holds flushMu for as long as the disk takes.
		seg.flushMu.Lock()
		var unlockOnce sync.Once
		unlock := func() { unlockOnce.Do(seg.flushMu.Unlock) }
		defer unlock()

		const appends = 6
		pending := make([]*PendingSync, appends)
		appended := make(chan error, 1)
		go func() {
			for i := range pending {
				var err error
				if pending[i], err = w.AppendEventDeferred(deferredTestEvents(fmt.Sprintf("b%d", i), 1)[0]); err != nil {
					appended <- err
					return
				}
			}
			appended <- nil
		}()
		select {
		case err := <-appended:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("deferred appends waited for the sync in flight")
		}
		if got := w.GetLastOffset(); got != appends {
			t.Fatalf("log ends at %d, want %d", got, appends)
		}
		if got := w.DurableThrough(); got != 0 {
			t.Fatalf("durable through %d while no sync has covered the deferred appends, want 0", got)
		}

		waited := make(chan error, appends)
		for _, p := range pending {
			go func() { waited <- p.Wait() }()
		}
		select {
		case err := <-waited:
			t.Fatalf("a deferred append was reported durable while the only sync was stalled: %v", err)
		case <-time.After(200 * time.Millisecond):
		}
		if got := w.DurableThrough(); got != 0 {
			t.Fatalf("durable through %d while the sync is stalled, want 0", got)
		}

		unlock()
		for i := 0; i < appends; i++ {
			select {
			case err := <-waited:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("a deferred append never became durable")
			}
		}
		if got := w.DurableThrough(); got != appends {
			t.Fatalf("durable through %d once every sync has returned, want %d", got, appends)
		}
	})
}

// Deferred appends made in order under a caller's lock, from many writers and
// across segment rotations, lose and reorder nothing, and all become durable.
func TestWAL_DeferredAppendsFromManyWriters(t *testing.T) {
	for _, mode := range []string{"every_event", "batch"} {
		t.Run(mode, func(t *testing.T) {
			cipher, err := NewSegmentCipher(bytes.Repeat([]byte{9}, 32), 0)
			if err != nil {
				t.Fatal(err)
			}
			w, err := NewWAL(t.TempDir(), 0, &WALConfig{SegmentSizeBytes: 16 << 10, IndexInterval: 10, FsyncMode: mode}, cipher)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()

			const writers, perWriter = 8, 40
			var order sync.Mutex // the caller's lock: a partition's publish lock
			var wg sync.WaitGroup
			errs := make(chan error, writers)
			for writer := 0; writer < writers; writer++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < perWriter; i++ {
						order.Lock()
						pending, err := w.AppendBatchDeferred(deferredTestEvents(fmt.Sprintf("w%d-%d", writer, i), 2))
						order.Unlock()
						if err == nil {
							err = pending.Wait()
						}
						if err != nil {
							errs <- err
							return
						}
					}
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Fatal(err)
			}

			const total = writers * perWriter * 2
			if got := w.DurableThrough(); got != total-1 {
				t.Fatalf("durable through %d, want %d", got, total-1)
			}
			events, err := w.ReadEvents(0, total-1)
			if err != nil {
				t.Fatal(err)
			}
			if len(events) != total {
				t.Fatalf("read %d events, want %d", len(events), total)
			}
			seen := make(map[string]bool, total)
			for i, event := range events {
				if event.Offset != int64(i) || seen[event.MessageId] {
					t.Fatalf("offset %d, id %s at position %d", event.Offset, event.MessageId, i)
				}
				seen[event.MessageId] = true
			}
		})
	}
}

// What a partition's publishes do to its log: writers that append one event
// each, in order under a lock. "inside" waits for the sync while holding the
// lock, as a publish used to; "after" waits once it is released.
func BenchmarkWAL_OrderedAppends(b *testing.B) {
	for _, mode := range []string{"every_event", "batch"} {
		for _, wait := range []string{"inside", "after"} {
			for _, writers := range []int{1, 4, 16} {
				b.Run(fmt.Sprintf("fsync=%s/wait=%s/writers=%d", mode, wait, writers), func(b *testing.B) {
					cipher, err := NewSegmentCipher(bytes.Repeat([]byte{9}, 32), 0)
					if err != nil {
						b.Fatal(err)
					}
					w, err := NewWAL(b.TempDir(), 0, &WALConfig{SegmentSizeBytes: 64 << 20, IndexInterval: 1000, FsyncMode: mode}, cipher)
					if err != nil {
						b.Fatal(err)
					}
					defer w.Close()
					payload := bytes.Repeat([]byte{7}, 1024)
					var order sync.Mutex
					var wg sync.WaitGroup
					errs := make(chan error, writers)
					b.ResetTimer()
					for writer := 0; writer < writers; writer++ {
						wg.Add(1)
						go func() {
							defer wg.Done()
							for i := writer; i < b.N; i += writers {
								event := &types.Event{MessageId: fmt.Sprintf("m-%d", i), Topic: "orders", Payload: payload, ScheduleTs: 1}
								var err error
								if wait == "inside" {
									order.Lock()
									err = w.AppendEvent(event)
									order.Unlock()
								} else {
									order.Lock()
									pending, appendErr := w.AppendEventDeferred(event)
									order.Unlock()
									if err = appendErr; err == nil {
										err = pending.Wait()
									}
								}
								if err != nil {
									errs <- err
									return
								}
							}
						}()
					}
					wg.Wait()
					b.StopTimer()
					close(errs)
					for err := range errs {
						b.Fatal(err)
					}
					b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "appends/s")
				})
			}
		}
	}
}

package main

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// A fast consumer can receive an event before PublishBatch returns. Concurrent
// redeliveries must still produce one unique delivery for that accepted ID.
func TestLedgerReconcilesDeliveryBeforePublishResponse(t *testing.T) {
	l := &ledger{prefix: "trial-", states: make([]atomic.Uint32, 100)}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range l.states {
				l.delivery(&types.Event{MessageId: "trial-" + strconv.Itoa(i), ScheduleTs: time.Now().UnixMilli()})
			}
		}()
	}
	wg.Wait()
	for i := range l.states {
		l.states[i].Or(1)
	}
	l.accepted.Store(100)
	if l.delivered.Load() != 100 || l.duplicates.Load() != 700 {
		t.Fatalf("unique=%d duplicates=%d; want 100 and 700", l.delivered.Load(), l.duplicates.Load())
	}
	for i := range l.states {
		if l.states[i].Load() != 3 {
			t.Fatalf("accepted event %d lacks delivery evidence", i)
		}
	}
	for _, id := range []string{"old-trial-1", "trial--1", "trial-100", "trial-invalid"} {
		l.delivery(&types.Event{MessageId: id})
	}
	if l.delivered.Load() != 100 || l.duplicates.Load() != 700 {
		t.Fatal("unrelated or invalid IDs changed the ledger")
	}
}

package delivery

import (
	"fmt"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"strings"
	"sync"
	"testing"
)

func TestAuditWorkerBoundsPayloadAndMetadataBytes(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()
	w := NewWorker(d, 10)
	w.maxQueueBytes = 1024
	large := &types.Event{Payload: make([]byte, 600)}
	w.AddReadyEvents([]*types.Event{large, large, {Meta: map[string]string{"large": strings.Repeat("x", 2048)}}})
	if stats := w.GetStats(); stats.QueueLength != 1 || stats.QueueBytes > 1024 {
		t.Fatalf("byte budget did not limit queued payload/metadata: %+v", stats)
	}
	w.processBatch()
	if stats := w.GetStats(); stats.QueueLength != 0 || stats.QueueBytes != 0 {
		t.Fatalf("dispatch did not release byte budget: %+v", stats)
	}
	w.AddReadyEvent(large)
	if stats := w.GetStats(); stats.QueueLength != 1 {
		t.Fatalf("queue did not recover after draining: %+v", stats)
	}
}

func TestAuditInFlightLimitReleasesAllUnsentGroups(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()
	d.config.MaxInFlightEvents = 1
	if !d.tryReserveInFlight(1) {
		t.Fatal("reserve initial capacity")
	}
	streams := make([]*mockStream, 3)
	for i := range streams {
		streams[i] = newMockStream()
		sub := makeTestSubscription(fmt.Sprintf("group%d:0:member", i), 0, streams[i])
		sub.ConsumerGroup = fmt.Sprintf("group%d", i)
		sub.MaxCredits = 1
		if err := d.Subscribe(sub); err != nil {
			t.Fatal(err)
		}
	}
	event := makeTestEvent(0, 0, "retry-after-capacity")
	if err := d.Dispatch(event); err == nil {
		t.Fatal("expected capacity error")
	}
	if err := d.decInFlight(1); err != nil {
		t.Fatal(err)
	}
	d.config.MaxInFlightEvents = 3
	if err := d.Dispatch(event); err != nil {
		t.Fatal(err)
	}
	for i, stream := range streams {
		if stream.SendCount() != 1 {
			t.Errorf("group %d did not recover after capacity became available", i)
		}
	}
}

func TestAuditDispatchChecksEveryRecipientTopic(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()
	allowed, secret := newMockStream(), newMockStream()
	for i, stream := range []*mockStream{allowed, secret} {
		sub := makeTestSubscription(fmt.Sprintf("test-group:0:member%d", i), 0, stream)
		sub.Topic = []string{"allowed", "secret"}[i]
		if err := d.Subscribe(sub); err != nil {
			t.Fatal(err)
		}
	}
	for i := int64(0); i < 2; i++ {
		event := makeTestEvent(0, i, fmt.Sprintf("allowed-%d", i))
		event.Topic = "allowed"
		if err := d.Dispatch(event); err != nil {
			t.Fatal(err)
		}
	}
	if secret.SendCount() != 0 || allowed.SendCount() != 2 {
		t.Fatalf("topic isolation failed: allowed=%d secret=%d", allowed.SendCount(), secret.SendCount())
	}
}

func TestAuditWorkerOwnsDispatchSlice(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()
	w := NewWorker(d, 100)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20000; i++ {
			w.AddReadyEvent(&types.Event{MessageId: "audit", PartitionId: 0, Offset: int64(i)})
		}
	}()
	for i := 0; i < 20000; i++ {
		w.processBatch()
	}
	wg.Wait()
}
func TestAuditDLQRetainsEntryAfterRestart(t *testing.T) {
	dir := t.TempDir()
	d, err := NewDeadLetterQueue(dir, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Add(&types.Event{MessageId: "audit", Payload: []byte("payload")}, "delivery", 3, "failure", "sub"); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d2, err := NewDeadLetterQueue(dir, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	if n := d2.Count(); n != 1 {
		t.Fatalf("durable DLQ entry missing after reopen: got %d, want 1", n)
	}
}

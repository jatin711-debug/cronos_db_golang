package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// TestSDKLifecycleAgainstAStandaloneServer drives cancel and reschedule
// through the Go SDK against a server with --event-lifecycle: the references
// the producer returns resolve, revisions chain, and the errors keep their
// kind.
func TestSDKLifecycleAgainstAStandaloneServer(t *testing.T) {
	cfg := &types.Config{DataDir: t.TempDir(), PartitionCount: 1, ReplicationFactor: 1, EventLifecycle: true, FsyncMode: "periodic", FlushIntervalMS: 10, TickMS: 10, WheelSize: 64, DedupTTLHours: 24, BloomCapacity: 1000}
	pm := partition.NewPartitionManager("sdk-lifecycle", cfg)
	defer pm.Close()
	if err := pm.CreatePartition(0, "orders"); err != nil {
		t.Fatal(err)
	}
	p, _ := pm.GetInternalPartition(0)
	scfg := DefaultConfig()
	scfg.Address = "127.0.0.1:0"
	scfg.SLORecorder = nil
	srv, err := NewGRPCServer(scfg)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()
	srv.RegisterServices(NewEventServiceHandler(pm, p.DedupStore, p.ConsumerGroup), nil, NewPartitionServiceHandler(pm, nil, "sdk-lifecycle"), nil)
	if err = srv.Start(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, client.DefaultConfig(srv.listener.Addr().String()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	prodCfg := client.DefaultProducerConfig()
	prodCfg.ReturnReferences = true
	producer, err := c.NewProducer(prodCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	sent, err := producer.Send(ctx, client.Message{MessageID: "order-1", Topic: "orders", Payload: []byte("x"), ScheduleAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	ref := sent.Reference()
	state, err := c.GetEventState(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if state.Reference != ref || state.Revision != ref.InitialRevision() || state.Canceled {
		t.Fatalf("state of a new event = %+v, reference %+v", state, ref)
	}

	at := time.Now().Add(2 * time.Hour).Truncate(time.Millisecond)
	move := client.EventChange{RequestID: "move-1", ExpectedRevision: state.Revision}
	moved, err := c.RescheduleEvent(ctx, ref, move, at)
	if err != nil {
		t.Fatal(err)
	}
	if !moved.ScheduleAt().Equal(at) || moved.Revision == state.Revision || moved.Reference != ref {
		t.Fatalf("rescheduled state = %+v", moved)
	}
	// The same request again answers with its result and writes nothing.
	again, err := c.RescheduleEvent(ctx, ref, move, at)
	if err != nil || again.Revision != moved.Revision {
		t.Fatalf("retried reschedule = %+v, %v; want revision %+v", again, err, moved.Revision)
	}
	// A change based on the old revision is a conflict.
	_, err = c.CancelEvent(ctx, ref, client.EventChange{RequestID: "stale", ExpectedRevision: state.Revision})
	if kind := errorKind(err); kind != client.ErrorKindConflict {
		t.Fatalf("stale cancel: %v (kind %q)", err, kind)
	}
	canceled, err := c.CancelEvent(ctx, ref, client.EventChange{RequestID: "cancel-1", ExpectedRevision: moved.Revision})
	if err != nil || !canceled.Canceled {
		t.Fatalf("cancel = %+v, %v", canceled, err)
	}
	_, err = c.RescheduleEvent(ctx, ref, client.EventChange{RequestID: "late", ExpectedRevision: canceled.Revision}, at)
	if kind := errorKind(err); kind != client.ErrorKindPrecondition {
		t.Fatalf("reschedule of a canceled event: %v (kind %q)", err, kind)
	}

	// Batch results carry complete references when asked to.
	batch, err := producer.SendBatch(ctx, []client.Message{
		{MessageID: "order-2", Topic: "orders", Payload: []byte("y"), ScheduleAt: time.Now().Add(time.Hour)},
		{MessageID: "order-3", Topic: "orders", Payload: []byte("z"), ScheduleAt: time.Now().Add(time.Hour)},
	})
	if err != nil || len(batch.Results) != 2 {
		t.Fatalf("batch = %+v, %v", batch, err)
	}
	for _, r := range batch.Results {
		s, err := c.GetEventState(ctx, r.Reference())
		if err != nil || s.Reference.MessageID != r.MessageID {
			t.Fatalf("batch reference %+v: %+v, %v", r.Reference(), s, err)
		}
	}

	// A retry of a completed reschedule still resolves after the requested
	// time has passed. The server must check the request ID before the time.
	last := batch.Results[len(batch.Results)-1].Reference()
	due := time.Now().Add(2 * time.Second)
	change := client.EventChange{RequestID: "move-and-retry", ExpectedRevision: last.InitialRevision()}
	before, err := c.RescheduleEvent(ctx, last, change, due)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Until(due.Add(10 * time.Millisecond)))
	after, err := c.RescheduleEvent(ctx, last, change, due)
	if err != nil || after.Revision != before.Revision {
		t.Fatalf("reschedule retry after due time = %+v, %v; want revision %+v", after, err, before.Revision)
	}
	unchanged := batch.Results[0].Reference()
	_, err = c.RescheduleEvent(ctx, unchanged, client.EventChange{RequestID: "past-schedule", ExpectedRevision: unchanged.InitialRevision()}, time.UnixMilli(0))
	if kind := errorKind(err); kind != client.ErrorKindValidation {
		t.Fatalf("reschedule with timestamp zero became a cancel: %v (kind %q)", err, kind)
	}
}

func errorKind(err error) client.ErrorKind {
	var ce *client.Error
	if errors.As(err, &ce) {
		return ce.Kind
	}
	return ""
}

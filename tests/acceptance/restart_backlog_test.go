//go:build acceptance

package acceptance

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
)

// A cluster goes down while it holds more undelivered events than a node may
// hold in memory, with nobody consuming them. A node that takes its backlog
// into memory when it starts does not come back from that. Each node has to
// start within its memory limit and leave the backlog on disk, and every event
// has to be delivered once a consumer asks for it, still within the limit.
func TestRestartWithABacklogLargerThanMemory(t *testing.T) {
	const (
		topic    = "backlog"
		group    = "backlog-workers"
		payload  = 16 << 10
		headroom = 64 << 20 // what a node may hold on top of what it holds idle
	)
	// Segments large enough for these events; the other tests use tiny ones.
	c := newCluster(t, "--segment-size=8388608")
	c.startAll()
	// The limit is set from what the nodes hold when idle, and the backlog from
	// the limit, so that megabytes and not gigabytes are too much to hold.
	idle := c.held(t)
	limit := int64(idle) + headroom
	count := (limit + limit/4) / payload
	t.Logf("idle nodes hold up to %.0f MiB; they will be restarted with a memory limit of %d MiB and a backlog of %d events of %d KiB, %d MiB",
		idle/(1<<20), limit>>20, count, payload>>10, count*payload>>20)

	events := newLedger()
	producer, err := c.dial().NewProducer(client.DefaultProducerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	// Every event is due when it is published, so all of them are waiting for a
	// consumer when the nodes go down.
	due := time.Now().UnixMilli()
	body := make([]byte, payload)
	var next, failed atomic.Int64
	var failures sync.Map
	publish := func(worker int) {
		for {
			seq := next.Add(1)
			if seq > count {
				return
			}
			id := fmt.Sprintf("backlog-%d-%06d", worker, seq)
			events.noteSent(id, due)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			result, err := producer.Send(ctx, client.Message{MessageID: id, Topic: topic, Payload: body, ScheduleTS: due})
			cancel()
			switch {
			case err == nil:
				events.noteAccepted(id, result.PartitionID, result.Offset)
			case acceptedEarlier(err):
				events.noteAccepted(id, -1, -1)
			default:
				failed.Add(1)
				failures.Store(id, err.Error())
				events.forget(id) // outcome unknown: not counted either way
			}
		}
	}
	step(t, "publishing %d events that are due at once, with no consumer", count)
	var publishers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		publishers.Add(1)
		go func() {
			defer publishers.Done()
			publish(worker)
		}()
	}
	publishers.Wait()

	accepted := events.accepted.Load()
	if failed.Load() > 0 {
		shown := 0
		failures.Range(func(id, reason any) bool {
			t.Logf("a publish failed: %s: %s", id, reason)
			shown++
			return shown < 5
		})
	}
	if accepted*payload <= limit {
		t.Fatalf("%d publishes were accepted and %d failed: a backlog of %d MiB is not larger than the limit of %d MiB",
			accepted, failed.Load(), accepted*payload>>20, limit>>20)
	}

	step(t, "killing every node with %d MiB undelivered, and starting them with a limit of %d MiB", accepted*payload>>20, limit>>20)
	for _, n := range c.nodes {
		c.kill(n)
	}
	c.extraArgs = append(c.extraArgs, fmt.Sprintf("--memory-limit=%d", limit))
	c.startAll()

	// watch notes the most any node holds, and fails the test when one has died.
	most := 0.0
	var series []string
	watch := func(during string) {
		t.Helper()
		for _, n := range c.nodes {
			select {
			case <-n.exitedChan():
				t.Fatalf("%s died %s", n.id, during)
			default:
			}
		}
		now := c.held(t)
		most = max(most, now)
		series = append(series, fmt.Sprintf("%.0f", now/(1<<20)))
	}
	within := func(during string) {
		t.Helper()
		t.Logf("the fullest node, in MiB, at each look %s: %s", during, strings.Join(series, " "))
		if most >= float64(limit) {
			t.Fatalf("a node held %.0f MiB %s, which is its whole memory limit of %d MiB; the backlog is %d MiB",
				most/(1<<20), during, limit>>20, accepted*payload>>20)
		}
		t.Logf("the fullest node held %.0f MiB of its %d MiB limit %s", most/(1<<20), limit>>20, during)
	}
	// The nodes are watched for a while: a walk of the logs that fills memory
	// takes its time, and goes on after a node has said that it is ready.
	for until := time.Now().Add(15 * time.Second); time.Now().Before(until); time.Sleep(time.Second) {
		watch("after the restart")
	}
	within("after the restart")

	step(t, "starting a consumer")
	consumerClient := c.dial()
	ctx, cancel := context.WithCancel(context.Background())
	consumed := make(chan error, 1)
	cfg := client.DefaultConsumerConfig(topic, group)
	cfg.ReconnectBackoff = 200 * time.Millisecond
	cfg.MaxReconnectBackoff = 2 * time.Second
	// The consumer takes a few events at a time, so the nodes have the rest of
	// the backlog to hold back while it is delivered.
	cfg.MaxBufferSize = 64
	go func() {
		consumed <- consumerClient.Subscribe(ctx, cfg, func(_ context.Context, d client.Delivery) error {
			now := time.Now().UnixMilli()
			if d.Event != nil {
				events.noteDelivered(d.Event.GetMessageId(), d.Event.GetScheduleTs(), now, d.Event.GetPartitionId(), d.Event.GetOffset())
			}
			for _, event := range d.Batch {
				events.noteDelivered(event.GetMessageId(), event.GetScheduleTs(), now, event.GetPartitionId(), event.GetOffset())
			}
			return nil
		})
	}()
	defer func() {
		cancel()
		select {
		case <-consumed:
		case <-time.After(30 * time.Second):
			t.Error("the consumer did not stop")
		}
	}()

	deadline := time.Now().Add(6 * time.Minute)
	for events.delivered.Load() < accepted {
		if time.Now().After(deadline) {
			missing := events.undelivered()
			t.Fatalf("%d of the %d acknowledged events were never delivered:\n  %s", len(missing), accepted, sample(missing, 15))
		}
		watch("while the backlog was delivered")
		time.Sleep(250 * time.Millisecond)
	}
	if missing := events.undelivered(); len(missing) > 0 {
		t.Fatalf("%d of the %d acknowledged events were never delivered:\n  %s", len(missing), accepted, sample(missing, 15))
	}
	step(t, "all %d acknowledged events were delivered", accepted)
	within("from the restart until the backlog was delivered")
	if early := events.tooEarly(); len(early) > 0 {
		t.Errorf("%d deliveries arrived before their scheduled time:\n  %s", len(early), sample(early, 15))
	}
}

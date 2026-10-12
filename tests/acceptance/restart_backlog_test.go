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
//
// Two tests do this. The first runs the nodes as processes and gives them
// their limit as a setting, which they keep to but nothing enforces; it reads
// what they hold and compares. The second runs them as containers that the
// kernel kills when they use more than they were given, with a backlog of a
// gigabyte.

// publishBacklog publishes count events of payload bytes that are due at once,
// with no consumer, and returns the record of them and how many were
// acknowledged. A node that refuses for lack of room is asked again.
func publishBacklog(t *testing.T, c *cluster, topic string, payload int, count int64, publishers int) (*ledger, int64) {
	t.Helper()
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
	deadline := time.Now().Add(10 * time.Minute)
	var next, failed, refused atomic.Int64
	var failures sync.Map
	publish := func(worker int) {
		for {
			seq := next.Add(1)
			if seq > count {
				return
			}
			id := fmt.Sprintf("backlog-%d-%06d", worker, seq)
			events.noteSent(id, due)
			for {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				result, err := producer.Send(ctx, client.Message{MessageID: id, Topic: topic, Payload: body, ScheduleTS: due})
				cancel()
				full := err != nil && (strings.Contains(err.Error(), "at capacity") || strings.Contains(err.Error(), "ResourceExhausted"))
				if full && time.Now().Before(deadline) {
					refused.Add(1)
					time.Sleep(50 * time.Millisecond)
					continue
				}
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
				break
			}
		}
	}
	step(t, "publishing %d events of %d KiB that are due at once, with no consumer", count, payload>>10)
	var workers sync.WaitGroup
	for worker := 0; worker < publishers; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			publish(worker)
		}()
	}
	workers.Wait()

	accepted := events.accepted.Load()
	shown := 0
	failures.Range(func(id, reason any) bool {
		t.Logf("a publish failed: %s: %s", id, reason)
		shown++
		return shown < 5
	})
	t.Logf("%d publishes were accepted, %d failed, and a node said %d times that it had no room", accepted, failed.Load(), refused.Load())
	return events, accepted
}

// deliverBacklog starts a consumer and waits until it has received every
// acknowledged event. look is called again and again while it waits.
func deliverBacklog(t *testing.T, c *cluster, events *ledger, accepted int64, topic, group string, look func(during string)) {
	t.Helper()
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

	deadline := time.Now().Add(8 * time.Minute)
	for events.delivered.Load() < accepted {
		if time.Now().After(deadline) {
			missing := events.undelivered()
			t.Fatalf("%d of the %d acknowledged events were never delivered:\n  %s", len(missing), accepted, sample(missing, 15))
		}
		look("while the backlog was delivered")
		time.Sleep(250 * time.Millisecond)
	}
	if missing := events.undelivered(); len(missing) > 0 {
		t.Fatalf("%d of the %d acknowledged events were never delivered:\n  %s", len(missing), accepted, sample(missing, 15))
	}
	step(t, "all %d acknowledged events were delivered", accepted)
	if early := events.tooEarly(); len(early) > 0 {
		t.Errorf("%d deliveries arrived before their scheduled time:\n  %s", len(early), sample(early, 15))
	}
}

// memoryWatch notes the most that any node of a cluster holds.
type memoryWatch struct {
	t      *testing.T
	c      *cluster
	most   float64
	series []string
}

// look reads what every node holds.
func (w *memoryWatch) look() {
	w.t.Helper()
	now := w.c.held(w.t)
	w.most = max(w.most, now)
	w.series = append(w.series, fmt.Sprintf("%.0f", now/(1<<20)))
}

// within fails the test when a node held as much as limit, and says what the
// fullest node held at each look.
func (w *memoryWatch) within(limit int64, during string) {
	w.t.Helper()
	w.t.Logf("the fullest node, in MiB, at each look %s: %s", during, strings.Join(w.series, " "))
	if w.most >= float64(limit) {
		w.t.Fatalf("a node held %.0f MiB %s, which is its whole memory limit of %d MiB", w.most/(1<<20), during, limit>>20)
	}
	w.t.Logf("the fullest node held %.0f MiB of its %d MiB limit %s", w.most/(1<<20), limit>>20, during)
}

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
	t.Logf("idle nodes hold up to %.0f MiB; they will be restarted with a memory limit of %d MiB and a backlog of %d MiB",
		idle/(1<<20), limit>>20, count*payload>>20)

	events, accepted := publishBacklog(t, c, topic, payload, count, 8)
	if accepted*payload <= limit {
		t.Fatalf("a backlog of %d MiB is not larger than the limit of %d MiB", accepted*payload>>20, limit>>20)
	}

	step(t, "killing every node with %d MiB undelivered, and starting them with a limit of %d MiB", accepted*payload>>20, limit>>20)
	for _, n := range c.nodes {
		c.kill(n)
	}
	c.extraArgs = append(c.extraArgs, fmt.Sprintf("--memory-limit=%d", limit))
	c.startAll()

	watch := &memoryWatch{t: t, c: c}
	look := func(during string) {
		t.Helper()
		for _, n := range c.nodes {
			select {
			case <-n.exitedChan():
				t.Fatalf("%s died %s", n.id, during)
			default:
			}
		}
		watch.look()
	}
	// The nodes are watched for a while: a walk of the logs that fills memory
	// takes its time, and goes on after a node has said that it is ready.
	for until := time.Now().Add(15 * time.Second); time.Now().Before(until); time.Sleep(time.Second) {
		look("after the restart")
	}
	watch.within(limit, "after the restart")

	deliverBacklog(t, c, events, accepted, topic, group, look)
	watch.within(limit, "from the restart until the backlog was delivered")
}

// alive fails the test when a container is not running, or was killed by the
// kernel for the memory it used.
func (c *cluster) alive(during string) {
	c.t.Helper()
	for _, n := range c.nodes {
		state := c.docker("inspect", "--format", "{{.State.Running}} {{.State.OOMKilled}}", n.container)
		if state != "true false" {
			c.t.Fatalf("%s %s: the answers to \"is it running\" and \"was it killed for its memory\" are %q", n.id, during, state)
		}
	}
}

// The same with a limit that is enforced. Each node is a container of the
// image that is shipped, and the kernel kills a container that uses more
// memory than it was given; there is no swap to go to. The nodes are given
// 384 MiB each and take in a gigabyte, which they have to do, restart with and
// deliver without being killed. A node finds the limit of its container by
// itself, as it does in production.
func TestRestartInContainersWithABacklogLargerThanTheirMemory(t *testing.T) {
	const (
		topic   = "backlog"
		group   = "backlog-workers"
		payload = 64 << 10
		limit   = 384 << 20
		backlog = 1 << 30
	)
	c := newContainerCluster(t, "--segment-size=8388608")
	c.runOptions = []string{fmt.Sprintf("--memory=%d", limit), fmt.Sprintf("--memory-swap=%d", limit)}
	c.startAll()
	c.alive("after they were started")

	events, accepted := publishBacklog(t, c, topic, payload, backlog/payload, 16)
	c.alive("while it took the backlog in")
	if accepted*payload < backlog*9/10 {
		t.Fatalf("%d MiB of the %d MiB that were sent were accepted, which is not the backlog the test is about", accepted*payload>>20, backlog>>20)
	}

	step(t, "killing every node with %d MiB undelivered, and starting them again", accepted*payload>>20)
	for _, n := range c.nodes {
		c.kill(n)
	}
	c.startAll()

	watch := &memoryWatch{t: t, c: c}
	look := func(during string) {
		t.Helper()
		c.alive(during)
		watch.look()
	}
	for until := time.Now().Add(20 * time.Second); time.Now().Before(until); time.Sleep(time.Second) {
		look("after the restart")
	}
	watch.within(limit, "after the restart")

	deliverBacklog(t, c, events, accepted, topic, group, look)
	watch.within(limit, "from the restart until the backlog was delivered")
	c.alive("after the backlog was delivered")
}

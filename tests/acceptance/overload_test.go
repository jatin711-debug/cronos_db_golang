//go:build acceptance

package acceptance

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
)

// gauge reads one unlabelled gauge from a node's metrics.
func (c *cluster) gauge(n *node, name string) (float64, error) {
	httpClient := http.Client{Timeout: 5 * time.Second}
	resp, err := httpClient.Get("http://" + n.httpAddr + "/metrics")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		if rest, found := strings.CutPrefix(scanner.Text(), name+" "); found {
			return strconv.ParseFloat(strings.TrimSpace(rest), 64)
		}
	}
	return 0, fmt.Errorf("%s has no gauge %s", n.id, name)
}

// held returns the most memory any node holds, in bytes.
func (c *cluster) held(t *testing.T) float64 {
	t.Helper()
	most := 0.0
	for _, n := range c.nodes {
		value, err := c.gauge(n, "cronos_memory_held_bytes")
		if err != nil {
			t.Fatalf("read the memory of %s: %v", n.id, err)
		}
		most = max(most, value)
	}
	return most
}

// Producers send more than the nodes can hold while nobody consumes. A node
// that takes everything it is sent runs out of memory, is killed, and comes
// back to the same backlog. Instead the nodes refuse publishes when they hold
// their share of their memory limit, stay up, and take publishes again when
// consumers have worked the backlog off. Nothing that was acknowledged is
// lost on the way.
func TestOverloadIsRefusedAndSurvived(t *testing.T) {
	const (
		topic    = "flood"
		group    = "flood-workers"
		payload  = 16 << 10
		headroom = 48 << 20 // what a node may take in before it refuses
		most     = 40000    // events; the nodes must have refused long before
	)
	// Segments large enough for these events; the other tests use tiny ones.
	c := newCluster(t, "--segment-size=8388608")
	c.startAll()
	// The limit is set from what the nodes hold when idle, so that the test
	// needs to send megabytes and not gigabytes to reach it.
	idle := c.held(t)
	limit := int64((idle + headroom) / 0.8)
	t.Logf("idle nodes hold up to %.0f MiB; restarting them with a memory limit of %d MiB", idle/(1<<20), limit>>20)
	for _, n := range c.nodes {
		c.kill(n)
	}
	c.extraArgs = append(c.extraArgs, fmt.Sprintf("--memory-limit=%d", limit))
	c.startAll()

	events := newLedger()
	producer, err := c.dial().NewProducer(client.DefaultProducerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	// Every event is due when the flood is over, and until then is held in
	// memory by the node that leads its partition.
	due := time.Now().Add(45 * time.Second)
	body := make([]byte, payload)
	var sent, refused atomic.Int64
	var failures sync.Map
	flood := func(worker int) {
		for {
			seq := sent.Add(1)
			if seq > most || refused.Load() >= 200 {
				return
			}
			id := fmt.Sprintf("flood-%d-%06d", worker, seq)
			events.noteSent(id, due.UnixMilli())
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			result, err := producer.Send(ctx, client.Message{MessageID: id, Topic: topic, Payload: body, ScheduleTS: due.UnixMilli()})
			cancel()
			switch {
			case err == nil:
				events.noteAccepted(id, result.PartitionID, result.Offset)
			case strings.Contains(err.Error(), "at capacity") || strings.Contains(err.Error(), "ResourceExhausted"):
				refused.Add(1)
				events.forget(id) // refused before anything was written
				time.Sleep(20 * time.Millisecond)
			case acceptedEarlier(err):
				events.noteAccepted(id, -1, -1)
			default:
				failures.Store(id, err.Error())
				events.forget(id) // outcome unknown: not counted either way
			}
		}
	}
	step(t, "publishing %d KiB events with no consumer until the nodes refuse", payload>>10)
	var publishers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		publishers.Add(1)
		go func() {
			defer publishers.Done()
			flood(worker)
		}()
	}
	publishers.Wait()

	accepted := events.accepted.Load()
	t.Logf("%d publishes accepted, %d refused for capacity", accepted, refused.Load())
	if refused.Load() == 0 {
		t.Fatalf("the nodes took all %d events (%d MiB) and refused nothing", accepted, accepted*payload>>20)
	}
	if accepted*payload < headroom/2 {
		t.Fatalf("the nodes refused after %d events (%d MiB), before they held anything to speak of", accepted, accepted*payload>>20)
	}
	failures.Range(func(id, reason any) bool {
		t.Logf("a publish failed for another reason: %s: %s", id, reason)
		return true
	})
	for _, n := range c.nodes {
		select {
		case <-n.exitedChan():
			t.Fatalf("%s died under the load", n.id)
		default:
		}
	}
	if now := c.held(t); now >= float64(limit) {
		t.Fatalf("a node holds %.0f MiB, which is its whole memory limit of %d MiB", now/(1<<20), limit>>20)
	} else {
		t.Logf("the fullest node holds %.0f MiB of its %d MiB limit", now/(1<<20), limit>>20)
	}

	// A consumer works the backlog off, and publishes are taken again.
	step(t, "starting a consumer")
	consumerClient := c.dial()
	ctx, cancel := context.WithCancel(context.Background())
	consumed := make(chan error, 1)
	cfg := client.DefaultConsumerConfig(topic, group)
	cfg.ReconnectBackoff = 200 * time.Millisecond
	cfg.MaxReconnectBackoff = 2 * time.Second
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

	deadline := due.Add(4 * time.Minute)
	for len(events.undelivered()) > 0 {
		if time.Now().After(deadline) {
			missing := events.undelivered()
			t.Fatalf("%d of the %d acknowledged events were never delivered:\n  %s", len(missing), accepted, sample(missing, 15))
		}
		time.Sleep(500 * time.Millisecond)
	}
	step(t, "all %d acknowledged events were delivered", accepted)

	again := time.Now().Add(90 * time.Second)
	for {
		id := fmt.Sprintf("after-%d", time.Now().UnixNano())
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		_, err := producer.Send(ctx, client.Message{MessageID: id, Topic: topic, Payload: []byte("small"), ScheduleTS: time.Now().Add(time.Hour).UnixMilli()})
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(again) {
			t.Fatalf("publishes are still refused after the backlog was delivered: %v", err)
		}
		time.Sleep(time.Second)
	}
	if early := events.tooEarly(); len(early) > 0 {
		t.Errorf("%d deliveries arrived before their scheduled time:\n  %s", len(early), sample(early, 15))
	}
}

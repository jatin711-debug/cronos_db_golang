//go:build acceptance

package acceptance

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
)

// sentEvent is what the test knows about one event it published.
type sentEvent struct {
	scheduleTS int64
	// accepted is true once the cluster acknowledged the publish. Until then
	// the event may or may not exist.
	accepted bool
	// partition and offset are where the acknowledgement placed it. offset is
	// -1 when the acknowledgement was the answer to a retry and carried none.
	partition int32
	offset    int64
	// deliveries counts how often the consumer group received it.
	deliveries int
}

// ledger records every publish and every delivery of a run, from outside the
// cluster.
type ledger struct {
	mu   sync.Mutex
	sent map[string]*sentEvent
	// early lists deliveries that arrived before their scheduled time, and
	// unknown deliveries of events this run never published.
	early   []earlyDelivery
	unknown []string
	// clockAhead is how many milliseconds the nodes' clocks may be ahead of
	// this machine's. It is zero when the nodes run on this machine. Nodes in
	// containers can run on a clock of their own, and what such a node
	// delivers on time is early by ours by the difference.
	clockAhead atomic.Int64

	accepted  atomic.Int64
	delivered atomic.Int64 // distinct events delivered
	// acceptedByPartition lets a fault wait until a partition takes publishes
	// again.
	acceptedByPartition [partitionCount]atomic.Int64
}

// earlyDelivery is a delivery that arrived before its time by this machine's
// clock, or under another time than the event was published with.
type earlyDelivery struct {
	what string
	by   int64 // milliseconds early
	// wrongTime is set when the delivery named another scheduled time than
	// the publish did, which no clock explains.
	wrongTime bool
}

func newLedger() *ledger { return &ledger{sent: make(map[string]*sentEvent)} }

// tooEarly lists the deliveries that arrived earlier than the difference
// between the clocks allows.
func (l *ledger) tooEarly() []string {
	allowed := l.clockAhead.Load()
	l.mu.Lock()
	defer l.mu.Unlock()
	var early []string
	for _, delivery := range l.early {
		if delivery.wrongTime || delivery.by > allowed {
			early = append(early, delivery.what)
		}
	}
	return early
}

// noteSent records an event before its first publish attempt: a delivery can
// arrive before the publish call returns.
func (l *ledger) noteSent(id string, scheduleTS int64) {
	l.mu.Lock()
	l.sent[id] = &sentEvent{scheduleTS: scheduleTS, partition: -1, offset: -1}
	l.mu.Unlock()
}

func (l *ledger) noteAccepted(id string, partition int32, offset int64) {
	l.mu.Lock()
	event := l.sent[id]
	first := !event.accepted
	event.accepted = true
	if offset >= 0 {
		event.partition, event.offset = partition, offset
	}
	l.mu.Unlock()
	if first {
		l.accepted.Add(1)
		if partition >= 0 && partition < partitionCount {
			l.acceptedByPartition[partition].Add(1)
		}
	}
}

func (l *ledger) noteDelivered(id string, scheduleTS, receivedAt int64, partition int32, offset int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	event, known := l.sent[id]
	if !known {
		l.unknown = append(l.unknown, fmt.Sprintf("%s (partition %d offset %d)", id, partition, offset))
		return
	}
	if receivedAt < event.scheduleTS || scheduleTS != event.scheduleTS {
		l.early = append(l.early, earlyDelivery{
			what: fmt.Sprintf("%s: scheduled for %d (delivery says %d), received at %d, %d ms early",
				id, event.scheduleTS, scheduleTS, receivedAt, event.scheduleTS-receivedAt),
			by:        event.scheduleTS - receivedAt,
			wrongTime: scheduleTS != event.scheduleTS,
		})
	}
	if event.deliveries == 0 {
		l.delivered.Add(1)
	}
	event.deliveries++
}

// forget removes an event from the record: the test has established that the
// cluster does not owe it.
func (l *ledger) forget(id string) {
	l.mu.Lock()
	delete(l.sent, id)
	l.mu.Unlock()
}

// undelivered lists the acknowledged events the consumer group has not
// received.
func (l *ledger) undelivered() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var missing []string
	for id, event := range l.sent {
		if event.accepted && event.deliveries == 0 {
			missing = append(missing, fmt.Sprintf("%s (partition %d offset %d, due %s)",
				id, event.partition, event.offset, time.UnixMilli(event.scheduleTS).Format("15:04:05.000")))
		}
	}
	sort.Strings(missing)
	return missing
}

// latestSchedule returns the latest scheduled time among the published events.
func (l *ledger) latestSchedule() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	latest := int64(0)
	for _, event := range l.sent {
		latest = max(latest, event.scheduleTS)
	}
	return time.UnixMilli(latest)
}

func (l *ledger) snapshot() map[string]sentEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]sentEvent, len(l.sent))
	for id, event := range l.sent {
		out[id] = *event
	}
	return out
}

func sample(items []string, limit int) string {
	if len(items) > limit {
		return strings.Join(items[:limit], "\n  ") + fmt.Sprintf("\n  ... and %d more", len(items)-limit)
	}
	return strings.Join(items, "\n  ")
}

// workload publishes scheduled events and consumes them with one consumer
// group, through the client library, until it is stopped.
type workload struct {
	t      *testing.T
	ledger *ledger
	topic  string
	group  string
	run    string

	stopPublishing chan struct{}
	publishers     sync.WaitGroup
	consumers      []*groupMember
}

// groupMember is one consumer of the workload's group, with its own
// connection to the cluster, as a separate program would have.
type groupMember struct {
	cancel context.CancelFunc
	done   chan error
	left   bool
}

// startWorkload starts publishers that each send about ratePerPublisher
// events a second, due between half a second and five seconds later, and a
// consumer group of the given size that acknowledges everything it receives.
func startWorkload(t *testing.T, c *cluster, topic, group string, publishers, ratePerPublisher, consumers int) *workload {
	t.Helper()
	w := &workload{
		t:              t,
		ledger:         newLedger(),
		topic:          topic,
		group:          group,
		run:            fmt.Sprintf("r%d", time.Now().UnixNano()%1_000_000),
		stopPublishing: make(chan struct{}),
	}

	for i := 0; i < consumers; i++ {
		consumerClient := c.dial()
		ctx, cancel := context.WithCancel(context.Background())
		member := &groupMember{cancel: cancel, done: make(chan error, 1)}
		w.consumers = append(w.consumers, member)
		cfg := client.DefaultConsumerConfig(topic, group)
		cfg.ReconnectBackoff = 200 * time.Millisecond
		cfg.MaxReconnectBackoff = 2 * time.Second
		go func() {
			member.done <- consumerClient.Subscribe(ctx, cfg, func(_ context.Context, d client.Delivery) error {
				now := time.Now().UnixMilli()
				if d.Event != nil {
					w.ledger.noteDelivered(d.Event.GetMessageId(), d.Event.GetScheduleTs(), now, d.Event.GetPartitionId(), d.Event.GetOffset())
				}
				for _, event := range d.Batch {
					w.ledger.noteDelivered(event.GetMessageId(), event.GetScheduleTs(), now, event.GetPartitionId(), event.GetOffset())
				}
				return nil
			})
		}()
	}

	producerClient := c.dial()
	producer, err := producerClient.NewProducer(client.DefaultProducerConfig())
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close() })
	interval := time.Second / time.Duration(ratePerPublisher)
	for worker := 0; worker < publishers; worker++ {
		w.publishers.Add(1)
		go func() {
			defer w.publishers.Done()
			w.publish(producer, worker, interval)
		}()
	}
	return w
}

// publish sends events one after another. An event whose publish fails is
// sent again under the same ID until the cluster acknowledges it, which is
// what an application that must not lose it does.
func (w *workload) publish(producer *client.Producer, worker int, interval time.Duration) {
	for seq := 0; ; seq++ {
		select {
		case <-w.stopPublishing:
			return
		case <-time.After(interval):
		}
		id := fmt.Sprintf("%s-w%d-%06d", w.run, worker, seq)
		due := time.Now().Add(500*time.Millisecond + time.Duration(seq%10)*500*time.Millisecond).UnixMilli()
		w.ledger.noteSent(id, due)

		// After the workload is told to stop, the event in hand still gets a
		// while to be acknowledged, so that few outcomes stay unknown.
		var giveUp <-chan time.Time
		stop := w.stopPublishing
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			result, err := producer.Send(ctx, client.Message{MessageID: id, Topic: w.topic, Payload: []byte(id), ScheduleTS: due})
			cancel()
			if err == nil {
				w.ledger.noteAccepted(id, result.PartitionID, result.Offset)
				break
			}
			if acceptedEarlier(err) {
				// An earlier attempt was accepted and its answer got lost.
				w.ledger.noteAccepted(id, -1, -1)
				break
			}
			select {
			case <-stop:
				stop, giveUp = nil, time.After(45*time.Second)
			case <-giveUp:
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
}

// acceptedEarlier reports whether a publish was refused because the cluster
// already accepted an event with its ID.
func acceptedEarlier(err error) bool {
	message := err.Error()
	return strings.Contains(message, "duplicate message_id") &&
		!strings.Contains(message, "in progress") && !strings.Contains(message, "unknown")
}

// consumerStopped reports whether a consumer ended by itself, which it must
// not: it is expected to follow the cluster through every fault.
func (w *workload) consumerStopped() (bool, error) {
	for _, member := range w.consumers {
		if member.left {
			continue
		}
		select {
		case err := <-member.done:
			member.done <- err
			return true, err
		default:
		}
	}
	return false, nil
}

// leave stops one consumer of the group, as a program that is shut down or
// crashes does. What it had received and not acknowledged is the group's to
// deliver again.
func (w *workload) leave(index int) {
	w.t.Helper()
	member := w.consumers[index]
	if stopped, err := w.consumerStopped(); stopped {
		w.t.Fatalf("a consumer stopped by itself: %v", err)
	}
	member.left = true
	member.cancel()
	select {
	case <-member.done:
	case <-time.After(30 * time.Second):
		w.t.Errorf("consumer %d did not stop", index)
	}
}

// waitProgress waits until read has grown by at least by, and fails the test
// with what if it has not within timeout.
func (w *workload) waitProgress(read func() int64, by int64, timeout time.Duration, what string) {
	w.t.Helper()
	start := read()
	deadline := time.Now().Add(timeout)
	for read()-start < by {
		if stopped, err := w.consumerStopped(); stopped {
			w.t.Fatalf("a consumer stopped by itself: %v", err)
		}
		if time.Now().After(deadline) {
			w.t.Fatalf("%s: only %d of the expected %d within %s", what, read()-start, by, timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// stopPublishers ends publishing and waits for the publishers to finish the
// events they have in hand.
func (w *workload) stopPublishers() {
	close(w.stopPublishing)
	w.publishers.Wait()
}

// stopConsumers ends the consumers that are still running and reports an
// error if one had already ended by itself.
func (w *workload) stopConsumers() {
	w.t.Helper()
	if stopped, err := w.consumerStopped(); stopped {
		w.t.Errorf("a consumer stopped by itself: %v", err)
		return
	}
	for index, member := range w.consumers {
		if !member.left {
			w.leave(index)
		}
	}
}

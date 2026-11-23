//go:build acceptance

package acceptance

import (
	"testing"
	"time"
)

// A leader that is lost and comes back is handed its partitions again. It
// must first be told what the consumer groups finished while it was away, or
// it delivers all of that a second time: up to v0.14.1 the handoff waited for
// its log alone, and a node that had been away for five minutes of a soak
// repeated 64,000 events per group twelve seconds after it returned.
//
// Losing a leader repeats some deliveries whatever is done: those on their
// way when it stopped, and the ones it had recorded as finished but not yet
// told its followers. Those are a few. What is appended to its partitions
// while it is away is hundreds here, and none of that may be repeated.
func TestLeaderThatReturnsDoesNotDeliverAgainWhatWasFinished(t *testing.T) {
	c := newCluster(t)
	c.startAll()
	observer := c.dial()
	w := startWorkload(t, c, "returning", "returning-workers", 3, 20, 2)
	accepted := func() int64 { return w.ledger.accepted.Load() }
	delivered := func() int64 { return w.ledger.delivered.Load() }
	w.waitProgress(accepted, 100, 60*time.Second, "publishes accepted by the healthy cluster")
	w.waitProgress(delivered, 50, 60*time.Second, "events delivered by the healthy cluster")

	// The node that leads the most partitions, so that as much as possible
	// is appended to what it will be handed back.
	leads := make(map[*node][]int32)
	for partition := int32(0); partition < partitionCount; partition++ {
		leader := c.leaderOf(observer, partition)
		leads[leader] = append(leads[leader], partition)
	}
	var away *node
	for n, partitions := range leads {
		if away == nil || len(partitions) > len(leads[away]) {
			away = n
		}
	}
	led := leads[away]

	const whileAway = 1600
	step(t, "stopping %s, which leads partitions %v, until %d more publishes are accepted", away.id, led, whileAway)
	c.kill(away)
	w.waitProgress(accepted, whileAway, 180*time.Second, "publishes accepted while "+away.id+" is away")
	w.waitProgress(delivered, whileAway/2, 180*time.Second, "events delivered while "+away.id+" is away")
	repeatedBefore := repeatedDeliveries(w)

	step(t, "starting %s again; %d events were delivered twice by losing it", away.id, repeatedBefore)
	c.start(away)
	c.waitReady(c.nodes...)
	deadline := time.Now().Add(120 * time.Second)
	for {
		back := 0
		for _, partition := range led {
			if c.leaderOf(observer, partition) == away {
				back++
			}
		}
		if back == len(led) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s leads %d of its %d partitions again after 120s", away.id, back, len(led))
		}
		time.Sleep(500 * time.Millisecond)
	}
	step(t, "%s leads partitions %v again", away.id, led)
	w.waitProgress(accepted, 200, 90*time.Second, "publishes accepted after the handoff")
	w.waitProgress(delivered, 200, 90*time.Second, "events delivered after the handoff")

	w.stopPublishers()
	checkDelivery(t, w)
	w.stopConsumers()

	// About whileAway*len(led)/partitionCount events were appended to the
	// partitions it was handed back. A tenth of that leaves room for the
	// deliveries a change of leader interrupts.
	appended := whileAway * len(led) / int(partitionCount)
	repeated := repeatedDeliveries(w) - repeatedBefore
	t.Logf("%d events were delivered twice after %s returned; about %d had been appended to its partitions while it was away", repeated, away.id, appended)
	if limit := appended / 10; repeated > limit {
		t.Fatalf("%d events were delivered twice after %s was handed its partitions back, want at most %d: it led before it knew what the group had finished",
			repeated, away.id, limit)
	}
	checkLogs(t, c, w.topic, w.ledger, false)
}

// repeatedDeliveries counts the events the workload's group was delivered
// more than once.
func repeatedDeliveries(w *workload) int {
	repeated := 0
	for _, event := range w.ledger.snapshot() {
		if event.deliveries > 1 {
			repeated++
		}
	}
	return repeated
}

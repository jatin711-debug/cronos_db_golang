//go:build acceptance

package acceptance

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

const (
	campaignTopic = "campaign"
	campaignGroup = "campaign-workers"
)

// The fault campaign. Producers and a consumer group work against a three-node
// cluster while its nodes are killed, frozen and restarted, one fault after
// another. Nothing a producer was told is stored may be missing afterwards.
func TestFaultCampaign(t *testing.T) {
	c := newCluster(t)
	c.startAll()
	observer := c.dial()
	w := startWorkload(t, c, campaignTopic, campaignGroup, 3, 20, 2)
	accepted := func() int64 { return w.ledger.accepted.Load() }
	delivered := func() int64 { return w.ledger.delivered.Load() }
	acceptedOn := func(partition int32) func() int64 {
		return func() int64 { return w.ledger.acceptedByPartition[partition].Load() }
	}

	// The cluster works before anything is done to it.
	w.waitProgress(accepted, 100, 60*time.Second, "publishes accepted by the healthy cluster")
	w.waitProgress(delivered, 50, 60*time.Second, "events delivered by the healthy cluster")

	// settle waits until the cluster is whole again and both directions flow.
	settle := func() {
		t.Helper()
		c.waitReady(c.nodes...)
		w.waitProgress(accepted, 60, 60*time.Second, "publishes accepted after the fault")
		w.waitProgress(delivered, 60, 60*time.Second, "events delivered after the fault")
	}
	killed := map[string]bool{}
	crash := func(n *node) {
		c.kill(n)
		killed[n.id] = true
	}

	// The leader of a partition is killed. The partition must be served by
	// another replica, without the node.
	leader := c.leaderOf(observer, 0)
	step(t, "killing %s, which leads partition 0", leader.id)
	crash(leader)
	w.waitProgress(acceptedOn(0), 30, 90*time.Second, "publishes accepted on partition 0 without its old leader")
	c.start(leader)
	settle()

	// A follower is killed.
	leader = c.leaderOf(observer, 0)
	follower := c.nodes[(leader.index+1)%len(c.nodes)]
	step(t, "killing %s, a follower of partition 0 (led by %s)", follower.id, leader.id)
	crash(follower)
	w.waitProgress(accepted, 150, 90*time.Second, "publishes accepted with one replica down")
	c.start(follower)
	settle()

	// A leader is frozen and comes back. The node does not know it stopped:
	// when it runs again it still believes it leads, with whatever it had
	// appended and not replicated.
	leader = c.leaderOf(observer, 0)
	step(t, "freezing %s, which leads partition 0", leader.id)
	c.freeze(leader)
	w.waitProgress(acceptedOn(0), 30, 90*time.Second, "publishes accepted on partition 0 while its old leader is frozen")
	step(t, "letting %s run again", leader.id)
	c.thaw(leader)
	settle()

	// One of the group's two consumers goes away. What it had been given and
	// not acknowledged goes to the one that is left.
	step(t, "one of the two consumers leaves")
	w.leave(1)
	settle()

	// Every node has been killed once, the one that created the cluster too.
	for _, n := range c.nodes {
		if killed[n.id] {
			continue
		}
		step(t, "killing %s", n.id)
		crash(n)
		w.waitProgress(accepted, 150, 90*time.Second, "publishes accepted with "+n.id+" down")
		c.start(n)
		settle()
	}

	// A node loses its disk and is replaced: it comes back under its name
	// with nothing, and is killed once more while it is being filled.
	replaced := c.nodes[(c.leaderOf(observer, 0).index+1)%len(c.nodes)]
	if replaced.index == 0 {
		// The first node is the one that creates a cluster when it finds no
		// state. Replacing it is a procedure of its own.
		replaced = c.nodes[(replaced.index+1)%len(c.nodes)]
	}
	step(t, "replacing %s with an empty node", replaced.id)
	c.kill(replaced)
	c.wipe(replaced)
	c.start(replaced)
	time.Sleep(1500 * time.Millisecond)
	step(t, "killing %s while it is being filled", replaced.id)
	c.kill(replaced)
	w.waitProgress(accepted, 60, 90*time.Second, "publishes accepted while "+replaced.id+" is down")
	c.start(replaced)
	settle()

	// The whole cluster is killed at once.
	step(t, "killing every node")
	for _, n := range c.nodes {
		c.kill(n)
	}
	time.Sleep(2 * time.Second)
	for _, n := range c.nodes {
		c.start(n)
	}
	settle()

	step(t, "faults done; waiting for the remaining deliveries")
	w.stopPublishers()
	checkDelivery(t, w)
	w.stopConsumers()
	checkLogs(t, c, w)
}

// step logs what the campaign does next, with the time, so that it can be
// matched with the node logs.
func step(t *testing.T, format string, a ...any) {
	t.Helper()
	t.Logf("%s  %s", time.Now().Format("15:04:05.000"), fmt.Sprintf(format, a...))
}

// checkDelivery waits until every acknowledged event has been delivered and
// reports the ones that were not, and everything that was delivered wrongly.
func checkDelivery(t *testing.T, w *workload) {
	t.Helper()
	deadline := w.ledger.latestSchedule().Add(90 * time.Second)
	for len(w.ledger.undelivered()) > 0 && time.Now().Before(deadline) {
		if stopped, err := w.consumerStopped(); stopped {
			t.Fatalf("a consumer stopped by itself: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
	}

	events := w.ledger.snapshot()
	acceptedCount, unknownOutcome, redelivered := 0, 0, 0
	for _, event := range events {
		if event.accepted {
			acceptedCount++
		} else {
			unknownOutcome++
		}
		if event.deliveries > 1 {
			redelivered++
		}
	}
	t.Logf("published %d events: %d acknowledged, %d with unknown outcome; %d delivered more than once",
		len(events), acceptedCount, unknownOutcome, redelivered)

	if missing := w.ledger.undelivered(); len(missing) > 0 {
		t.Errorf("%d acknowledged events were never delivered:\n  %s", len(missing), sample(missing, 25))
	}
	w.ledger.mu.Lock()
	early, unknown := w.ledger.early, w.ledger.unknown
	w.ledger.mu.Unlock()
	if len(early) > 0 {
		t.Errorf("%d deliveries arrived before their scheduled time:\n  %s", len(early), sample(early, 25))
	}
	if len(unknown) > 0 {
		t.Errorf("%d deliveries were of events nobody published:\n  %s", len(unknown), sample(unknown, 25))
	}
}

// checkLogs compares the log of every partition across its replicas, and the
// log with what producers were told.
func checkLogs(t *testing.T, c *cluster, w *workload) {
	t.Helper()
	logs := make([][]*types.Event, partitionCount)
	for partition := int32(0); partition < partitionCount; partition++ {
		logs[partition] = agreedLog(t, c, partition, w.topic)
	}

	type place struct {
		partition int32
		offset    int64
	}
	places := make(map[string][]place)
	for partition, log := range logs {
		for _, event := range log {
			places[event.GetMessageId()] = append(places[event.GetMessageId()], place{int32(partition), event.GetOffset()})
		}
	}
	var wrong []string
	for id, event := range w.ledger.snapshot() {
		at := places[id]
		switch {
		case len(at) > 1:
			wrong = append(wrong, fmt.Sprintf("%s is in the log %d times: %v", id, len(at), at))
		case event.accepted && len(at) == 0:
			wrong = append(wrong, fmt.Sprintf("%s was acknowledged and is not in the log", id))
		case event.accepted && event.offset >= 0 && (at[0].partition != event.partition || at[0].offset != event.offset):
			wrong = append(wrong, fmt.Sprintf("%s was acknowledged at partition %d offset %d and is at partition %d offset %d",
				id, event.partition, event.offset, at[0].partition, at[0].offset))
		}
	}
	if len(wrong) > 0 {
		t.Errorf("%d events are not where their producers were told:\n  %s", len(wrong), sample(wrong, 25))
	}
}

// agreedLog waits until the replicas of a partition hold logs of the same
// length, checks that the logs are the same entry for entry, and returns it.
func agreedLog(t *testing.T, c *cluster, partition int32, topic string) []*types.Event {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var logs [][]*types.Event
	for {
		logs = logs[:0]
		same := true
		for _, n := range c.nodes {
			log, err := c.readLog(n, partition, topic)
			if err != nil {
				same = false
				if time.Now().After(deadline) {
					t.Fatalf("read the log of partition %d on %s: %v", partition, n.id, err)
				}
				break
			}
			logs = append(logs, log)
			same = same && len(log) == len(logs[0])
		}
		if same || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	reference := logs[0]
	for i, log := range logs[1:] {
		other := c.nodes[i+1]
		if len(log) != len(reference) {
			t.Errorf("partition %d: %s holds %d entries and %s holds %d", partition, c.nodes[0].id, len(reference), other.id, len(log))
		}
		for j := 0; j < min(len(log), len(reference)); j++ {
			a, b := reference[j], log[j]
			if a.GetOffset() != b.GetOffset() || a.GetTerm() != b.GetTerm() || a.GetMessageId() != b.GetMessageId() ||
				a.GetScheduleTs() != b.GetScheduleTs() || !bytes.Equal(a.GetPayload(), b.GetPayload()) {
				t.Errorf("partition %d: the logs differ at entry %d:\n  %s: offset %d term %d id %s\n  %s: offset %d term %d id %s",
					partition, j, c.nodes[0].id, a.GetOffset(), a.GetTerm(), a.GetMessageId(), other.id, b.GetOffset(), b.GetTerm(), b.GetMessageId())
				break
			}
		}
	}
	for j, event := range reference {
		if event.GetOffset() != int64(j) {
			t.Errorf("partition %d: entry %d has offset %d", partition, j, event.GetOffset())
			break
		}
	}
	t.Logf("partition %d: %d entries on every replica", partition, len(reference))
	return reference
}

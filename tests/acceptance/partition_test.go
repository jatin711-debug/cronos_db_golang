//go:build acceptance

package acceptance

import (
	"testing"
	"time"
)

// The network between the nodes fails while producers and consumers work.
// No node stops: each goes on running, and applications still reach every
// one of them, but some of the nodes cannot hear each other. This is where a
// replicated system is tempted to have two leaders for one partition, each
// taking publishes, and to throw one side's away when the network returns.
//
// What must hold is what holds for the other faults: nothing that was
// acknowledged is lost, nothing is delivered before its time, and the
// replicas end with the same log. A node that is cut off from the others
// must therefore acknowledge nothing: it cannot know that a second replica
// has what it took.
func TestNetworkPartitions(t *testing.T) {
	c := newContainerCluster(t)
	c.startAll()
	observer := c.dial()
	w := startWorkload(t, c, "partitioned", "partitioned-workers", 2, 20, 2)
	accepted := func() int64 { return w.ledger.accepted.Load() }
	delivered := func() int64 { return w.ledger.delivered.Load() }
	acceptedOn := func(partition int32) func() int64 {
		return func() int64 { return w.ledger.acceptedByPartition[partition].Load() }
	}
	w.waitProgress(accepted, 100, 60*time.Second, "publishes accepted by the healthy cluster")
	w.waitProgress(delivered, 50, 60*time.Second, "events delivered by the healthy cluster")

	// restore brings every link back and waits until the cluster is whole
	// and both directions flow.
	restore := func() {
		t.Helper()
		step(t, "restoring the links")
		c.heal()
		c.waitReady(c.nodes...)
		w.waitProgress(accepted, 60, 90*time.Second, "publishes accepted after the links were restored")
		w.waitProgress(delivered, 60, 90*time.Second, "events delivered after the links were restored")
	}
	isolate := func(n *node) {
		t.Helper()
		for _, other := range c.others(n) {
			c.cut(n, other)
		}
	}

	// The leader of a partition loses both other nodes. It still runs and
	// still answers applications. The two that see each other must take the
	// partition over; the one on its own must stop acknowledging.
	leader := c.leaderOf(observer, 0)
	step(t, "cutting %s, which leads partition 0, off from the other nodes", leader.id)
	isolate(leader)
	w.waitProgress(acceptedOn(0), 30, 90*time.Second, "publishes accepted on partition 0 by the two nodes that still see each other")
	time.Sleep(10 * time.Second) // long enough for the node on its own to act on what it believes
	alone := leader
	restore()

	// One link fails: two nodes cannot reach each other, and both reach the
	// third. Each of the two counts the other as gone while the third sees
	// everybody, so the nodes disagree about who is there.
	leader = c.leaderOf(observer, 0)
	other := c.others(leader)[0]
	step(t, "cutting the link between %s, which leads partition 0, and %s", leader.id, other.id)
	c.cut(leader, other)
	w.waitProgress(accepted, 150, 90*time.Second, "publishes accepted with one link down")
	time.Sleep(15 * time.Second)
	restore()

	// Every other node on its own in turn. One of the three leads Raft, so
	// the cluster loses the node that decides who leads what at least once.
	for _, n := range c.nodes {
		if n == alone {
			continue
		}
		step(t, "cutting %s off from the other nodes", n.id)
		isolate(n)
		w.waitProgress(accepted, 150, 90*time.Second, "publishes accepted with "+n.id+" cut off")
		restore()
	}

	// No node reaches any other. Nothing can be acknowledged, and nothing
	// may be; when the links return the cluster must find itself again.
	step(t, "cutting every link")
	c.cut(c.nodes[0], c.nodes[1])
	c.cut(c.nodes[0], c.nodes[2])
	c.cut(c.nodes[1], c.nodes[2])
	time.Sleep(20 * time.Second)
	restore()

	step(t, "faults done; waiting for the remaining deliveries")
	w.stopPublishers()
	checkDelivery(t, w)
	w.stopConsumers()
	checkLogs(t, c, w.topic, w.ledger, false)
}

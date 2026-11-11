//go:build acceptance

package acceptance

import (
	"testing"
	"time"
)

// The node that created the cluster loses its disk and comes back under its
// name with nothing. It must find the cluster it belonged to and be filled
// from it. It must not do again what it did the first time it had no state,
// which was to create a cluster: there would then be two.
//
// In the production chart this node is pod 0 with a replaced volume.
func TestFirstNodeReplaced(t *testing.T) {
	c := newCluster(t)
	c.startAll()
	w := startWorkload(t, c, "replaced", "replaced-workers", 2, 20, 1)
	accepted := func() int64 { return w.ledger.accepted.Load() }
	delivered := func() int64 { return w.ledger.delivered.Load() }
	w.waitProgress(accepted, 100, 60*time.Second, "publishes accepted by the healthy cluster")
	w.waitProgress(delivered, 50, 60*time.Second, "events delivered by the healthy cluster")

	first, second := c.nodes[0], c.nodes[1]
	step(t, "replacing %s, which created the cluster, with an empty node", first.id)
	c.kill(first)
	c.wipe(first)
	c.start(first)
	c.waitReady(c.nodes...)
	// It created the cluster once, when the test began, and not again now.
	if created := c.logged(first, "Bootstrapping new Raft cluster"); created != 1 {
		t.Fatalf("%s created a cluster %d times; the second is a second cluster beside the one it belonged to", first.id, created)
	}
	w.waitProgress(accepted, 100, 90*time.Second, "publishes accepted after "+first.id+" was replaced")
	w.waitProgress(delivered, 100, 90*time.Second, "events delivered after "+first.id+" was replaced")

	// With another node down, the cluster needs the replaced one: for the
	// second acknowledgement of every publish, and for a majority in Raft.
	step(t, "killing %s; the cluster now depends on %s", second.id, first.id)
	c.kill(second)
	w.waitProgress(accepted, 100, 90*time.Second, "publishes accepted by "+first.id+" and one other node")
	c.start(second)
	c.waitReady(c.nodes...)
	w.waitProgress(delivered, 60, 90*time.Second, "events delivered once every node is back")

	step(t, "waiting for the remaining deliveries")
	w.stopPublishers()
	checkDelivery(t, w)
	w.stopConsumers()
	// Every acknowledged event is where its producer was told, not only the
	// same on every replica: replicas that all removed their logs agree too.
	checkLogs(t, c, w.topic, w.ledger, false)
}

// A node that leads a partition loses its disk and comes back under its name
// at once, before the cluster has seen it go. The committed assignment still
// names it leader. It must not lead with an empty log: the other replicas
// would take its log for the leader's and remove everything they hold to match
// it, events acknowledged and not yet delivered among them.
//
// In the production chart this is a pod whose volume is replaced and that is
// started again within the failure detector's timeout.
func TestLeaderReplacedByAnEmptyNode(t *testing.T) {
	c := newCluster(t)
	c.startAll()
	observer := c.dial()
	w := startWorkload(t, c, "wiped", "wiped-workers", 2, 20, 1)
	accepted := func() int64 { return w.ledger.accepted.Load() }
	delivered := func() int64 { return w.ledger.delivered.Load() }
	w.waitProgress(accepted, 200, 60*time.Second, "publishes accepted by the healthy cluster")
	w.waitProgress(delivered, 100, 60*time.Second, "events delivered by the healthy cluster")

	// A failover first, so that the log starts with entries of an earlier
	// term than the leader's. An empty leader's first entry then differs in
	// term from the followers' first entry, and they used to remove their
	// whole logs to match it, rather than refuse it as a conflict within one
	// term (which stopped the partition instead).
	first := c.leaderOf(observer, 0)
	step(t, "killing %s, which leads partition 0, for a failover", first.id)
	c.kill(first)
	w.waitProgress(accepted, 100, 90*time.Second, "publishes accepted after the failover")
	c.start(first)
	c.waitReady(c.nodes...)
	w.waitProgress(accepted, 100, 90*time.Second, "publishes accepted once "+first.id+" is back")

	leader := c.leaderOf(observer, 0)
	step(t, "replacing %s, which leads partition 0, with an empty node", leader.id)
	c.kill(leader)
	c.wipe(leader)
	c.start(leader)
	c.waitReady(c.nodes...)
	w.waitProgress(accepted, 100, 90*time.Second, "publishes accepted after "+leader.id+" was replaced")
	w.waitProgress(delivered, 100, 90*time.Second, "events delivered after "+leader.id+" was replaced")

	step(t, "waiting for the remaining deliveries")
	w.stopPublishers()
	checkDelivery(t, w)
	w.stopConsumers()
	checkLogs(t, c, w.topic, w.ledger, false)
}

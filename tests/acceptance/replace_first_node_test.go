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
	for partition := int32(0); partition < partitionCount; partition++ {
		agreedLog(t, c, partition, w.topic)
	}
}

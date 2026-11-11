//go:build acceptance

package acceptance

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

// replicationFactor is the --replication-factor every test cluster runs with.
const replicationFactor = 3

// placementTimeout bounds how long replicas take to settle after the
// membership changed: placement moves them, and the new ones catch up.
const placementTimeout = 5 * time.Minute

// running returns the nodes that run.
func (c *cluster) running() []*node {
	var up []*node
	for _, n := range c.nodes {
		if n.running() {
			up = append(up, n)
		}
	}
	return up
}

// waitPlacement waits until every running node answers with a topology in
// which each partition has replicationFactor replicas, all of them running
// and in sync, reported by a running leader, and the same in every answer. It
// returns each partition's replicas, sorted.
func (c *cluster) waitPlacement(running []*node, what string, timeout time.Duration) map[int32][]string {
	c.t.Helper()
	alive := make(map[string]bool, len(running))
	for _, n := range running {
		alive[n.id] = true
	}
	check := func() (map[int32][]string, string) {
		placement := make(map[int32][]string)
		for _, n := range running {
			topology, err := c.topology(n)
			if err != nil {
				return nil, fmt.Sprintf("%s: %v", n.id, err)
			}
			if len(topology.GetPartitions()) != partitionCount {
				return nil, fmt.Sprintf("%s lists %d partitions", n.id, len(topology.GetPartitions()))
			}
			for _, p := range topology.GetPartitions() {
				id := p.GetPartitionId()
				replicas := slices.Sorted(slices.Values(p.GetReplicas()))
				inSync := slices.Sorted(slices.Values(p.GetIsr()))
				switch {
				case !p.GetLeaderReported():
					return nil, fmt.Sprintf("%s: partition %d is not reported by its leader %q", n.id, id, p.GetLeaderId())
				case !alive[p.GetLeaderId()]:
					return nil, fmt.Sprintf("%s: partition %d is led by %q, which does not run", n.id, id, p.GetLeaderId())
				case len(replicas) != replicationFactor:
					return nil, fmt.Sprintf("%s: partition %d has replicas %v", n.id, id, replicas)
				case !slices.Equal(replicas, inSync):
					return nil, fmt.Sprintf("%s: partition %d has replicas %v and %v in sync", n.id, id, replicas, inSync)
				}
				for _, replica := range replicas {
					if !alive[replica] {
						return nil, fmt.Sprintf("%s: partition %d has %s as a replica, which does not run", n.id, id, replica)
					}
					if _, ok := p.GetFollowerOffsets()[replica]; !ok {
						return nil, fmt.Sprintf("%s: partition %d has no offset for %s", n.id, id, replica)
					}
				}
				if earlier, seen := placement[id]; seen && !slices.Equal(earlier, replicas) {
					return nil, fmt.Sprintf("partition %d has replicas %v for one node and %v for %s", id, earlier, replicas, n.id)
				}
				placement[id] = replicas
			}
		}
		return placement, ""
	}
	deadline := time.Now().Add(timeout)
	for {
		placement, problem := check()
		if problem == "" {
			return placement
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("placement %s: %s", what, problem)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// holders returns the cluster as far as one partition is concerned: its
// replicas, where the cluster has more nodes than a partition has replicas.
func (c *cluster) holders(partition int32) *cluster {
	c.t.Helper()
	if len(c.nodes) <= replicationFactor {
		return c
	}
	placement := c.waitPlacement(c.running(), "before the logs are compared", placementTimeout)
	sub := *c
	sub.nodes = nil
	for _, id := range placement[partition] {
		sub.nodes = append(sub.nodes, c.nodeByID(id))
	}
	return &sub
}

// waitNewReplica waits until the placement of a partition names one of the
// newcomers, and returns the partition, the node that leads it, which is not
// a newcomer, and whether the newcomer was still catching up.
func (c *cluster) waitNewReplica(newcomers []*node, timeout time.Duration) (int32, *node, bool) {
	c.t.Helper()
	isNew := func(id string) bool {
		return slices.ContainsFunc(newcomers, func(n *node) bool { return n.id == id })
	}
	deadline := time.Now().Add(timeout)
	for {
		for _, from := range c.running() {
			topology, err := c.topology(from)
			if err != nil {
				continue
			}
			for _, p := range topology.GetPartitions() {
				leader := c.nodeByID(p.GetLeaderId())
				if leader == nil || isNew(leader.id) || !leader.running() {
					continue
				}
				for _, replica := range p.GetReplicas() {
					if isNew(replica) {
						return p.GetPartitionId(), leader, !slices.Contains(p.GetIsr(), replica)
					}
				}
			}
			break
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("no partition was placed on the new nodes within %s", timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func describePlacement(placement map[int32][]string) string {
	parts := make([]string, 0, len(placement))
	for p := int32(0); p < partitionCount; p++ {
		parts = append(parts, fmt.Sprintf("%d:%s", p, strings.Join(placement[p], ",")))
	}
	return strings.Join(parts, " ")
}

// A cluster of three grows to five while producers and a consumer group
// work, and is then run the way a cluster of five is: placement moves
// replicas to the new nodes; a leader is lost while a new replica is being
// filled; nodes are restarted one at a time; one is replaced by an empty
// node; one is lost for good and removed, and the four that are left take
// over its replicas. Nothing acknowledged may be lost on the way, and every
// partition's replicas must end with the same log.
func TestClusterGrowsFromThreeToFive(t *testing.T) {
	c := newCluster(t)
	tool := adminBinary(t)
	c.startAll()
	observer := c.dial()
	w := startWorkload(t, c, "growth", "growth-workers", 3, 20, 2)
	accepted := func() int64 { return w.ledger.accepted.Load() }
	delivered := func() int64 { return w.ledger.delivered.Load() }
	everyPartition := func(what string) {
		t.Helper()
		for partition := int32(0); partition < partitionCount; partition++ {
			byPartition := func() int64 { return w.ledger.acceptedByPartition[partition].Load() }
			w.waitProgress(byPartition, 5, 90*time.Second, fmt.Sprintf("publishes accepted on partition %d %s", partition, what))
		}
	}
	w.waitProgress(accepted, 100, 60*time.Second, "publishes accepted by the cluster of three")
	w.waitProgress(delivered, 50, 60*time.Second, "events delivered by the cluster of three")
	placement := c.waitPlacement(c.nodes, "with three nodes", placementTimeout)
	t.Logf("placement with three nodes: %s", describePlacement(placement))

	fourth, fifth := c.addNode(), c.addNode()
	step(t, "adding %s and %s", fourth.id, fifth.id)
	grew := time.Now()
	c.start(fourth)
	c.start(fifth)

	// A leader is lost while a replica on a new node is being filled.
	partition, leader, catchingUp := c.waitNewReplica([]*node{fourth, fifth}, placementTimeout)
	led := func() []int32 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = observer.ForceMetadataRefresh(ctx)
		cancel()
		var ids []int32
		for _, meta := range observer.PartitionMetadata() {
			if meta.LeaderID == leader.id {
				ids = append(ids, meta.PartitionID)
			}
		}
		return ids
	}()
	step(t, "partition %d has a replica on a new node (still catching up: %v); killing its leader %s", partition, catchingUp, leader.id)
	killed := time.Now()
	c.kill(leader)
	everyPartition("without " + leader.id)
	t.Logf("every partition took publishes again %.1f s after %s was killed (it led %v)", time.Since(killed).Seconds(), leader.id, led)
	c.start(leader)
	c.waitReady(c.nodes...)

	placement = c.waitPlacement(c.nodes, "with five nodes", placementTimeout)
	t.Logf("placement with five nodes, settled %.1f s after they were added: %s", time.Since(grew).Seconds(), describePlacement(placement))
	for _, n := range []*node{fourth, fifth} {
		holds := 0
		for _, replicas := range placement {
			if slices.Contains(replicas, n.id) {
				holds++
			}
		}
		if holds == 0 {
			t.Errorf("%s joined and holds no replica: %s", n.id, describePlacement(placement))
		}
	}
	w.waitProgress(delivered, 100, 90*time.Second, "events delivered by the cluster of five")

	// Every node is taken out and brought back one at a time, an original
	// and a new one, as an orchestrator does when it drains hosts.
	for _, n := range []*node{c.nodes[1], fourth} {
		step(t, "restarting %s", n.id)
		c.kill(n)
		everyPartition("with " + n.id + " down")
		c.start(n)
		c.waitReady(c.nodes...)
		c.waitPlacement(c.nodes, "after "+n.id+" was restarted", placementTimeout)
	}

	// A new node loses its disk and comes back empty.
	step(t, "replacing %s with an empty node", fifth.id)
	c.kill(fifth)
	c.wipe(fifth)
	c.start(fifth)
	c.waitReady(c.nodes...)
	c.waitPlacement(c.nodes, "after "+fifth.id+" was replaced", placementTimeout)
	everyPartition("after " + fifth.id + " was replaced")

	// A node is lost for good and removed; the others take over its replicas.
	step(t, "%s is lost for good and removed", fourth.id)
	c.kill(fourth)
	asked := c.nodes[0]
	// A node that still counts as running is not removed: the cluster has
	// to notice first that it is gone, which takes its failure timeout.
	for deadline := time.Now().Add(90 * time.Second); ; time.Sleep(time.Second) {
		out, err := c.admin(tool, asked, "cluster", "remove-node", fourth.id)
		if err == nil {
			break
		}
		if !strings.Contains(out, "is running") || time.Now().After(deadline) {
			t.Fatalf("%s did not remove %s: %v: %s", asked.id, fourth.id, err, out)
		}
	}
	c.nodes = slices.DeleteFunc(c.nodes, func(n *node) bool { return n == fourth })
	placement = c.waitPlacement(c.nodes, "with "+fourth.id+" removed", placementTimeout)
	t.Logf("placement with four nodes: %s", describePlacement(placement))
	everyPartition("with " + fourth.id + " removed")
	w.waitProgress(delivered, 100, 90*time.Second, "events delivered by the cluster of four")

	step(t, "waiting for the remaining deliveries")
	w.stopPublishers()
	checkDelivery(t, w)
	w.stopConsumers()
	checkLogs(t, c, w.topic, w.ledger, false)
}

//go:build acceptance

package acceptance

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// Which replicas keep up with a partition is known to the partition's leader
// alone. A node used to know it only for the partitions it led itself, and
// only while it also coordinated the cluster: in the topology it returned,
// every other partition had the in-sync replicas of the moment its placement
// last changed, often the leader alone.
//
// The test asks every node for the topology while a workload runs. Every
// partition has to be reported by its leader, the same in every answer, with
// the running replicas in sync and their offsets. Then a node is killed, and
// later started again, and the topology has to follow: the node out of every
// partition's in-sync replicas, and back in.
func TestTopologyShowsWhatEachLeaderReports(t *testing.T) {
	c := newCluster(t)
	c.startAll()
	w := startWorkload(t, c, "topology", "topology-workers", 2, 20, 1)
	w.waitProgress(func() int64 { return w.ledger.accepted.Load() }, 100, 60*time.Second, "publishes accepted")

	leaders := c.waitTopology(c.nodes, "with every node running")
	coordinator := c.coordinator()
	if coordinator == nil {
		t.Fatal("no node says it coordinates the cluster")
	}
	ledElsewhere := 0
	for _, leader := range leaders {
		if leader != coordinator.id {
			ledElsewhere++
		}
	}
	if ledElsewhere == 0 {
		t.Fatalf("every partition is led by %s, which coordinates the cluster; the test needs a partition led by another node", coordinator.id)
	}

	// A node that is not the coordinator, so that the cluster goes on being
	// coordinated by the same node.
	victim := c.nodes[0]
	for _, n := range c.nodes {
		if n != coordinator {
			victim = n
			break
		}
	}
	running := make([]*node, 0, len(c.nodes)-1)
	for _, n := range c.nodes {
		if n != victim {
			running = append(running, n)
		}
	}

	step(t, "%s is killed", victim.id)
	c.kill(victim)
	c.waitTopology(running, "without "+victim.id)

	step(t, "%s is started again", victim.id)
	c.start(victim)
	c.waitReady(victim)
	c.waitTopology(c.nodes, "with "+victim.id+" back")

	w.stopPublishers()
	w.stopConsumers()
}

// topology returns the topology node n answers with.
func (c *cluster) topology(n *node) (*types.ClusterTopology, error) {
	httpClient := http.Client{Timeout: 10 * time.Second}
	resp, err := httpClient.Get("http://" + n.httpAddr + "/api/admin/topology")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %s", resp.Status)
	}
	var topology types.ClusterTopology
	if err := json.NewDecoder(resp.Body).Decode(&topology); err != nil {
		return nil, err
	}
	return &topology, nil
}

// waitTopology waits until every running node answers with a topology in
// which each partition is reported by a running leader, the same in every
// answer, with exactly the running nodes in sync and an offset for each. It
// returns the leader of each partition.
func (c *cluster) waitTopology(running []*node, what string) map[int32]string {
	c.t.Helper()
	ids := make([]string, 0, len(running))
	for _, n := range running {
		ids = append(ids, n.id)
	}
	slices.Sort(ids)

	check := func() (map[int32]string, string) {
		leaders := make(map[int32]string)
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
				switch {
				case !p.GetLeaderReported():
					return nil, fmt.Sprintf("%s: partition %d is not reported by its leader %q", n.id, id, p.GetLeaderId())
				case !slices.Contains(ids, p.GetLeaderId()):
					return nil, fmt.Sprintf("%s: partition %d is led by %q, which does not run", n.id, id, p.GetLeaderId())
				}
				if leader, seen := leaders[id]; seen && leader != p.GetLeaderId() {
					return nil, fmt.Sprintf("partition %d is led by %s for one node and by %s for %s", id, leader, p.GetLeaderId(), n.id)
				}
				leaders[id] = p.GetLeaderId()
				inSync := slices.Sorted(slices.Values(p.GetIsr()))
				if !slices.Equal(inSync, ids) || p.GetInSyncCount() != int32(len(ids)) {
					return nil, fmt.Sprintf("%s: partition %d has %v in sync (count %d)", n.id, id, inSync, p.GetInSyncCount())
				}
				for _, replica := range ids {
					if _, ok := p.GetFollowerOffsets()[replica]; !ok {
						return nil, fmt.Sprintf("%s: partition %d has no offset for %s: %v", n.id, id, replica, p.GetFollowerOffsets())
					}
				}
			}
		}
		return leaders, ""
	}

	deadline := time.Now().Add(readyTimeout)
	for {
		leaders, problem := check()
		if problem == "" {
			return leaders
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("topology %s: %s", what, problem)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

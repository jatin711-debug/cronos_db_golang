//go:build acceptance

package acceptance

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// addNode prepares one more node for the cluster, without starting it. Like
// the others it names every node as a seed.
func (c *cluster) addNode() *node {
	c.t.Helper()
	ports := freePorts(c.t, 5)
	addr := func(k int) string { return fmt.Sprintf("127.0.0.1:%d", ports[k]) }
	id := fmt.Sprintf("node%d", len(c.nodes)+1)
	n := &node{
		index:       len(c.nodes),
		id:          id,
		dataDir:     filepath.Join(c.dir, id),
		logPath:     filepath.Join(c.dir, "logs", id+".log"),
		grpcAddr:    addr(0),
		httpAddr:    addr(1),
		gossipAddr:  addr(2),
		clusterAddr: addr(3),
		raftAddr:    addr(4),
	}
	c.nodes = append(c.nodes, n)
	return n
}

// admin runs the administration tool against the port a node uses with the
// other nodes, with the cluster's certificates, as an operator does from a
// machine that has them. It returns what the tool printed.
func (c *cluster) admin(tool string, n *node, args ...string) (string, error) {
	args = append(args, "--server", n.clusterAddr, "--tls", "--tls-ca", c.caFile, "--tls-cert", c.certFile, "--tls-key", c.keyFile)
	cmd := exec.Command(tool, args...)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

// voters returns the addresses of the nodes that count towards a majority in
// Raft, as n has them.
func (c *cluster) voters(tool string, n *node) ([]string, error) {
	out, err := c.admin(tool, n, "cluster", "voters")
	if err != nil {
		return nil, fmt.Errorf("%v: %s", err, out)
	}
	var voters []string
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "  ") {
			voters = append(voters, strings.TrimSpace(line))
		}
	}
	return voters, nil
}

// A node that is lost stays a member until it is removed, and keeps counting
// towards what a majority is. A cluster of four with one node lost needs
// three of four, which is every node that still runs: one more failure and
// nobody can decide who leads what. With the lost node removed the three
// that are left need two.
//
// The test loses one of four nodes for good, removes it through the
// administration tool, and then kills another node. That last step is what
// the removal is for: two of four could not have chosen new leaders for what
// the killed node led, and two of three can.
func TestLostNodeIsRemoved(t *testing.T) {
	c := newCluster(t, "--cluster-expected-nodes=4")
	lost := c.addNode()
	tool := adminBinary(t)
	c.startAll()
	first, second, third := c.nodes[0], c.nodes[1], c.nodes[2]
	running := []*node{first, second, third}

	w := startWorkload(t, c, "removed-node", "removed-node-workers", 2, 20, 1)
	accepted := func() int64 { return w.ledger.accepted.Load() }
	delivered := func() int64 { return w.ledger.delivered.Load() }
	everyPartition := func(what string) {
		t.Helper()
		for partition := int32(0); partition < partitionCount; partition++ {
			byPartition := func() int64 { return w.ledger.acceptedByPartition[partition].Load() }
			w.waitProgress(byPartition, 5, 90*time.Second, fmt.Sprintf("publishes accepted on partition %d %s", partition, what))
		}
	}
	waitVoters := func(want int, what string) {
		t.Helper()
		deadline := time.Now().Add(60 * time.Second)
		for {
			problem := ""
			for _, n := range running {
				voters, err := c.voters(tool, n)
				switch {
				case err != nil:
					problem = fmt.Sprintf("%s: %v", n.id, err)
				case len(voters) != want:
					problem = fmt.Sprintf("%s counts %d nodes towards a majority: %v", n.id, len(voters), voters)
				}
			}
			if problem == "" {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s, %d nodes should count towards a majority: %s", what, want, problem)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	w.waitProgress(accepted, 100, 60*time.Second, "publishes accepted by the healthy cluster")
	w.waitProgress(delivered, 50, 60*time.Second, "events delivered by the healthy cluster")
	waitVoters(4, "in the healthy cluster")

	step(t, "%s is lost for good", lost.id)
	c.kill(lost)
	everyPartition("without " + lost.id)

	// What cannot be removed: a node that runs, and a node nobody knows.
	if out, err := c.admin(tool, first, "cluster", "remove-node", second.id); err == nil {
		t.Fatalf("%s runs and was removed: %s", second.id, out)
	} else if !strings.Contains(out, "is running") {
		t.Fatalf("removing %s, which runs, was refused with: %s", second.id, out)
	}
	if out, err := c.admin(tool, first, "cluster", "remove-node", "nobody"); err == nil {
		t.Fatalf("a node that does not exist was removed: %s", out)
	}
	waitVoters(4, "with "+lost.id+" lost and not removed")

	// Asked of a node that does not coordinate the cluster, which has to pass
	// the request on to the one that does.
	asked := first
	if coordinator := c.coordinator(); coordinator == first {
		asked = second
	}
	step(t, "removing %s, asking %s", lost.id, asked.id)
	if out, err := c.admin(tool, asked, "cluster", "remove-node", lost.id); err != nil {
		t.Fatalf("%s did not remove %s: %v: %s", asked.id, lost.id, err, out)
	}
	waitVoters(3, "with "+lost.id+" removed")
	voters, err := c.voters(tool, first)
	if err != nil {
		t.Fatal(err)
	}
	for _, voter := range voters {
		if strings.Contains(voter, lost.raftAddr) {
			t.Fatalf("%s still counts towards a majority: %s", lost.id, voter)
		}
	}
	// Every node has the lost node on its list no more.
	deadline := time.Now().Add(30 * time.Second)
	for _, n := range running {
		for c.logged(n, "Node "+lost.id+" was removed from the cluster and is forgotten here") == 0 {
			if time.Now().After(deadline) {
				t.Fatalf("%s does not say that it has forgotten %s", n.id, lost.id)
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
	if out, err := c.admin(tool, asked, "cluster", "remove-node", lost.id); err == nil {
		t.Fatalf("%s was removed a second time: %s", lost.id, out)
	} else if !strings.Contains(out, "not a member") {
		t.Fatalf("removing %s a second time was refused with: %s", lost.id, out)
	}
	// The lost node is not part of the cluster the checks below look at.
	c.nodes = running
	everyPartition("with " + lost.id + " removed")

	step(t, "killing %s; two of the three nodes that count are left", second.id)
	c.kill(second)
	everyPartition("with " + first.id + " and " + third.id + " alone")
	w.waitProgress(delivered, 60, 90*time.Second, "events delivered by "+first.id+" and "+third.id)
	c.start(second)
	c.waitReady(c.nodes...)

	step(t, "waiting for the remaining deliveries")
	w.stopPublishers()
	checkDelivery(t, w)
	w.stopConsumers()
	for partition := int32(0); partition < partitionCount; partition++ {
		agreedLog(t, c, partition, w.topic)
	}
}

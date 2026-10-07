package cluster

import (
	"context"
	"net"
	"testing"
	"time"
)

// freeAddr reserves a loopback address and releases it for the test to use.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func seedMember(t *testing.T, id, addr string, seeds []string) *Membership {
	t.Helper()
	m, err := NewMembership(&ClusterConfig{
		NodeID:            id,
		BindAddr:          addr,
		SeedNodes:         seeds,
		HeartbeatInterval: time.Second,
		FailureTimeout:    time.Minute,
		SuspectTimeout:    time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func startMember(t *testing.T, m *Membership) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	if err := m.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		m.Stop()
	})
}

func knows(m *Membership, id string) bool {
	_, err := m.GetNode(id)
	return err == nil
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// The nodes of a new cluster start together and share one seed list that
// names all of them. A node that comes up before the others must keep trying
// them; its own entry in the list is not a cluster to join.
func TestSeedJoin_NodeStartedFirstStillJoins(t *testing.T) {
	addrA, addrB := freeAddr(t), freeAddr(t)
	seeds := []string{addrA, addrB}

	b := seedMember(t, "node-b", addrB, seeds)
	local := b.GetLocalNode()
	startMember(t, b)

	// B is alone for longer than one round of its seed list.
	time.Sleep(seedRetryInterval + 300*time.Millisecond)
	if nodes := b.GetNodes(); len(nodes) != 1 {
		t.Fatalf("node-b knows %d nodes while it is alone, want itself only", len(nodes))
	}
	if got, err := b.GetNode("node-b"); err != nil || got != local {
		t.Fatalf("node-b's own membership record was replaced (err=%v)", err)
	}

	// The bootstrap node has no seeds, like ordinal zero of the chart.
	a := seedMember(t, "node-a", addrA, nil)
	startMember(t, a)

	eventually(t, "node-b knows node-a", func() bool { return knows(b, "node-a") })
	eventually(t, "node-a knows node-b", func() bool { return knows(a, "node-b") })
}

// Two nodes that find each other first must still reach the third, or the
// cluster stays split from the node that bootstrapped it.
func TestSeedJoin_ReachesEverySeed(t *testing.T) {
	addrA, addrB, addrC := freeAddr(t), freeAddr(t), freeAddr(t)
	seeds := []string{addrA, addrB, addrC}

	b := seedMember(t, "node-b", addrB, seeds)
	c := seedMember(t, "node-c", addrC, seeds)
	startMember(t, b)
	startMember(t, c)
	eventually(t, "node-b and node-c know each other", func() bool { return knows(b, "node-c") && knows(c, "node-b") })

	a := seedMember(t, "node-a", addrA, nil)
	startMember(t, a)
	eventually(t, "every node knows the other two", func() bool {
		return knows(a, "node-b") && knows(a, "node-c") && knows(b, "node-a") && knows(c, "node-a")
	})
}

package cluster

import (
	"testing"
	"time"
)

func quickMember(t *testing.T, id, addr string, seeds []string) *Membership {
	t.Helper()
	m, err := NewMembership(&ClusterConfig{
		NodeID:            id,
		BindAddr:          addr,
		GRPCAddr:          "grpc-of-" + id,
		RaftAddr:          "raft-of-" + id,
		SeedNodes:         seeds,
		HeartbeatInterval: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func alive(m *Membership, id string) bool {
	for _, node := range m.GetAliveNodes() {
		if node.ID == id {
			return true
		}
	}
	return false
}

// Two nodes that could not reach each other for a while each count the other
// as failed. When they can reach each other again they must notice: nodes
// used to stop sending heartbeats to a node they suspected, so both sides
// went quiet and stayed apart until one of them was restarted.
func TestMembership_NodesThatLostEachOtherMeetAgain(t *testing.T) {
	addrA, addrB := freeAddr(t), freeAddr(t)
	a := quickMember(t, "node-a", addrA, nil)
	b := quickMember(t, "node-b", addrB, []string{addrA})
	startMember(t, a)
	startMember(t, b)
	eventually(t, "the two nodes know each other", func() bool { return alive(a, "node-b") && alive(b, "node-a") })

	// What a network fault that outlasts the failure timeout leaves behind.
	if err := a.MarkDead("node-b"); err != nil {
		t.Fatal(err)
	}
	if err := b.MarkDead("node-a"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "each node counts the other as alive again", func() bool { return alive(a, "node-b") && alive(b, "node-a") })
}

// The node that created the cluster names no seeds. Restarted, it has nobody
// to introduce itself to, and it used to stay alone: the others had stopped
// trying it. They keep trying now, and their heartbeats tell it who they are.
func TestMembership_RestartedNodeWithoutSeedsIsFoundAgain(t *testing.T) {
	addrA, addrB := freeAddr(t), freeAddr(t)
	first := quickMember(t, "node-a", addrA, nil)
	b := quickMember(t, "node-b", addrB, []string{addrA})
	startMember(t, first)
	startMember(t, b)
	eventually(t, "the two nodes know each other", func() bool { return alive(first, "node-b") && alive(b, "node-a") })

	first.Stop()
	eventually(t, "the stopped node is counted as failed", func() bool { return !alive(b, "node-a") })

	restarted := quickMember(t, "node-a", addrA, nil)
	joined := make(chan *Node, 4)
	restarted.OnJoin(func(node *Node) { joined <- node })
	startMember(t, restarted)
	eventually(t, "the restarted node knows the other and is counted as alive", func() bool {
		return alive(restarted, "node-b") && alive(b, "node-a")
	})

	// It learned enough about the other node to work with it.
	select {
	case node := <-joined:
		if node.ID != "node-b" || node.GossipAddr != addrB || node.RaftAddr != "raft-of-node-b" || node.Address != "grpc-of-node-b" {
			t.Fatalf("learned %+v about node-b", node)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the restarted node was not told that node-b joined")
	}
}

// A process that was frozen has not heard from anyone for as long as it was
// frozen. That is its own gap, not theirs: it must not come back and count
// every other node as failed.
func TestMembership_APauseIsNotTakenForEveryoneElseFailing(t *testing.T) {
	m := quickMember(t, "node-a", freeAddr(t), nil)
	long := 10 * m.config.HeartbeatInterval
	other := &Node{ID: "node-b", GossipAddr: "127.0.0.1:1", State: NodeStateAlive, UpdatedAt: time.Now()}
	if err := m.Join(other); err != nil {
		t.Fatal(err)
	}

	// The detector ran, then nothing ran for a long time.
	m.detectFailures()
	m.mu.Lock()
	m.lastDetect = time.Now().Add(-long)
	other.UpdatedAt = time.Now().Add(-long)
	m.mu.Unlock()
	m.detectFailures()
	if !alive(m, "node-b") {
		t.Fatal("after a pause of this node, the other node was counted as failed at once")
	}

	// A node that then stays silent is suspected as usual.
	m.mu.Lock()
	other.UpdatedAt = time.Now().Add(-long)
	m.mu.Unlock()
	m.detectFailures()
	if alive(m, "node-b") {
		t.Fatal("a node that stayed silent was not suspected")
	}
}

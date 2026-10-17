package cluster

import (
	"testing"
	"time"
)

func memberWithClientAddr(t *testing.T, id, addr, clientAddr string, seeds []string) *Membership {
	t.Helper()
	m, err := NewMembership(&ClusterConfig{
		NodeID:            id,
		BindAddr:          addr,
		GRPCAddr:          id + ":7947",
		ClientAddr:        clientAddr,
		SeedNodes:         seeds,
		HeartbeatInterval: 100 * time.Millisecond,
		FailureTimeout:    time.Minute,
		SuspectTimeout:    time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func clientAddrOf(m *Membership, id string) string {
	node, err := m.GetNode(id)
	if err != nil {
		return "?"
	}
	return node.ClientAddr
}

// The address a node gives for clients reaches every other node, whether it
// learns of the node by its join, by the join reply, by a broadcast or by a
// heartbeat, and stays apart from the cluster address. A node without one
// is known with none.
func TestMembership_CarriesTheAddressForClients(t *testing.T) {
	addrA, addrB, addrC := freeAddr(t), freeAddr(t), freeAddr(t)
	seeds := []string{addrA, addrB, addrC}

	a := memberWithClientAddr(t, "node-a", addrA, "a.example:9000", seeds)
	b := memberWithClientAddr(t, "node-b", addrB, "b.example:9000", seeds)
	c := memberWithClientAddr(t, "node-c", addrC, "", seeds)
	startMember(t, a)
	startMember(t, b)
	startMember(t, c)

	want := map[string]string{"node-a": "a.example:9000", "node-b": "b.example:9000", "node-c": ""}
	for _, m := range []*Membership{a, b, c} {
		for id, addr := range want {
			eventually(t, m.GetLocalNode().ID+" knows the client address of "+id, func() bool {
				return clientAddrOf(m, id) == addr
			})
		}
	}
	if node, _ := b.GetNode("node-a"); node.Address != "node-a:7947" {
		t.Fatalf("the cluster address of node-a is %q, want node-a:7947", node.Address)
	}
}

// A heartbeat without a client address, as a node of v0.6.0 sends, does not
// take away the one on record; a heartbeat with another one replaces it.
func TestMembership_HeartbeatWithoutClientAddrKeepsIt(t *testing.T) {
	m := memberWithClientAddr(t, "node-a", freeAddr(t), "", nil)
	m.handleHeartbeatMessage(&GossipMessage{Type: "heartbeat", NodeID: "node-b", Address: "node-b:7947", ClientAddr: "b.example:9000", GossipAddr: "node-b:7946"})
	m.handleHeartbeatMessage(&GossipMessage{Type: "heartbeat", NodeID: "node-b", Address: "node-b:7947", GossipAddr: "node-b:7946"})
	if got := clientAddrOf(m, "node-b"); got != "b.example:9000" {
		t.Fatalf("after a heartbeat without one, the client address is %q", got)
	}
	m.handleHeartbeatMessage(&GossipMessage{Type: "heartbeat", NodeID: "node-b", Address: "node-b:7947", ClientAddr: "b2.example:9000", GossipAddr: "node-b:7946"})
	if got := clientAddrOf(m, "node-b"); got != "b2.example:9000" {
		t.Fatalf("after a heartbeat with a new one, the client address is %q", got)
	}
}

package cluster

import (
	"testing"
	"time"
)

// askedAt starts a node on addr that answers the question of whether a
// cluster exists with formed.
func askedAt(t *testing.T, addr, id string, formed bool) {
	t.Helper()
	m := quickMember(t, id, addr, nil)
	m.SetClusterFormed(func() bool { return formed })
	startMember(t, m)
}

func asked(t *testing.T, id string, formed bool) string {
	t.Helper()
	addr := freeAddr(t)
	askedAt(t, addr, id, formed)
	return addr
}

// candidate is a manager for a node whose seed list names itself and others.
func candidate(t *testing.T, bootstrap bool, others ...string) *Manager {
	t.Helper()
	self := freeAddr(t)
	m := NewManager(&Config{NodeID: "node-0", GossipAddr: self, SeedNodes: append([]string{self}, others...), Bootstrap: bootstrap, ReplicationFactor: 3})
	t.Cleanup(m.cancel)
	return m
}

func decide(t *testing.T, m *Manager, hasState bool) bool {
	t.Helper()
	create, err := m.createsCluster(hasState)
	if err != nil {
		t.Fatal(err)
	}
	return create
}

// A node creates a cluster the first time it has no state. The second time
// it has none, because its disk was lost, a cluster exists, and creating one
// more would leave the other nodes with two histories of who leads what. It
// asks them, and one node that belongs to a cluster settles it.
func TestBootstrap_NotBesideAnExistingCluster(t *testing.T) {
	member := asked(t, "node-1", true)
	silent := freeAddr(t) // nothing listens here: the third node is down
	if decide(t, candidate(t, true, member, silent), false) {
		t.Fatal("a node without state created a cluster although another node belongs to one")
	}
}

// The first start of a new cluster: every other node says it has nothing.
func TestBootstrap_CreatesWhenNoOtherNodeHasACluster(t *testing.T) {
	if !decide(t, candidate(t, true, asked(t, "node-1", false), asked(t, "node-2", false)), false) {
		t.Fatal("the node named to create the cluster did not, although no other node has one")
	}
}

// A node that does not answer may be the one that still has the cluster. The
// node waits for it instead of deciding without it.
func TestBootstrap_WaitsForNodesThatDoNotAnswer(t *testing.T) {
	late := freeAddr(t)
	m := candidate(t, true, asked(t, "node-1", false), late)
	decided := make(chan bool, 1)
	go func() {
		create, _ := m.createsCluster(false)
		decided <- create
	}()
	select {
	case create := <-decided:
		t.Fatalf("decided (create=%v) while one of the other nodes had not answered", create)
	case <-time.After(probeInterval + 500*time.Millisecond):
	}

	askedAt(t, late, "node-2", true) // it comes up, and it has the cluster
	select {
	case create := <-decided:
		if create {
			t.Fatal("created a cluster although the node that answered last belongs to one")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("still undecided after every other node answered")
	}
}

// What is on disk decides before anything else, and only the node that was
// named creates a cluster.
func TestBootstrap_OnlyTheNamedNodeWithoutState(t *testing.T) {
	nobody := freeAddr(t)
	if decide(t, candidate(t, true, nobody), true) {
		t.Fatal("a node that has Raft state created a cluster")
	}
	if decide(t, candidate(t, false, nobody), false) {
		t.Fatal("a node that was not named to create the cluster did")
	}
	// A node that names no other node has nobody to ask.
	alone := NewManager(&Config{NodeID: "node-0", GossipAddr: freeAddr(t), ReplicationFactor: 1})
	t.Cleanup(alone.cancel)
	if !decide(t, alone, false) {
		t.Fatal("a node on its own did not create its cluster")
	}
}

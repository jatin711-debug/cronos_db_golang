package cluster

import "testing"

// A node that joins copies a partition's whole log from its leader only when
// it holds nothing of it and has read the cluster state. Up to v0.14.1 it
// copied whenever it knew the leader: a node that had only restarted
// downloaded the whole log however little it lacked, and one that had not
// read the cluster state yet downloaded it and then refused to install it.
func TestJoin_CopiesOnlyAnEmptyPartitionOnceTheClusterStateIsRead(t *testing.T) {
	f := newLeadershipFixture(t, "node-2", "node-1", 3, 2, "node-1", "node-2", "node-3")
	metadata := &fakeMetadata{}
	metadata.set(true)
	f.manager.mu.Lock()
	f.manager.metadata = metadata
	f.manager.mu.Unlock()

	f.positions.set("node-2", ReplicaPosition{LastTerm: 1, LastOffset: 160025})
	if f.manager.copiesAtJoin(0, "node-1") {
		t.Fatal("a node that holds the partition's log to offset 160025 copies all of it again at join")
	}

	// A partition directory with an empty log is nothing to keep.
	f.positions.set("node-2", ReplicaPosition{LastOffset: -1})
	if !f.manager.copiesAtJoin(0, "node-1") {
		t.Fatal("a node with an empty log does not copy the partition at join")
	}

	// Without the cluster state the node cannot tell which log format the
	// partition requires, and would refuse the copy once it had it.
	metadata.set(false)
	if f.manager.copiesAtJoin(0, "node-1") {
		t.Fatal("a node that has not read the cluster state copies the partition at join")
	}
}

package cluster

import (
	"bytes"
	"log"
	"os"
	"strings"
	"testing"
)

// The leader of a partition logs when fewer replicas than the replication
// factor are in sync with it, and when all are again. It does so whether or
// not it coordinates the cluster: the coordinator used to log this for every
// partition, from in-sync replicas recorded when placement last changed, so
// it reported partitions as under-replicated that were not, and missed
// others.
func TestUpdateReplicaOffsetsLogsChangesInTheInSyncReplicas(t *testing.T) {
	var logged bytes.Buffer
	log.SetOutput(&logged)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	local := &Node{ID: "node-1", Address: "127.0.0.1:9001", GossipAddr: "127.0.0.1:8001", State: NodeStateAlive}
	membership := &mockMembershipService{local: local, nodes: map[string]*Node{
		"node-1": local,
		"node-2": {ID: "node-2", Address: "127.0.0.1:9002", GossipAddr: "127.0.0.1:8002", State: NodeStateAlive},
		"node-3": {ID: "node-3", Address: "127.0.0.1:9003", GossipAddr: "127.0.0.1:8003", State: NodeStateAlive},
	}}
	accessor := &MockPartitionAccessor{
		syncCalls:     make(map[int32]string),
		promoteCalls:  make(map[int32]int64),
		followerCalls: make(map[int32]string),
		demoteCalls:   make(map[int32]bool),
		isr:           map[int32][]string{},
	}
	router := NewRouter(membership, 1, 3, 150, accessor)
	router.KeepLeader(0, "node-1")
	mgr := NewManager(&Config{
		NodeID: "node-1", GossipAddr: "127.0.0.1:8001", GRPCAddr: "127.0.0.1:9001", RaftAddr: "127.0.0.1:10001",
		RaftDir: t.TempDir(), PartitionCount: 1, ReplicationFactor: 3,
	})
	mgr.partitionAccessor = accessor
	mgr.router = router

	round := func(inSync ...string) string {
		t.Helper()
		logged.Reset()
		accessor.isr[0] = inSync
		mgr.updateReplicaOffsets()
		return strings.TrimSpace(logged.String())
	}
	expect := func(got, want string) {
		t.Helper()
		if want == "" && got != "" || want != "" && !strings.Contains(got, want) {
			t.Fatalf("logged %q, want %q", got, want)
		}
	}

	expect(round("node-1", "node-2", "node-3"), "")
	expect(round("node-1", "node-2"), "Partition 0 under-replicated: 2 of 3 replicas in sync")
	expect(round("node-1", "node-2"), "")
	expect(round("node-1"), "Partition 0 under-replicated: 1 of 3 replicas in sync")
	expect(round("node-1", "node-2", "node-3"), "Partition 0 has all 3 replicas in sync again")
	expect(round("node-1", "node-2", "node-3"), "")

	// Once another node leads the partition it is that node's to report, and
	// this one starts afresh if it leads it again.
	router.KeepLeader(0, "node-2")
	expect(round("node-1"), "")
	router.KeepLeader(0, "node-1")
	expect(round("node-1", "node-2", "node-3"), "")
}

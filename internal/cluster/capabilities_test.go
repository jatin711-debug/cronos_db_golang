package cluster

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
)

const testCap = "test-format/1"

func TestTermStartUpgradeWaitsForEveryAssignedReplicaAndVoter(t *testing.T) {
	f := newLeadershipFixture(t, "a", "a", 3, 2, "a", "b", "c", "d")
	info := f.committed(t)
	info.Replicas, info.ISR = []string{"a", "b", "c"}, []string{"a", "b", "c"}
	if err := f.store.ProposePartition(&info, false); err != nil {
		t.Fatal(err)
	}
	f.store.voters = []string{"a", "b", "c", "d"}
	f.advertise([]string{CapabilityTermStart}, "a", "b")
	f.manager.enableTermStartRequirements()
	if f.manager.PartitionRequiresTermStart(0) {
		t.Fatal("upgraded with an incompatible assigned replica")
	}
	f.advertise([]string{CapabilityTermStart}, "c")
	f.manager.enableTermStartRequirements()
	if f.manager.PartitionRequiresTermStart(0) {
		t.Fatal("upgraded with an incompatible voter")
	}
	f.advertise([]string{CapabilityTermStart}, "d")
	f.manager.enableTermStartRequirements()
	if !f.manager.PartitionRequiresTermStart(0) {
		t.Fatal("compatible cluster did not commit the format")
	}
	count := f.store.proposals
	f.manager.enableTermStartRequirements()
	if f.store.proposals != count {
		t.Fatal("idempotent requirement proposed repeatedly")
	}
	if f.manager.admitsVoter(&Node{ID: "legacy"}) {
		t.Fatal("legacy replacement voter admitted after upgrade")
	}
	f.advertise(nil, "c")
	if !f.manager.PartitionRequiresTermStart(0) {
		t.Fatal("capability loss lifted a durable requirement")
	}
}

func TestElectionPrefersReplicaWithCommittedTermStartOverConflictingOldEntry(t *testing.T) {
	f := newLeadershipFixture(t, "b", "a", 3, 2, "a", "b", "c")
	f.requireDirectly(t, CapabilityTermStart)
	f.advertise([]string{CapabilityTermStart}, "a", "b", "c")
	f.positions.set("b", ReplicaPosition{LastTerm: 3, LastOffset: 3, Epoch: 3})
	f.positions.set("c", ReplicaPosition{LastTerm: 4, LastOffset: 4, Epoch: 4})
	delete(f.membership.nodes, "a")
	f.manager.checkPartitionHealth()
	if got := f.committed(t); got.LeaderID != "c" {
		t.Fatalf("elected stale conflicting replica %q instead of c", got.LeaderID)
	}
}

// advertise sets what each named member of the fixture advertises.
func (f *leadershipFixture) advertise(caps []string, ids ...string) {
	for _, id := range ids {
		f.membership.nodes[id].Capabilities = caps
	}
}

// A requirement only grows. An update proposed by a build that does not know
// requirements carries none and must not lift it, and a snapshot keeps it.
func TestFSM_PartitionRequirementsOnlyGrow(t *testing.T) {
	store := &fsmStore{fsm: NewClusterFSM(), leader: true}
	if err := store.ProposePartition(&PartitionInfo{ID: 0, LeaderID: "a", Replicas: []string{"a", "b"}, Epoch: 1}, true); err != nil {
		t.Fatal(err)
	}
	if err := store.RequirePartition(0, []string{"x/1"}); err != nil {
		t.Fatal(err)
	}
	epoch := store.fsm.state.Partitions[0].Epoch

	// An older coordinator's update: the JSON has no "requires".
	old, _ := json.Marshal(map[string]any{"id": 0, "leader_id": "b", "replicas": []string{"a", "b"}, "epoch": epoch + 1})
	data, _ := json.Marshal(Command{Type: CommandTypeUpdatePartition, Payload: old})
	if err, ok := store.fsm.Apply(&raft.Log{Data: data}).(error); ok && err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Partition(0); !slices.Equal(got.Requires, []string{"x/1"}) || got.LeaderID != "b" {
		t.Fatalf("after an update without requirements: requires=%v leader=%s, want [x/1] and b", got.Requires, got.LeaderID)
	}
	if got, _ := store.Partition(0); got.Epoch != epoch+1 {
		t.Fatalf("epoch = %d, want %d: the requirement itself must not have raised it", got.Epoch, epoch+1)
	}

	if err := store.RequirePartition(0, []string{"w/1", "x/1"}); err != nil {
		t.Fatal(err)
	}
	snap, _ := store.fsm.Snapshot()
	var buf bytes.Buffer
	if err := snap.(*FSMSnapshot).Persist(&bufferSink{Buffer: &buf}); err != nil {
		t.Fatal(err)
	}
	restored := NewClusterFSM()
	if err := restored.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatal(err)
	}
	if got, _ := restored.Partition(0); !slices.Equal(got.Requires, []string{"w/1", "x/1"}) {
		t.Fatalf("restored requires = %v, want [w/1 x/1]", got.Requires)
	}
	if err := store.RequirePartition(7, []string{"x/1"}); err == nil {
		t.Fatal("a requirement for a partition without an assignment was accepted")
	}
}

// bufferSink is a raft.SnapshotSink that writes to a buffer.
type bufferSink struct{ *bytes.Buffer }

func (bufferSink) ID() string    { return "test" }
func (bufferSink) Cancel() error { return nil }
func (bufferSink) Close() error  { return nil }

// A partition is made to require a capability only when every node it is
// assigned to and every Raft voter is alive and advertises it.
func TestRequirePartitionCapabilities_NeedsEveryReplicaAndVoter(t *testing.T) {
	f := newLeadershipFixture(t, "a", "a", 3, 2, "a", "b", "c")
	f.store.voters = []string{"a", "b", "c", "d"}
	f.advertise([]string{testCap}, "a", "b")

	err := f.manager.RequirePartitionCapabilities(0, []string{testCap})
	if err == nil || !strings.Contains(err.Error(), "c lacks "+testCap) || !strings.Contains(err.Error(), "d is not reachable") {
		t.Fatalf("err = %v, want a refusal naming c, which lacks it, and voter d, which is not alive", err)
	}
	if got := f.committed(t); len(got.Requires) != 0 {
		t.Fatalf("requires = %v after a refusal", got.Requires)
	}

	f.advertise([]string{"other/1", testCap}, "c")
	f.store.voters = []string{"a", "b", "c"}
	if err := f.manager.RequirePartitionCapabilities(0, []string{testCap}); err != nil {
		t.Fatal(err)
	}
	if got := f.committed(t); !slices.Equal(got.Requires, []string{testCap}) {
		t.Fatalf("requires = %v, want [%s]", got.Requires, testCap)
	}
	proposals := f.store.proposals
	if err := f.manager.RequirePartitionCapabilities(0, []string{testCap}); err != nil || f.store.proposals != proposals {
		t.Fatalf("asking again: err=%v, %d more proposals; want nothing committed", err, f.store.proposals-proposals)
	}

	f.store.leader = false
	if err := f.manager.RequirePartitionCapabilities(0, []string{"next/1"}); !errors.Is(err, ErrNotCoordinator) {
		t.Fatalf("err = %v on a node that does not lead Raft, want ErrNotCoordinator", err)
	}
}

// requireDirectly commits a requirement without the checks, as for a
// partition whose nodes advertised it at the time.
func (f *leadershipFixture) requireDirectly(t *testing.T, caps ...string) {
	t.Helper()
	if err := f.store.RequirePartition(0, caps); err != nil {
		t.Fatal(err)
	}
}

// Only a replica that advertises what the partition requires is elected,
// even when another, which would win the tie on its name, holds the same log.
func TestElection_OnlyACapableReplicaLeads(t *testing.T) {
	f := newLeadershipFixture(t, "b", "c", 3, 2, "a", "b", "c")
	f.requireDirectly(t, testCap)
	f.advertise([]string{testCap}, "b", "c")
	f.positions.set("a", ReplicaPosition{LastTerm: 2, LastOffset: 40})
	f.positions.set("b", ReplicaPosition{LastTerm: 2, LastOffset: 40})
	delete(f.membership.nodes, "c") // the leader dies

	f.manager.checkPartitionHealth()
	if got := f.committed(t); got.LeaderID != "b" {
		t.Fatalf("elected %q, want b: a does not advertise %s", got.LeaderID, testCap)
	}
}

// When the replica holding the most of the log does not advertise what the
// partition requires, nobody is elected: a capable replica with less of the
// log could be missing acknowledged writes.
func TestElection_WaitsWhenOnlyAnIncapableReplicaHasTheWholeLog(t *testing.T) {
	f := newLeadershipFixture(t, "b", "c", 3, 2, "a", "b", "c")
	f.requireDirectly(t, testCap)
	f.advertise([]string{testCap}, "b", "c")
	f.positions.set("a", ReplicaPosition{LastTerm: 2, LastOffset: 41})
	f.positions.set("b", ReplicaPosition{LastTerm: 2, LastOffset: 40})
	delete(f.membership.nodes, "c")

	f.manager.checkPartitionHealth()
	if got := f.committed(t); got.LeaderID != "" {
		t.Fatalf("elected %q, want nobody while only a holds offset 41", got.LeaderID)
	}

	// a is upgraded and advertises the capability: it can lead.
	f.advertise([]string{testCap}, "a")
	f.manager.checkPartitionHealth()
	if got := f.committed(t); got.LeaderID != "a" {
		t.Fatalf("elected %q, want a once it advertises %s", got.LeaderID, testCap)
	}
}

// The ring's placement is followed only onto nodes that advertise what the
// partition requires; until then the partition stays on its replicas.
func TestPlacement_StaysOffNodesWithoutTheRequirement(t *testing.T) {
	f := newLeadershipFixture(t, "a", "a", 3, 2, "a", "b", "c", "d")
	current := f.committed(t).clone()
	current.Replicas, current.ISR = []string{"a", "b", "c"}, []string{"a", "b", "c"}
	if err := f.store.ProposePartition(&current, false); err != nil {
		t.Fatal(err)
	}
	f.requireDirectly(t, testCap)
	f.advertise([]string{testCap}, "a", "b", "c")
	wanted := []string{"a", "b", "d"}
	f.manager.router.UpdatePartitionAssignment(0, "a", wanted, wanted)

	f.manager.syncClusterState()
	if got := f.committed(t); !stringSliceSetEqual(got.Replicas, []string{"a", "b", "c"}) {
		t.Fatalf("replicas = %v, want a, b, c while d does not advertise %s", got.Replicas, testCap)
	}

	f.advertise([]string{testCap}, "d")
	f.manager.syncClusterState()
	if got := f.committed(t); !stringSliceSetEqual(got.Replicas, wanted) {
		t.Fatalf("replicas = %v, want %v once d advertises %s", got.Replicas, wanted, testCap)
	}
}

// Leadership is not handed to a replica that does not advertise what the
// partition requires, and a handoff under way is abandoned when its target
// stops advertising it.
func TestHandoff_OnlyToACapableReplica(t *testing.T) {
	f := newLeadershipFixture(t, "a", "a", 3, 2, "a", "b", "c")
	f.requireDirectly(t, testCap)
	f.advertise([]string{testCap}, "a", "c")
	nodes := []string{"b", "a", "c"}
	f.manager.router.UpdatePartitionAssignment(0, "b", nodes, nodes) // the ring prefers b
	f.positions.set("a", ReplicaPosition{LastTerm: 1, LastOffset: 100, AcceptingWrites: true})
	f.positions.set("b", ReplicaPosition{LastTerm: 1, LastOffset: 100})

	f.manager.syncClusterState()
	if got := f.committed(t); got.TransferTo != "" || got.LeaderID != "a" {
		t.Fatalf("leader=%s transfer_to=%q, want a staying leader: b does not advertise %s", got.LeaderID, got.TransferTo, testCap)
	}

	f.advertise([]string{testCap}, "b")
	f.manager.syncClusterState()
	if got := f.committed(t); got.TransferTo != "b" {
		t.Fatalf("transfer_to = %q, want b once it advertises %s", got.TransferTo, testCap)
	}

	f.advertise(nil, "b") // restarted on an older build
	f.manager.advanceTransfers()
	if got := f.committed(t); got.TransferTo != "" || got.LeaderID != "a" {
		t.Fatalf("leader=%s transfer_to=%q, want the handoff to b abandoned", got.LeaderID, got.TransferTo)
	}
}

// Once a partition requires a capability, a node that does not advertise it
// is not made a Raft voter: as Raft leader it would place the partition.
func TestVoters_MustAdvertiseWhatPartitionsRequire(t *testing.T) {
	f := newLeadershipFixture(t, "a", "a", 3, 2, "a", "b", "c")
	if !f.manager.admitsVoter(&Node{ID: "e"}) {
		t.Fatal("a node was refused as voter while no partition requires anything")
	}
	f.requireDirectly(t, testCap)
	if f.manager.admitsVoter(&Node{ID: "e"}) {
		t.Fatalf("a node without %s was admitted as voter", testCap)
	}
	if !f.manager.admitsVoter(&Node{ID: "e", Capabilities: []string{testCap}}) {
		t.Fatalf("a node advertising %s was refused as voter", testCap)
	}
}

// What a node advertises reaches the others through membership. A heartbeat
// without capabilities, as an older build sends, replaces them with none;
// addresses missing from a heartbeat are kept instead.
func TestMembership_CarriesCapabilities(t *testing.T) {
	addrA, addrB := freeAddr(t), freeAddr(t)
	seeds := []string{addrA, addrB}
	newMember := func(id, addr string, caps []string) *Membership {
		m, err := NewMembership(&ClusterConfig{NodeID: id, BindAddr: addr, GRPCAddr: id + ":7947", Capabilities: caps,
			SeedNodes: seeds, HeartbeatInterval: 100e6, FailureTimeout: 60e9, SuspectTimeout: 60e9})
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	a := newMember("node-a", addrA, []string{"y/1", "x/1"})
	b := newMember("node-b", addrB, []string{"x/1"})
	startMember(t, a)
	startMember(t, b)
	capsOf := func(m *Membership, id string) []string {
		node, err := m.GetNode(id)
		if err != nil {
			return []string{"?"}
		}
		return node.Capabilities
	}
	eventually(t, "b knows what a advertises", func() bool { return slices.Equal(capsOf(b, "node-a"), []string{"x/1", "y/1"}) })
	eventually(t, "a knows what b advertises", func() bool { return slices.Equal(capsOf(a, "node-b"), []string{"x/1"}) })

	m := newMember("node-c", freeAddr(t), nil)
	m.handleHeartbeatMessage(&GossipMessage{Type: "heartbeat", NodeID: "node-d", Address: "node-d:7947", ClientAddr: "d:9000", GossipAddr: "node-d:7946", Capabilities: []string{"x/1"}})
	m.handleHeartbeatMessage(&GossipMessage{Type: "heartbeat", NodeID: "node-d", Address: "node-d:7947", GossipAddr: "node-d:7946"})
	node, _ := m.GetNode("node-d")
	if len(node.Capabilities) != 0 || node.ClientAddr != "d:9000" {
		t.Fatalf("after a heartbeat without them: capabilities=%v client_addr=%q, want none and d:9000", node.Capabilities, node.ClientAddr)
	}
}

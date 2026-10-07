package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// fsmStore commits assignments straight into a real ClusterFSM, so the tests
// run the same apply logic (including the epoch rule) as a Raft cluster.
type fsmStore struct {
	fsm       *ClusterFSM
	leader    bool
	proposals int
}

func (s *fsmStore) IsLeader() bool                           { return s.leader }
func (s *fsmStore) Partition(id int32) (PartitionInfo, bool) { return s.fsm.Partition(id) }
func (s *fsmStore) Partitions() map[int32]PartitionInfo      { return s.fsm.Partitions() }
func (s *fsmStore) ProposePartition(info *PartitionInfo, assign bool) error {
	if !s.leader {
		return fmt.Errorf("not leader")
	}
	s.proposals++
	payload, _ := json.Marshal(info)
	cmdType := CommandTypeUpdatePartition
	if assign {
		cmdType = CommandTypeAssignPartition
	}
	data, _ := json.Marshal(Command{Type: cmdType, Payload: payload})
	if err, ok := s.fsm.Apply(&raft.Log{Data: data}).(error); ok && err != nil {
		return err
	}
	return nil
}

// fakePositions answers position queries by node address ("" is this node).
type fakePositions struct {
	mu          sync.Mutex
	local       string
	byNode      map[string]ReplicaPosition
	unreachable map[string]bool
}

func (f *fakePositions) set(node string, position ReplicaPosition) {
	f.mu.Lock()
	defer f.mu.Unlock()
	position.Found = true
	f.byNode[node] = position
}

func (f *fakePositions) ReplicaLogPosition(_ context.Context, addr string, _ int32) (bool, int64, int64, int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	node := addr
	if addr == "" {
		node = f.local
	}
	if f.unreachable[node] {
		return false, 0, 0, 0, false, fmt.Errorf("node %s unreachable", node)
	}
	p, ok := f.byNode[node]
	if !ok {
		return false, -1, 0, 0, false, nil
	}
	return p.Found, p.LastOffset, p.LastTerm, p.Epoch, p.AcceptingWrites, nil
}

type leadershipFixture struct {
	manager    *Manager
	store      *fsmStore
	positions  *fakePositions
	accessor   *MockPartitionAccessor
	membership *mockMembershipService
}

// newLeadershipFixture builds a manager for node self in a cluster of nodes,
// each node's address equal to its ID. Partition 0 is committed with leader as
// its leader and every node as a replica.
func newLeadershipFixture(t *testing.T, self, leader string, replicationFactor, minISR int, nodes ...string) *leadershipFixture {
	t.Helper()
	membership := &mockMembershipService{nodes: map[string]*Node{}}
	for _, id := range nodes {
		membership.nodes[id] = &Node{ID: id, Address: id, State: NodeStateAlive}
	}
	membership.local = membership.nodes[self]
	accessor := &MockPartitionAccessor{
		syncCalls:     make(map[int32]string),
		promoteCalls:  make(map[int32]int64),
		followerCalls: make(map[int32]string),
		demoteCalls:   make(map[int32]bool),
	}
	m := NewManager(&Config{NodeID: self, PartitionCount: 1, ReplicationFactor: replicationFactor, MinInSyncReplicas: minISR})
	t.Cleanup(m.cancel)
	store := &fsmStore{fsm: NewClusterFSM(), leader: true}
	positions := &fakePositions{local: self, byNode: map[string]ReplicaPosition{}, unreachable: map[string]bool{}}
	m.membership = membership
	m.partitionAccessor = accessor
	m.assignments = store
	m.positions = positions
	m.router = NewRouter(membership, 1, replicationFactor, 16, accessor)
	m.router.SetCommittedSource(m.committed)
	// Pin the ring's wish to the same leader until a test changes it.
	m.router.UpdatePartitionAssignment(0, leader, nodes, nodes)

	if err := store.ProposePartition(&PartitionInfo{ID: 0, LeaderID: leader, Replicas: nodes, ISR: nodes, Epoch: 1, State: PartitionStateOnline}, true); err != nil {
		t.Fatal(err)
	}
	return &leadershipFixture{manager: m, store: store, positions: positions, accessor: accessor, membership: membership}
}

func (f *leadershipFixture) committed(t *testing.T) PartitionInfo {
	t.Helper()
	info, ok := f.store.Partition(0)
	if !ok {
		t.Fatal("partition 0 is not committed")
	}
	return info
}

func TestCleanElectionQuorum(t *testing.T) {
	for _, tc := range []struct {
		replicas, minISR, need int
		guaranteed             bool
	}{
		{replicas: 3, minISR: 2, need: 2, guaranteed: true},  // both survivors must answer
		{replicas: 3, minISR: 3, need: 1, guaranteed: true},  // every replica has every write
		{replicas: 5, minISR: 3, need: 3, guaranteed: true},  // any three of the four survivors
		{replicas: 3, minISR: 1, need: 3, guaranteed: false}, // writes may exist only on the dead leader
		{replicas: 1, minISR: 1, need: 1, guaranteed: false}, // nothing survives the only replica
		{replicas: 3, minISR: 0, need: 3, guaranteed: false},
	} {
		need, guaranteed := cleanElectionQuorum(tc.replicas, tc.minISR)
		if need != tc.need || guaranteed != tc.guaranteed {
			t.Errorf("cleanElectionQuorum(%d, %d) = (%d, %v), want (%d, %v)", tc.replicas, tc.minISR, need, guaranteed, tc.need, tc.guaranteed)
		}
	}
}

// A failed leader is replaced by the replica whose log ends last. Choosing by
// name, or by offsets a leader reported some time ago, can pick a replica that
// is missing writes the old leader had already acknowledged.
func TestElection_PicksMostCompleteReplica(t *testing.T) {
	for name, tc := range map[string]struct {
		node2, node3 ReplicaPosition
		want         string
	}{
		"longer log wins":                 {ReplicaPosition{LastTerm: 3, LastOffset: 90}, ReplicaPosition{LastTerm: 3, LastOffset: 100}, "node-3"},
		"newer term beats a longer log":   {ReplicaPosition{LastTerm: 4, LastOffset: 50}, ReplicaPosition{LastTerm: 3, LastOffset: 100}, "node-2"},
		"equal logs pick the lowest name": {ReplicaPosition{LastTerm: 3, LastOffset: 100}, ReplicaPosition{LastTerm: 3, LastOffset: 100}, "node-2"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newLeadershipFixture(t, "node-2", "node-1", 3, 2, "node-1", "node-2", "node-3")
			f.positions.set("node-2", tc.node2)
			f.positions.set("node-3", tc.node3)
			delete(f.membership.nodes, "node-1") // the leader dies
			before := f.committed(t)

			f.manager.checkPartitionHealth()

			after := f.committed(t)
			if after.LeaderID != tc.want {
				t.Fatalf("elected %s, want %s", after.LeaderID, tc.want)
			}
			if after.Epoch <= before.Epoch {
				t.Fatalf("epoch %d did not rise above %d with the new leader", after.Epoch, before.Epoch)
			}
			if leader, err := f.manager.router.GetPartitionLeader(0); err != nil || leader.ID != tc.want {
				t.Fatalf("router reports leader %v (%v), want %s", leader, err, tc.want)
			}
		})
	}
}

// With writes acknowledged by two of three replicas, one surviving replica
// alone cannot show that it has them all. The election must wait for the
// other rather than risk electing a replica that is missing acknowledged data.
func TestElection_RefusesWhenTooFewReplicasAnswer(t *testing.T) {
	f := newLeadershipFixture(t, "node-2", "node-1", 3, 2, "node-1", "node-2", "node-3")
	f.positions.set("node-2", ReplicaPosition{LastTerm: 1, LastOffset: 10})
	f.positions.unreachable["node-3"] = true
	delete(f.membership.nodes, "node-1")

	f.manager.checkPartitionHealth()
	if got := f.committed(t); got.LeaderID != "node-1" {
		t.Fatalf("elected %s although only one of the two surviving replicas answered", got.LeaderID)
	}

	// Once the other replica answers, the election goes ahead.
	f.positions.unreachable["node-3"] = false
	f.positions.set("node-3", ReplicaPosition{LastTerm: 1, LastOffset: 25})
	f.manager.checkPartitionHealth()
	if got := f.committed(t); got.LeaderID != "node-3" {
		t.Fatalf("leader = %s, want node-3 once both replicas answered", got.LeaderID)
	}
}

// With min-insync-replicas of 1 a write may exist only on the failed leader,
// so no election can promise completeness; the best reachable replica leads.
func TestElection_BestEffortWhenLeaderAloneAcknowledges(t *testing.T) {
	f := newLeadershipFixture(t, "node-2", "node-1", 3, 1, "node-1", "node-2", "node-3")
	f.positions.set("node-2", ReplicaPosition{LastTerm: 1, LastOffset: 10})
	f.positions.unreachable["node-3"] = true
	delete(f.membership.nodes, "node-1")

	f.manager.checkPartitionHealth()
	if got := f.committed(t); got.LeaderID != "node-2" {
		t.Fatalf("leader = %s, want the one reachable replica node-2", got.LeaderID)
	}
}

// Every node follows the committed assignment, not the ring: it leads what it
// is named leader of, at the committed epoch, and steps down from the rest.
func TestReconcile_FollowsCommittedLeadership(t *testing.T) {
	f := newLeadershipFixture(t, "node-1", "node-2", 3, 2, "node-1", "node-2", "node-3")
	// The ring would rather have node-1 lead; node-2 is the committed leader.
	f.manager.router.UpdatePartitionAssignment(0, "node-1", []string{"node-1", "node-2", "node-3"}, nil)

	f.manager.reconcileLocalLeadership()
	if _, promoted := f.accessor.promoteCalls[0]; promoted {
		t.Fatal("node promoted itself because the ring prefers it, against the committed leader")
	}
	if !f.accessor.demoteCalls[0] {
		t.Fatal("node did not step down from a partition another node is committed to lead")
	}
	if f.manager.IsPartitionLeader(0) || f.manager.IsPartitionWritable(0) {
		t.Fatal("node reports itself leader of a partition committed to another node")
	}

	// Leadership is committed to this node: it leads at the committed epoch.
	committed := f.committed(t)
	committed.LeaderID = "node-1"
	if err := f.store.ProposePartition(&committed, false); err != nil {
		t.Fatal(err)
	}
	f.manager.reconcileLocalLeadership()
	want := f.committed(t).Epoch
	if got, promoted := f.accessor.promoteCalls[0]; !promoted || got != want {
		t.Fatalf("promoted at epoch %d (promoted=%v), want the committed epoch %d", got, promoted, want)
	}
	if f.accessor.followerCalls[0] == "" {
		t.Fatal("the new leader did not start replicating to its followers")
	}
	if !f.manager.IsPartitionWritable(0) || f.manager.GetPartitionEpoch(0) != want {
		t.Fatalf("writable=%v epoch=%d, want writable at epoch %d", f.manager.IsPartitionWritable(0), f.manager.GetPartitionEpoch(0), want)
	}
}

// A node keeps a partition loaded only while it has a part in it. Once the
// committed assignment names it neither leader, replica nor handoff target, it
// lets the partition go; otherwise the first node of a cluster, which leads
// everything until the others join, would hold every partition for good.
func TestReconcile_ReleasesPartitionsThisNodeNoLongerHolds(t *testing.T) {
	f := newLeadershipFixture(t, "node-1", "node-2", 1, 1, "node-1", "node-2")
	commit := func(change func(*PartitionInfo)) {
		t.Helper()
		info := f.committed(t)
		change(&info)
		if err := f.store.ProposePartition(&info, false); err != nil {
			t.Fatal(err)
		}
		f.manager.reconcileLocalLeadership()
	}

	// node-2 leads and node-1 is still listed as a replica: keep it.
	f.manager.reconcileLocalLeadership()
	if f.accessor.releaseCalls[0] != 0 {
		t.Fatal("released a partition this node is a replica of")
	}

	// node-1 is the target of a handoff: it is about to lead, keep it.
	commit(func(info *PartitionInfo) {
		info.Replicas, info.ISR = []string{"node-2"}, []string{"node-2"}
		info.State, info.TransferTo = PartitionStateRebalancing, "node-1"
	})
	if f.accessor.releaseCalls[0] != 0 {
		t.Fatal("released a partition that is being handed to this node")
	}

	// The handoff is abandoned and node-2 holds the partition alone.
	commit(func(info *PartitionInfo) {
		info.State, info.TransferTo = PartitionStateOnline, ""
	})
	if f.accessor.releaseCalls[0] == 0 {
		t.Fatal("kept a partition this node neither leads nor replicates")
	}
	if !f.accessor.demoteCalls[0] {
		t.Fatal("released a partition without stepping down from it first")
	}

	// It is assigned back: the node leads it again and does not release it.
	released := f.accessor.releaseCalls[0]
	commit(func(info *PartitionInfo) {
		info.LeaderID, info.Replicas, info.ISR = "node-1", []string{"node-1"}, []string{"node-1"}
	})
	if f.accessor.releaseCalls[0] != released {
		t.Fatal("released a partition this node leads")
	}
	if _, promoted := f.accessor.promoteCalls[0]; !promoted {
		t.Fatal("node did not take up a partition assigned back to it")
	}
}

// The ring preferring another live replica must not move leadership by itself.
// The leader first stops taking publishes; the target takes over only when the
// leader reports it has stopped and both logs end at the same entry. Until
// then nothing changes hands, so no acknowledged write can be left behind.
func TestHandoff_CompletesOnlyWhenLeaderStoppedAndLogsEqual(t *testing.T) {
	f := newLeadershipFixture(t, "node-1", "node-1", 3, 2, "node-1", "node-2", "node-3")
	nodes := []string{"node-2", "node-1", "node-3"}
	f.manager.router.UpdatePartitionAssignment(0, "node-2", nodes, nodes) // the ring now prefers node-2
	f.positions.set("node-1", ReplicaPosition{LastTerm: 1, LastOffset: 100, AcceptingWrites: true})
	f.positions.set("node-2", ReplicaPosition{LastTerm: 1, LastOffset: 98})
	startEpoch := f.committed(t).Epoch

	f.manager.syncClusterState()
	started := f.committed(t)
	if started.LeaderID != "node-1" || started.TransferTo != "node-2" || started.State != PartitionStateRebalancing {
		t.Fatalf("after planning: leader=%s transfer_to=%q state=%v; want node-1 handing over to node-2", started.LeaderID, started.TransferTo, started.State)
	}
	if f.manager.IsPartitionWritable(0) {
		t.Fatal("the leader still accepts publishes during a handoff")
	}
	if !f.manager.IsPartitionLeader(0) {
		t.Fatal("the leader stopped being leader before the handoff completed")
	}

	steps := []struct {
		why            string
		leader, target ReplicaPosition
	}{
		{"the leader has not stopped yet", ReplicaPosition{LastTerm: 1, LastOffset: 100, AcceptingWrites: true}, ReplicaPosition{LastTerm: 1, LastOffset: 100}},
		{"the target is behind", ReplicaPosition{LastTerm: 1, LastOffset: 101}, ReplicaPosition{LastTerm: 1, LastOffset: 100}},
		{"the last entries differ in term", ReplicaPosition{LastTerm: 2, LastOffset: 101}, ReplicaPosition{LastTerm: 1, LastOffset: 101}},
	}
	for _, step := range steps {
		f.positions.set("node-1", step.leader)
		f.positions.set("node-2", step.target)
		f.manager.advanceTransfers()
		if got := f.committed(t); got.LeaderID != "node-1" || got.TransferTo != "node-2" {
			t.Fatalf("leadership moved although %s (leader=%s transfer_to=%q)", step.why, got.LeaderID, got.TransferTo)
		}
	}

	f.positions.set("node-1", ReplicaPosition{LastTerm: 2, LastOffset: 101})
	f.positions.set("node-2", ReplicaPosition{LastTerm: 2, LastOffset: 101})
	f.manager.advanceTransfers()
	done := f.committed(t)
	if done.LeaderID != "node-2" || done.TransferTo != "" || done.State != PartitionStateOnline {
		t.Fatalf("after completion: leader=%s transfer_to=%q state=%v; want node-2 online", done.LeaderID, done.TransferTo, done.State)
	}
	if done.Epoch <= startEpoch {
		t.Fatalf("epoch %d did not rise above %d across the handoff", done.Epoch, startEpoch)
	}

	// The old leader follows the commit and steps down.
	f.manager.reconcileLocalLeadership()
	if !f.accessor.demoteCalls[0] || f.manager.IsPartitionWritable(0) {
		t.Fatal("the old leader did not step down after the handoff")
	}
}

// With replication, a handoff does not start (and so does not stop publishes)
// while the target is far behind; the target is only made a replica so it can
// catch up.
func TestHandoff_WaitsForTargetToCatchUp(t *testing.T) {
	f := newLeadershipFixture(t, "node-1", "node-1", 3, 2, "node-1", "node-2", "node-3")
	nodes := []string{"node-2", "node-1", "node-3"}
	f.manager.router.UpdatePartitionAssignment(0, "node-2", nodes, nodes)
	f.positions.set("node-1", ReplicaPosition{LastTerm: 1, LastOffset: 10 * transferStartLag, AcceptingWrites: true})
	f.positions.set("node-2", ReplicaPosition{LastTerm: 1, LastOffset: 10})

	f.manager.syncClusterState()
	if got := f.committed(t); got.TransferTo != "" || got.LeaderID != "node-1" || !f.manager.IsPartitionWritable(0) {
		t.Fatalf("handoff started with the target far behind: leader=%s transfer_to=%q", got.LeaderID, got.TransferTo)
	}

	f.positions.set("node-2", ReplicaPosition{LastTerm: 1, LastOffset: 10*transferStartLag - 5})
	f.manager.syncClusterState()
	if got := f.committed(t); got.TransferTo != "node-2" {
		t.Fatalf("handoff did not start once the target had nearly caught up (transfer_to=%q)", got.TransferTo)
	}
}

// A handoff whose target disappears is abandoned: the leader keeps leading and
// takes publishes again, and the handoff is not restarted straight away.
func TestHandoff_AbandonedWhenTargetDies(t *testing.T) {
	f := newLeadershipFixture(t, "node-1", "node-1", 3, 2, "node-1", "node-2", "node-3")
	nodes := []string{"node-2", "node-1", "node-3"}
	f.manager.router.UpdatePartitionAssignment(0, "node-2", nodes, nodes)
	f.positions.set("node-1", ReplicaPosition{LastTerm: 1, LastOffset: 100, AcceptingWrites: true})
	f.positions.set("node-2", ReplicaPosition{LastTerm: 1, LastOffset: 100})
	f.manager.syncClusterState()
	if f.committed(t).TransferTo != "node-2" {
		t.Fatal("setup: handoff did not start")
	}

	target := f.membership.nodes["node-2"]
	delete(f.membership.nodes, "node-2")
	f.manager.advanceTransfers()
	if got := f.committed(t); got.LeaderID != "node-1" || got.TransferTo != "" || got.State != PartitionStateOnline {
		t.Fatalf("after the target died: leader=%s transfer_to=%q state=%v; want node-1 online", got.LeaderID, got.TransferTo, got.State)
	}
	if !f.manager.IsPartitionWritable(0) {
		t.Fatal("the leader does not accept publishes after the handoff was abandoned")
	}

	// The target returns at once; the handoff must not flap back on.
	f.membership.nodes["node-2"] = target
	f.manager.syncClusterState()
	if got := f.committed(t); got.TransferTo != "" {
		t.Fatal("an abandoned handoff restarted immediately")
	}
}

// A handoff that cannot finish in time is abandoned instead of refusing
// publishes indefinitely.
func TestHandoff_AbandonedWhenItTakesTooLong(t *testing.T) {
	f := newLeadershipFixture(t, "node-1", "node-1", 3, 2, "node-1", "node-2", "node-3")
	stuck := f.committed(t)
	stuck.State, stuck.TransferTo = PartitionStateRebalancing, "node-2"
	stuck.TransferStartedMs = time.Now().Add(-2 * transferTimeout).UnixMilli()
	if err := f.store.ProposePartition(&stuck, false); err != nil {
		t.Fatal(err)
	}
	nodes := []string{"node-2", "node-1", "node-3"}
	f.manager.router.UpdatePartitionAssignment(0, "node-2", nodes, nodes)
	f.positions.set("node-1", ReplicaPosition{LastTerm: 1, LastOffset: 100})
	f.positions.set("node-2", ReplicaPosition{LastTerm: 1, LastOffset: 5})

	f.manager.advanceTransfers()
	if got := f.committed(t); got.TransferTo != "" || got.LeaderID != "node-1" {
		t.Fatalf("a handoff past its time limit was left in place (leader=%s transfer_to=%q)", got.LeaderID, got.TransferTo)
	}
}

// Only the Raft leader changes assignments; a follower's ring opinion and
// position data must not produce commits.
func TestLeadershipChangesNeedRaftLeadership(t *testing.T) {
	f := newLeadershipFixture(t, "node-1", "node-1", 3, 2, "node-1", "node-2", "node-3")
	nodes := []string{"node-2", "node-1", "node-3"}
	f.manager.router.UpdatePartitionAssignment(0, "node-2", nodes, nodes)
	f.positions.set("node-1", ReplicaPosition{LastTerm: 1, LastOffset: 100})
	f.positions.set("node-2", ReplicaPosition{LastTerm: 1, LastOffset: 100})
	f.store.leader = false
	before := f.store.proposals

	f.manager.syncClusterState()
	f.manager.advanceTransfers()
	if f.store.proposals != before {
		t.Fatal("a node that is not the Raft leader proposed an assignment change")
	}
}

// A node acts as leader only of what the cluster has committed to it. The
// hash ring is not enough: a node that has just joined does not have the
// committed assignments yet, and its ring would have it take over partitions
// another node is leading.
func TestLeadershipComesOnlyFromCommittedAssignments(t *testing.T) {
	f := newLeadershipFixture(t, "node-1", "node-2", 3, 2, "node-1", "node-2", "node-3")
	nodes := []string{"node-1", "node-2", "node-3"}
	// Partition 1 is in the ring, which prefers this node, and has no
	// committed assignment.
	f.manager.router.UpdatePartitionAssignment(1, "node-1", nodes, nodes)

	f.manager.reconcileLocalLeadership()
	if _, promoted := f.accessor.promoteCalls[1]; promoted {
		t.Fatal("node promoted itself for a partition with no committed assignment")
	}
	if f.manager.IsPartitionWritable(1) || f.manager.IsPartitionLeader(1) {
		t.Fatal("node reports itself leader of a partition with no committed assignment")
	}
	if leader, err := f.manager.router.GetPartitionLeader(1); err == nil {
		t.Fatalf("router names %v leader of a partition with no committed assignment", leader)
	}

	// Once the assignment is committed, the node takes the partition up.
	if err := f.store.ProposePartition(&PartitionInfo{ID: 1, LeaderID: "node-1", Replicas: nodes, ISR: nodes, Epoch: 1, State: PartitionStateOnline}, true); err != nil {
		t.Fatal(err)
	}
	f.manager.reconcileLocalLeadership()
	if _, promoted := f.accessor.promoteCalls[1]; !promoted {
		t.Fatal("node did not take up a partition committed to it")
	}
	if !f.manager.IsPartitionWritable(1) {
		t.Fatal("committed leader does not accept publishes")
	}
}

// Partitions that never had a leader get one only when the cluster is known
// to be complete, so that a cluster whose nodes start one after another is
// assigned once, across all of them, instead of to the first node alone.
func TestInitialAssignmentWaitsForTheClusterToForm(t *testing.T) {
	nodes := []string{"node-2", "node-1", "node-3"}
	assigned := func(f *leadershipFixture) bool {
		f.manager.syncClusterState()
		_, committed := f.store.Partition(1)
		return committed
	}

	t.Run("membership unchanged for the formation wait", func(t *testing.T) {
		f := newLeadershipFixture(t, "node-1", "node-1", 3, 2, "node-1", "node-2", "node-3")
		f.manager.config.FormationWait = 5 * time.Second
		f.manager.router.UpdatePartitionAssignment(1, "node-2", nodes, nodes)

		f.manager.noteMembershipChange() // a node has just joined
		if assigned(f) {
			t.Fatal("partition assigned while the cluster was still forming")
		}
		// Partitions that already have a leader are not held up by this.
		if !f.manager.IsPartitionWritable(0) || !f.manager.PartitionHasLeader(0) || f.manager.PartitionHasLeader(1) {
			t.Fatal("leadership of assigned and unassigned partitions is misreported while forming")
		}

		f.manager.membershipChangedAt.Store(time.Now().Add(-6 * time.Second).UnixNano())
		if !assigned(f) {
			t.Fatal("partition not assigned after membership settled")
		}
		if info, _ := f.store.Partition(1); info.LeaderID != "node-2" {
			t.Fatalf("assigned to %q, want the ring's choice node-2", info.LeaderID)
		}
	})

	t.Run("the expected number of nodes is up", func(t *testing.T) {
		f := newLeadershipFixture(t, "node-1", "node-1", 3, 2, "node-1", "node-2", "node-3")
		f.manager.config.FormationWait = time.Hour
		f.manager.router.UpdatePartitionAssignment(1, "node-2", nodes, nodes)
		f.manager.noteMembershipChange()

		f.manager.config.ExpectedNodes = 4
		if assigned(f) {
			t.Fatal("partition assigned with three of four expected nodes up")
		}
		f.manager.config.ExpectedNodes = 3
		if !assigned(f) {
			t.Fatal("partition not assigned although every expected node is up")
		}
	})

	t.Run("no wait configured", func(t *testing.T) {
		f := newLeadershipFixture(t, "node-1", "node-1", 3, 2, "node-1", "node-2", "node-3")
		f.manager.router.UpdatePartitionAssignment(1, "node-2", nodes, nodes)
		f.manager.noteMembershipChange()
		if !assigned(f) {
			t.Fatal("partition not assigned at once with a zero formation wait")
		}
	})
}

// Readiness is about serving publishes: every partition needs a committed
// leader, and this node must have taken up the ones committed to it.
func TestLeadershipReady(t *testing.T) {
	f := newLeadershipFixture(t, "node-1", "node-1", 3, 2, "node-1", "node-2", "node-3")
	nodes := []string{"node-1", "node-2", "node-3"}
	expect := func(want bool, when string) {
		t.Helper()
		if ready, detail := f.manager.LeadershipReady(); ready != want {
			t.Fatalf("%s: ready=%v (%s), want %v", when, ready, detail, want)
		}
	}

	expect(false, "partition committed to this node but not yet taken up")
	f.manager.reconcileLocalLeadership()
	expect(true, "after taking up its partition")

	f.manager.router.UpdatePartitionAssignment(1, "node-2", nodes, nodes)
	expect(false, "a partition has no committed leader")
	if err := f.store.ProposePartition(&PartitionInfo{ID: 1, LeaderID: "node-2", Replicas: nodes, ISR: nodes, Epoch: 1, State: PartitionStateOnline}, true); err != nil {
		t.Fatal(err)
	}
	expect(true, "the other partition is led by another node")

	handoff := f.committed(t)
	handoff.State, handoff.TransferTo = PartitionStateRebalancing, "node-2"
	if err := f.store.ProposePartition(&handoff, false); err != nil {
		t.Fatal(err)
	}
	expect(false, "its partition is being handed over")
}

// The first assignment of a partition takes account of what its replicas
// already hold. In a new cluster that is nothing. After a restore it is logs
// of different lengths, since every node's backup is its own, and epochs
// from the cluster's previous life.
func TestFirstAssignment_ContinuesFromWhatReplicasHold(t *testing.T) {
	nodes := []string{"node-1", "node-2", "node-3"}
	setup := func(t *testing.T, minISR int) *leadershipFixture {
		f := newLeadershipFixture(t, "node-1", "node-1", 3, minISR, nodes...)
		f.manager.router.UpdatePartitionAssignment(1, "node-1", nodes, nodes)
		return f
	}
	assigned := func(f *leadershipFixture) (PartitionInfo, bool) {
		f.manager.syncClusterState()
		return f.store.Partition(1)
	}

	t.Run("a new cluster follows the ring", func(t *testing.T) {
		f := setup(t, 2)
		info, ok := assigned(f)
		if !ok || info.LeaderID != "node-1" || info.Epoch != 1 {
			t.Fatalf("assignment = %+v (committed=%v), want node-1 at epoch 1", info, ok)
		}
	})

	t.Run("restored replicas: the most complete leads, above every accepted epoch", func(t *testing.T) {
		f := setup(t, 2)
		// node-1, the ring's choice, was backed up a moment before node-2.
		f.positions.set("node-1", ReplicaPosition{LastOffset: 90, LastTerm: 7, Epoch: 7})
		f.positions.set("node-2", ReplicaPosition{LastOffset: 100, LastTerm: 7, Epoch: 7})
		f.positions.set("node-3", ReplicaPosition{LastOffset: 120, LastTerm: 6, Epoch: 9})
		info, ok := assigned(f)
		if !ok {
			t.Fatal("partition was not assigned")
		}
		if info.LeaderID != "node-2" {
			t.Fatalf("leader = %s, want node-2: its log ends in the latest term, and furthest within it", info.LeaderID)
		}
		if info.Epoch != 10 {
			t.Fatalf("epoch = %d, want 10: one replica has accepted epoch 9 and would refuse anything lower", info.Epoch)
		}
	})

	t.Run("equal logs keep the ring's choice", func(t *testing.T) {
		f := setup(t, 2)
		for _, id := range nodes {
			f.positions.set(id, ReplicaPosition{LastOffset: 100, LastTerm: 3, Epoch: 3})
		}
		if info, ok := assigned(f); !ok || info.LeaderID != "node-1" || info.Epoch != 4 {
			t.Fatalf("assignment = %+v (committed=%v), want node-1 at epoch 4", info, ok)
		}
	})

	t.Run("it waits for the replica the ring would have lead", func(t *testing.T) {
		f := setup(t, 2)
		f.positions.unreachable["node-1"] = true
		if _, ok := assigned(f); ok {
			t.Fatal("partition assigned without knowing what its intended leader holds")
		}
		delete(f.positions.unreachable, "node-1")
		if info, ok := assigned(f); !ok || info.LeaderID != "node-1" {
			t.Fatalf("assignment after the leader answered = %+v (committed=%v)", info, ok)
		}
	})

	t.Run("it waits for enough replicas to rule out a longer log", func(t *testing.T) {
		// Writes acknowledged by the leader alone may be on any one replica,
		// so all three have to answer.
		f := setup(t, 1)
		f.positions.unreachable["node-3"] = true
		if _, ok := assigned(f); ok {
			t.Fatal("partition assigned although a replica that may hold the longest log has not answered")
		}
		delete(f.positions.unreachable, "node-3")
		if _, ok := assigned(f); !ok {
			t.Fatal("partition not assigned after every replica answered")
		}
	})
}

// An election also commits above every epoch the replicas hold, not just
// above the cluster's own count.
func TestElection_EpochIsAboveWhatReplicasAccepted(t *testing.T) {
	f := newLeadershipFixture(t, "node-1", "node-3", 3, 2, "node-1", "node-2", "node-3")
	f.membership.nodes["node-3"].State = NodeStateDead
	f.positions.set("node-1", ReplicaPosition{LastOffset: 10, LastTerm: 4, Epoch: 4})
	f.positions.set("node-2", ReplicaPosition{LastOffset: 12, LastTerm: 4, Epoch: 12})

	info := f.committed(t)
	f.manager.electNewLeader(0, &info)
	got := f.committed(t)
	if got.LeaderID != "node-2" || got.Epoch != 13 {
		t.Fatalf("elected %s at epoch %d, want node-2 at epoch 13", got.LeaderID, got.Epoch)
	}
}

// The FSM raises the epoch on every change and honours a proposal for a
// higher one; it never lowers it.
func TestFSM_UpdateHonoursAHigherProposedEpoch(t *testing.T) {
	store := &fsmStore{fsm: NewClusterFSM(), leader: true}
	propose := func(epoch int64, assign bool) int64 {
		t.Helper()
		if err := store.ProposePartition(&PartitionInfo{ID: 0, LeaderID: "node-1", Replicas: []string{"node-1"}, Epoch: epoch, State: PartitionStateOnline}, assign); err != nil {
			t.Fatal(err)
		}
		info, _ := store.Partition(0)
		return info.Epoch
	}
	if got := propose(1, true); got != 1 {
		t.Fatalf("assigned at epoch %d, want 1", got)
	}
	if got := propose(1, false); got != 2 {
		t.Fatalf("an ordinary update gave epoch %d, want 2", got)
	}
	if got := propose(40, false); got != 40 {
		t.Fatalf("an update asking for epoch 40 gave %d", got)
	}
	if got := propose(3, false); got != 41 {
		t.Fatalf("an update asking for a lower epoch gave %d, want 41", got)
	}
}

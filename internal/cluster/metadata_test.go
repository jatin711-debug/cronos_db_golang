package cluster

import (
	"bytes"
	"encoding/json"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func plainRaft(t *testing.T, id, addr, dir string) *RaftNode {
	t.Helper()
	node, err := NewRaftNode(&ClusterConfig{
		NodeID:            id,
		RaftAddr:          addr,
		RaftDataDir:       dir,
		HeartbeatInterval: 500 * time.Millisecond,
		ElectionTimeout:   time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return node
}

// A node that stops takes a snapshot, and its next start begins from it: the
// assignments are there before the node has heard from anyone, instead of
// being replayed from the start of a log that only grew.
func TestMetadata_SnapshotAtShutdownServesTheNextStart(t *testing.T) {
	dir, addr := t.TempDir(), freeAddr(t)
	node := plainRaft(t, "node-a", addr, dir)
	if err := node.Bootstrap(); err != nil {
		t.Fatal(err)
	}
	if err := node.WaitForLeader(15 * time.Second); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the leader's metadata is current", node.MetadataCurrent)
	for epoch := int64(1); epoch <= 5; epoch++ {
		if err := node.ProposePartition(&PartitionInfo{ID: 7, LeaderID: "node-a", Replicas: []string{"node-a"}, Epoch: epoch}, epoch == 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := node.Shutdown(); err != nil {
		t.Fatal(err)
	}

	again := plainRaft(t, "node-a", addr, dir)
	defer func() { _ = again.Shutdown() }()
	snapshots, err := again.snapshotStore.List()
	if err != nil || len(snapshots) == 0 {
		t.Fatalf("no snapshot of the metadata after a shutdown (%v)", err)
	}
	// Before any election: what is there was restored, not replayed.
	if info, ok := again.Partition(7); !ok || info.LeaderID != "node-a" {
		t.Fatalf("partition 7 is %+v (found=%v) at start; want it restored from the snapshot", info, ok)
	}
	if again.MetadataCurrent() {
		t.Fatal("the metadata counts as current before the node has heard from any leader")
	}
}

// A follower counts its copy as current only once it has heard from the
// leader and applied what the leader committed.
func TestMetadata_FollowerIsCurrentOnceCaughtUp(t *testing.T) {
	leader := plainRaft(t, "node-a", freeAddr(t), t.TempDir())
	defer func() { _ = leader.Shutdown() }()
	follower := plainRaft(t, "node-b", freeAddr(t), t.TempDir())
	defer func() { _ = follower.Shutdown() }()
	if err := leader.Bootstrap(); err != nil {
		t.Fatal(err)
	}
	if err := leader.WaitForLeader(15 * time.Second); err != nil {
		t.Fatal(err)
	}
	for epoch := int64(1); epoch <= 20; epoch++ {
		if err := leader.ProposePartition(&PartitionInfo{ID: 3, LeaderID: "node-a", Replicas: []string{"node-a", "node-b"}, Epoch: epoch}, epoch == 1); err != nil {
			t.Fatal(err)
		}
	}
	if follower.MetadataCurrent() {
		t.Fatal("a node that has not joined counts its metadata as current")
	}
	if err := leader.Join("node-b", follower.config.RaftAddr); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the follower's metadata is current", follower.MetadataCurrent)
	if info, ok := follower.Partition(3); !ok || info.Epoch != leader.mustPartition(t, 3).Epoch {
		t.Fatalf("follower holds %+v when it counts as current; the leader holds epoch %d", info, leader.mustPartition(t, 3).Epoch)
	}
}

// A follower that restarts with its log hears the leader's heartbeats before
// the leader's replication tells it how far the log is committed. It must not
// count itself caught up meanwhile: its copy then holds nothing of its log, or
// only the start of it, and it would act on assignments long replaced.
// Whether the gap opens here depends on how far the leader's retries to the
// follower have backed off by the time it is back; the next test produces it
// every time.
func TestMetadata_RestartedFollowerIsNotCurrentBeforeItHasReplayed(t *testing.T) {
	leader := plainRaft(t, "node-a", freeAddr(t), t.TempDir())
	defer func() { _ = leader.Shutdown() }()
	followerAddr, followerDir := freeAddr(t), t.TempDir()
	follower := plainRaft(t, "node-b", followerAddr, followerDir)
	if err := leader.Bootstrap(); err != nil {
		t.Fatal(err)
	}
	if err := leader.WaitForLeader(15 * time.Second); err != nil {
		t.Fatal(err)
	}
	if err := leader.Join("node-b", followerAddr); err != nil {
		t.Fatal(err)
	}
	for epoch := int64(1); epoch <= 20; epoch++ {
		if err := leader.ProposePartition(&PartitionInfo{ID: 3, LeaderID: "node-a", Replicas: []string{"node-a", "node-b"}, Epoch: epoch}, epoch == 1); err != nil {
			t.Fatal(err)
		}
	}
	want := leader.mustPartition(t, 3).Epoch
	eventually(t, "the follower has applied everything", func() bool {
		info, ok := follower.Partition(3)
		return ok && info.Epoch == want
	})
	// Killed: no snapshot at shutdown, as when a process dies.
	if err := follower.raft.Shutdown().Error(); err != nil {
		t.Fatal(err)
	}
	_ = follower.transport.Close()
	if closer, ok := follower.logStore.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
	time.Sleep(2 * time.Second) // the leader's replication to it backs off meanwhile

	again := plainRaft(t, "node-b", followerAddr, followerDir)
	defer func() { _ = again.Shutdown() }()
	deadline := time.Now().Add(30 * time.Second)
	for !again.MetadataCurrent() {
		if time.Now().After(deadline) {
			t.Fatal("the restarted follower never counted its metadata as current")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if info, ok := again.Partition(3); !ok || info.Epoch != want {
		t.Fatalf("the restarted follower counts itself current holding partition 3 as %+v (found=%v); the cluster has it at epoch %d", info, ok, want)
	}
}

type fakeProgress struct {
	mu                    sync.Mutex
	commit, applied, last uint64
}

func (f *fakeProgress) State() raft.RaftState  { return raft.Follower }
func (f *fakeProgress) LastContact() time.Time { return time.Now() } // heartbeats keep coming

func (f *fakeProgress) CommitIndex() uint64  { f.mu.Lock(); defer f.mu.Unlock(); return f.commit }
func (f *fakeProgress) AppliedIndex() uint64 { f.mu.Lock(); defer f.mu.Unlock(); return f.applied }
func (f *fakeProgress) LastIndex() uint64    { f.mu.Lock(); defer f.mu.Unlock(); return f.last }

func (f *fakeProgress) set(commit, applied uint64) {
	f.mu.Lock()
	f.commit, f.applied = commit, applied
	f.mu.Unlock()
}

// The order of events above, without waiting for Raft's retries to produce
// it: the restarted follower holds 40 entries and hears heartbeats well before
// the leader's replication says how far they are committed.
func TestMetadata_FollowerIsNotCurrentUntilItsLogIsCommittedAndApplied(t *testing.T) {
	node := &RaftNode{contactWithin: time.Second}
	progress := &fakeProgress{last: 40}
	notCurrentFor := func(what string) {
		t.Helper()
		for until := time.Now().Add(metadataSettle + 300*time.Millisecond); time.Now().Before(until); {
			if node.followerCurrent(progress) {
				t.Fatalf("counts as current %s", what)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	notCurrentFor("on heartbeats alone, with nothing of its log committed")
	progress.set(25, 25)
	notCurrentFor("with only part of its log committed")
	progress.set(40, 30)
	notCurrentFor("before it has applied what is committed")
	progress.set(40, 40)
	eventually(t, "current once its whole log is committed and applied", func() bool {
		return node.followerCurrent(progress)
	})
}

func (n *RaftNode) mustPartition(t *testing.T, id int32) PartitionInfo {
	t.Helper()
	info, ok := n.Partition(id)
	if !ok {
		t.Fatalf("partition %d is not committed", id)
	}
	return info
}

// A snapshot replaces entries, and with them what applying them would have
// set off: a running node that is sent one learns of new assignments and of
// nodes removed meanwhile.
func TestFSMRestore_ReportsChangesTheSnapshotCarries(t *testing.T) {
	fsm := NewClusterFSM()
	fsm.state.Nodes["node-a"] = &Node{ID: "node-a"}
	fsm.state.Nodes["node-b"] = &Node{ID: "node-b"}
	var (
		mu      sync.Mutex
		removed []string
		changes int
	)
	done := make(chan struct{}, 1)
	fsm.SetOnNodeRemoved(func(id string) {
		mu.Lock()
		removed = append(removed, id)
		mu.Unlock()
		done <- struct{}{}
	})
	fsm.SetOnPartitionChange(func() {
		mu.Lock()
		changes++
		mu.Unlock()
	})

	data, err := json.Marshal(&ClusterState{
		Nodes:      map[string]*Node{"node-a": {ID: "node-a"}},
		Partitions: map[int32]*PartitionInfo{0: {ID: 0, LeaderID: "node-a", Epoch: 4}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := fsm.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the removal of node-b was not reported")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(removed) != 1 || removed[0] != "node-b" {
		t.Fatalf("removed %v, want [node-b]", removed)
	}
	if changes != 1 {
		t.Fatalf("partition changes reported %d times, want once", changes)
	}
	if info, ok := fsm.Partition(0); !ok || info.Epoch != 4 {
		t.Fatalf("partition 0 is %+v after the restore", info)
	}
}

type fakeMetadata struct {
	mu      sync.Mutex
	current bool
}

func (f *fakeMetadata) MetadataCurrent() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.current
}

func (f *fakeMetadata) set(current bool) {
	f.mu.Lock()
	f.current = current
	f.mu.Unlock()
}

// Until its copy of the committed assignments is current, a node takes up
// nothing, says it is not ready, and as the Raft leader decides nothing.
func TestManager_WaitsForCurrentMetadata(t *testing.T) {
	f := newLeadershipFixture(t, "node-1", "node-1", 3, 2, "node-1", "node-2", "node-3")
	metadata := &fakeMetadata{}
	f.manager.metadata = metadata
	proposals := f.store.proposals

	f.manager.reconcileLocalLeadership()
	if _, promoted := f.accessor.promoteCalls[0]; promoted {
		t.Fatal("took up a partition before the metadata was current")
	}
	if ready, detail := f.manager.LeadershipReady(); ready {
		t.Fatalf("ready (%s) before the metadata was current", detail)
	}
	delete(f.membership.nodes, "node-2")
	f.manager.performLeaderTasks()
	f.manager.syncClusterState()
	f.manager.advanceTransfers()
	if f.store.proposals != proposals {
		t.Fatalf("%d proposals made before the metadata was current", f.store.proposals-proposals)
	}

	metadata.set(true)
	f.manager.reconcileLocalLeadership()
	if _, promoted := f.accessor.promoteCalls[0]; !promoted {
		t.Fatal("did not take up its partition once the metadata was current")
	}
	if ready, detail := f.manager.LeadershipReady(); !ready {
		t.Fatalf("not ready (%s) once the metadata was current", detail)
	}
}

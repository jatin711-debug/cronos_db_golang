package partition

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func storedEpoch(t *testing.T, path string) leadershipRecord {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	record, err := parseLeadershipRecord(data)
	if err != nil {
		t.Fatalf("epoch file %q: %v", data, err)
	}
	return record
}

// Leadership reconciliation calls PromoteToLeader with the unchanged epoch on
// every tick while holding the manager lock. Re-writing (and fsyncing) the
// epoch file each time stalled every publish on the node, so an unchanged
// epoch must not touch the disk.
func TestPersistEpochSkipsUnchangedEpoch(t *testing.T) {
	p := &Partition{DataDir: t.TempDir()}
	epochFile := filepath.Join(p.DataDir, "epoch.json")

	if err := p.PersistEpoch(5); err != nil {
		t.Fatal(err)
	}
	if got := storedEpoch(t, epochFile); got.Epoch != 5 {
		t.Fatalf("stored epoch = %+v, want 5", got)
	}

	// Removing the file makes any further write observable.
	if err := os.Remove(epochFile); err != nil {
		t.Fatal(err)
	}
	if err := p.PersistEpoch(5); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(epochFile); !os.IsNotExist(err) {
		t.Fatalf("unchanged epoch was written again (stat err = %v)", err)
	}

	if err := p.PersistEpoch(6); err != nil {
		t.Fatal(err)
	}
	if got := storedEpoch(t, epochFile); got.Epoch != 6 {
		t.Fatalf("stored epoch = %+v, want 6", got)
	}
	if err := p.PersistEpoch(5); err == nil {
		t.Fatal("epoch regression must still be rejected")
	}
}

// A restarted partition must recognise the epoch it already has on disk, and
// still persist a newer one.
func TestPersistEpochSurvivesRestart(t *testing.T) {
	dataDir := t.TempDir()
	cfg := &types.Config{DataDir: dataDir, PartitionCount: 1, FsyncMode: "periodic", FlushIntervalMS: 10, TickMS: 10, WheelSize: 64, DedupTTLHours: 1, BloomCapacity: 1000}

	pm := NewPartitionManager("node", cfg)
	if err := pm.CreatePartition(0, "topic"); err != nil {
		t.Fatal(err)
	}
	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.PersistEpoch(3); err != nil {
		t.Fatal(err)
	}
	epochFile := filepath.Join(p.DataDir, "epoch.json")
	if err := pm.Close(); err != nil {
		t.Fatal(err)
	}

	pm = NewPartitionManager("node", cfg)
	defer pm.Close()
	if err := pm.CreatePartition(0, "topic"); err != nil {
		t.Fatal(err)
	}
	p, err = pm.GetInternalPartition(0)
	if err != nil {
		t.Fatal(err)
	}
	if p.Epoch() != 3 {
		t.Fatalf("epoch after restart = %d, want 3", p.Epoch())
	}
	before, err := os.Stat(epochFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.PersistEpoch(3); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(epochFile)
	if err != nil {
		t.Fatal(err)
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatal("epoch loaded from disk was rewritten although unchanged")
	}
	if err := p.PersistEpoch(4); err != nil {
		t.Fatal(err)
	}
	if got := storedEpoch(t, epochFile); got.Epoch != 4 {
		t.Fatalf("stored epoch = %+v, want 4", got)
	}
}

// A term has one writer. Once a replica has accepted a leader for an epoch, a
// second node claiming the same epoch is refused, across restarts too; only a
// newer epoch moves leadership. Without this, two nodes that both believe they
// lead at the same epoch would each get their writes accepted.
func TestAcceptLeadershipAllowsOneHolderPerEpoch(t *testing.T) {
	dir := t.TempDir()
	p := &Partition{DataDir: dir}

	if err := p.AcceptLeadership(3, "node-a"); err != nil {
		t.Fatal(err)
	}
	if err := p.AcceptLeadership(3, "node-a"); err != nil {
		t.Fatalf("the holder re-asserting its epoch was refused: %v", err)
	}
	if err := p.AcceptLeadership(3, "node-b"); err == nil {
		t.Fatal("a second node was accepted as leader of an epoch that already has one")
	}
	if err := p.AcceptLeadership(2, "node-b"); err == nil {
		t.Fatal("an older epoch was accepted")
	}

	// The holder is remembered across a restart.
	restarted := &Partition{DataDir: dir}
	record := storedEpoch(t, filepath.Join(dir, "epoch.json"))
	restarted.restoreLeadership(record)
	if err := restarted.AcceptLeadership(3, "node-b"); err == nil {
		t.Fatal("after a restart a second node was accepted for the same epoch")
	}
	if err := restarted.AcceptLeadership(4, "node-b"); err != nil {
		t.Fatalf("a newer epoch must move leadership: %v", err)
	}
	if restarted.Epoch() != 4 || restarted.EpochLeader() != "node-b" {
		t.Fatalf("leadership = (%d, %q), want (4, node-b)", restarted.Epoch(), restarted.EpochLeader())
	}
}

// An epoch recorded before leaders identified themselves is claimed by the
// first node that does, and files written by older builds still load.
func TestAcceptLeadershipUpgradesUnattributedEpoch(t *testing.T) {
	dir := t.TempDir()
	epochFile := filepath.Join(dir, "epoch.json")
	if err := os.WriteFile(epochFile, []byte("7"), 0600); err != nil {
		t.Fatal(err)
	}
	record := storedEpoch(t, epochFile)
	if record.Epoch != 7 || record.LeaderID != "" {
		t.Fatalf("legacy epoch file read as %+v, want epoch 7 with no holder", record)
	}

	p := &Partition{DataDir: dir}
	p.restoreLeadership(record)
	if err := p.AcceptLeadership(7, "node-a"); err != nil {
		t.Fatal(err)
	}
	if err := p.AcceptLeadership(7, "node-b"); err == nil {
		t.Fatal("the epoch was claimed twice")
	}
	if got := storedEpoch(t, epochFile); got.Epoch != 7 || got.LeaderID != "node-a" {
		t.Fatalf("stored leadership = %+v, want epoch 7 held by node-a", got)
	}
}

// A node reports the position of a partition it holds on disk even when it
// has not loaded it, which is how every partition starts after a restart. A
// partition it has never held stays unknown, and asking does not create it.
func TestLocalReplicaPosition_LoadsThePartitionFromDisk(t *testing.T) {
	cfg := unacceptedTestConfig(t)
	cfg.PartitionCount = 4
	cfg.ClusterEnabled = true

	first := NewPartitionManager("node-1", cfg)
	if err := first.PromoteToLeader(2, 7); err != nil {
		t.Fatal(err)
	}
	p, _ := first.GetInternalPartition(2)
	for i := 0; i < 5; i++ {
		if err := p.Wal.AppendEvent(&types.Event{MessageId: fmt.Sprintf("m%d", i), Topic: "orders", Payload: []byte("p"), ScheduleTs: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	// The node restarts. In a cluster nothing is loaded until it is used.
	restarted := NewPartitionManager("node-1", cfg)
	t.Cleanup(func() { restarted.Close() })
	if _, err := restarted.GetInternalPartition(2); err == nil {
		t.Fatal("setup: the partition is loaded before anything asked for it")
	}

	position := restarted.LocalReplicaPosition(2)
	if !position.Found || position.LastOffset != 4 || position.Epoch != 7 {
		t.Fatalf("position of a partition on disk = %+v, want found, last offset 4, epoch 7", position)
	}
	if position.AcceptingWrites {
		t.Fatal("a partition loaded to report its position claims to accept writes")
	}

	// The epoch it reports is what a new cluster has to continue from: this
	// replica refuses to lead at an epoch it has already passed.
	if err := restarted.PromoteToLeader(2, 1); err == nil {
		t.Fatal("a replica that accepted epoch 7 was promoted at epoch 1")
	}
	if err := restarted.PromoteToLeader(2, position.Epoch+1); err != nil {
		t.Fatalf("promotion above the reported epoch: %v", err)
	}

	if unknown := restarted.LocalReplicaPosition(3); unknown.Found || unknown.LastOffset != -1 {
		t.Fatalf("position of a partition this node never held = %+v", unknown)
	}
	if _, err := os.Stat(filepath.Join(cfg.DataDir, "partitions", "3")); !os.IsNotExist(err) {
		t.Fatalf("asking for the position of an unknown partition created it (err=%v)", err)
	}
}

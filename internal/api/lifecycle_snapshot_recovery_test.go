package api

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/internal/recoverystate"
	"github.com/jatin711-debug/cronos_db_golang/internal/replication"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func snapshotLifecycleState(t *testing.T) (*replicatedLifecycle, *types.EventReference, *types.EventReference) {
	t.Helper()
	c := newReplicatedLifecycle(t, true)
	canceled, done, pending := c.publish("canceled"), c.publish("done"), c.publish("pending")
	rev, err := c.state(canceled)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.cancel(canceled, "cancel", rev); err != nil {
		t.Fatal(err)
	}
	if err := c.leader.p.ConsumerGroup.CreateGroup("workers", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	e, err := c.leader.p.Wal.ReadEvent(done.Offset)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.leader.p.DLQ.Add(e, "failed", 3, "failed", "workers"); err != nil {
		t.Fatal(err)
	}
	if err := c.leader.p.ConsumerGroup.CommitDelivery("workers", 0, []*types.Event{e}); err != nil {
		t.Fatal(err)
	}
	return c, done, pending
}

func TestLifecycleSnapshotDoesNotApplyStateFromAnInterruptedTransfer(t *testing.T) {
	c, done, pending := snapshotLifecycleState(t)
	flaky := &flakySnapshots{ReplicationServiceServer: c.leader.h, failAfterFiles: 1}
	addr := serveReplicationService(t, flaky)
	dir := t.TempDir()
	r := newNamedClusterReplica(t, dir, "replacement")
	requireLifecycle(r)
	if err := r.p.Wal.AppendBatch(entries("old", 0, 0, pending.Offset)); err != nil {
		t.Fatal(err)
	}
	if err := r.p.Wal.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := r.p.ConsumerGroup.CreateGroup("workers", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	old, err := r.p.Wal.ReadEvent(pending.Offset)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.p.ConsumerGroup.CommitDelivery("workers", 0, []*types.Event{old}); err != nil {
		t.Fatal(err)
	}
	if err := r.pm.SyncPartitionFromLeader(0, addr); err == nil {
		t.Fatal("interrupted snapshot succeeded")
	}
	if !r.p.ConsumerGroup.IsCompleted("workers", 0, pending.Offset) || len(r.p.DLQ.GetByPartition(0)) != 0 || r.log()[0].MessageId != "old-0" {
		t.Fatal("partial transfer changed the live generation")
	}
	if err := r.pm.SyncPartitionFromLeader(0, addr); err != nil {
		t.Fatal(err)
	}
	if !r.p.ConsumerGroup.IsCompleted("workers", 0, done.Offset) || r.p.ConsumerGroup.IsCompleted("workers", 0, pending.Offset) || len(r.p.DLQ.GetByPartition(0)) != 1 {
		t.Fatal("installed generation mixed old completions or lost dead letters")
	}
	r.stop()
	r = newNamedClusterReplica(t, dir, "replacement")
	if !r.p.ConsumerGroup.IsCompleted("workers", 0, done.Offset) || r.p.ConsumerGroup.IsCompleted("workers", 0, pending.Offset) || len(r.p.DLQ.GetByPartition(0)) != 1 {
		t.Fatal("snapshot state was not durable")
	}
}

func TestLifecycleSnapshotRecoversAfterWALPublicationBeforeStateImport(t *testing.T) {
	c, done, pending := snapshotLifecycleState(t)
	dir := t.TempDir()
	r := newNamedClusterReplica(t, dir, "replacement")
	requireLifecycle(r)
	f := replication.NewFollower(0, r.p.Wal, "replacement", nil)
	f.SetLifecycleHooks(1, func() error { return r.pm.EnableReplicatedLifecycle(r.p) }, func() error { return nil })
	f.SetTermStartHook(1, r.p.EnableReplicatedTermStart)
	f.SetRecoveryHook(1, func() error { return errors.New("crash after WAL publication") })
	if err := f.InstallSnapshot(context.Background(), c.leader.serveReplication(), 0, 0); err == nil {
		t.Fatal("injected interruption did not fail")
	}
	if _, err := os.Stat(filepath.Join(r.p.DataDir, "segments", recoverystate.Filename)); err != nil {
		t.Fatal("recovery file did not stay with installed WAL")
	}
	r.stop()
	r = newNamedClusterReplica(t, dir, "replacement")
	if !r.p.ConsumerGroup.IsCompleted("workers", 0, done.Offset) || r.p.ConsumerGroup.IsCompleted("workers", 0, pending.Offset) || len(r.p.DLQ.GetByPartition(0)) != 1 {
		t.Fatal("restart did not finish the published recovery generation")
	}
	if _, err := os.Stat(filepath.Join(r.p.DataDir, "segments", recoverystate.Filename)); !os.IsNotExist(err) {
		t.Fatalf("applied recovery file retained: %v", err)
	}
}

func TestStandaloneLifecycleStillRefusesElectionFencesAndReplicatedProgress(t *testing.T) {
	cfg := &types.Config{DataDir: t.TempDir(), PartitionCount: 1, ReplicationFactor: 1, EventLifecycle: true, TickMS: 10, WheelSize: 64, SegmentSizeBytes: 1024, IndexInterval: 1, BloomCapacity: 1000, FsyncMode: "every_event"}
	pm := partition.NewPartitionManager("standalone", cfg)
	defer pm.Close()
	if err := pm.CreatePartition(0, "orders"); err != nil {
		t.Fatal(err)
	}
	h := NewReplicationServiceHandler(pm)
	if _, err := h.Position(context.Background(), &types.ReplicationPositionRequest{PartitionId: 0, FenceEpoch: 8}); err == nil {
		t.Fatal("standalone partition accepted an election fence")
	}
	if _, err := h.SyncConsumerProgress(context.Background(), &types.ReplicationProgressRequest{PartitionId: 0, Term: 8, LeaderId: "remote"}); err == nil {
		t.Fatal("standalone partition accepted replicated progress")
	}
}

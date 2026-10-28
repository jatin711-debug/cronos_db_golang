package partition

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/dedup"
	"github.com/jatin711-debug/cronos_db_golang/internal/logcontrol"
	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
)

type termModeFollower struct{ termFollower }

func (f *termModeFollower) AppendTermStart(ctx context.Context, req *types.ReplicationTermAppendRequest) (*types.ReplicationAppendResponse, error) {
	return f.Append(ctx, req.GetAppend())
}

func startTermModeFollower(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	types.RegisterReplicationServiceServer(srv, &termModeFollower{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func TestTermStartReadPathsRetentionBackupAndRecovery(t *testing.T) {
	cfg := unacceptedTestConfig(t)
	cfg.ClusterEnabled, cfg.ReplicationFactor, cfg.MinInSyncReplicas = true, 2, 2
	// Rotate after each record so the control can be pruned independently
	// of the pending user record in the following segment.
	cfg.SegmentSizeBytes, cfg.FsyncMode = 64, "every_event"
	cfg.HotWindowMinutes = 1
	pm := NewPartitionManager("leader", cfg)
	t.Cleanup(func() { pm.Close() })
	pm.SetTermStartRequirement(func(int32) bool { return true })
	sink := &feedSink{}
	pm.SetChangeFeed(sink.take)
	if err := pm.PromoteToLeader(0, 1); err != nil {
		t.Fatal(err)
	}
	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatal(err)
	}
	barrier, err := p.Wal.ReadEvent(0)
	if err != nil || !logcontrol.IsTermStart(barrier) || p.ActiveEvent(barrier) {
		t.Fatalf("term-start record: %v %v", barrier, err)
	}
	user := appendMaintenanceEvent(t, p, "user", "orders", time.Now().Add(time.Hour), 2048)
	if n, err := p.PruneWAL(context.Background(), storage.PruneOptions{AllCompleted: true}); err != nil || n != 0 {
		t.Fatalf("unaccepted term-start record was pruned: %d %v", n, err)
	}
	if err := pm.AddFollower(0, "follower", startTermModeFollower(t)); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "current-term prefix commit", func() bool { return p.AcceptedThrough() >= user.Offset })
	if err := p.Scheduler.Schedule(user); err != nil {
		t.Fatal(err)
	}
	sink.expect(t, 0, "[1]")
	if err := pm.PromoteToLeader(0, 2); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "new term-start record commit", func() bool { return p.AcceptedThrough() >= 2 })
	waitUntil(t, "change-feed cursor skips control-only tail", func() bool { at, _ := p.ChangeFeedPosition(); return at >= 2 })
	if sink.got(0) != "[1]" {
		t.Fatal("change feed exported an internal record")
	}
	if p.Scheduler.GetColdStoreCount() != 1 {
		t.Fatal("term-start record was scheduled as work")
	}
	if _, found, err := p.DedupStore.GetOffset(dedup.Key("", "")); err != nil || found {
		t.Fatalf("internal record acquired a dedup claim: %v %v", found, err)
	}
	if n, err := p.PruneWAL(context.Background(), storage.PruneOptions{AllCompleted: true}); err != nil || n == 0 || p.Wal.GetFirstOffset() != user.Offset {
		t.Fatalf("control pins retention or pending user was removed: n=%d first=%d error=%v", n, p.Wal.GetFirstOffset(), err)
	}
	backup := t.TempDir()
	if err := pm.Backup(backup); err != nil {
		t.Fatal(err)
	}
	if err := pm.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopening durable data keeps the mode even before membership is ready.
	pm = NewPartitionManager("leader", cfg)
	if err := pm.CreatePartition(0, "orders"); err != nil {
		t.Fatal(err)
	}
	p, _ = pm.GetInternalPartition(0)
	if !p.TermStartEnabled() {
		t.Fatal("restart forgot durable format")
	}
	if err := pm.PromoteToLeader(0, 3); err != nil {
		t.Fatal(err)
	}
	if p.Scheduler.GetColdStoreCount() != 1 {
		t.Fatal("restart scheduled control records")
	}
	if _, found, err := p.DedupStore.GetOffset(dedup.Key("", "")); err != nil || found {
		t.Fatal("restart seeded an internal dedup ID")
	}
	if err := pm.Close(); err != nil {
		t.Fatal(err)
	}
	restored := t.TempDir()
	if _, err := storage.RestoreBackup(backup, restored, storage.RestoreOptions{}); err != nil {
		t.Fatal(err)
	}
	cfgCopy := *cfg
	cfgCopy.DataDir = restored
	pm = NewPartitionManager("restored", &cfgCopy)
	if err := pm.CreatePartition(0, "orders"); err != nil {
		t.Fatal(err)
	}
	p, _ = pm.GetInternalPartition(0)
	if !p.TermStartEnabled() {
		t.Fatal("backup/restore lost the format requirement")
	}
	if err := pm.PromoteToLeader(0, 3); err != nil {
		t.Fatal(err)
	}
	if p.Scheduler.GetColdStoreCount() != 1 {
		t.Fatal("restored internal record became pending work")
	}
}

func TestTermStartLegacyPartitionDoesNotWriteControls(t *testing.T) {
	cfg := replicatedTestConfig(t)
	pm := NewPartitionManager("leader", cfg)
	defer pm.Close()
	if err := pm.PromoteToLeader(0, 1); err != nil {
		t.Fatal(err)
	}
	p, _ := pm.GetInternalPartition(0)
	if p.TermStartEnabled() || p.Wal.GetLastOffset() != -1 {
		t.Fatal("legacy partition changed its log without capability agreement")
	}
}

func TestTermStartIsNotMigratedAsAPublishByPartitionSplit(t *testing.T) {
	cfg := unacceptedTestConfig(t)
	cfg.PartitionCount, cfg.FsyncMode = 2, "every_event"
	pm := NewPartitionManager("split", cfg)
	defer pm.Close()
	if err := pm.CreatePartition(0, "orders"); err != nil {
		t.Fatal(err)
	}
	p, _ := pm.GetInternalPartition(0)
	p.Wal.SetCurrentTerm(1)
	if err := p.Wal.AppendEvent(logcontrol.NewTermStart(0)); err != nil {
		t.Fatal(err)
	}
	appendMaintenanceEvent(t, p, "move", "orders", time.Now().Add(time.Hour), 32)
	if err := NewSplitManager(pm).SplitPartition(0, 1, 0, ""); err != nil {
		t.Fatal(err)
	}
	dest, _ := pm.GetInternalPartition(1)
	if err := dest.Wal.Flush(); err != nil {
		t.Fatal(err)
	}
	events, err := dest.Wal.ReadEvents(0, dest.Wal.GetLastOffset())
	if err != nil || len(events) != 1 || events[0].MessageId != "move" {
		t.Fatalf("migrated a control as a publish: %v %v", events, err)
	}
}

package api

import (
	"context"
	"github.com/jatin711-debug/cronos_db_golang/internal/replication"
	"strings"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/delivery"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestDeadLettersSurviveLeaderLossAfterRetention(t *testing.T) {
	a, b := newPrunableReplica(t, "node-a"), newPrunableReplica(t, "node-b")
	if err := a.pm.PromoteToLeader(0, 1); err != nil {
		t.Fatal(err)
	}
	if err := a.pm.AddFollower(0, "node-b", b.serveReplication()); err != nil {
		t.Fatal(err)
	}
	handler := NewEventServiceHandler(a.pm, a.p.DedupStore, a.p.ConsumerGroup)
	for _, e := range bigOrders("dead-letter", 3) {
		r, err := handler.PublishBatch(context.Background(), &types.PublishBatchRequest{Events: []*types.Event{e}})
		if err != nil || !r.GetSuccess() {
			t.Fatalf("publish: %v %v", r, err)
		}
	}
	if err := a.p.ConsumerGroup.CreateGroup("g", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	event, err := a.p.Wal.ReadEvent(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.p.DLQ.Add(event, "failed-delivery", 5, "exhausted retries", "subscriber"); err != nil {
		t.Fatal(err)
	}
	a.finish("g", 0, 1)
	deadline := time.Now().Add(15 * time.Second)
	for !a.p.ReplLeader.DeadLettersReplicated(a.p.DLQ.Version()) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !a.p.ReplLeader.DeadLettersReplicated(a.p.DLQ.Version()) {
		t.Fatal("DLQ did not reach follower")
	}
	if n := a.prune(); n == 0 {
		t.Fatal("no completed segments pruned")
	}
	if err := b.p.FollowLogStart(a.p.Wal.GetFirstOffset()); err != nil {
		t.Fatal(err)
	}
	if _, err := b.p.Wal.ReadEvent(0); err == nil {
		t.Fatal("test failed to remove original WAL event")
	}
	// Replacement replicas join after the failed event's WAL segment is
	// gone. A snapshot must install its journal before exposing that log.
	addr := a.serveReplication()
	replacement := newPrunableReplica(t, "replacement")
	if err := replacement.pm.SyncPartitionFromLeader(0, addr); err != nil {
		t.Fatal(err)
	}
	if got := replacement.p.DLQ.Get(); len(got) != 1 || got[0].Event.MessageId != event.MessageId {
		t.Fatalf("replacement snapshot lost dead letter: %+v", got)
	}
	if replacement.p.Wal.GetFirstOffset() != a.p.Wal.GetFirstOffset() {
		t.Fatal("replacement did not install retained log")
	}
	failed := newPrunableReplica(t, "failed-sink")
	if err := failed.p.DLQ.Close(); err != nil {
		t.Fatal(err)
	}
	if err := failed.pm.SyncPartitionFromLeader(0, addr); err == nil {
		t.Fatal("snapshot installed despite failed dead-letter write")
	}
	if failed.p.Wal.GetLastOffset() != -1 {
		t.Fatal("failed journal write exposed snapshot WAL")
	}
	legacy := newPrunableReplica(t, "legacy")
	receiver := replication.NewFollower(0, legacy.p.Wal, "legacy", nil)
	if err := receiver.InstallSnapshot(context.Background(), addr, 0, 0); err == nil || !strings.Contains(err.Error(), "dead-letter replication support") {
		t.Fatalf("legacy receiver was not safely refused: %v", err)
	}
	if err := a.pm.DemoteFromLeader(0); err != nil {
		t.Fatal(err)
	}
	if err := a.pm.ReleasePartition(0); err != nil {
		t.Fatal(err)
	}
	if err := b.p.DLQ.Close(); err != nil {
		t.Fatal(err)
	}
	b.p.DLQ, err = delivery.NewDeadLetterQueue(b.p.DataDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	copies := b.p.DLQ.Get()
	if len(copies) != 1 || copies[0].Event.MessageId != event.MessageId || len(copies[0].Event.Payload) != len(event.Payload) || copies[0].Attempts != 5 {
		t.Fatalf("lost failed event after leader loss: %+v", copies)
	}
	if err := b.pm.PromoteToLeader(0, 2); err != nil {
		t.Fatal(err)
	}
}

func TestDeadLetterWriteFailureDoesNotInstallCompletion(t *testing.T) {
	r := newReplicaLog(t, "follower")
	if !r.send("leader", 1, entries("x", 1, 0, 0)).Success {
		t.Fatal("append failed")
	}
	if err := r.p.DLQ.Close(); err != nil {
		t.Fatal(err)
	}
	resp, err := r.h.SyncConsumerProgress(context.Background(), &types.ReplicationProgressRequest{PartitionId: 0, Term: 1, LeaderId: "leader", RequireDeadLetterAck: true, DeadLetters: []*types.DeadLetterRecord{{Event: entries("x", 1, 0, 0)[0], DeliveryId: "d"}}, Groups: []*types.ConsumerGroupProgress{{GroupId: "g", Topic: "orders", CommittedOffset: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Success || resp.DeadLettersDurable || r.p.ConsumerGroup.IsCompleted("g", 0, 0) {
		t.Fatal("failed DLQ write released completion")
	}
}

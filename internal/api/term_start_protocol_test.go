package api

import (
	"context"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/internal/logcontrol"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc/codes"
)

func termStartBatch() []*types.Event {
	e := logcontrol.NewTermStart(0)
	e.Offset, e.Term = 0, 1
	return []*types.Event{e}
}

func (r *replicaLog) sendTerm(events []*types.Event) (*types.ReplicationAppendResponse, error) {
	return r.h.AppendTermStart(context.Background(), &types.ReplicationTermAppendRequest{TermStartFormat: 1, Append: &types.ReplicationAppendRequest{PartitionId: 0, Term: 1, LeaderId: "leader", Events: events}})
}

func TestTermStartAppendValidatesBeforeWritingAndRejectsLegacyPaths(t *testing.T) {
	r := newClusterReplica(t, t.TempDir())
	_, err := r.sendTerm(termStartBatch())
	wantCode(t, err, codes.FailedPrecondition, "missing committed requirement")
	if r.p.Wal.GetNextOffset() != 0 || r.p.TermStartEnabled() {
		t.Fatal("unapproved format changed the partition")
	}
	r.pm.SetTermStartRequirement(func(int32) bool { return true })
	for name, corrupt := range map[string]func(*types.Event){
		"payload":   func(e *types.Event) { e.Payload = []byte("forged") },
		"version":   func(e *types.Event) { e.Meta[logcontrol.TermStartKey] = "2" },
		"partition": func(e *types.Event) { e.PartitionId = 1 },
		"metadata":  func(e *types.Event) { e.Meta["other"] = "x" },
	} {
		t.Run(name, func(t *testing.T) {
			batch := termStartBatch()
			corrupt(batch[0])
			_, err := r.sendTerm(batch)
			wantCode(t, err, codes.InvalidArgument, "malformed control record")
			if r.p.Wal.GetNextOffset() != 0 || r.p.TermStartEnabled() {
				t.Fatal("malformed control changed the partition")
			}
		})
	}
	r.pm.SetLifecycleRequirement(func(int32) bool { return true })
	_, err = r.sendLifecycle(1, 1, termStartBatch())
	wantCode(t, err, codes.FailedPrecondition, "term record on lifecycle-only RPC")
	resp, err := r.sendTerm(termStartBatch())
	if err != nil || !resp.GetSuccess() || !r.p.TermStartEnabled() {
		t.Fatalf("valid term append: %v %v", resp, err)
	}
	if r.p.LifecycleEnabled() {
		t.Fatal("ordinary term-start mode unnecessarily enabled lifecycle mutations")
	}
	_, err = r.h.Append(context.Background(), &types.ReplicationAppendRequest{PartitionId: 0, Term: 1, LeaderId: "leader"})
	wantCode(t, err, codes.FailedPrecondition, "empty legacy probe after format switch")
	_, err = r.sendLifecycle(1, 1, nil)
	wantCode(t, err, codes.FailedPrecondition, "lifecycle-only probe after format switch")
	if r.p.ActiveEvent(r.log()[0]) {
		t.Fatal("internal record is deliverable")
	}
}

func TestTermStartSnapshotChecksCapabilityAndPreservesModeOnRestart(t *testing.T) {
	source := newClusterReplica(t, t.TempDir())
	source.pm.SetTermStartRequirement(func(int32) bool { return true })
	if resp, err := source.sendTerm(termStartBatch()); err != nil || !resp.GetSuccess() {
		t.Fatalf("append: %v %v", resp, err)
	}
	stream := &mockReplicationSnapshotStream{ctx: context.Background()}
	err := source.h.Snapshot(&types.ReplicationSnapshotRequest{PartitionId: 0, SupportsDeadLetters: true}, stream)
	wantCode(t, err, codes.FailedPrecondition, "legacy snapshot receiver")
	if len(stream.chunks) != 0 {
		t.Fatal("sent snapshot data before rejecting unsupported receiver")
	}
	stream = &mockReplicationSnapshotStream{ctx: context.Background()}
	if err := source.h.Snapshot(&types.ReplicationSnapshotRequest{PartitionId: 0, SupportsDeadLetters: true, TermStartFormat: 1}, stream); err != nil {
		t.Fatal(err)
	}
	if last := stream.chunks[len(stream.chunks)-1].GetTrailer(); last.GetTermStartFormat() != 1 {
		t.Fatalf("snapshot lost its format: %v", last)
	}
	addr := serveReplicationService(t, source.h)
	refused := newClusterReplica(t, t.TempDir())
	if err := refused.pm.SyncPartitionFromLeader(0, addr); err == nil {
		t.Fatal("installed snapshot before committing the format requirement")
	}
	if refused.p.Wal.GetNextOffset() != 0 || refused.p.TermStartEnabled() {
		t.Fatal("refusal replaced local state")
	}
	dir := t.TempDir()
	r := newClusterReplica(t, dir)
	r.pm.SetTermStartRequirement(func(int32) bool { return true })
	if err := r.pm.SyncPartitionFromLeader(0, addr); err != nil {
		t.Fatal(err)
	}
	if !r.p.TermStartEnabled() || r.p.ActiveEvent(r.log()[0]) {
		t.Fatal("snapshot installed control as a user event")
	}
	r.stop()
	again := newClusterReplica(t, dir)
	if !again.p.TermStartEnabled() {
		t.Fatal("restart forgot the format marker")
	}
	if err := again.pm.StartPartition(0); err != nil {
		t.Fatal(err)
	}
	if again.p.ActiveEvent(again.log()[0]) {
		t.Fatal("restart resurrected control as a user event")
	}
}

func TestTermStartInterruptedSnapshotKeepsThePreviousGenerationUntilRetry(t *testing.T) {
	_, source, service, addr := snapshotSource(t, 12)
	func() {
		source.ReplicateMu.Lock()
		defer source.ReplicateMu.Unlock()
		if err := source.EnableReplicatedTermStart(); err != nil {
			t.Fatal(err)
		}
		source.Wal.SetCurrentTerm(1)
		if err := source.PersistEpoch(1); err != nil {
			t.Fatal(err)
		}
		if err := source.Wal.AppendEvent(logcontrol.NewTermStart(0)); err != nil {
			t.Fatal(err)
		}
		if err := source.Wal.Flush(); err != nil {
			t.Fatal(err)
		}
	}()
	r := newClusterReplica(t, t.TempDir())
	r.pm.SetTermStartRequirement(func(int32) bool { return true })
	if err := r.p.Wal.AppendEvent(&types.Event{MessageId: "keep", Topic: "orders", Payload: []byte("old")}); err != nil {
		t.Fatal(err)
	}
	if err := r.p.Wal.Flush(); err != nil {
		t.Fatal(err)
	}
	service.mu.Lock()
	service.failAfterFiles = 1
	service.mu.Unlock()
	if err := r.pm.SyncPartitionFromLeader(0, addr); err == nil {
		t.Fatal("interrupted snapshot reported success")
	}
	if e, err := r.p.Wal.ReadEvent(0); err != nil || e.MessageId != "keep" || r.p.TermStartEnabled() {
		t.Fatalf("interrupted transfer changed active state: %v %v", e, err)
	}
	if err := r.pm.SyncPartitionFromLeader(0, addr); err != nil {
		t.Fatal(err)
	}
	if !r.p.TermStartEnabled() || len(r.log()) != 13 || r.p.ActiveEvent(r.log()[12]) {
		t.Fatal("retry did not install a complete control-aware generation")
	}
}

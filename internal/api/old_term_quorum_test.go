package api

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/logcontrol"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Accept and durably acknowledge the old prefix, but withhold the term-start
// record. This models catch-up stopping between those records on real WALs.
type termStartGate struct {
	types.ReplicationServiceServer
	r         *replicaLog
	allow     atomic.Bool
	loseReply atomic.Bool
	lost      atomic.Int32
}

func (g *termStartGate) AppendTermStart(ctx context.Context, req *types.ReplicationTermAppendRequest) (*types.ReplicationAppendResponse, error) {
	if g.allow.Load() {
		resp, err := g.ReplicationServiceServer.AppendTermStart(ctx, req)
		if err == nil && resp.GetSuccess() && g.loseReply.Load() {
			g.lost.Add(1)
			return nil, status.Error(codes.Unavailable, "success reply was lost after durable append")
		}
		return resp, err
	}
	for n, e := range req.GetAppend().GetEvents() {
		if !logcontrol.IsTermStart(e) {
			continue
		}
		if n == 0 {
			return &types.ReplicationAppendResponse{Error: "term-start record withheld", Term: g.r.p.Epoch(), LastOffset: g.r.p.Wal.GetLastOffset(), NextOffset: g.r.p.Wal.GetNextOffset()}, nil
		}
		trimmed := proto.Clone(req).(*types.ReplicationTermAppendRequest)
		trimmed.Append.Events, trimmed.Append.Checksum = trimmed.Append.Events[:n], 0
		resp, err := g.ReplicationServiceServer.AppendTermStart(ctx, trimmed)
		if err == nil && resp.GetSuccess() {
			if err := g.r.p.Wal.Flush(); err != nil {
				return nil, err
			}
		}
		return resp, err
	}
	return g.ReplicationServiceServer.AppendTermStart(ctx, req)
}

func oldTermScenario(t *testing.T) (a, b, c *replicaLog, gate *termStartGate) {
	t.Helper()
	a, b, c = newReplicaLog(t, "node-a"), newReplicaLog(t, "node-b"), newReplicaLog(t, "node-c")
	for _, r := range []*replicaLog{a, b, c} {
		if resp := r.send("leader-1", 1, entries("base", 1, 0, 2)); !resp.GetSuccess() {
			t.Fatal(resp.GetError())
		}
	}
	if resp := a.send("node-a", 2, entries("y", 2, 3, 3)); !resp.GetSuccess() {
		t.Fatal(resp.GetError())
	}
	if resp := b.send("node-b", 3, entries("z", 3, 3, 3)); !resp.GetSuccess() {
		t.Fatal(resp.GetError())
	}
	for _, r := range []*replicaLog{a, b, c} {
		r.pm.SetTermStartRequirement(func(int32) bool { return true })
		if err := r.p.Wal.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.pm.PromoteToLeader(0, 4); err != nil {
		t.Fatal(err)
	}
	gate = &termStartGate{ReplicationServiceServer: c.h, r: c}
	if err := a.pm.AddFollower(0, "node-c", serveReplicationService(t, gate)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for a.p.ReplLeader.GetFollowerOffsets()["node-c"] != 3 {
		if time.Now().After(deadline) {
			t.Fatal("old prefix never reached C before the term-start record")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return
}

// An entry of an earlier term that a new leader brings to a quorum is not
// therefore safe: a later election can still pick a replica whose log ends
// in a newer term without it, and the entry is removed from the log after it
// was delivered. Raft's rule is that a leader counts replicas only for an
// entry of its own term; earlier entries are safe once one after them is.
//
// Three replicas, two needed. Term 1 wrote offsets 0-2 everywhere. Leader A
// of term 2 wrote y at offset 3 and reached nobody. Leader B of term 3 wrote
// z at offset 3 and reached nobody. A leads again in term 4 and sends y to C.
func TestNewLeaderDoesNotDeliverAnOldTermEntryOnlyItsReplicasCount(t *testing.T) {
	a, _, c, _ := oldTermScenario(t)
	// y is on A and C now, two of three. Whether that makes it deliverable:
	through := a.p.DeliverableThrough()
	stream := &mockRegionFetchStream{ctx: context.Background()}
	if err := NewCrossRegionServer(a.pm).FetchEvents(&types.RegionFetchRequest{RegionId: "remote", PartitionId: 0}, stream); err != nil || len(stream.responses) != 0 {
		t.Fatalf("cross-region fetch exported an uncommitted old prefix: %v %v", stream.responses, err)
	}
	if accepted := a.p.AcceptedThrough(); accepted >= 3 {
		t.Errorf("old-term entry accepted solely by replica count: %d", accepted)
	}
	if err := a.p.WithActiveEvent(a.log()[3], func(*types.Event) error {
		t.Error("old-term entry reached public replay before current-term quorum")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// B's log ends in term 3, after C's (term 2) at the same offset, so an
	// election that hears B and C picks B, and B's log replaces C's:
	resp, err := c.h.AppendTermStart(context.Background(), &types.ReplicationTermAppendRequest{TermStartFormat: 1, Append: &types.ReplicationAppendRequest{PartitionId: 0, Term: 5, LeaderId: "node-b", Events: entries("z", 3, 3, 3)}})
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("term 5 from B: %v", resp.GetError())
	}
	if got := c.log()[3].GetMessageId(); got == "y-3" {
		t.Fatalf("C kept y; the scenario did not play out")
	}
	if through >= 3 {
		t.Fatalf("leader A of term 4 delivers through offset %d, y included, before any entry of its own term is on a quorum; "+
			"y was then removed when B led (C now holds %s at offset 3)", through, c.log()[3].GetMessageId())
	}
}

func TestTermStartCommitsIdlePrefixAndNeverBecomesAUserEvent(t *testing.T) {
	a, b, c, gate := oldTermScenario(t)
	if a.p.DeliverableThrough() >= 3 {
		t.Fatal("old prefix released too early")
	}
	gate.allow.Store(true)
	deadline := time.Now().Add(10 * time.Second)
	for a.p.DeliverableThrough() < 4 {
		if time.Now().After(deadline) {
			t.Fatal("idle term-start record did not reach quorum")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if lastTerm, err := c.p.Wal.GetTermForOffset(4); err != nil || lastTerm != 4 {
		t.Fatalf("C lacks current-term proof: %d %v", lastTerm, err)
	}
	if lastTerm, err := b.p.Wal.GetTermForOffset(3); err != nil || lastTerm >= 4 {
		t.Fatalf("stale B no longer has the older election position: %d %v", lastTerm, err)
	}
	for _, e := range a.log() {
		sent := false
		if err := a.p.WithActiveEvent(e, func(*types.Event) error { sent = true; return nil }); err != nil {
			t.Fatal(err)
		}
		if sent == logcontrol.IsTermStart(e) {
			t.Fatalf("wrong replay disposition at offset %d", e.Offset)
		}
	}
	end := a.p.Wal.GetLastOffset()
	if err := a.pm.PromoteToLeader(0, 4); err != nil || a.p.Wal.GetLastOffset() != end {
		t.Fatalf("idempotent reconcile appended again: %v", err)
	}
	gate.allow.Store(false)
	if err := a.pm.PromoteToLeader(0, 5); err != nil {
		t.Fatal(err)
	}
	if got := a.p.Wal.GetLastOffset(); got != end+1 {
		t.Fatalf("new epoch has no single new term-start record: %d", got)
	}
	if a.p.DeliverableThrough() >= end+1 {
		t.Fatal("old epoch's quorum was reused for a new term")
	}
}

func TestTermStartCrossRegionFetchAdvancesPastAControlOnlyPage(t *testing.T) {
	a, _, _, gate := oldTermScenario(t)
	gate.allow.Store(true)
	deadline := time.Now().Add(10 * time.Second)
	for a.p.DeliverableThrough() < 4 {
		if time.Now().After(deadline) {
			t.Fatal("term-start record did not commit")
		}
		time.Sleep(10 * time.Millisecond)
	}
	stream := &mockRegionFetchStream{ctx: context.Background()}
	if err := NewCrossRegionServer(a.pm).FetchEvents(&types.RegionFetchRequest{RegionId: "remote", PartitionId: 0, StartOffset: 4}, stream); err != nil {
		t.Fatal(err)
	}
	if len(stream.responses) != 1 || len(stream.responses[0].Events) != 0 || stream.responses[0].NextOffset != 5 || stream.responses[0].HasMore {
		t.Fatalf("control-only fetch did not preserve cursor: %v", stream.responses)
	}
}

func TestTermStartLostSuccessReplyAndLeaderDeathPreserveThePrefix(t *testing.T) {
	a, b, c, gate := oldTermScenario(t)
	gate.loseReply.Store(true)
	gate.allow.Store(true)
	deadline := time.Now().Add(10 * time.Second)
	for gate.lost.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no durable append with a lost reply")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if a.p.DeliverableThrough() >= 3 {
		t.Fatal("leader guessed commitment after losing the success reply")
	}
	if err := a.pm.DemoteFromLeader(0); err != nil {
		t.Fatal(err)
	}
	// C, holding term 4, wins over B's conflicting term 3 (the coordinator's
	// comparison is exercised in the paired cluster election regression).
	if err := c.pm.PromoteToLeader(0, 5); err != nil {
		t.Fatal(err)
	}
	if err := c.pm.AddFollower(0, "node-b", b.serveReplication()); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for c.p.DeliverableThrough() < 5 {
		if time.Now().After(deadline) {
			t.Fatal("new leader could not commit its term-start record")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, r := range []*replicaLog{b, c} {
		if e, err := r.p.Wal.ReadEvent(3); err != nil || e.GetMessageId() != "y-3" {
			t.Fatalf("new leader lost the prefix: %v %v", e, err)
		}
	}
}

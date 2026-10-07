package api

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// sendAfter sends events as a leader does: naming the entry they follow and
// the term that entry has in the leader's log.
func (r *replicaLog) sendAfter(leader string, term, prevTerm int64, events []*types.Event) *types.ReplicationAppendResponse {
	r.t.Helper()
	resp, err := r.h.Append(context.Background(), &types.ReplicationAppendRequest{
		PartitionId: 0, Term: term, LeaderId: leader, Events: events,
		HasPrevLog: true, PrevLogOffset: events[0].Offset - 1, PrevLogTerm: prevTerm,
	})
	if err != nil {
		r.t.Fatalf("append from %s at term %d: %v", leader, term, err)
	}
	return resp
}

func describe(log []*types.Event) string {
	out := ""
	for _, event := range log {
		out += fmt.Sprintf("%d:%s@%d ", event.Offset, event.MessageId, event.Term)
	}
	return out
}

// New entries are not appended after an entry the leader does not have. The
// replica says where its log stops agreeing instead, and what the leader then
// resends replaces the part that differs.
func TestReplication_AppendChecksTheEntryItFollows(t *testing.T) {
	r := newReplicaLog(t, "node-c")
	// Offsets 0-7 were written under node-a at term 1. Only 0-4 ever reached a
	// quorum; node-b, which leads at term 2, wrote its own 5-9.
	if resp := r.send("node-a", 1, entries("a", 1, 0, 7)); !resp.GetSuccess() {
		t.Fatalf("setup: %s", resp.GetError())
	}

	resp := r.sendAfter("node-b", 2, 2, entries("b", 2, 8, 9))
	if resp.GetSuccess() || !resp.GetLogMismatch() {
		t.Fatalf("entries were appended after one the leader does not have: %+v", resp)
	}
	if resp.GetConflictTerm() != 1 || resp.GetConflictFirstOffset() != 0 {
		t.Fatalf("mismatch reported term %d from offset %d, want term 1 from offset 0", resp.GetConflictTerm(), resp.GetConflictFirstOffset())
	}
	if got := len(r.log()); got != 8 {
		t.Fatalf("a refused append changed the log: %d entries, want 8", got)
	}

	// The leader resends from where the logs agree: after offset 4, which both
	// hold at term 1.
	if resp := r.sendAfter("node-b", 2, 1, entries("b", 2, 5, 9)); !resp.GetSuccess() || resp.GetLastOffset() != 9 {
		t.Fatalf("resend from the point of agreement: %+v", resp)
	}
	want := describe(append(entries("a", 1, 0, 4), entries("b", 2, 5, 9)...))
	if got := describe(r.log()); got != want {
		t.Fatalf("log after the resend:\n got %s\nwant %s", got, want)
	}

	// An append that follows an entry both hold is unaffected.
	if resp := r.sendAfter("node-b", 2, 2, entries("b", 2, 10, 11)); !resp.GetSuccess() || resp.GetLastOffset() != 11 {
		t.Fatalf("append after a matching entry: %+v", resp)
	}
}

// A request can arrive late, after requests the leader built later. What it
// says about the end of the leader's log is then out of date, and the entries
// the replica has beyond that point are the leader's own. They stay.
func TestReplication_LateRequestDoesNotRemoveItsOwnLeadersEntries(t *testing.T) {
	r := newReplicaLog(t, "node-c")
	if resp := r.send("node-a", 1, entries("a", 1, 0, 4)); !resp.GetSuccess() {
		t.Fatalf("setup: %s", resp.GetError())
	}
	if resp := r.sendAfter("node-b", 2, 1, entries("b", 2, 5, 9)); !resp.GetSuccess() {
		t.Fatalf("setup: %s", resp.GetError())
	}
	want := describe(r.log())

	// A probe node-b built when its log ended at offset 4, delivered now.
	resp, err := r.h.Append(context.Background(), &types.ReplicationAppendRequest{
		PartitionId: 0, Term: 2, LeaderId: "node-b",
		HasPrevLog: true, PrevLogOffset: 4, PrevLogTerm: 1, LeaderLogEnd: 5,
	})
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("late probe: %+v %v", resp, err)
	}
	if got := describe(r.log()); got != want {
		t.Fatalf("a late request removed entries of the leader that sent it:\n got %s\nwant %s", got, want)
	}

	// The same probe from a leader of a later term, whose log does end there,
	// removes them: they were written under another leader.
	resp, err = r.h.Append(context.Background(), &types.ReplicationAppendRequest{
		PartitionId: 0, Term: 3, LeaderId: "node-a",
		HasPrevLog: true, PrevLogOffset: 4, PrevLogTerm: 1, LeaderLogEnd: 5,
	})
	if err != nil || !resp.GetSuccess() || resp.GetNextOffset() != 5 {
		t.Fatalf("probe from the leader of a later term: %+v %v", resp, err)
	}
	if got, want := describe(r.log()), describe(entries("a", 1, 0, 4)); got != want {
		t.Fatalf("log after the probe:\n got %s\nwant %s", got, want)
	}
}

// An old leader that comes back with entries it never replicated ends up with
// the new leader's log, whether its unreplicated tail is shorter or longer
// than what the new leader has written since. Nothing has to be published for
// that to happen.
func TestReplication_ReturningLeaderIsBroughtInLine(t *testing.T) {
	for _, tc := range []struct {
		name          string
		unreplicated  int // events the old leader appended alone
		sinceTakeover int // events the new leader accepted afterwards
	}{
		{"shorter tail than the new leader's log", 3, 6},
		{"longer tail than the new leader's log", 9, 4},
		// The new leader has nothing to send that would replace the tail.
		{"a tail beyond the end of a leader that has accepted nothing", 3, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b, c := newReplicaLog(t, "node-a"), newReplicaLog(t, "node-b"), newReplicaLog(t, "node-c")
			publishOn := func(r *replicaLog) func([]*types.Event) *types.PublishBatchResponse {
				handler := NewEventServiceHandler(r.pm, r.p.DedupStore, r.p.ConsumerGroup)
				return func(events []*types.Event) *types.PublishBatchResponse {
					t.Helper()
					resp, err := handler.PublishBatch(context.Background(), &types.PublishBatchRequest{Events: events})
					if err != nil {
						t.Fatal(err)
					}
					return resp
				}
			}

			// node-a leads at term 1 with node-b behind a link that can fail.
			if err := a.pm.PromoteToLeader(0, 1); err != nil {
				t.Fatal(err)
			}
			link := &gatedReplication{ReplicationServiceServer: b.h}
			if err := a.pm.AddFollower(0, "node-b", serveReplicationService(t, link)); err != nil {
				t.Fatal(err)
			}
			publishA := publishOn(a)
			if resp := publishA(orders("shared", 5)); !resp.GetSuccess() {
				t.Fatalf("setup publish: %+v", resp)
			}

			// node-a is cut off. It keeps appending what it can no longer
			// replicate, and refuses those publishes.
			link.refuse(true)
			if resp := publishA(orders("lost", tc.unreplicated)); resp.GetSuccess() {
				t.Fatal("a publish without a quorum succeeded")
			}
			if got := len(a.log()); got != 5+tc.unreplicated {
				t.Fatalf("setup: old leader's log has %d entries, want %d", got, 5+tc.unreplicated)
			}

			// node-b takes over at term 2 with node-c, and accepts more.
			if err := b.pm.PromoteToLeader(0, 2); err != nil {
				t.Fatal(err)
			}
			if err := b.pm.AddFollower(0, "node-c", c.serveReplication()); err != nil {
				t.Fatal(err)
			}
			if tc.sinceTakeover > 0 {
				if resp := publishOn(b)(orders("kept", tc.sinceTakeover)); !resp.GetSuccess() {
					t.Fatalf("publish on the new leader: %+v", resp)
				}
			}
			want := describe(b.log())

			// node-a returns as a follower of node-b.
			if err := b.pm.AddFollower(0, "node-a", a.serveReplication()); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(20 * time.Second)
			for describe(a.log()) != want {
				if time.Now().After(deadline) {
					t.Fatalf("the returning leader's log was not brought in line:\n got %s\nwant %s", describe(a.log()), want)
				}
				time.Sleep(20 * time.Millisecond)
			}
			if a.p.IsLeader() {
				t.Fatal("the old leader still leads")
			}
			// The third replica is filled without a publish too.
			for describe(c.log()) != want {
				if time.Now().After(deadline) {
					t.Fatalf("the third replica's log:\n got %s\nwant %s", describe(c.log()), want)
				}
				time.Sleep(20 * time.Millisecond)
			}
		})
	}
}

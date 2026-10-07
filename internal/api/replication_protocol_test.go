package api

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
)

// replicaLog is a follower under test.
type replicaLog struct {
	t  *testing.T
	pm *partition.PartitionManager
	h  *ReplicationServiceHandler
	p  *partition.Partition
}

func newReplicaLog(t *testing.T, nodeID string) *replicaLog {
	t.Helper()
	cfg := &types.Config{DataDir: t.TempDir(), PartitionCount: 1, ReplicationFactor: 3, MinInSyncReplicas: 2, TickMS: 10, WheelSize: 100, SegmentSizeBytes: 1 << 20, IndexInterval: 1, FsyncMode: "periodic", FlushIntervalMS: 50, DedupTTLHours: 24, BloomCapacity: 1000}
	pm := partition.NewPartitionManager(nodeID, cfg)
	t.Cleanup(func() { pm.Close() })
	if err := pm.CreatePartition(0, "orders"); err != nil {
		t.Fatal(err)
	}
	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatal(err)
	}
	return &replicaLog{t: t, pm: pm, h: NewReplicationServiceHandler(pm), p: p}
}

// entries builds log entries [from, to] written under term by a leader whose
// payloads are tagged with tag.
func entries(tag string, term, from, to int64) []*types.Event {
	out := make([]*types.Event, 0, to-from+1)
	for offset := from; offset <= to; offset++ {
		out = append(out, &types.Event{
			MessageId:  fmt.Sprintf("%s-%d", tag, offset),
			Topic:      "orders",
			Offset:     offset,
			Term:       term,
			ScheduleTs: 1,
			Payload:    []byte(tag),
		})
	}
	return out
}

func (r *replicaLog) send(leader string, term int64, events []*types.Event) *types.ReplicationAppendResponse {
	r.t.Helper()
	resp, err := r.h.Append(context.Background(), &types.ReplicationAppendRequest{PartitionId: 0, Term: term, LeaderId: leader, Events: events})
	if err != nil {
		r.t.Fatalf("append from %s at term %d: %v", leader, term, err)
	}
	return resp
}

func (r *replicaLog) log() []*types.Event {
	r.t.Helper()
	last := r.p.Wal.GetLastOffset()
	if last < 0 {
		return nil
	}
	events, err := r.p.Wal.ReadEvents(0, last)
	if err != nil {
		r.t.Fatal(err)
	}
	return events
}

// Two nodes that both believe they lead at the same term must not both get
// their writes accepted, and once a newer term is accepted the older leader is
// shut out for good.
func TestReplication_OneLeaderPerTerm(t *testing.T) {
	r := newReplicaLog(t, "follower")

	if resp := r.send("node-a", 1, entries("a", 1, 0, 1)); !resp.GetSuccess() {
		t.Fatalf("first leader rejected: %s", resp.GetError())
	}
	if resp := r.send("node-b", 1, entries("b", 1, 2, 2)); resp.GetSuccess() {
		t.Fatal("a second node wrote at a term another leader already holds")
	}
	if resp := r.send("node-a", 1, entries("a", 1, 2, 2)); !resp.GetSuccess() {
		t.Fatalf("the term's holder was rejected after a rival's attempt: %s", resp.GetError())
	}

	if resp := r.send("node-b", 2, entries("b", 2, 3, 3)); !resp.GetSuccess() {
		t.Fatalf("leader of the newer term rejected: %s", resp.GetError())
	}
	if resp := r.send("node-a", 1, entries("a", 1, 4, 4)); resp.GetSuccess() {
		t.Fatal("the deposed leader wrote after a newer term was accepted")
	}
	if got := len(r.log()); got != 4 {
		t.Fatalf("log has %d entries, want 4", got)
	}
}

// A replica that kept writing as leader after it was replaced has a tail the
// new leader never had. When the new leader's entries arrive, that tail is
// removed and replaced, so the replica can follow again without a full
// snapshot. Entries both logs share are left alone.
func TestReplication_DivergentTailIsReplacedByNewerTerm(t *testing.T) {
	r := newReplicaLog(t, "follower")
	if resp := r.send("node-a", 1, entries("a", 1, 0, 4)); !resp.GetSuccess() {
		t.Fatalf("setup: %s", resp.GetError())
	}

	// node-b was elected with offsets 0-2; it never had 3 and 4.
	batch := append(entries("a", 1, 2, 2), entries("b", 2, 3, 5)...)
	if resp := r.send("node-b", 2, batch); !resp.GetSuccess() {
		t.Fatalf("new leader's entries rejected: %s", resp.GetError())
	}

	got := r.log()
	if len(got) != 6 {
		t.Fatalf("log has %d entries, want 6", len(got))
	}
	for offset, event := range got {
		wantTag, wantTerm := "a", int64(1)
		if offset >= 3 {
			wantTag, wantTerm = "b", 2
		}
		if event.Offset != int64(offset) || event.Term != wantTerm || event.MessageId != fmt.Sprintf("%s-%d", wantTag, offset) {
			t.Fatalf("offset %d holds %s (term %d), want %s-%d (term %d)", offset, event.MessageId, event.Term, wantTag, offset, wantTerm)
		}
	}
}

// Resending entries the replica already has must change nothing, and entries
// that differ under the same term are a corruption that is refused rather than
// papered over by deleting history.
func TestReplication_SameTermNeverTruncates(t *testing.T) {
	r := newReplicaLog(t, "follower")
	if resp := r.send("node-a", 1, entries("a", 1, 0, 4)); !resp.GetSuccess() {
		t.Fatalf("setup: %s", resp.GetError())
	}
	if resp := r.send("node-a", 1, entries("a", 1, 1, 2)); !resp.GetSuccess() {
		t.Fatalf("retry of held entries rejected: %s", resp.GetError())
	}
	if resp := r.send("node-a", 1, entries("other", 1, 3, 3)); resp.GetSuccess() {
		t.Fatal("a different entry at the same offset and term was accepted")
	}
	got := r.log()
	if len(got) != 5 {
		t.Fatalf("log has %d entries after same-term resends, want 5", len(got))
	}
	for offset, event := range got {
		if event.MessageId != fmt.Sprintf("a-%d", offset) {
			t.Fatalf("offset %d changed to %s", offset, event.MessageId)
		}
	}
}

// A node that still leads when a newer leader's write arrives must stop
// leading before it applies that write.
func TestReplication_SupersededLeaderStepsDown(t *testing.T) {
	r := newReplicaLog(t, "node-a")
	if err := r.pm.PromoteToLeader(0, 1); err != nil {
		t.Fatal(err)
	}
	if !r.p.IsLeader() || r.p.ReplLeader == nil {
		t.Fatal("setup: node is not leading")
	}

	if resp := r.send("node-b", 1, entries("b", 1, 0, 0)); resp.GetSuccess() {
		t.Fatal("a leader accepted a rival's write at its own term")
	}
	if !r.p.IsLeader() {
		t.Fatal("a rival at the same term made the leader step down")
	}

	if resp := r.send("node-b", 2, entries("b", 2, 0, 0)); !resp.GetSuccess() {
		t.Fatalf("newer leader rejected: %s", resp.GetError())
	}
	if r.p.IsLeader() || r.p.ReplLeader != nil {
		t.Fatal("the superseded leader kept leading after accepting a newer term")
	}
	if r.p.Epoch() != 2 || r.p.EpochLeader() != "node-b" {
		t.Fatalf("leadership = (%d, %q), want (2, node-b)", r.p.Epoch(), r.p.EpochLeader())
	}
}

// While an old and a new leader both send, the replica's log must end up as a
// prefix from the old term followed only by the new term: once it has taken an
// entry from the new leader, nothing from the old one may land after it.
func TestReplication_ConcurrentLeaderChangeKeepsLogConsistent(t *testing.T) {
	r := newReplicaLog(t, "follower")
	const oldEntries, newEntries = 300, 50

	var wg sync.WaitGroup
	var rejectedOld int
	wg.Add(2)
	go func() { // node-a keeps appending under term 1
		defer wg.Done()
		next := int64(0)
		for i := 0; i < oldEntries; i++ {
			resp := r.send("node-a", 1, entries("a", 1, next, next))
			if resp.GetSuccess() {
				next++
			} else {
				rejectedOld++
				next = resp.GetNextOffset() // follow what the replica reports, as a leader would
			}
		}
	}()
	started := make(chan struct{})
	go func() { // node-b takes over under term 2 from wherever the log is
		defer wg.Done()
		close(started)
		next := int64(-1)
		for i := 0; i < newEntries; {
			if next < 0 {
				resp, err := r.h.Position(context.Background(), &types.ReplicationPositionRequest{PartitionId: 0})
				if err != nil {
					t.Error(err)
					return
				}
				next = resp.GetLastOffset() + 1
			}
			resp := r.send("node-b", 2, entries("b", 2, next, next))
			if resp.GetSuccess() {
				next++
				i++
			} else {
				next = -1
			}
		}
	}()
	<-started
	wg.Wait()

	got := r.log()
	sawNew := false
	newCount := 0
	for offset, event := range got {
		if event.Offset != int64(offset) {
			t.Fatalf("offset %d at position %d: log is not contiguous", event.Offset, offset)
		}
		switch event.Term {
		case 1:
			if sawNew {
				t.Fatalf("an entry of the old term landed at offset %d, after the new leader's entries", offset)
			}
		case 2:
			sawNew = true
			newCount++
			if !strings.HasPrefix(event.MessageId, "b-") {
				t.Fatalf("offset %d is marked term 2 but holds %s", offset, event.MessageId)
			}
		default:
			t.Fatalf("offset %d has term %d", offset, event.Term)
		}
	}
	if newCount != newEntries {
		t.Fatalf("log holds %d entries of the new term, want all %d", newCount, newEntries)
	}
	if r.p.Epoch() != 2 || r.p.EpochLeader() != "node-b" {
		t.Fatalf("leadership = (%d, %q), want (2, node-b)", r.p.Epoch(), r.p.EpochLeader())
	}
	if rejectedOld == 0 {
		t.Log("the old leader finished before the new one started; ordering was not exercised this run")
	}
}

// A newer leader's term is recorded even when its entries cannot be applied
// yet, so the old leader is fenced on this replica from that moment.
func TestReplication_NewTermIsRecordedEvenWhenEntriesAreRejected(t *testing.T) {
	r := newReplicaLog(t, "follower")
	if resp := r.send("node-a", 1, entries("a", 1, 0, 0)); !resp.GetSuccess() {
		t.Fatalf("setup: %s", resp.GetError())
	}
	// node-b is ahead of this replica: its batch leaves a gap.
	if resp := r.send("node-b", 2, entries("b", 2, 5, 5)); resp.GetSuccess() {
		t.Fatal("an append that leaves a gap was accepted")
	}
	if resp := r.send("node-a", 1, entries("a", 1, 1, 1)); resp.GetSuccess() {
		t.Fatal("the old leader wrote after the replica had heard from a newer term")
	}
}

// Position reports the end of the log and whether the node takes publishes.
func TestReplication_PositionReportsLogEnd(t *testing.T) {
	r := newReplicaLog(t, "node-a")
	position := func() *types.ReplicationPositionResponse {
		t.Helper()
		resp, err := r.h.Position(context.Background(), &types.ReplicationPositionRequest{PartitionId: 0})
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	if got := position(); !got.GetFound() || got.GetLastOffset() != -1 || got.GetLastTerm() != 0 || got.GetAcceptingWrites() {
		t.Fatalf("empty follower position = %v", got)
	}
	if resp := r.send("node-b", 3, entries("b", 3, 0, 4)); !resp.GetSuccess() {
		t.Fatalf("setup: %s", resp.GetError())
	}
	if got := position(); got.GetLastOffset() != 4 || got.GetLastTerm() != 3 || got.GetEpoch() != 3 || got.GetAcceptingWrites() {
		t.Fatalf("follower position = %v, want offset 4, term 3, epoch 3, not accepting", got)
	}

	missing, err := r.h.Position(context.Background(), &types.ReplicationPositionRequest{PartitionId: 9})
	if err != nil || missing.GetFound() || missing.GetLastOffset() != -1 {
		t.Fatalf("position of a partition this node does not hold = %v, %v", missing, err)
	}

	if err := r.pm.PromoteToLeader(0, 4); err != nil {
		t.Fatal(err)
	}
	if got := position(); !got.GetAcceptingWrites() {
		t.Fatal("a leader with no cluster restriction reports that it does not accept writes")
	}
	// The cluster stops the leader for a handoff. While a publish that began
	// earlier is still in flight the log may yet grow, so the node must keep
	// reporting that it accepts writes; only afterwards is its position final.
	r.pm.SetWritableCheck(func(int32) bool { return false })
	r.p.BeginPublish()
	if got := position(); !got.GetAcceptingWrites() {
		t.Fatal("a stopped leader with a publish in flight reported a final position")
	}
	r.p.EndPublish()
	if got := position(); got.GetAcceptingWrites() {
		t.Fatal("a leader the cluster has stopped still reports that it accepts writes")
	}
}

// Consumer progress sent by the leader is applied on the follower, and only
// the current leader may send it.
func TestReplication_ConsumerProgressIsAppliedFromCurrentLeader(t *testing.T) {
	r := newReplicaLog(t, "follower")
	if resp := r.send("node-a", 2, entries("a", 2, 0, 9)); !resp.GetSuccess() {
		t.Fatalf("setup: %s", resp.GetError())
	}
	sync := func(leader string, term int64, floor int64, completed ...int64) *types.ReplicationProgressResponse {
		t.Helper()
		resp, err := r.h.SyncConsumerProgress(context.Background(), &types.ReplicationProgressRequest{
			PartitionId: 0, Term: term, LeaderId: leader,
			Groups: []*types.ConsumerGroupProgress{{GroupId: "workers", Topic: "orders", CommittedOffset: floor, CompletedOffsets: completed}},
		})
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	if resp := sync("node-a", 2, 4, 6); !resp.GetSuccess() {
		t.Fatalf("progress from the leader rejected: %s", resp.GetError())
	}
	for offset, want := range map[int64]bool{0: true, 3: true, 4: false, 5: false, 6: true, 7: false} {
		if got := r.p.ConsumerGroup.IsCompleted("workers", 0, offset); got != want {
			t.Errorf("follower IsCompleted(%d) = %v, want %v", offset, got, want)
		}
	}
	if committed, _ := r.p.ConsumerGroup.GetCommittedOffset("workers", 0); committed != 4 {
		t.Fatalf("follower committed offset = %d, want 4", committed)
	}

	if resp := sync("node-b", 2, 9); resp.GetSuccess() {
		t.Fatal("progress from a node that does not hold the term was applied")
	}
	if resp := sync("node-a", 1, 9); resp.GetSuccess() {
		t.Fatal("progress from an older term was applied")
	}
	if r.p.ConsumerGroup.IsCompleted("workers", 0, 7) {
		t.Fatal("rejected progress changed the follower's state")
	}
}

// serveReplication exposes a replica's replication handler over gRPC.
func (r *replicaLog) serveReplication() string {
	r.t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		r.t.Fatal(err)
	}
	srv := grpc.NewServer()
	types.RegisterReplicationServiceServer(srv, r.h)
	go func() { _ = srv.Serve(lis) }()
	r.t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// End to end: what consumers finish on the leader reaches a follower through
// the replication channel, so a follower that takes over knows not to deliver
// it again, including work that completed out of order.
func TestReplication_FailoverKeepsConsumerProgress(t *testing.T) {
	follower := newReplicaLog(t, "node-b")
	leader := newReplicaLog(t, "node-a")
	if err := leader.pm.PromoteToLeader(0, 1); err != nil {
		t.Fatal(err)
	}
	if err := leader.pm.AddFollower(0, "node-b", follower.serveReplication()); err != nil {
		t.Fatal(err)
	}

	// Ten events are published and replicated; a consumer group on the leader
	// finishes 0-4 and, out of order, 7.
	batch := make([]*types.Event, 10)
	for i := range batch {
		batch[i] = &types.Event{MessageId: fmt.Sprintf("order-%d", i), Topic: "orders", Payload: []byte("payload"), ScheduleTs: 1}
	}
	if err := leader.p.Wal.AppendBatch(batch); err != nil {
		t.Fatal(err)
	}
	if err := leader.p.ReplLeader.Replicate(batch); err != nil {
		t.Fatalf("replicate: %v", err)
	}
	if err := leader.p.ConsumerGroup.CreateGroup("workers", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	done := append(append([]*types.Event{}, batch[:5]...), batch[7])
	if err := leader.p.ConsumerGroup.CommitDelivery("workers", 0, done); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for !follower.p.ConsumerGroup.IsCompleted("workers", 0, 7) {
		if time.Now().After(deadline) {
			t.Fatal("consumer progress never reached the follower")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// The leader is lost and the follower is promoted with a newer epoch.
	if err := follower.pm.PromoteToLeader(0, 2); err != nil {
		t.Fatal(err)
	}
	for offset := int64(0); offset < 10; offset++ {
		want := offset < 5 || offset == 7
		if got := follower.p.ConsumerGroup.IsCompleted("workers", 0, offset); got != want {
			t.Errorf("after failover IsCompleted(%d) = %v, want %v", offset, got, want)
		}
	}
	if committed, _ := follower.p.ConsumerGroup.GetCommittedOffset("workers", 0); committed != 5 {
		t.Fatalf("after failover the group resumes at offset %d, want 5", committed)
	}
	if got := len(follower.log()); got != 10 {
		t.Fatalf("follower log has %d entries, want 10", got)
	}
	for _, event := range follower.log() {
		if event.Term != 1 {
			t.Fatalf("offset %d stored with term %d on the follower, want the leader's term 1", event.Offset, event.Term)
		}
	}
}

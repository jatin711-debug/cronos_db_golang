package replication

import (
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/logcontrol"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestTermStartCommitNeedsCurrentTermAndSurvivesQuorumLoss(t *testing.T) {
	w := newLeaderTestWAL(t)
	l := NewLeader(0, 1, time.Hour, w, 2, "a", nil)
	defer l.Stop()
	old := &types.Event{MessageId: "old", Payload: []byte("p"), ScheduleTs: 1}
	if err := w.AppendEvent(old); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	f := &FollowerInfo{ID: "c", LastAckTS: 1, HighWatermark: old.Offset}
	l.followers["c"] = f
	if got := l.ReplicatedThrough(); got != old.Offset {
		t.Fatalf("legacy fixture did not cache its replica-count proof: %d", got)
	}
	l.SetEpoch(4)
	l.SetTermStartFormat(1)
	if got := l.ReplicatedThrough(); got != -1 {
		t.Fatalf("format activation reused the legacy cached proof: %d", got)
	}
	barrier := logcontrol.NewTermStart(0)
	if err := w.AppendEvent(barrier); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	l.SetTermStartOffset(barrier.Offset, 4)
	if got := l.QuorumOffset(); got != -1 {
		t.Fatalf("old-term count was treated as a commit: %d", got)
	}
	f.HighWatermark = barrier.Offset
	if got := l.QuorumOffset(); got != barrier.Offset {
		t.Fatalf("current-term proof did not commit prefix: %d", got)
	}
	if err := l.RemoveFollower("c"); err != nil {
		t.Fatal(err)
	}
	if got := l.QuorumOffset(); got != barrier.Offset {
		t.Fatalf("a proven commitment regressed after quorum loss: %d", got)
	}
	l.SetEpoch(5)
	if got := l.ReplicatedThrough(); got != -1 {
		t.Fatalf("previous term's cached proof survived promotion: %d", got)
	}
	l.SetTermStartOffset(barrier.Offset, 4) // a delayed old-generation callback
	if got := l.ReplicatedThrough(); got != -1 {
		t.Fatalf("old generation installed a proof in the new term: %d", got)
	}
}

func TestTermStartTransportNeverFallsBackToLegacyAppend(t *testing.T) {
	w := newLeaderTestWAL(t)
	l := NewLeader(0, 1, time.Hour, w, 2, "a", nil)
	defer l.Stop()
	l.SetTermStartFormat(1)
	e := logcontrol.NewTermStart(0)
	if err := w.AppendEvent(e); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	l.SetTermStartOffset(e.Offset, 1)
	legacy, addr := startGatedReplServer(t, true)
	if err := l.AddFollower("old", addr); err != nil {
		t.Fatal(err)
	}
	if err := l.Replicate([]*types.Event{e}); err == nil {
		t.Fatal("legacy receiver acknowledged a term-start record")
	}
	if offsets, _ := legacy.snapshot(); len(offsets) != 0 {
		t.Fatalf("legacy append received internal data: %v", offsets)
	}
	if l.QuorumOffset() != -1 {
		t.Fatal("unsupported receiver counted toward commitment")
	}
}

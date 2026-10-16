package replication

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
)

type deadLetterClient struct {
	types.ReplicationServiceClient
	supports atomic.Bool
	durable  atomic.Bool
	groups   atomic.Int32
	early    atomic.Bool
	logStart atomic.Int64
}

func (c *deadLetterClient) SyncConsumerProgress(_ context.Context, r *types.ReplicationProgressRequest, _ ...grpc.CallOption) (*types.ReplicationProgressResponse, error) {
	if r.RequireDeadLetterAck && c.supports.Load() {
		c.durable.Store(true)
	}
	if len(r.Groups) > 0 {
		c.groups.Add(1)
		if !c.durable.Load() {
			c.early.Store(true)
		}
	}
	return &types.ReplicationProgressResponse{Success: true, DeadLettersDurable: r.RequireDeadLetterAck && c.supports.Load()}, nil
}

func TestDeadLetterAcknowledgementGatesProgressAndRetention(t *testing.T) {
	c := &deadLetterClient{}
	f := &FollowerInfo{ID: "f", client: c, Connected: true, InSync: true}
	l := makeLeaderForTest(2, map[string]*FollowerInfo{"f": f})
	l.SetProgressPageSource(func() uint64 { return 1 }, func(types.ProgressCursor) ([]*types.ConsumerGroupProgress, types.ProgressCursor, bool, error) {
		return []*types.ConsumerGroupProgress{{GroupId: "g", CommittedOffset: 1}}, types.ProgressCursor{}, true, nil
	})
	l.SetDeadLetterSource(func() uint64 { return 1 }, func(types.DeadLetterCursor) ([]*types.DeadLetterRecord, types.DeadLetterCursor, bool, error) {
		return []*types.DeadLetterRecord{{DeliveryId: "d", Event: &types.Event{MessageId: "m"}}}, types.DeadLetterCursor{}, true, nil
	})
	l.shipPagedConsumerProgress()
	waitFor(t, "legacy follower reply", func() bool { f.mu.Lock(); defer f.mu.Unlock(); return !f.progressSending })
	if l.DeadLettersReplicated(1) || c.groups.Load() != 0 {
		t.Fatal("legacy reply released completion/retention")
	}
	if len(l.GetInSyncReplicas()) != 0 {
		t.Fatal("replica missing dead letters was reported in sync")
	}
	c.supports.Store(true)
	deadline := time.Now().Add(5 * time.Second)
	for c.groups.Load() == 0 && time.Now().Before(deadline) {
		l.shipPagedConsumerProgress()
		time.Sleep(time.Millisecond)
	}
	if c.groups.Load() == 0 || c.early.Load() || !l.DeadLettersReplicated(1) {
		t.Fatal("durable DLQ did not precede completion")
	}
	if len(l.GetInSyncReplicas()) != 1 {
		t.Fatal("caught-up replica missing from ISR")
	}
}

func (c *deadLetterClient) Append(_ context.Context, req *types.ReplicationAppendRequest, _ ...grpc.CallOption) (*types.ReplicationAppendResponse, error) {
	c.logStart.Store(req.GetLogStartOffset())
	return &types.ReplicationAppendResponse{Success: true, LastOffset: 39, NextOffset: 40}, nil
}

func TestDeadLetterAcknowledgementGatesDiscardedPrefix(t *testing.T) {
	wal := newLeaderTestWAL(t)
	if err := wal.ResetTo(40); err != nil {
		t.Fatal(err)
	}
	c := &deadLetterClient{}
	f := &FollowerInfo{ID: "replacement", client: c}
	l := makeLeaderForTest(2, map[string]*FollowerInfo{"replacement": f})
	l.wal = wal
	l.SetDeadLetterSource(func() uint64 { return 1 }, nil)
	if err := l.appendToFollower(f, nil, 0); err != nil {
		t.Fatal(err)
	}
	if c.logStart.Load() != 0 {
		t.Fatal("replacement was allowed to skip the WAL before receiving its dead letters")
	}
	f.deadLetterSent, f.deadLetterSentAt = 1, time.Now()
	if err := l.appendToFollower(f, nil, 0); err != nil {
		t.Fatal(err)
	}
	if c.logStart.Load() != 40 {
		t.Fatal("durable journal did not release log catch-up")
	}
}

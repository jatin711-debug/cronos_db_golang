package api

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A message ID is unique within its topic. The same ID used on two topics
// used to meet in one partition and be refused the second time, so a
// principal who may publish only to beta could block, and learn of, IDs used
// on alpha.
func TestSameMessageIDOnTwoTopicsIsTwoEvents(t *testing.T) {
	ts := newTenants(t)
	bob, bobCtx := ts.raw("bob")
	alice, aliceCtx := ts.raw("alice")
	at := time.Now().UnixMilli() + 60_000
	publish := func(c types.EventServiceClient, ctx context.Context, topic, payload string) *types.PublishResponse {
		t.Helper()
		resp, err := c.Publish(ctx, &types.PublishRequest{Event: &types.Event{MessageId: "order-42", Topic: topic, Payload: []byte(payload), ScheduleTs: at}})
		if err != nil {
			t.Fatalf("publish to %s: %v", topic, err)
		}
		return resp
	}
	if resp := publish(bob, bobCtx, "beta", "bob"); !resp.GetSuccess() {
		t.Fatalf("bob's publish to beta: %q", resp.GetError())
	}
	if resp := publish(alice, aliceCtx, "alpha", "alice"); !resp.GetSuccess() {
		t.Fatalf("alice's publish of order-42 to alpha was refused after bob used the ID on beta: %q", resp.GetError())
	}
	// Within a topic the ID still names one event.
	if resp := publish(alice, aliceCtx, "alpha", "again"); resp.GetSuccess() || !strings.Contains(resp.GetError(), "duplicate") {
		t.Fatalf("a second publish of order-42 to alpha: success=%v error=%q, want a duplicate", resp.GetSuccess(), resp.GetError())
	}
}

// subscribeOnce opens a subscription for group and returns the status it
// ends with, or nil once the group exists.
func subscribeOnce(t *testing.T, ts *tenants, group string) error {
	t.Helper()
	events, ctx := ts.raw("alice")
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := events.Subscribe(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&types.SubscribeRequest{ConsumerGroup: group, Topic: "alpha", PartitionId: 0, SubscriptionId: "s"}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		done <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			return err
		default:
		}
		for _, g := range ts.p.ConsumerGroup.ListGroups() {
			if g.GroupID == group {
				return nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("subscription for group %.20q neither failed nor created its group", group)
	return nil
}

// Subscribe creates a consumer group for whatever name it is given, and the
// group stays after the subscription ends. Names and the number of groups
// are bounded now: eight subscriptions used to leave 16 MiB of names behind.
func TestSubscribeBoundsTheGroupsItCreates(t *testing.T) {
	ts := newTenants(t)
	if err := subscribeOnce(t, ts, strings.Repeat("g", 2<<20)); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("a 2 MiB group name: %v, want InvalidArgument", err)
	}

	ts.p.ConsumerGroup.SetMaxGroups(len(ts.p.ConsumerGroup.ListGroups()) + 2)
	for i := 0; i < 2; i++ {
		if err := subscribeOnce(t, ts, fmt.Sprintf("group-%d", i)); err != nil {
			t.Fatalf("group-%d: %v", i, err)
		}
	}
	if err := subscribeOnce(t, ts, "one-too-many"); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("a group beyond the limit: %v, want ResourceExhausted", err)
	}
	// An existing group can still be joined.
	if err := subscribeOnce(t, ts, "group-0"); err != nil {
		t.Fatalf("joining an existing group at the limit: %v", err)
	}
}

// A replay paces events by its speed. The pause did not end when the client
// cancelled, and a tiny speed asked for months of it: every cancelled replay
// kept its handler and the chunk of log it held.
func TestReplayCancellationEndsThePause(t *testing.T) {
	ts := newTenants(t)
	if err := ts.publish("alice", "alpha", 3); err != nil {
		t.Fatal(err)
	}
	events, ctx := ts.raw("alice")

	stream, err := events.Replay(ctx, &types.ReplayRequest{Topic: "alpha", PartitionId: 0, Count: 10, Speed: 1e-9})
	if err == nil {
		_, err = stream.Recv()
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("a replay at speed 1e-9: %v, want InvalidArgument", err)
	}

	countHandlers := func() int {
		buf := make([]byte, 16<<20)
		return strings.Count(string(buf[:runtime.Stack(buf, true)]), "(*EventServiceHandler).Replay(")
	}
	before := countHandlers()
	const n = 5
	for i := 0; i < n; i++ {
		rctx, cancel := context.WithCancel(ctx)
		// 10 s between events at the slowest speed allowed.
		stream, err := events.Replay(rctx, &types.ReplayRequest{Topic: "alpha", PartitionId: 0, Count: 10, Speed: minReplaySpeed})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Recv(); err != nil {
			t.Fatalf("first replay event: %v", err)
		}
		cancel()
	}
	deadline := time.Now().Add(3 * time.Second)
	for countHandlers() > before && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if left := countHandlers() - before; left > 0 {
		t.Fatalf("%d cancelled replays are still pausing on the server", left)
	}
}

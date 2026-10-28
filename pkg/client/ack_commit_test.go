package client

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
)

type recordingCheckpointStore struct {
	*MemoryCheckpointStore
	saved chan int64
}

func (s *recordingCheckpointStore) SaveOffset(ctx context.Context, group, topic string, partition int32, offset int64) error {
	if err := s.MemoryCheckpointStore.SaveOffset(ctx, group, topic, partition, offset); err != nil {
		return err
	}
	s.saved <- offset
	return nil
}

type floorAckServer struct {
	types.UnimplementedEventServiceServer
	subs          chan *types.SubscribeRequest
	acks          chan *types.AckRequest
	allowResponse <-chan struct{}
	floor         int64  // the completion floor every answer reports
	refuse        string // when set, every acknowledgement is refused with it
}

func (s *floorAckServer) Subscribe(stream grpc.BidiStreamingServer[types.SubscribeRequest, types.Delivery]) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	s.subs <- req
	if err := stream.Send(&types.Delivery{
		DeliveryId: "g:0:delivery", Attempt: 1,
		Event: &types.Event{MessageId: "later-offset", Topic: "t", PartitionId: 0, Offset: 10},
	}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (s *floorAckServer) Ack(stream grpc.BidiStreamingServer[types.AckRequest, types.AckResponse]) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		s.acks <- req
		if s.allowResponse != nil {
			select {
			case <-s.allowResponse:
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
		}
		// Offset 10 completed first; an earlier offset may still be unfinished.
		if err := stream.Send(&types.AckResponse{Success: s.refuse == "", Error: s.refuse, CommittedOffset: s.floor}); err != nil {
			return err
		}
	}
}

// The server acknowledges delivery of offset 10 while an earlier offset is
// unfinished, and reports a completion floor of 0. The SDK used to save 11,
// in both commit modes, and to resume from it: the server does not redeliver
// below the offset a subscription starts from, so the unfinished events were
// skipped.
func TestCheckpointIsTheServersCompletionFloor(t *testing.T) {
	for _, strategy := range []CommitStrategy{CommitStrategyAsync, CommitStrategySync} {
		t.Run(string(strategy), func(t *testing.T) {
			srv := &floorAckServer{subs: make(chan *types.SubscribeRequest, 4), acks: make(chan *types.AckRequest, 4)}
			addr := startTestEventServer(t, srv, &testPartitionServer{})
			cfg := DefaultConfig(addr)
			cfg.Security.Insecure = true
			cl, err := Dial(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			store := &recordingCheckpointStore{MemoryCheckpointStore: NewMemoryCheckpointStore(), saved: make(chan int64, 4)}
			cc := DefaultConsumerConfig("t", "g")
			cc.PartitionID, cc.CommitStrategy, cc.CheckpointStore = 0, strategy, store
			co, err := NewConsumer(cl, cc, func(context.Context, Delivery) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- co.consumeFromNode(ctx, addr) }()
			select {
			case <-srv.subs:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case saved := <-store.saved:
				t.Logf("server committed floor is 0, client saved %d", saved)
			case <-ctx.Done():
				t.Fatal("checkpoint was not saved")
			}
			cancel()
			<-done
			ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel2()
			go func() { done <- co.consumeFromNode(ctx2, addr) }()
			var resumed int64
			select {
			case req := <-srv.subs:
				resumed = req.GetStartOffset()
			case <-ctx2.Done():
				t.Fatal(ctx2.Err())
			}
			cancel2()
			<-done
			if resumed > 0 {
				t.Fatalf("reconnected with StartOffset=%d while server committed floor is 0: retained unfinished lower offsets are skipped", resumed)
			}
		})
	}
}

// With CommitStrategySync an acknowledgement returns the server's answer. It
// used to return as soon as it was queued.
func TestSyncAckWaitsForTheServer(t *testing.T) {
	allow := make(chan struct{})
	srv := &floorAckServer{subs: make(chan *types.SubscribeRequest, 4), acks: make(chan *types.AckRequest, 4), allowResponse: allow}
	addr := startTestEventServer(t, srv, &testPartitionServer{})
	cfg := DefaultConfig(addr)
	cfg.Security.Insecure = true
	cl, err := Dial(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	cc := DefaultConsumerConfig("t", "g")
	cc.PartitionID, cc.CommitStrategy, cc.AckMode = 0, CommitStrategySync, AckModeManual
	returned := make(chan error, 1)
	co, err := NewConsumer(cl, cc, func(ctx context.Context, d Delivery) error {
		ackCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()
		returned <- d.AckSuccess(ackCtx)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- co.consumeFromNode(ctx, addr) }()
	select {
	case err = <-returned:
	case <-ctx.Done():
		t.Fatal("Ack did not respect its own deadline")
	}
	cancel()
	<-done
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("synchronous AckSuccess returned %v although the server has sent no acknowledgement", err)
	}
}

// In async mode the checkpoint is the floor the server reports in its answer,
// not the offset after the delivery, which used to be saved as the
// acknowledgement was sent.
func TestAsyncCheckpointIsTheReportedFloor(t *testing.T) {
	srv := &floorAckServer{subs: make(chan *types.SubscribeRequest, 4), acks: make(chan *types.AckRequest, 4), floor: 7}
	addr := startTestEventServer(t, srv, &testPartitionServer{})
	cfg := DefaultConfig(addr)
	cfg.Security.Insecure = true
	cl, err := Dial(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	store := &recordingCheckpointStore{MemoryCheckpointStore: NewMemoryCheckpointStore(), saved: make(chan int64, 4)}
	cc := DefaultConsumerConfig("t", "g")
	cc.PartitionID, cc.CommitStrategy, cc.CheckpointStore = 0, CommitStrategyAsync, store
	co, err := NewConsumer(cl, cc, func(context.Context, Delivery) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- co.consumeFromNode(ctx, addr) }()
	select {
	case saved := <-store.saved:
		if saved != 7 {
			t.Fatalf("saved %d, want the reported floor 7", saved)
		}
	case <-ctx.Done():
		t.Fatal("no checkpoint was saved")
	}
	cancel()
	<-done
}

// A synchronous acknowledgement that the server refuses returns the refusal.
func TestSyncAckReturnsARefusal(t *testing.T) {
	srv := &floorAckServer{subs: make(chan *types.SubscribeRequest, 4), acks: make(chan *types.AckRequest, 4), refuse: "unknown delivery"}
	addr := startTestEventServer(t, srv, &testPartitionServer{})
	cfg := DefaultConfig(addr)
	cfg.Security.Insecure = true
	cl, err := Dial(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	cc := DefaultConsumerConfig("t", "g")
	cc.PartitionID, cc.CommitStrategy, cc.AckMode = 0, CommitStrategySync, AckModeManual
	returned := make(chan error, 1)
	co, err := NewConsumer(cl, cc, func(ctx context.Context, d Delivery) error {
		returned <- d.AckSuccess(ctx)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- co.consumeFromNode(ctx, addr) }()
	select {
	case err = <-returned:
	case <-ctx.Done():
		t.Fatal("the acknowledgement did not return")
	}
	cancel()
	<-done
	if err == nil || !strings.Contains(err.Error(), "unknown delivery") {
		t.Fatalf("a refused synchronous acknowledgement returned %v", err)
	}
}

package client

import (
	"context"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ackTestServer sends one delivery and then holds the subscription open. Its
// ack stream either fails at once or reports the first ack it receives.
type ackTestServer struct {
	types.UnimplementedEventServiceServer
	delivery *types.Delivery
	failAcks bool
	acks     chan *types.AckRequest
}

func (s *ackTestServer) Subscribe(stream grpc.BidiStreamingServer[types.SubscribeRequest, types.Delivery]) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	if err := stream.Send(s.delivery); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (s *ackTestServer) Ack(stream grpc.BidiStreamingServer[types.AckRequest, types.AckResponse]) error {
	if s.failAcks {
		return status.Error(codes.Unavailable, "simulated ack stream failure")
	}
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		select {
		case s.acks <- req:
		default:
		}
		if err := stream.Send(&types.AckResponse{Success: true}); err != nil {
			return err
		}
	}
}

func outOfOrderDelivery() *types.Delivery {
	return &types.Delivery{
		DeliveryId: "sub-batch-4-3",
		Attempt:    1,
		Batch: []*types.Event{
			{MessageId: "a", Topic: "t", Offset: 6},
			{MessageId: "b", Topic: "t", Offset: 4},
			{MessageId: "c", Topic: "t", Offset: 5},
		},
	}
}

func dialAckTest(t *testing.T, srv *ackTestServer) (*Consumer, string) {
	t.Helper()
	addr := startTestEventServer(t, srv, &testPartitionServer{})
	cfg := DefaultConfig(addr)
	cfg.Security.Insecure = true
	cl, err := Dial(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close() })
	co, err := NewConsumer(cl, DefaultConsumerConfig("t", "g"), func(context.Context, Delivery) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	return co, addr
}

// The acknowledgement of a batch names the offset after its highest event,
// which is the one the server takes, even when the batch came out of order.
func TestConsumer_AckNamesTheOffsetAfterTheHighest(t *testing.T) {
	srv := &ackTestServer{delivery: outOfOrderDelivery(), acks: make(chan *types.AckRequest, 1)}
	co, addr := dialAckTest(t, srv)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = co.consumeFromNode(ctx, addr) }()
	select {
	case ack := <-srv.acks:
		if ack.GetNextOffset() != 7 || !ack.GetSuccess() {
			t.Fatalf("ack: next offset %d, success %v; want 7, true", ack.GetNextOffset(), ack.GetSuccess())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no acknowledgement")
	}
}

// A subscription whose ack stream fails ends, so that the reconnect opens
// both streams again. It used to go on receiving deliveries it could not
// acknowledge, until the server's credits for it ran out and it got nothing.
func TestConsumer_AckStreamFailureEndsTheSubscription(t *testing.T) {
	srv := &ackTestServer{delivery: outOfOrderDelivery(), failAcks: true}
	co, addr := dialAckTest(t, srv)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- co.consumeFromNode(ctx, addr) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		cancel()
		<-done
		t.Fatal("subscription went on after its ack stream failed")
	}
}

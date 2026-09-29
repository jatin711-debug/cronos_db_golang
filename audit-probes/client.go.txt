package client

import (
	"context"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
	"time"
)

type auditClosingServer struct {
	types.UnimplementedEventServiceServer
	closed chan struct{}
}

func (s *auditClosingServer) Subscribe(stream grpc.BidiStreamingServer[types.SubscribeRequest, types.Delivery]) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	close(s.closed)
	return status.Error(codes.Unavailable, "simulated disconnect")
}
func (s *auditClosingServer) Ack(stream grpc.BidiStreamingServer[types.AckRequest, types.AckResponse]) error {
	<-stream.Context().Done()
	return stream.Context().Err()
}
func TestAuditConsumerReturnsAfterStreamFailure(t *testing.T) {
	srv := &auditClosingServer{closed: make(chan struct{})}
	addr := startTestEventServer(t, srv, &testPartitionServer{})
	cfg := DefaultConfig(addr)
	cfg.Security.Insecure = true
	cl, err := Dial(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	co, err := NewConsumer(cl, DefaultConsumerConfig("audit", "audit"), func(context.Context, Delivery) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- co.consumeFromNode(ctx, addr) }()
	select {
	case <-srv.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("subscription did not open")
	}
	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		cancel()
		<-done
		t.Fatal("consumer stuck after subscribe failure; reconnect cannot run until outer context cancels")
	}
}

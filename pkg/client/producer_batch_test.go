package client

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client/internal/circuitbreaker"
	"github.com/jatin711-debug/cronos_db_golang/pkg/client/internal/retry"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testEventServer struct {
	types.UnimplementedEventServiceServer
	firstOffset int64
	publishErr  error
}

func (s *testEventServer) Publish(ctx context.Context, req *types.PublishRequest) (*types.PublishResponse, error) {
	if s.publishErr != nil {
		return nil, s.publishErr
	}
	return &types.PublishResponse{Success: true, PartitionId: 0, Offset: s.firstOffset}, nil
}

func (s *testEventServer) PublishBatch(ctx context.Context, req *types.PublishBatchRequest) (*types.PublishBatchResponse, error) {
	if s.publishErr != nil {
		return nil, s.publishErr
	}
	return &types.PublishBatchResponse{
		Success:        true,
		PublishedCount: int32(len(req.Events)),
		FirstOffset:    s.firstOffset,
		LastOffset:     s.firstOffset + int64(len(req.Events)) - 1,
	}, nil
}

func startTestEventServer(t *testing.T, eventSrv types.EventServiceServer, partitionSrv types.PartitionServiceServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	types.RegisterEventServiceServer(s, eventSrv)
	types.RegisterPartitionServiceServer(s, partitionSrv)
	go func() {
		if err := s.Serve(lis); err != nil {
			t.Logf("test server serve error: %v", err)
		}
	}()
	t.Cleanup(func() {
		s.Stop()
		_ = lis.Close()
	})
	return lis.Addr().String()
}

func TestSendBatchReturnsOffsetsAndPartition(t *testing.T) {
	partitionSrv := &testPartitionServer{firstOffset: 0, lastOffset: 100}
	eventSrv := &testEventServer{firstOffset: 42}
	addr := startTestEventServer(t, eventSrv, partitionSrv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := Dial(ctx, Config{
		BootstrapAddresses: []string{addr},
		NodeIDToAddress:    map[string]string{"node1": addr},
		PartitionCount:     1,
		DialTimeout:        2 * time.Second,
		RequestTimeout:     2 * time.Second,
		Metadata: MetadataConfig{
			TTL: 5 * time.Minute,
		},
		Security: SecurityConfig{Insecure: true},
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	producer, err := client.NewProducer(ProducerConfig{})
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer producer.Close()

	msgs := []Message{
		{MessageID: "m-1", Topic: "test-topic", Payload: []byte("a"), ScheduleTS: time.Now().Add(time.Second).UnixMilli()},
		{MessageID: "m-2", Topic: "test-topic", Payload: []byte("b"), ScheduleTS: time.Now().Add(time.Second).UnixMilli()},
		{MessageID: "m-3", Topic: "test-topic", Payload: []byte("c"), ScheduleTS: time.Now().Add(time.Second).UnixMilli()},
	}

	res, err := producer.SendBatch(ctx, msgs)
	if err != nil {
		t.Fatalf("SendBatch: %v", err)
	}
	if res.PublishedCount != 3 {
		t.Fatalf("expected PublishedCount=3, got %d", res.PublishedCount)
	}
	if len(res.Results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(res.Results))
	}
	for i, r := range res.Results {
		if r.PartitionID != 0 {
			t.Errorf("result %d: expected PartitionID=0, got %d", i, r.PartitionID)
		}
		if r.Offset != 42+int64(i) {
			t.Errorf("result %d: expected Offset=%d, got %d", i, 42+int64(i), r.Offset)
		}
		if r.LeaderID != "node1" {
			t.Errorf("result %d: expected LeaderID=node1, got %q", i, r.LeaderID)
		}
		if r.NodeAddress != addr {
			t.Errorf("result %d: expected NodeAddress=%s, got %q", i, addr, r.NodeAddress)
		}
	}
}

func TestCircuitBreakerRecordsRetryableErrors(t *testing.T) {
	partitionSrv := &testPartitionServer{firstOffset: 0, lastOffset: 100}
	eventSrv := &testEventServer{publishErr: status.Error(codes.Unavailable, "not leader")}
	addr := startTestEventServer(t, eventSrv, partitionSrv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := Dial(ctx, Config{
		BootstrapAddresses: []string{addr},
		NodeIDToAddress:    map[string]string{"node1": addr},
		PartitionCount:     1,
		DialTimeout:        2 * time.Second,
		RequestTimeout:     2 * time.Second,
		Metadata: MetadataConfig{
			TTL: 5 * time.Minute,
		},
		Security: SecurityConfig{Insecure: true},
		// The per-address breaker is created from the CLIENT config, so the
		// threshold must be set here. (A non-positive threshold disables the
		// breaker rather than opening on the first failure.)
		CircuitBreaker: circuitbreaker.Config{
			FailureThreshold: 2,
			SuccessThreshold: 1,
			Timeout:          time.Minute,
		},
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	producer, err := client.NewProducer(ProducerConfig{
		RetryPolicy: retry.Policy{
			MaxAttempts: 1,
			MinBackoff:  0,
			MaxBackoff:  0,
		},
		CircuitBreaker: circuitbreaker.Config{
			FailureThreshold: 2,
			SuccessThreshold: 1,
			Timeout:          time.Minute,
		},
	})
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer producer.Close()

	msg := Message{MessageID: "m-1", Topic: "test-topic", Payload: []byte("a"), ScheduleTS: time.Now().Add(time.Second).UnixMilli()}
	for i := 0; i < 2; i++ {
		_, _ = producer.Send(ctx, msg)
	}

	cb := producer.breakerForAddress(addr)
	if cb.Allow() {
		t.Fatal("expected breaker to open after retryable failures")
	}
}

func TestSendAsyncContextCancellation(t *testing.T) {
	partitionSrv := &testPartitionServer{firstOffset: 0, lastOffset: 100}
	// The server blocks until the context is canceled so we can verify the
	// caller's context is propagated to the RPC.
	blockCh := make(chan struct{})
	eventSrv := &testEventServer{
		publishErr: status.Error(codes.DeadlineExceeded, "timeout"),
	}
	_ = blockCh
	_ = eventSrv

	// Use a server that observes cancellation.
	blockingSrv := &blockingEventServer{blockUntil: make(chan struct{})}
	addr := startTestEventServer(t, blockingSrv, partitionSrv)

	client, err := Dial(context.Background(), Config{
		BootstrapAddresses: []string{addr},
		NodeIDToAddress:    map[string]string{"node1": addr},
		PartitionCount:     1,
		DialTimeout:        2 * time.Second,
		RequestTimeout:     5 * time.Second,
		Metadata: MetadataConfig{
			TTL: 5 * time.Minute,
		},
		Security: SecurityConfig{Insecure: true},
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	producer, err := client.NewProducer(ProducerConfig{})
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer producer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	msg := Message{MessageID: "m-1", Topic: "test-topic", Payload: []byte("a"), ScheduleTS: time.Now().Add(time.Second).UnixMilli()}
	future, err := producer.SendAsyncContext(ctx, msg, nil)
	if err != nil {
		t.Fatalf("SendAsyncContext: %v", err)
	}
	_, asyncErr := future.Wait(context.Background())
	if asyncErr == nil || !isContextError(asyncErr) {
		t.Fatalf("expected context error, got %v", asyncErr)
	}
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

type blockingEventServer struct {
	types.UnimplementedEventServiceServer
	blockUntil chan struct{}
}

func (s *blockingEventServer) Publish(ctx context.Context, req *types.PublishRequest) (*types.PublishResponse, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.blockUntil:
		return nil, context.DeadlineExceeded
	}
}

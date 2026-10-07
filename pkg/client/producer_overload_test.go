package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client/internal/circuitbreaker"
	"github.com/jatin711-debug/cronos_db_golang/pkg/client/internal/retry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A node that has no room says so. The application must hear exactly that,
// every time, so that it can send less. The answer used to be counted
// against the node's circuit breaker, which opened after a few of them; the
// publishes that followed went to the nodes that do not lead the partition
// and came back as "not the leader" or "circuit breaker open".
func TestProducer_ReportsANodeAtCapacityAsOverloaded(t *testing.T) {
	partitionSrv := &testPartitionServer{firstOffset: 0, lastOffset: 100}
	eventSrv := &testEventServer{publishErr: status.Error(codes.ResourceExhausted, "partition 0 is at capacity; retry with backoff")}
	addr := startTestEventServer(t, eventSrv, partitionSrv)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	breaker := circuitbreaker.Config{FailureThreshold: 2, SuccessThreshold: 1, Timeout: time.Minute}
	client, err := Dial(ctx, Config{
		BootstrapAddresses: []string{addr},
		NodeIDToAddress:    map[string]string{"node1": addr},
		PartitionCount:     1,
		DialTimeout:        2 * time.Second,
		RequestTimeout:     2 * time.Second,
		Metadata:           MetadataConfig{TTL: 5 * time.Minute},
		Security:           SecurityConfig{Insecure: true},
		CircuitBreaker:     breaker,
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	producer, err := client.NewProducer(ProducerConfig{
		RetryPolicy:    retry.Policy{MaxAttempts: 3},
		CircuitBreaker: breaker,
	})
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer producer.Close()

	msg := Message{MessageID: "m-1", Topic: "test-topic", Payload: []byte("a"), ScheduleTS: time.Now().Add(time.Second).UnixMilli()}
	for i := 0; i < 6; i++ {
		_, err := producer.Send(ctx, msg)
		var typed *Error
		if !errors.As(err, &typed) || typed.Kind != ErrorKindOverloaded {
			t.Fatalf("publish %d to a node at capacity: %v, want an error of kind %q", i+1, err, ErrorKindOverloaded)
		}
		if status.Code(typed.Err) != codes.ResourceExhausted {
			t.Fatalf("publish %d: the node's own answer is not in the error: %v", i+1, err)
		}
	}
	if !producer.breakerForAddress(addr).Allow() {
		t.Fatal("the breaker of a node that answers that it is at capacity opened")
	}
}

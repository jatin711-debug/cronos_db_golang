package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client/internal/retry"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type refusedPublishServer struct {
	types.UnimplementedEventServiceServer
	response *types.PublishResponse
	err      error
}

func (s *refusedPublishServer) Publish(context.Context, *types.PublishRequest) (*types.PublishResponse, error) {
	return s.response, s.err
}

func TestProducerPublishErrorContracts(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response *types.PublishResponse
		err      error
		want     ErrorKind
	}{
		{"post-append failure", &types.PublishResponse{Error: "replication failed after append", ErrorCode: int32(codes.Internal)}, nil, ErrorKindInternal},
		{"validation", &types.PublishResponse{Error: "schema validation failed", ErrorCode: int32(codes.InvalidArgument)}, nil, ErrorKindValidation},
		{"legacy response", &types.PublishResponse{Error: "duplicate message_id"}, nil, ErrorKindValidation},
		{"leadership exhausted", nil, status.Error(codes.FailedPrecondition, "not the leader"), ErrorKindLeaderChange},
		{"internal status", nil, status.Error(codes.Internal, "record publish completion"), ErrorKindInternal},
		{"permission denied", nil, status.Error(codes.PermissionDenied, "publish denied"), ErrorKindAuth},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := startTestEventServer(t, &refusedPublishServer{response: tc.response, err: tc.err}, &testPartitionServer{lastOffset: 100})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			c, err := Dial(ctx, Config{BootstrapAddresses: []string{addr}, NodeIDToAddress: map[string]string{"node1": addr}, PartitionCount: 1, Security: SecurityConfig{Insecure: true}})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			p, err := c.NewProducer(ProducerConfig{RetryPolicy: retry.Policy{MaxAttempts: 1}})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			_, err = p.Send(ctx, Message{MessageID: "error-contract", Topic: "test-topic", Payload: []byte("payload"), ScheduleTS: time.Now().Add(time.Minute).UnixMilli()})
			var typed *Error
			if !errors.As(err, &typed) || typed.Kind != tc.want {
				t.Fatalf("got %v, want %s", err, tc.want)
			}
		})
	}
}

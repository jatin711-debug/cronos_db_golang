package api

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/tx"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestAuditExperimentalRPCsDisabledByDefault(t *testing.T) {
	s, err := NewGRPCServer(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer s.server.Stop()
	// Even an accidentally supplied transaction handler must stay unregistered.
	s.SetTransactionHandler(&tx.Handler{})
	s.RegisterServices(&EventServiceHandler{}, nil, &PartitionServiceHandler{}, nil)
	listener := bufconn.Listen(1 << 20)
	defer listener.Close()
	go s.server.Serve(listener)
	conn, err := grpc.NewClient("passthrough:///audit", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = types.NewTransactionServiceClient(conn).BeginTransaction(ctx, &types.BeginTransactionRequest{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("transaction API exposed: %v", err)
	}
	_, err = types.NewPartitionServiceClient(conn).SplitPartition(ctx, &types.SplitPartitionRequest{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("split API exposed: %v", err)
	}
}

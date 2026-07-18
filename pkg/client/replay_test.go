package client

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
)

type testPartitionServer struct {
	types.UnimplementedPartitionServiceServer
	firstOffset int64
	lastOffset  int64
}

func (s *testPartitionServer) ListPartitions(context.Context, *types.ListPartitionsRequest) (*types.ListPartitionsResponse, error) {
	return &types.ListPartitionsResponse{
		Partitions: []*types.PartitionInfo{
			{
				PartitionId: 0,
				Topic:       "test-topic",
				LeaderId:    "node1",
				LastOffset:  s.lastOffset,
			},
		},
	}, nil
}

func (s *testPartitionServer) GetPartition(context.Context, *types.GetPartitionRequest) (*types.PartitionInfo, error) {
	return &types.PartitionInfo{
		PartitionId: 0,
		Topic:       "test-topic",
		LeaderId:    "node1",
		LastOffset:  s.lastOffset,
	}, nil
}

func (s *testPartitionServer) GetWALStatus(context.Context, *types.GetWALStatusRequest) (*types.WALStatus, error) {
	return &types.WALStatus{
		PartitionId: 0,
		FirstOffset: s.firstOffset,
		LastOffset:  s.lastOffset,
	}, nil
}

func startTestPartitionServer(t *testing.T, srv types.PartitionServiceServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	types.RegisterPartitionServiceServer(s, srv)
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

func TestSeekEarliest(t *testing.T) {
	addr := startTestPartitionServer(t, &testPartitionServer{firstOffset: 10, lastOffset: 100})

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

	offset, err := client.SeekEarliest(ctx, 0)
	if err != nil {
		t.Fatalf("SeekEarliest: %v", err)
	}
	if offset != 10 {
		t.Fatalf("expected first offset 10, got %d", offset)
	}
}

func TestSeekLatest(t *testing.T) {
	addr := startTestPartitionServer(t, &testPartitionServer{firstOffset: 0, lastOffset: 100})

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

	offset, err := client.SeekLatest(ctx, 0)
	if err != nil {
		t.Fatalf("SeekLatest: %v", err)
	}
	if offset != 101 {
		t.Fatalf("expected latest offset 101, got %d", offset)
	}
}

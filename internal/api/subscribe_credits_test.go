package api

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// A subscriber that does not ask for a credit limit gets the node's
// --max-credits: that many deliveries may be unacknowledged at once.
func TestSubscribeWithoutABufferSizeGetsTheNodesCreditLimit(t *testing.T) {
	const credits, published = 5, 20
	cfg := &types.Config{DataDir: t.TempDir(), PartitionCount: 1, FsyncMode: "periodic", FlushIntervalMS: 10, TickMS: 10, WheelSize: 64, DedupTTLHours: 1, BloomCapacity: 10000, MaxDeliveryCredits: credits}
	pm := partition.NewPartitionManager("node", cfg)
	defer pm.Close()
	if err := pm.CreatePartition(0, "orders"); err != nil {
		t.Fatal(err)
	}
	if err := pm.StartPartition(0); err != nil {
		t.Fatal(err)
	}
	p, _ := pm.GetInternalPartition(0)
	scfg := DefaultConfig()
	scfg.Address = "127.0.0.1:0"
	scfg.SLORecorder = nil
	srv, err := NewGRPCServer(scfg)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()
	srv.RegisterServices(NewEventServiceHandler(pm, p.DedupStore, p.ConsumerGroup), nil, NewPartitionServiceHandler(pm, nil, "node"), nil)
	if err = srv.Start(); err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(srv.listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	events := types.NewEventServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	now := time.Now().UnixMilli()
	for i := 0; i < published; i++ {
		resp, err := events.Publish(ctx, &types.PublishRequest{Event: &types.Event{MessageId: fmt.Sprintf("m-%d", i), Topic: "orders", ScheduleTs: now, Payload: []byte("x")}})
		if err != nil || !resp.GetSuccess() {
			t.Fatalf("publish %d: %v %v", i, resp, err)
		}
	}
	stream, err := events.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = stream.Send(&types.SubscribeRequest{ConsumerGroup: "g", Topic: "orders", PartitionId: 0, StartOffset: -1, SubscriptionId: "s"}); err != nil {
		t.Fatal(err)
	}
	var received atomic.Int64
	go func() {
		for {
			d, err := stream.Recv()
			if err != nil {
				return
			}
			received.Add(int64(len(d.GetBatch())))
			if d.GetEvent() != nil {
				received.Add(1)
			}
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for received.Load() < credits && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	// Nothing is acknowledged, so no more may follow.
	time.Sleep(time.Second)
	if got := received.Load(); got != credits {
		t.Fatalf("received %d unacknowledged events, want the credit limit %d", got, credits)
	}
}

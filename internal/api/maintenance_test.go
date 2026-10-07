package api

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestMaintenanceOperatorPruningPreservesUnacknowledgedEvents(t *testing.T) {
	pm := auditManager(t)
	p, _ := pm.GetInternalPartition(0)
	for _, id := range []string{"zero", "one"} {
		if err := p.Wal.AppendEvent(&types.Event{MessageId: id, Topic: "audit", ScheduleTs: time.Now().Add(-48 * time.Hour).UnixMilli(), Payload: make([]byte, 1<<20)}); err != nil {
			t.Fatal(err)
		}
	}
	before := len(p.Wal.GetSegments())
	if before < 2 {
		t.Fatal("no closed segments")
	}
	h := NewPartitionServiceHandler(pm, nil, "audit")
	resp, err := h.Compact(context.Background(), &types.CompactRequest{PartitionId: 0, Force: true})
	if err != nil || !resp.Success || resp.SegmentsCompacted != 0 {
		t.Fatalf("force compact: %v %v", resp, err)
	}
	retained, err := h.RunRetention(context.Background(), &types.RetentionRequest{PartitionId: 0, MinOffset: 100, MaxAgeHours: 1, MaxSizeBytes: 1})
	if err != nil || !retained.Success || retained.SegmentsDeleted != 0 {
		t.Fatalf("retention: %v %v", retained, err)
	}
	admin := NewAdminServiceHandler(pm, nil, nil, "audit", pm.GetDataDir(), nil, nil)
	adminResult, err := admin.RunRetention(context.Background(), &types.RunRetentionRequest{MaxSizeBytes: 1})
	if err != nil || !adminResult.Success || adminResult.SegmentsDeleted != 0 {
		t.Fatalf("admin retention: %v %v", adminResult, err)
	}
	if len(p.Wal.GetSegments()) != before {
		t.Fatal("operator request removed unfinished work")
	}
	events, err := p.Wal.ReadEvents(0, 1)
	if err != nil || len(events) != 2 {
		t.Fatalf("retained events unreadable: %v %v", events, err)
	}
}

func TestMaintenancePublishDeliverAckAndPruneOverGRPC(t *testing.T) {
	pm := auditManager(t)
	p, _ := pm.GetInternalPartition(0)
	if err := pm.StartPartition(0); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.Address = "127.0.0.1:0"
	server, err := NewGRPCServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server.RegisterServices(NewEventServiceHandler(pm, p.DedupStore, p.ConsumerGroup), nil, NewPartitionServiceHandler(pm, nil, "audit"), nil)
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient(server.Address(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	client := types.NewEventServiceClient(conn)
	sub, err := client.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.Send(&types.SubscribeRequest{ConsumerGroup: "pipeline", Topic: "audit", PartitionId: 0, StartOffset: 0, SubscriptionId: "pipeline-sub", MaxBufferSize: 128}); err != nil {
		t.Fatal(err)
	}
	acks, err := client.Ack(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Wait for server registration rather than relying on a fixed sleep.
	for {
		if _, exists := p.ConsumerGroup.GetGroup("pipeline"); exists {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	const count = 64
	events := make([]*types.Event, count)
	for i := range events {
		events[i] = &types.Event{MessageId: fmt.Sprintf("pipeline-%d", i), Topic: "audit", Payload: make([]byte, 20*1024), ScheduleTs: time.Now().Add(100 * time.Millisecond).UnixMilli()}
	}
	pub, err := client.PublishBatch(ctx, &types.PublishBatchRequest{Events: events})
	if err != nil || !pub.GetSuccess() || pub.PublishedCount != count {
		t.Fatalf("batch acceptance: %v %v", pub, err)
	}
	future, err := client.Publish(ctx, &types.PublishRequest{Event: &types.Event{MessageId: "future", Topic: "audit", Payload: []byte("keep"), ScheduleTs: time.Now().Add(time.Hour).UnixMilli()}})
	if err != nil || !future.GetSuccess() {
		t.Fatalf("future acceptance: %v %v", future, err)
	}
	seen := make(map[string]bool)
	for len(seen) < count {
		delivery, err := sub.Recv()
		if err != nil {
			t.Fatalf("received %d/%d: %v", len(seen), count, err)
		}
		batch := delivery.Batch
		if delivery.Event != nil {
			batch = append(batch, delivery.Event)
		}
		var next int64
		for _, event := range batch {
			if event.Offset < 0 || event.Offset >= count || event.MessageId != fmt.Sprintf("pipeline-%d", event.Offset) {
				t.Fatalf("unexpected delivery: %s offset=%d", event.MessageId, event.Offset)
			}
			seen[event.MessageId] = true
			next = max(next, event.Offset+1)
		}
		if err := acks.Send(&types.AckRequest{DeliveryId: delivery.DeliveryId, Success: true, NextOffset: next}); err != nil {
			t.Fatal(err)
		}
		ack, err := acks.Recv()
		if err != nil || !ack.GetSuccess() {
			t.Fatalf("ACK: %v %v", ack, err)
		}
	}
	for offset := int64(0); offset < count; offset++ {
		if !p.ConsumerGroup.IsCompleted("pipeline", 0, offset) {
			t.Fatalf("missing durable completion at %d", offset)
		}
	}
	if err := pm.Backup(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	n, err := p.PruneWAL(ctx, storage.PruneOptions{AllCompleted: true})
	if err != nil || n == 0 {
		t.Fatalf("completed segment not pruned: %d %v", n, err)
	}
	kept, err := p.Wal.ReadEvents(future.Offset, future.Offset)
	if err != nil || len(kept) != 1 || kept[0].MessageId != "future" {
		t.Fatalf("future timer lost: %v %v", kept, err)
	}
}

package api

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// startPartitionedServer runs an in-process node with every partition created,
// as a standalone node does at startup.
func startPartitionedServer(t *testing.T, partitions int) (*partition.PartitionManager, string) {
	t.Helper()
	cfg := &types.Config{
		DataDir:         t.TempDir(),
		PartitionCount:  partitions,
		FsyncMode:       "periodic",
		FlushIntervalMS: 10,
		TickMS:          10,
		WheelSize:       64,
		DedupTTLHours:   1,
		BloomCapacity:   10000,
	}
	pm := partition.NewPartitionManager("node", cfg)
	for id := int32(0); id < int32(partitions); id++ {
		if err := pm.CreatePartition(id, "orders"); err != nil {
			pm.Close()
			t.Fatalf("create partition %d: %v", id, err)
		}
		if err := pm.StartPartition(id); err != nil {
			pm.Close()
			t.Fatalf("start partition %d: %v", id, err)
		}
	}
	first, err := pm.GetInternalPartition(0)
	if err != nil {
		pm.Close()
		t.Fatal(err)
	}

	serverCfg := DefaultConfig()
	serverCfg.Address = "127.0.0.1:0"
	serverCfg.SLORecorder = nil
	server, err := NewGRPCServer(serverCfg)
	if err != nil {
		pm.Close()
		t.Fatal(err)
	}
	server.RegisterServices(NewEventServiceHandler(pm, first.DedupStore, first.ConsumerGroup), nil, NewPartitionServiceHandler(pm, nil, "node"), nil)
	if err := server.Start(); err != nil {
		pm.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		server.Stop()
		pm.Close()
	})
	return pm, server.Address()
}

// Publishes without a partition key are spread over every partition by message
// ID. A consumer that does not pin a partition must still receive all of them.
func TestSDKConsumerReadsEveryPartitionByDefault(t *testing.T) {
	const partitions, total = 4, 120
	pm, addr := startPartitionedServer(t, partitions)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clientCfg := client.DefaultConfig(addr)
	clientCfg.Security.Insecure = true
	c, err := client.Dial(ctx, clientCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	producer, err := c.NewProducer(client.DefaultProducerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	messages := make([]client.Message, total)
	for i := range messages {
		messages[i] = client.Message{
			MessageID:  fmt.Sprintf("order-%d", i),
			Topic:      "orders",
			Payload:    []byte("payload"),
			ScheduleTS: time.Now().UnixMilli(),
		}
	}
	result, err := producer.SendBatch(ctx, messages)
	if err != nil {
		t.Fatal(err)
	}
	if result.PublishedCount != total {
		t.Fatalf("published %d of %d events (errors=%d)", result.PublishedCount, total, result.ErrorCount)
	}

	used := 0
	for id := int32(0); id < partitions; id++ {
		p, err := pm.GetInternalPartition(id)
		if err != nil {
			t.Fatal(err)
		}
		if p.Wal.GetNextOffset() > 0 {
			used++
		}
	}
	if used < 2 {
		t.Fatalf("test setup: events landed on %d partition(s); need a spread", used)
	}

	var mu sync.Mutex
	received := make(map[string]int32, total)
	allReceived := make(chan struct{})
	consumerCtx, stopConsumer := context.WithCancel(ctx)
	defer stopConsumer()
	done := make(chan error, 1)
	go func() {
		consumerCfg := client.DefaultConsumerConfig("orders", "order-workers")
		consumerCfg.StartOffset = 0
		done <- c.Subscribe(consumerCtx, consumerCfg, func(_ context.Context, d client.Delivery) error {
			events := d.Batch
			if d.Event != nil {
				events = append(events, d.Event)
			}
			mu.Lock()
			defer mu.Unlock()
			for _, event := range events {
				received[event.GetMessageId()] = event.GetPartitionId()
			}
			if len(received) == total {
				select {
				case <-allReceived:
				default:
					close(allReceived)
				}
			}
			return nil
		})
	}()

	select {
	case <-allReceived:
	case err := <-done:
		t.Fatalf("consumer stopped early: %v", err)
	case <-ctx.Done():
		mu.Lock()
		got := len(received)
		mu.Unlock()
		t.Fatalf("default consumer received %d of %d events; events on other partitions were never delivered", got, total)
	}

	mu.Lock()
	seenPartitions := make(map[int32]struct{})
	for _, partitionID := range received {
		seenPartitions[partitionID] = struct{}{}
	}
	mu.Unlock()
	if len(seenPartitions) != used {
		t.Fatalf("deliveries came from %d partitions, events were stored on %d", len(seenPartitions), used)
	}

	stopConsumer()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not stop after its context was cancelled")
	}
}

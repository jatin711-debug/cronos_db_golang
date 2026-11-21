package api

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// startDeliveryBenchServer is one node with its gRPC server on a loopback port. Its
// log syncs in the background, so that what a benchmark measures is the
// node's own work and not this machine's disk.
func startDeliveryBenchServer(b *testing.B, partitions int) string {
	b.Helper()
	cfg := &types.Config{
		DataDir:            b.TempDir(),
		PartitionCount:     partitions,
		FsyncMode:          "periodic",
		FlushIntervalMS:    50,
		TickMS:             10,
		WheelSize:          64,
		SegmentSizeBytes:   64 << 20,
		IndexInterval:      1000,
		DedupTTLHours:      1,
		BloomCapacity:      2000000,
		MaxDeliveryCredits: 50000,
	}
	pm := partition.NewPartitionManager("node", cfg)
	b.Cleanup(func() { pm.Close() })
	for id := int32(0); id < int32(partitions); id++ {
		if err := pm.CreatePartition(id, "orders"); err != nil {
			b.Fatalf("create partition %d: %v", id, err)
		}
		if err := pm.StartPartition(id); err != nil {
			b.Fatalf("start partition %d: %v", id, err)
		}
	}
	first, err := pm.GetInternalPartition(0)
	if err != nil {
		b.Fatal(err)
	}
	serverCfg := DefaultConfig()
	serverCfg.Address = "127.0.0.1:0"
	serverCfg.SLORecorder = nil
	server, err := NewGRPCServer(serverCfg)
	if err != nil {
		b.Fatal(err)
	}
	server.RegisterServices(NewEventServiceHandler(pm, first.DedupStore, first.ConsumerGroup), nil, NewPartitionServiceHandler(pm, nil, "node"), nil)
	if err := server.Start(); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(server.Stop)
	return server.listener.Addr().String()
}

// Events through one node, from a publish to the acknowledgement of their
// delivery: a producer sends b.N events in batches, due at once, and a
// consumer group takes and acknowledges them.
//
//   - order=live: the group is subscribed first, and gets each event from the
//     scheduler as it falls due.
//   - order=backlog: the events are all published first, and the group reads
//     them from the log when it subscribes.
func BenchmarkPublishDeliverAck(b *testing.B) {
	for _, order := range []string{"live", "backlog"} {
		for _, credits := range []int32{64, 2000} {
			b.Run(fmt.Sprintf("order=%s/credits=%d", order, credits), func(b *testing.B) {
				addr := startDeliveryBenchServer(b, 1)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer cancel()
				clientCfg := client.DefaultConfig(addr)
				clientCfg.Security.Insecure = true
				c, err := client.Dial(ctx, clientCfg)
				if err != nil {
					b.Fatal(err)
				}
				defer c.Close()
				producer, err := c.NewProducer(client.DefaultProducerConfig())
				if err != nil {
					b.Fatal(err)
				}
				defer producer.Close()

				var received atomic.Int64
				subscribe := func() {
					go func() {
						cfg := client.DefaultConsumerConfig("orders", "bench")
						cfg.PartitionID = 0
						cfg.StartOffset = 0
						cfg.MaxBufferSize = credits
						_ = c.Subscribe(ctx, cfg, func(_ context.Context, d client.Delivery) error {
							n := int64(len(d.Batch))
							if d.Event != nil {
								n++
							}
							received.Add(n)
							return nil
						})
					}()
				}
				publish := func() {
					payload := make([]byte, 512)
					const batchSize = 500
					for sent := 0; sent < b.N; sent += batchSize {
						messages := make([]client.Message, min(batchSize, b.N-sent))
						now := time.Now().UnixMilli()
						for i := range messages {
							messages[i] = client.Message{MessageID: fmt.Sprintf("m-%d", sent+i), Topic: "orders", PartitionKey: "orders", Payload: payload, ScheduleTS: now}
						}
						result, err := producer.SendBatch(ctx, messages)
						if err != nil || int(result.PublishedCount) != len(messages) {
							b.Fatalf("publish: %+v %v", result, err)
						}
					}
				}
				wait := func() {
					deadline := time.Now().Add(4 * time.Minute)
					for received.Load() < int64(b.N) {
						if time.Now().After(deadline) {
							b.Fatalf("received %d of %d events", received.Load(), b.N)
						}
						time.Sleep(time.Millisecond)
					}
				}

				if order == "live" {
					subscribe()
					time.Sleep(200 * time.Millisecond) // the subscription is open
					b.ResetTimer()
					publish()
					wait()
				} else {
					publish()
					b.ResetTimer()
					subscribe()
					wait()
				}
				b.StopTimer()
				b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "events/s")
			})
		}
	}
}

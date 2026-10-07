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

// startBenchServer starts an in-process gRPC server on localhost:0 and returns
// the server, its address, and a cleanup function.
func startBenchServer(b *testing.B, fsyncMode string) (*GRPCServer, string, func()) {
	b.Helper()

	cfg := &types.Config{
		DataDir:         b.TempDir(),
		PartitionCount:  8,
		FsyncMode:       fsyncMode,
		FlushIntervalMS: 10,
		TickMS:          10,
		WheelSize:       1024,
		DedupTTLHours:   24,
		BloomCapacity:   1000000,
	}
	pm := partition.NewPartitionManager("bench-node", cfg)
	for id := int32(0); id < int32(cfg.PartitionCount); id++ {
		if err := pm.CreatePartition(id, "bench"); err != nil {
			pm.Close()
			b.Fatalf("CreatePartition: %v", err)
		}
	}

	serverCfg := DefaultConfig()
	serverCfg.Address = "localhost:0"
	serverCfg.SLORecorder = nil // disable SLO for benchmark isolation
	grpcServer, err := NewGRPCServer(serverCfg)
	if err != nil {
		b.Fatalf("NewGRPCServer: %v", err)
	}

	handler := NewEventServiceHandler(pm, nil, nil)
	partitionHandler := NewPartitionServiceHandler(pm, nil, "bench-node")
	grpcServer.RegisterServices(handler, nil, partitionHandler, nil)

	if err := grpcServer.Start(); err != nil {
		b.Fatalf("Start: %v", err)
	}

	addr := grpcServer.Address()
	if addr == "" {
		b.Fatal("server address not available")
	}

	cleanup := func() {
		grpcServer.Stop()
		pm.Close()
	}
	return grpcServer, addr, cleanup
}

// BenchmarkPublishBatch_EndToEnd_Matrix measures the full gRPC publish path
// from the client SDK through the handler to the WAL.
func BenchmarkPublishBatch_EndToEnd_Matrix(b *testing.B) {
	for _, fsyncMode := range []string{"every_event", "batch", "periodic"} {
		for payloadName, payloadSize := range map[string]int{
			"64B":  64,
			"256B": 256,
			"4KB":  4096,
			"64KB": 64 * 1024,
		} {
			for _, batchSize := range []int{1, 10, 100, 1000} {
				for _, par := range []int{1, 4, 16} {
					name := fmt.Sprintf("fsync=%s/payload=%s/batch=%d/par=%d", fsyncMode, payloadName, batchSize, par)
					b.Run(name, func(b *testing.B) {
						_, addr, cleanup := startBenchServer(b, fsyncMode)
						defer cleanup()

						ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						defer cancel()

						clientCfg := client.DefaultConfig(addr)
						clientCfg.Security.Insecure = true
						clientCfg.RequestTimeout = 30 * time.Second
						c, err := client.Dial(ctx, clientCfg)
						if err != nil {
							b.Fatalf("client.Dial: %v", err)
						}
						defer c.Close()

						producer, err := c.NewProducer(client.DefaultProducerConfig())
						if err != nil {
							b.Fatalf("NewProducer: %v", err)
						}
						defer producer.Close()

						payload := make([]byte, payloadSize)
						for i := range payload {
							payload[i] = byte('a' + i%26)
						}

						b.ResetTimer()
						b.SetBytes(int64(batchSize * payloadSize))
						b.SetParallelism(par) // workers = par * GOMAXPROCS
						var nextID atomic.Uint64

						b.RunParallel(func(pb *testing.PB) {
							msgs := make([]client.Message, batchSize)
							for pb.Next() {
								base := nextID.Add(uint64(batchSize))
								for i := 0; i < batchSize; i++ {
									msgs[i] = client.Message{
										MessageID:    fmt.Sprintf("bench-%d", base+uint64(i)),
										Topic:        "bench",
										PartitionKey: "bench",
										Payload:      payload,
										ScheduleTS:   time.Now().Add(30 * time.Minute).UnixMilli(),
									}
								}
								result, err := producer.SendBatch(ctx, msgs)
								if err != nil {
									b.Errorf("SendBatch: %v", err)
									return
								}
								if result.PublishedCount != int32(batchSize) || result.ErrorCount != 0 || result.DuplicateCount != 0 {
									b.Errorf("incomplete batch: published=%d duplicates=%d errors=%d", result.PublishedCount, result.DuplicateCount, result.ErrorCount)
									return
								}
							}
						})
						b.StopTimer()
						b.ReportMetric(float64(b.N*batchSize)/b.Elapsed().Seconds(), "events/s")
					})
				}
			}
		}
	}
}

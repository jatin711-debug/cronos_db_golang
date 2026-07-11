package api

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// benchNoopDedup is a DedupManager that never reports duplicates and adds no
// I/O to the publish path. It isolates WAL/dispatcher overhead in the
// end-to-end benchmark.
type benchNoopDedup struct{}

func (benchNoopDedup) IsDuplicate(messageID string, offset int64) (bool, error) {
	return false, nil
}

func (benchNoopDedup) IsDuplicateBatch(messageIDs []string, offsets []int64) ([]bool, error) {
	out := make([]bool, len(messageIDs))
	return out, nil
}

// startBenchServer starts an in-process gRPC server on localhost:0 and returns
// the server, its address, and a cleanup function.
func startBenchServer(b *testing.B, fsyncMode string) (*GRPCServer, string, func()) {
	b.Helper()

	cfg := &types.Config{
		DataDir:         b.TempDir(),
		PartitionCount:  8,
		FsyncMode:       fsyncMode,
		FlushIntervalMS: 10,
	}
	pm := partition.NewPartitionManager("bench-node", cfg)

	serverCfg := DefaultConfig()
	serverCfg.Address = "localhost:0"
	serverCfg.SLORecorder = nil // disable SLO for benchmark isolation
	grpcServer, err := NewGRPCServer(serverCfg)
	if err != nil {
		b.Fatalf("NewGRPCServer: %v", err)
	}

	handler := NewEventServiceHandler(pm, &benchNoopDedup{}, nil)
	partitionHandler := NewPartitionServiceHandler(pm, nil, "bench-node")
	grpcServer.RegisterServices(handler, nil, partitionHandler)

	if err := grpcServer.Start(); err != nil {
		b.Fatalf("Start: %v", err)
	}

	addr := grpcServer.Address()
	if addr == "" {
		b.Fatal("server address not available")
	}

	cleanup := func() {
		grpcServer.Stop()
		pm.StopAllPartitions()
	}
	return grpcServer, addr, cleanup
}

// BenchmarkPublishBatch_EndToEnd_Matrix measures the full gRPC publish path
// from the client SDK through the handler to the WAL.
func BenchmarkPublishBatch_EndToEnd_Matrix(b *testing.B) {
	for _, fsyncMode := range []string{"every_event", "batch", "periodic"} {
		for payloadName, payloadSize := range map[string]int{
			"64B":  64,
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

						b.RunParallel(func(pb *testing.PB) {
							msgs := make([]client.Message, batchSize)
							for pb.Next() {
								for i := 0; i < batchSize; i++ {
									msgs[i] = client.Message{
										Topic:        "bench",
										PartitionKey: "bench",
										Payload:      payload,
										ScheduleTS:   time.Now().Add(time.Hour).UnixMilli(),
									}
								}
								if _, err := producer.SendBatch(context.Background(), msgs); err != nil {
									b.Fatalf("SendBatch: %v", err)
								}
							}
						})
					})
				}
			}
		}
	}
}

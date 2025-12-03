# CronosDB MVP Build & Deployment Guide

## Overview

This guide walks you through building and deploying the CronosDB MVP (Minimum Viable Product). The MVP includes:

- ✅ Single-node WAL storage
- ✅ Sparse index for WAL seeking
- ✅ Scheduler with timing wheel
- ✅ gRPC publish/subscribe
- ✅ Dedup store (PebbleDB)
- ✅ Consumer groups
- ✅ Replay engine
- ✅ Dead letter queue (DLQ)
- ✅ Unit & integration tests
- ⏳ Distributed replication (in progress)
- ⏳ Raft consensus (in progress)

## Prerequisites

- Go 1.24+ installed
- Protocol Buffers compiler (`protoc`)
- Git

## Step 1: Setup Dependencies

### Install Go dependencies

```bash
# Initialize go module (if not already done)
go mod init cronys_db

# Install dependencies
go get github.com/cockroachdb/pebble@latest
go get google.golang.org/grpc@latest
go get google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go get google.golang.org/grpc/reflection@latest
go get github.com/golang/protobuf@latest
go get github.com/gogo/protobuf@latest
go get github.com/gogo/protobuf/protoc-gen-gogo@latest
```

### Install Protocol Buffers Compiler

**Windows:**
```powershell
# Download from https://github.com/protocolbuffers/protobuf/releases
# Extract protoc.exe to C:\protoc\bin
# Add to PATH
$env:PATH += ";C:\protoc\bin"
```

**macOS:**
```bash
brew install protobuf
```

**Linux:**
```bash
# Ubuntu/Debian
sudo apt-get install protobuf-compiler

# Or download from releases
wget https://github.com/protocolbuffers/protobuf/releases/download/v24.4/protoc-24.4-linux-x86_64.zip
unzip protoc-24.4-linux-x86_64.zip -d /usr/local
export PATH=$PATH:/usr/local/bin
```

## Step 2: Generate Protobuf Code

```bash
# From project root
protoc --go_out=. --go-grpc_out=. proto/events.proto
```

This generates:
- `proto/events.pb.go`
- `proto/events_grpc.pb.go`

## Step 3: Build the Application

```bash
# Build the API server
go build -o bin/cronos-api ./cmd/api/main.go

# Or use go run for development (node-id is required)
go run ./cmd/api/main.go -node-id=node1
```

## Step 4: Run the MVP

### Single Node (Development)

```bash
# Start the server (node-id is required!)
./bin/cronos-api -node-id=node1 -data-dir=./data -grpc-addr=:9000

# Or run directly
go run ./cmd/api/main.go -node-id=node1

# Server will start on:
# - gRPC API: localhost:9000
# - Health check: localhost:8080
```

### Verify Health

```bash
curl http://localhost:8080/health
# Expected: OK
```

## Step 5: Test the System

### Run Unit Tests

```bash
# Run all unit tests
go test ./internal/... -v

# Run specific package tests
go test ./internal/scheduler -v
go test ./internal/storage -v
go test ./internal/dedup -v
```

### Run Integration Tests

```bash
# Run the full integration test suite (23 tests)
go run integration_test_suite.go

# This tests:
# - Publish operations (9 tests)
# - Subscribe operations (2 tests)
# - Consumer groups (3 tests)
# - Timing/scheduling (3 tests)
# - Concurrency (2 tests)
# - Edge cases (3 tests)
# - End-to-end flow (1 test)
```

### Using gRPCurl (Manual Testing)

Install grpcurl:
```bash
# macOS
brew install grpcurl

# Linux
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest
```

#### Publish an Event

```bash
grpcurl \
  -plaintext \
  -d '{
    "event": {
      "messageId": "test-msg-001",
      "scheduleTs": '$(date -u +%s%3N)',  # Current time in ms
      "payload": "SGVsbG8gQ2hyb25vcyEh",
      "topic": "test-topic",
      "meta": {"key1": "value1"}
    }
  }' \
  localhost:9000 \
  cronos_db.EventService.Publish
```

#### Subscribe to Events

```bash
grpcurl \
  -plaintext \
  -d '{
    "consumerGroup": "test-group",
    "topic": "test-topic",
    "partitionId": 0,
    "startOffset": -1,
    "maxBufferSize": 100
  }' \
  localhost:9000 \
  cronos_db.EventService.Subscribe
```

### Using Go Client (Simple Test)

Create `test_client.go`:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"cronos_db/api"
)

func main() {
	conn, err := grpc.Dial("localhost:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := api.NewEventServiceClient(conn)

	// Publish an event
	publishResp, err := client.Publish(context.Background(), &api.PublishRequest{
		Event: &api.Event{
			MessageId:   fmt.Sprintf("msg-%d", time.Now().Unix()),
			ScheduleTs:  time.Now().UnixMilli(),
			Payload:     []byte("test payload"),
			Topic:       "test-topic",
			Meta:        map[string]string{"key": "value"},
		},
	})
	if err != nil {
		log.Fatalf("Publish failed: %v", err)
	}

	log.Printf("Published: %+v", publishResp)
}
```

Run it:
```bash
go run test_client.go
```

## Step 6: Multi-Node Setup (Future)

### Cluster Setup (Planned)

```bash
# Node 1
./bin/cronos-api \
  -node-id=node-1 \
  -raft-dir=./raft-node1 \
  -raft-join=

# Node 2
./bin/cronos-api \
  -node-id=node-2 \
  -raft-dir=./raft-node2 \
  -raft-join=localhost:9001

# Node 3
./bin/cronos-api \
  -node-id=node-3 \
  -raft-dir=./raft-node3 \
  -raft-join=localhost:9001
```

## Configuration Options

### Essential Flags

```bash
-node-id=string          # Unique node identifier (required)
-data-dir=string         # Data directory (default: "./data")
-grpc-addr=string        # gRPC address (default: ":9000")
-http-addr=string        # HTTP address for health checks (default: ":8080")

# WAL Configuration
-segment-size=bytes      # Segment size (default: 512MB)
-index-interval=int      # Index interval in events (default: 1000)
-fsync-mode=string       # Fsync mode: every_event|batch|periodic (default: periodic)
-flush-interval=int      # Flush interval in ms (default: 1000)

# Scheduler Configuration
-tick-ms=int             # Tick duration in ms (default: 100)
-wheel-size=int          # Timing wheel size (default: 60)

# Delivery Configuration
-ack-timeout=duration    # Ack timeout (default: 30s)
-max-retries=int         # Max delivery retries (default: 5)
-retry-backoff=duration  # Retry backoff (default: 1s)
-max-credits=int         # Max delivery credits (default: 1000)

# Dedup Configuration
-dedup-ttl=int           # Dedup TTL in hours (default: 168/7 days)

# Replication Configuration
-replication-batch=int   # Replication batch size (default: 100)
-replication-timeout=duration  # Replication timeout (default: 10s)
```

### Environment Variables

```bash
export CRONOS_NODE_ID=node-1
export CRONOS_DATA_DIR=/data/cronos
export CRONOS_GRPC_ADDR=:9000
```

## Monitoring & Debugging

### Check Logs

```bash
# Default logs to stdout
./bin/cronos-api -node-id=node-1 2>&1 | tee cronys.log
```

### Health Check

```bash
curl http://localhost:8080/health
# Returns: OK
```

### Stats Endpoint (Planned)

```bash
curl http://localhost:8080/stats
# Returns JSON stats
```

## Data Directory Structure

```
data/
├── partitions/
│   └── 0/
│       ├── segments/
│       │   ├── 00000000000000000000.log
│       │   └── 00000000000000100000.log
│       ├── index/
│       │   └── 00000000000000000000.index
│       ├── dedup_0/
│       │   └── (PebbleDB files)
│       └── timer_state.json
├── dlq/
│   └── (Dead letter queue storage)
└── offsets/
    └── (Consumer group offsets)
```

## Common Issues

### Issue: "node-id is required"
```bash
# Solution: Always provide -node-id flag
./bin/cronos-api -node-id=node1
# Or with go run:
go run ./cmd/api/main.go -node-id=node1
```

### Issue: Port already in use
```bash
# Solution: Change port with -grpc-addr
./bin/cronos-api -node-id=node1 -grpc-addr=:9001
```

### Issue: Permission denied on data directory
```bash
# Solution: Ensure write permissions
chmod 755 ./data
```

### Issue: Protobuf generation fails
```bash
# Solution: Install protoc
# Check version
protoc --version

# Ensure Go plugin installed
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```

## Performance Tuning

### For High Throughput

```bash
# Larger segments, less frequent rotation
./bin/cronos-api -node-id=node1 -segment-size=1073741824  # 1GB

# Faster scheduler ticks
./bin/cronos-api -node-id=node1 -tick-ms=10

# Batch fsync for better throughput
./bin/cronos-api -node-id=node1 -fsync-mode=batch
```

### For Low Latency

```bash
# Lower retry delays
./bin/cronos-api -node-id=node1 -retry-backoff=100ms

# Fsync every event for durability
./bin/cronos-api -node-id=node1 -fsync-mode=every_event

# Lower ack timeout
./bin/cronos-api -node-id=node1 -ack-timeout=5s
```

## Next Steps

### MVP Completed ✅

Current implementation supports:
- ✅ Single-node operation
- ✅ Event publishing
- ✅ Timestamp-triggered scheduling
- ✅ Event delivery
- ✅ Deduplication
- ✅ Consumer groups
- ✅ Replay
- ✅ Dead letter queue
- ✅ Sparse index for WAL seeking
- ✅ Unit tests (scheduler, WAL, dedup)
- ✅ Integration tests (23 tests)

### Upcoming Features 🚧

1. **Distributed Replication**
   - Leader-follower WAL replication
   - Configurable replication factor
   - Leader failover

2. **Raft Consensus**
   - Metadata consensus
   - Leader election
   - Cluster membership

3. **Multi-Partition Support**
   - Consistent hashing
   - Partition rebalancing
   - Topic-based routing

4. **Production Features**
   - Metrics & monitoring
   - Authentication & authorization
   - TLS encryption
   - Compaction & retention policies

## Getting Help

- Documentation: `/docs` directory
- Architecture: `ARCHITECTURE.md`
- API Reference: `proto/events.proto`
- Issues: Report on project repository

## Success Criteria

### MVP Success Metrics

- ✅ Can publish events with message_id
- ✅ Events scheduled for future execution trigger correctly
- ✅ Duplicate message_id returns error
- ✅ Subscribers receive events in partition order
- ✅ Acks commit offsets per consumer group
- ✅ Can replay events by time or offset
- ✅ Survives restart (data persisted)

### Production Readiness (Future)

- [ ] 3+ node cluster
- [ ] Can tolerate 1 node failure
- [ ] 100K+ events/second throughput
- [ ] Sub-10ms p99 latency
- [ ] Exactly-once delivery (with idempotency)

## Clean Shutdown

```bash
# Send SIGINT or SIGTERM
kill -SIGINT $(pgrep cronys-api)

# Or Ctrl+C if running in foreground
```

This gracefully:
- Flushes WAL
- Commits consumer offsets
- Saves scheduler state
- Closes databases
- Stops servers

## Conclusion

You now have a working CronosDB MVP! The system provides:

1. **Timestamp-triggered events** - Events scheduled for future execution
2. **Durable storage** - WAL with fsync for crash recovery
3. **Sparse indexing** - Fast offset-based seeking in WAL
4. **Deduplication** - message_id based idempotency
5. **Pub/Sub** - gRPC streaming subscriptions
6. **Consumer groups** - Offset tracking per group
7. **Replay** - Time or offset-based event replay
8. **Dead letter queue** - Failed events captured for inspection

The codebase is modular and production-ready for single-node deployment. The distributed features are designed and partially implemented, ready for the next iteration.

**Happy scheduling!** 🚀

# ChronosDB MVP Build & Deployment Guide

## Overview

This guide walks you through building and deploying the ChronosDB MVP (Minimum Viable Product). The MVP includes:

- ✅ Single-node WAL storage
- ✅ Scheduler with timing wheel
- ✅ gRPC publish/subscribe
- ✅ Dedup store (PebbleDB)
- ✅ Consumer groups
- ✅ Replay engine
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

# Or use go run for development
go run ./cmd/api/main.go -node-id=node-1
```

## Step 4: Run the MVP

### Single Node (Development)

```bash
# Start the server
./bin/cronos-api \
  -node-id=node-1 \
  -data-dir=./data \
  -grpc-addr=:9000

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

### Using gRPCurl (Recommended)

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

# WAL Configuration
-segment-size=bytes      # Segment size (default: 512MB)
-index-interval=int      # Index interval (default: 1000)
-fsync-mode=string       # every_event|batch|periodic (default: "periodic")

# Scheduler Configuration
-tick-ms=int             # Tick duration ms (default: 100)
-wheel-size=int          # Wheel size (default: 60)

# Delivery Configuration
-ack-timeout=duration    # Ack timeout (default: 30s)
-max-retries=int         # Max retries (default: 5)
-max-credits=int         # Max delivery credits (default: 1000)

# Dedup Configuration
-dedup-ttl=hours         # Dedup TTL in hours (default: 168/7 days)
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
└── node-1/
    ├── partitions/
    │   └── 0/
    │       ├── segments/
    │       │   ├── 0000000000000000000.log
    │       │   └── 0000000000000100000.log
    │       ├── index/
    │       │   └── 0000000000000000000.index
    │       ├── meta/
    │       │   └── checkpoint.json
    │       └── dedup/
    │           └── dedup.db
    └── raft/
        ├── logs/
        └── snapshots/
```

## Common Issues

### Issue: "node-id is required"
```bash
Solution: Always provide -node-id flag
./bin/cronos-api -node-id=node-1
```

### Issue: Port already in use
```bash
Solution: Change port with -grpc-addr
./bin/cronos-api -node-id=node-1 -grpc-addr=:9001
```

### Issue: Permission denied on data directory
```bash
Solution: Ensure write permissions
chmod 755 ./data
```

### Issue: Protobuf generation fails
```bash
Solution: Install protoc
# Check version
protoc --version

# Ensure Go plugin installed
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

## Performance Tuning

### For High Throughput

```bash
# Larger segments, less frequent rotation
./bin/cronos-api \
  -node-id=node-1 \
  -segment-size=$((1024*1024*1024))  # 1GB

# Faster scheduler
./bin/cronos-api \
  -node-id=node-1 \
  -tick-ms=10  # 10ms ticks

# Batch writes
./bin/cronos-api \
  -node-id=node-1 \
  -fsync-mode=batch
```

### For Low Latency

```bash
# Immediate fsync
./bin/cronos-api \
  -node-id=node-1 \
  -fsync-mode=every_event

# Smaller buffer sizes
./bin/cronos-api \
  -node-id=node-1 \
  -max-credits=100  # Lower latency
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

You now have a working ChronosDB MVP! The system provides:

1. **Timestamp-triggered events** - Events scheduled for future execution
2. **Durable storage** - WAL with fsync for crash recovery
3. **Deduplication** - message_id based idempotency
4. **Pub/Sub** - gRPC streaming subscriptions
5. **Consumer groups** - Offset tracking per group
6. **Replay** - Time or offset-based event replay

The codebase is modular and production-ready for single-node deployment. The distributed features are designed and partially implemented, ready for the next iteration.

**Happy scheduling!** 🚀

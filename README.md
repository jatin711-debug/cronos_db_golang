# CronosDB

> Distributed Timestamp-Triggered Database with Built-in Scheduler & Pub/Sub

[![Go](https://img.shields.io/badge/Go-1.24+-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-MVP-brightgreen.svg)](#)

CronosDB is a distributed database designed for timestamp-triggered event processing. It combines the durability of a write-ahead log (WAL), the precision of a timing wheel scheduler, and the scalability of partitioned, replicated storage.

## Features

### Core Features ✅
- **Timestamp-Triggered Events** - Schedule events for future execution
- **Append-Only WAL** - Durable, segmented storage with CRC32 checksums
- **Timing Wheel Scheduler** - O(1) timer management for millions of events
- **gRPC API** - High-performance streaming pub/sub with batch support
- **Bloom Filter + PebbleDB Dedup** - Lock-free deduplication with two-tier lookup
- **Consumer Groups** - Kafka-style offset tracking
- **Replay Engine** - Time-range or offset-based event replay
- **Backpressure Control** - Flow control with delivery credits

### Distributed Features ✅
- **Multi-Node Clustering** - 3+ node clusters with automatic partition distribution
- **Leader-Follower Replication** - Async WAL replication
- **Raft Consensus** - Metadata consistency via HashiCorp Raft
- **Consistent Hashing** - Automatic partition routing

## Quick Start

### Prerequisites
- Go 1.24+
- protoc (Protocol Buffers compiler)

### Build & Run

```bash
# 1. Generate protobuf code
protoc --go_out=. --go-grpc_out=. proto/events.proto

# 2. Build the server
go build -o bin/cronos-api ./cmd/api/main.go

# 3. Run single node
./bin/cronos-api -node-id=node1 -data-dir=./data

# Or use Makefile for cluster mode (recommended)
make node1  # Terminal 1 - Leader
make node2  # Terminal 2 - Follower
make node3  # Terminal 3 - Follower

# 4. Run load test
make loadtest-batch BATCH_SIZE=100

# 5. Check health
curl http://localhost:8080/health
# Expected: OK
```

### Cluster Mode (3 Nodes)

```bash
# Terminal 1: Start leader node
make node1

# Terminal 2: Start follower (joins leader)
make node2

# Terminal 3: Start follower (joins leader)
make node3

# Run benchmark
make loadtest-batch PUBLISHERS=30 EVENTS=3333 BATCH_SIZE=100
# Expected: ~300K events/sec
```

### Test with grpcurl

```bash
# Publish an event (single)
grpcurl -plaintext \
  -d '{"event":{"messageId":"test-1","scheduleTs":'$(date -u +%s%3N)',"payload":"SGVsbG8=","topic":"test-topic"}}' \
  localhost:9000 cronos_db.EventService.Publish

# Publish batch (high throughput)
grpcurl -plaintext \
  -d '{"events":[{"messageId":"batch-1","scheduleTs":'$(date -u +%s%3N)',"payload":"SGVsbG8=","topic":"test"}]}' \
  localhost:9000 cronos_db.EventService.PublishBatch

# Subscribe to events
grpcurl -plaintext \
  -d '{"consumerGroup":"group-1","topic":"test-topic","partitionId":0}' \
  localhost:9000 cronos_db.EventService.Subscribe
```

See [MVP_BUILD_GUIDE.md](MVP_BUILD_GUIDE.md) for detailed instructions.

## Architecture

```
┌─────────────┐
│   Client    │
└──────┬──────┘
       │ gRPC
       ▼
┌─────────────────────┐
│   API Gateway       │ (gRPC server)
└──────┬──────────────┘
       │
       ├─────────────────────────────┬─────────────────────────────┐
       │                             │                             │
       ▼                             ▼                             ▼
┌──────────────┐            ┌──────────────┐            ┌──────────────┐
│ Partition 0  │            │ Partition 1  │            │ Partition N  │
│  (Leader)    │◄──────────►│  (Leader)    │◄──────────►│  (Leader)    │
└──────┬───────┘            └──────┬───────┘            └──────┬───────┘
       │                            │                            │
       ├────────────┬───────────────┼────────────┬───────────────┤
       │            │               │            │               │
       ▼            ▼               ▼            ▼               ▼
   [WAL]      [Scheduler]      [Delivery]   [Dedup]      [Consumer]
   [DB]       [TimingWheel]    [Worker]     [Store]      [Groups]
```

**Key Components:**
- **WAL Storage** - Append-only, segmented logs with sparse indexes & 1MB buffered writes
- **Timing Wheel** - Hierarchical scheduler for O(1) timer management with batch scheduling
- **Bloom Filter** - Lock-free in-memory filter for fast dedup (skips 99% of PebbleDB reads)
- **Dedup Store** - PebbleDB-backed message deduplication with 64MB memtable
- **Delivery Worker** - Backpressure-controlled event dispatch
- **Consumer Groups** - Offset tracking per group

## Documentation

| Document | Description |
|----------|-------------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Complete system architecture & design |
| [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md) | Directory layout & file formats |
| [MVP_BUILD_GUIDE.md](MVP_BUILD_GUIDE.md) | Build, deployment & testing guide |
| [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md) | Implementation details & status |
| [proto/events.proto](proto/events.proto) | Complete API specification |

## Performance

### Benchmarks (3-Node Cluster)

| Metric | Value | Notes |
|--------|-------|-------|
| **Cluster Throughput** | **303,351 events/sec** | Batch mode, 100 events/batch |
| **Per-Node Throughput** | **101,117 events/sec** | 3 nodes, round-robin |
| **Publish Latency P50** | **225µs** | Batch publish |
| **Publish Latency P95** | **607µs** | Batch publish |
| **Publish Latency P99** | **739µs** | Batch publish |
| **Success Rate** | **100%** | Zero errors |
| **Scheduler Tick** | 100ms | Configurable (1-1000ms) |

### Single Node Performance

| Metric | Value |
|--------|-------|
| Write Throughput (batch) | ~100K events/sec |
| Write Throughput (single) | ~10K events/sec |
| Latency P99 (batch) | <1ms |

### Performance Optimizations Applied

1. **Lock-Free Bloom Filter** - Atomic CAS operations, skips PebbleDB for new keys
2. **Batch APIs** - `PublishBatch` for 100-500 events per call
3. **Batch Scheduling** - Single lock acquisition per batch
4. **PebbleDB Tuning** - 64MB memtable, disabled internal WAL, NoSync
5. **WAL Buffering** - 1MB buffered writer, batch append

> **Benchmark Command:** `make loadtest-batch PUBLISHERS=30 EVENTS=3333 BATCH_SIZE=100`

## Use Cases

1. **Scheduled Tasks** - Execute workflows at specific times
2. **Event Sourcing** - Durable event stream with replay
3. **Temporal Workflows** - Time-based business logic
4. **Distributed Cron** - Cluster-wide scheduled execution
5. **Time-Series Events** - Ordered event streams
6. **Message Queue** - Durable pub/sub with scheduling

## Configuration

### Essential Flags

```bash
-node-id=string          # Node identifier (required)
-data-dir=string         # Data directory (default: "./data")
-grpc-addr=string        # gRPC address (default: ":9000")

# WAL
-segment-size=bytes      # Segment size (default: 512MB)
-fsync-mode=mode         # Fsync mode: every_event|batch|periodic (default: periodic)

# Scheduler
-tick-ms=int             # Tick duration in ms (default: 100)
-wheel-size=int          # Timing wheel size (default: 60)

# Delivery
-ack-timeout=duration    # Ack timeout (default: 30s)
-max-retries=int         # Max delivery retries (default: 5)
-retry-backoff=duration  # Retry backoff (default: 1s)
-max-credits=int         # Max delivery credits (default: 1000)

# Dedup
-dedup-ttl=int           # Dedup TTL in hours (default: 168/7 days)
```

## Project Structure

```
cronos_db/
├── cmd/
│   └── api/
│       └── main.go              # Main entry point
├── internal/
│   ├── api/                     # gRPC server & handlers
│   ├── partition/               # Partition management
│   ├── storage/                 # WAL, segments & sparse index
│   ├── scheduler/               # Timing wheel
│   ├── delivery/                # Event delivery & DLQ
│   ├── consumer/                # Consumer groups
│   ├── dedup/                   # Deduplication
│   ├── replay/                  # Replay engine
│   ├── replication/             # Leader-follower
│   └── config/                  # Configuration
├── pkg/
│   ├── types/                   # Shared types & protobuf
│   └── utils/                   # Utility functions
├── proto/
│   └── events.proto             # Protobuf schema
├── integration_test_suite.go    # Integration tests (23 tests)
├── ARCHITECTURE.md
├── PROJECT_STRUCTURE.md
├── MVP_BUILD_GUIDE.md
├── IMPLEMENTATION_SUMMARY.md
└── README.md
```

## Status

### MVP ✅ Complete
- [x] Single-node operation
- [x] WAL storage with segments
- [x] Sparse index for WAL seeking
- [x] Timing wheel scheduler
- [x] gRPC pub/sub
- [x] Deduplication (Bloom filter + PebbleDB)
- [x] Consumer groups
- [x] Replay engine
- [x] Delivery worker
- [x] Dead letter queue
- [x] Unit tests (scheduler, WAL, dedup)
- [x] Integration tests (23 tests)

### Distributed ✅ Complete
- [x] Multi-node clustering (3+ nodes)
- [x] Leader-follower replication
- [x] Raft consensus for metadata
- [x] Multi-partition support (8 partitions default)
- [x] Consistent hashing for partition routing
- [x] Cluster membership & discovery

### Performance ✅ Optimized
- [x] Batch publish API (100-500 events/call)
- [x] Lock-free bloom filter deduplication
- [x] Batch WAL writes (single syscall per batch)
- [x] Batch scheduling (single lock per batch)
- [x] PebbleDB tuning (64MB memtable, NoSync)
- [x] Timer pooling with sync.Pool
- [x] **300K+ events/sec achieved** 🚀

### Production Hardening 🚧
- [ ] Metrics & monitoring (Prometheus/OpenTelemetry)
- [ ] Distributed tracing
- [ ] Rate limiting & quota management
- [ ] Graceful shutdown & draining
- [ ] Backup & restore utilities
- [ ] Admin CLI & dashboard

## Technology Stack

- **Language**: Go 1.24+
- **gRPC**: High-performance RPC with streaming
- **Storage Engine**: PebbleDB (LSM tree, CockroachDB) with 64MB memtable
- **Consensus**: HashiCorp Raft for metadata
- **Serialization**: Protocol Buffers
- **Concurrency**: Lock-free bloom filter, sync.Pool, atomic operations

## Contributing

This is a reference implementation for educational purposes. The code demonstrates production-ready patterns for distributed systems design.

## License

Apache 2.0

## Resources

- [Building a Distributed Database](https://martinfowler.com/articles/patterns-of-distributed-systems/)
- [Raft Consensus Algorithm](https://raft.github.io/)
- [Timing Wheels](https://www.cs.columbia.edu/~nahum/6998/papers/sosp87-timing-wheels.pdf)
- [Write-Ahead Logging](https://en.wikipedia.org/wiki/Write-ahead_logging)

## Author

Designed and implemented following production-distributed systems best practices.

---

**CronosDB** - Where time meets data. ⏰📊

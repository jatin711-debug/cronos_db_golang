# ChronosDB Directory Structure

## Root Layout

```
cronos_db/
├── cmd/                      # Entry points
│   └── api/                  # Main API server
│       └── main.go
├── internal/                 # Private application code
│   ├── api/                  # gRPC handlers
│   │   ├── consumer_handler.go
│   │   ├── grpc_server.go
│   │   └── handlers.go
│   ├── config/               # Configuration management
│   │   ├── config.go
│   │   └── defaults.go
│   ├── consumer/             # Consumer group management
│   │   ├── group.go
│   │   └── offset_store.go
│   ├── dedup/                # Deduplication store
│   │   ├── dedup_test.go     # Unit tests
│   │   ├── pebble_store.go
│   │   └── store.go
│   ├── delivery/             # Event delivery worker
│   │   ├── dispatcher.go
│   │   ├── dlq.go            # Dead letter queue
│   │   └── worker.go
│   ├── partition/            # Partition management
│   │   └── manager.go
│   ├── replay/               # Replay engine
│   │   └── engine.go
│   ├── replication/          # Leader-follower replication
│   │   ├── follower.go
│   │   └── leader.go
│   ├── scheduler/            # Timing wheel & scheduling
│   │   ├── scheduler.go
│   │   ├── scheduler_test.go # Unit tests
│   │   └── timing_wheel.go
│   └── storage/              # WAL & storage
│       ├── index.go          # Sparse index
│       ├── segment.go
│       ├── wal.go
│       └── wal_test.go       # Unit tests
├── pkg/                      # Public libraries
│   ├── types/
│   │   ├── errors.go
│   │   ├── event.go
│   │   ├── events.pb.go
│   │   ├── events_grpc.pb.go
│   │   ├── grpc.go
│   │   └── helpers.go
│   └── utils/
│       └── hash.go
├── proto/                    # Protobuf definitions
│   └── events.proto
├── data/                     # Runtime data directory
│   └── partitions/
│       └── {partition-id}/
│           ├── segments/     # WAL segment files
│           ├── index/        # Sparse index files
│           ├── dedup_{id}/   # PebbleDB dedup store
│           └── timer_state.json
├── integration_test_suite.go # Integration tests (23 tests)
├── test_client.go            # Simple test client
├── go.mod
├── go.sum
├── ARCHITECTURE.md
├── PROJECT_STRUCTURE.md
├── MVP_BUILD_GUIDE.md
├── IMPLEMENTATION_SUMMARY.md
└── README.md
```

## Component Directory Structure

### 1. **cmd/api/main.go**
Main entry point for the API server
- Parses configuration flags
- Initializes partition manager
- Starts gRPC server
- Sets up health check endpoint

### 2. **internal/api/**
```
internal/api/
├── grpc_server.go            # gRPC server setup
├── handlers.go               # Publish, Subscribe, Ack handlers
└── consumer_handler.go       # Consumer group handlers
```

### 3. **internal/storage/**
```
internal/storage/
├── wal.go                    # WAL main interface
├── segment.go                # Segment file management
├── index.go                  # Sparse index for fast seeking
└── wal_test.go               # Unit tests
```

### 4. **internal/scheduler/**
```
internal/scheduler/
├── timing_wheel.go           # Hierarchical timing wheel
├── scheduler.go              # Scheduler main logic
└── scheduler_test.go         # Unit tests
```

### 5. **internal/delivery/**
```
internal/delivery/
├── dispatcher.go             # Event dispatcher with retries
├── dlq.go                    # Dead letter queue
└── worker.go                 # Delivery worker
```

### 6. **internal/consumer/**
```
internal/consumer/
├── group.go                  # Consumer group struct
└── offset_store.go           # PebbleDB offset tracking
```

### 7. **internal/dedup/**
```
internal/dedup/
├── store.go                  # Deduplication store interface
├── pebble_store.go           # PebbleDB implementation
└── dedup_test.go             # Unit tests
```

### 8. **internal/replication/**
```
internal/replication/
├── leader.go                 # Leader replication logic
└── follower.go               # Follower append logic
```

### 9. **internal/replay/**
```
internal/replay/
└── engine.go                 # Replay engine (time/offset based)
```

### 10. **internal/config/**
```
internal/config/
├── config.go                 # Config struct & flag parsing
└── defaults.go               # Default constant values
```

### 11. **pkg/types/**
```
pkg/types/
├── event.go                  # Event & Config types
├── errors.go                 # Error types
├── events.pb.go              # Generated protobuf
├── events_grpc.pb.go         # Generated gRPC
├── grpc.go                   # gRPC helpers
└── helpers.go                # Utility functions
```

## File Formats

### WAL Segment File: `0000000000000000000.log`
```
[Header: 64 bytes]
├─ Magic number: "CRNOS1"
├─ Version: 1
├─ First offset: 0
├─ Created timestamp
└─ CRC32 of header

[Events]
├─ Event 1:
│   ├─ Length: 4 bytes
│   ├─ CRC32: 4 bytes
│   ├─ Offset: 8 bytes
│   ├─ Timestamp: 8 bytes
│   ├─ Message ID len: 2 bytes
│   ├─ Message ID: N bytes
│   ├─ Topic len: 2 bytes
│   ├─ Topic: N bytes
│   ├─ Payload len: 4 bytes
│   ├─ Payload: N bytes
│   ├─ Meta count: 2 bytes
│   └─ Meta: N bytes
├─ Event 2: ...
└─ ...
```

### Index File: `00000000000000000000.index`
```
[Sparse Index Entries - Binary format]
Each entry: 16 bytes
├─ Offset: 8 bytes (int64, little-endian)
├─ Position: 8 bytes (int64, little-endian)

Index is created every N events (configurable via SparseIndexInterval)
Allows O(log n) seeking to approximate offset, then linear scan
```

### Timer State: `timer_state.json`
```json
{
  "last_tick": 1703123456000,
  "active_timers": 1234
}
```

### Manifest: `manifest.json` (Planned)
```json
{
  "partition_id": 0,
  "segments": [
    {
      "filename": "00000000000000000000.log",
      "first_offset": 0,
      "last_offset": 99999,
      "size_bytes": 104857600,
      "is_active": false
    }
  ],
  "high_watermark": 150000
}
```

### Checkpoint: `checkpoint.json` (Planned)
```json
{
  "partition_id": 0,
  "last_offset": 199999,
  "high_watermark": 150000,
  "scheduler_state": {
    "tick_ms": 100,
    "active_timers": 1234
  },
  "consumer_offsets": {
    "group-1": 150000,
    "group-2": 145000
  }
}
```

## Build & Test Commands

```bash
# Generate protobuf
protoc --go_out=. --go-grpc_out=. proto/events.proto

# Build
go build -o bin/cronos-api ./cmd/api/main.go

# Run unit tests
go test ./internal/... -v

# Run integration tests
go run integration_test_suite.go

# Run server
go run ./cmd/api/main.go -node-id=node1
```

## Configuration

### Data Directory Layout
```
data/
├── partitions/
│   └── {partition-id}/
│       ├── segments/         # WAL segment files
│       ├── index/            # Sparse index files
│       ├── dedup_{id}/       # PebbleDB dedup store
│       └── timer_state.json  # Scheduler state
├── dlq/                      # Dead letter queue (if enabled)
└── offsets/                  # Consumer group offsets
```

### Runtime Files
- `*.log`: WAL segment files (20-char zero-padded offset)
- `*.index`: Sparse offset→position index
- `timer_state.json`: Scheduler tick state
- PebbleDB directories: `dedup_*`, `offsets_*`

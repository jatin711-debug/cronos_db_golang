# ChronosDB Directory Structure

## Root Layout

```
cronos_db/
├── cmd/                      # Entry points
│   ├── api/                  # Main API server
│   │   └── main.go
│   └── tools/                # Admin tools
│       └── benchmark.go
├── internal/                 # Private application code
│   ├── api/                  # gRPC handlers
│   ├── partition/            # Partition management
│   ├── storage/              # WAL & storage
│   ├── scheduler/            # Timing wheel & scheduling
│   ├── delivery/             # Event delivery worker
│   ├── consumer/             # Consumer group management
│   ├── dedup/                # Deduplication store
│   ├── replication/          # Leader-follower replication
│   ├── replay/               # Replay engine
│   ├── raft/                 # Raft consensus layer
│   ├── metadata/             # Metadata store
│   ├── proto/                # Generated protobuf code
│   └── config/               # Configuration management
├── pkg/                      # Public libraries
│   ├── types/                # Shared types
│   ├── utils/                # Utility functions
│   └── errors/               # Error types
├── data/                     # Runtime data directory
│   ├── node-1/               # Node-specific data
│   │   ├── partitions/       # Partition data
│   │   │   ├── partition-0/
│   │   │   │   ├── segments/
│   │   │   │   │   ├── 0000000000000000000.log
│   │   │   │   │   ├── 0000000000000100000.log
│   │   │   │   │   └── ...
│   │   │   │   ├── index/
│   │   │   │   │   ├── 0000000000000000000.index
│   │   │   │   │   └── manifest.json
│   │   │   │   ├── meta/
│   │   │   │   │   ├── checkpoint.json
│   │   │   │   │   └── high_watermark
│   │   │   │   ├── dedup/
│   │   │   │   │   └── dedup.db
│   │   │   │   ├── consumer_offsets/
│   │   │   │   │   └── consumer_offsets.db
│   │   │   │   └── timer_state/
│   │   │   │       └── active_timers.dat
│   │   │   ├── partition-1/
│   │   │   └── ...
│   │   ├── raft/
│   │   │   ├── logs/
│   │   │   ├── snapshots/
│   │   │   └── node.json
│   │   └── wal_cache/
│   └── node-N/
├── proto/                    # Protobuf definitions
│   ├── events.proto
│   └── Makefile              # Proto generation
├── scripts/                  # Build and deployment scripts
│   ├── build.sh
│   ├── deploy.sh
│   └── run-local.sh
├── test/                     # Test data
│   ├── fixtures/
│   └── integration/
├── docs/                     # Documentation
│   ├── api.md
│   ├── deployment.md
│   └── troubleshooting.md
├── go.mod
├── go.sum
├── Makefile
└── README.md
```

## Component Directory Structure

### 1. **cmd/api/main.go**
Main entry point for the API server
- Starts gRPC server
- Initializes partition manager
- Sets up Raft cluster
- Starts background workers

### 2. **internal/partition/**
```
internal/partition/
├── partition.go              # Partition struct & methods
├── manager.go                # Partition manager
├── consistent_hash.go        # Partition routing
└── follower.go               # Follower implementation
```

### 3. **internal/storage/**
```
internal/storage/
├── wal.go                    # WAL main interface
├── segment.go                # Segment file management
├── index.go                  # Sparse index
├── manifest.go               # Segment manifest
├── compaction.go             # Compaction logic
└── recovery.go               # Crash recovery
```

### 4. **internal/scheduler/**
```
internal/scheduler/
├── timing_wheel.go           # Timing wheel implementation
├── scheduler.go              # Scheduler main logic
├── timer.go                  # Timer event structure
├── checkpoint.go             # Timer state persistence
└── worker.go                 # Scheduler worker loop
```

### 5. **internal/delivery/**
```
internal/delivery/
├── dispatcher.go             # Event dispatcher
├── backpressure.go           # Flow control
├── ack_manager.go            # Ack tracking & retry
└── worker.go                 # Delivery worker
```

### 6. **internal/consumer/**
```
internal/consumer/
├── group.go                  # Consumer group struct
├── offset_manager.go         # Offset tracking
├── rebalance.go              # Rebalancing logic
└── state.go                  # Group state management
```

### 7. **internal/dedup/**
```
internal/dedup/
├── store.go                  # Deduplication store interface
├── pebble_store.go           # PebbleDB implementation
├── memory_store.go           # In-memory (testing)
└── ttl_manager.go            # TTL pruning
```

### 8. **internal/replication/**
```
internal/replication/
├── leader.go                 # Leader replication logic
├── follower.go               # Follower append logic
├── batch.go                  # Batch management
├── stream.go                 # Stream protocol
└── state.go                  # Replication state
```

### 9. **internal/replay/**
```
internal/replay/
├── engine.go                 # Replay engine
├── time_range.go             # Time-range replay
├── offset_replay.go          # Offset-based replay
├── scanner.go                # Segment scanner
└── checkpoint.go             # Replay checkpoint
```

### 10. **internal/raft/**
```
internal/raft/
├── node.go                   # Raft node wrapper
├── metadata.go               # Metadata management
└── transport.go              # Raft transport
```

### 11. **internal/api/**
```
internal/api/
├── grpc_server.go            # gRPC server setup
├── handlers.go               # RPC handlers
├── middleware.go             # Interceptors
└── validation.go             # Request validation
```

### 12. **internal/metadata/**
```
internal/metadata/
├── store.go                  # Metadata store interface
├── partition_assignments.go  # Partition ownership
├── consumer_groups.go        # Consumer group metadata
└── checkpoint.go             # Metadata checkpointing
```

### 13. **internal/config/**
```
internal/config/
├── config.go                 # Config struct
├── loader.go                 # Config loading
├── defaults.go               # Default values
└── flags.go                  # CLI flags
```

### 14. **pkg/**
```
pkg/
├── types/
│   ├── event.go              # Event type definitions
│   ├── partition.go          # Partition types
│   └── errors.go             # Error types
├── utils/
│   ├── file.go               # File utilities
│   ├── hash.go               # Hashing utilities
│   └── time.go               # Time utilities
└── errors/
    ├── errors.go             # Error types
    └── codes.go              # Error codes
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

### Index File: `0000000000000000000.index`
```
[Header]
├─ Magic: "CRNIDX1"
├─ Version
└─ Index version

[Sparse Index Entries]
├─ Entry 0:
│   ├─ Timestamp: 8 bytes
│   └─ Offset: 8 bytes
├─ Entry 1:
│   ├─ Timestamp: 1703123456000
│   └─ Offset: 100000
└─ ...
```

### Manifest: `manifest.json`
```json
{
  "partition_id": 0,
  "segments": [
    {
      "filename": "0000000000000000000.log",
      "first_offset": 0,
      "last_offset": 99999,
      "first_ts": 1703123456000,
      "last_ts": 1703123556000,
      "size_bytes": 104857600,
      "is_active": false
    },
    {
      "filename": "0000000000010000000.log",
      "first_offset": 100000,
      "last_offset": 199999,
      "first_ts": 1703123556000,
      "last_ts": 1703123656000,
      "size_bytes": 104857600,
      "is_active": true
    }
  ],
  "high_watermark": 150000,
  "last_compaction_ts": 1703120000000,
  "created_ts": 1703123456000,
  "updated_ts": 1703123656000
}
```

### Checkpoint: `checkpoint.json`
```json
{
  "partition_id": 0,
  "last_offset": 199999,
  "last_segment": "0000000000010000000.log",
  "high_watermark": 150000,
  "scheduler_state": {
    "tick_ms": 100,
    "wheel_size": 60,
    "active_timers": 1234,
    "next_tick_ts": 1703123660000,
    "watermark_ts": 1703123456000
  },
  "consumer_offsets": {
    "group-1": 150000,
    "group-2": 145000
  },
  "dedup_db_stats": {
    "ttl_hours": 168,
    "approximate_count": 50000
  },
  "created_ts": 1703123456000,
  "updated_ts": 1703123656000
}
```

## Build Structure

```
# Generate protobuf
make proto

# Build
make build

# Run tests
make test

# Run locally
make run-local

# Build Docker image
make docker-build

# Deploy
make deploy
```

## Configuration

### Data Directory Layout
```
data/
└── {node-id}/
    ├── config.yaml          # Node config
    ├── partitions/          # Partition data
    │   └── {partition-id}/
    ├── raft/                # Raft data
    └── logs/                # Application logs
```

### Runtime Files
- `*.log`: WAL segment files
- `*.index`: Sparse timestamp→offset index
- `manifest.json`: Segment manifest
- `checkpoint.json`: Periodic state snapshot
- `*.db`: PebbleDB stores (dedup, consumer offsets, metadata)

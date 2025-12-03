# ChronosDB: Distributed Timestamp-Triggered Database

## I. High-Level Architecture

### System Overview

ChronosDB is a distributed, timestamp-triggered database with built-in scheduling, pub/sub messaging, and WAL-based persistence. Events are scheduled for future execution and triggered based on their timestamp.

### Component Architecture

```
┌─────────────┐
│   Client    │
└──────┬──────┘
       │
       │ gRPC
       ▼
┌─────────────────────┐
│   API Gateway       │ (gRPC front layer)
│  :port 9000         │
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
       │                            │                            │
       ▼                            ▼                            ▼
┌──────────────┐            ┌──────────────┐            ┌──────────────┐
│   Followers  │            │   Followers  │            │   Followers  │
└──────────────┘            └──────────────┘            └──────────────┘
       │                            │                            │
       ├────────────┬───────────────┼────────────┬───────────────┤
       │            │               │            │               │
       ▼            ▼               ▼            ▼               ▼
┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
│   WAL    │ │Scheduler │ │Delivery  │ │Dedup    │ │Consumer  │
│  Storage │ │   Worker │ │  Worker  │ │  Store  │ │  Groups  │
└──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘
```

### Communication Flow

1. **Publish Path**:
   ```
   Client → API Gateway → Consistent Hashing → Partition Leader
                                        ↓
                                    WAL Append
                                        ↓
                                    Deduplication Check
                                        ↓
                                    If schedule_ts ≤ now: → Scheduler Timing Wheel
                                        ↓
                                    Replication to Followers
                                        ↓
                                    Ack to Client
   ```

2. **Subscription Path**:
   ```
   Client → API Gateway → Consumer Group Lookup
                                ↓
                            Partition Lookup (via Raft metadata)
                                ↓
                            Subscribe to Partition Leader
                                ↓
                            Delivery Worker pushes events
                                ↓
                            Client receives via gRPC stream
                                ↓
                            Client sends Ack
                                ↓
                            Offset committed to consumer group state
   ```

3. **Scheduling Path**:
   ```
   Timing Wheel Tick → Check expired events
                            ↓
                     Send to Delivery Worker
                            ↓
                     Dispatch to subscriber
                            ↓
                     Wait for ack / retry on timeout
   ```

### Component Responsibilities

#### 1. **API Gateway**
- Single gRPC endpoint for all clients
- Load balancing across partitions
- Request routing based on consistent hashing
- Authentication & rate limiting

#### 2. **Partition Nodes**
Each partition is a logical grouping of data with:
- **Leader**: Accepts writes, coordinates scheduling
- **Followers**: Replicate WAL for durability
- **WAL Storage**: Append-only, segmented logs with sparse indexes
- **Scheduler**: In-memory timing wheel for timestamp-triggered events
- **Delivery Worker**: Manages event dispatch to subscribers
- **Dedup Store**: Idempotency check using message_id

#### 3. **Consensus Layer (Raft)**
- Metadata only (partition ownership, consumer group state)
- Not for WAL replication (simpler custom protocol)
- Leader election for partition metadata
- 3-5 node clusters recommended

#### 4. **Consumer Groups**
- Kafka-style consumer groups
- Offset tracking per group
- Partition rebalancing on member changes
- At-least-once delivery semantics

## II. Partitioning Strategy

### Consistent Hashing
- 256-ring hash space
- Partition assignment based on topic hash
- Minimal data movement on add/remove nodes

### Leader Election
- Raft consensus for metadata leadership
- Custom WAL replication (simpler than Raft for log replication)
- Follower → Leader promotion on leader failure

### Replication Model
**Decision: Hybrid Approach**

1. **Raft for Metadata**:
   - Partition ownership
   - Consumer group membership
   - Offset commits
   - Leader election metadata

2. **Custom Replication for WAL**:
   - Async leader → follower streaming
   - Acks at segment boundary
   - Simpler than Raft for data plane
   - Lower latency

**Tradeoffs**:
- Metadata consistency critical → Raft
- Data plane needs throughput → Custom async replication
- Handles partition leader failure → Metadata Raft promotes follower
- WAL replication gaps → Use idempotency + replay on recovery

## III. Scheduler Design

### Timing Wheel Pattern
```
┌─────────────────────────────────────────────────────────┐
│                   Timing Wheel                          │
├─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────────┤
│ 0   │ 1   │ 2   │ 3   │...  │ N   │     │     │         │
├─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────────┤
│ [E1]│     │ [E2]│     │ ... │     │     │     │         │
│ [E3]│     │     │     │     │     │     │     │         │
└─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────────┘
  ↑ Current tick

Overflow Wheel (for events beyond base window)
┌─────────────────────────────────────────────────────────┐
│             Hierarchical Overflow                       │
├─────────────────────────────────────────────────────────┤
│  Level 0: 0-6 seconds    (60 slots × 100ms tick)       │
│  Level 1: 6-60 seconds   (overflow wheel)              │
│  Level 2: 60+ seconds    (overflow wheel)              │
└─────────────────────────────────────────────────────────┘
```

**Components**:
- **Tick Granularity**: 100ms default (configurable)
- **Wheel Size**: 60 slots
- **Overflow Wheel**: Hierarchical wheels for events beyond base window
- **Expiration Check**: Move events from wheel to ready queue
- **Persistence**: Periodic checkpoint of active timers

**Key Design: Absolute Time Tracking**
- Timers store absolute `ExpirationMs` (Unix timestamp in milliseconds)
- Each wheel level calculates its own delay from absolute time
- Prevents timing drift when events cascade through overflow wheels
- Formula: `delay = (ExpirationMs - now) / tickDuration`

### Scheduler Workflow

```
Publish Event
    ↓
WAL Append (persistent)
    ↓
Check schedule_ts:
    ↓
    ├─ If timestamp ≤ active window → Add to timing wheel
    └─ If timestamp > active window → Add to overflow wheel
    ↓
Timing Wheel Tick
    ↓
Expired Events → Delivery Queue
    ↓
Delivery Worker → Dispatch to subscribers
    ↓
Wait for Ack
    ↓
On timeout → Retry (with configurable backoff)
    ↓
After max retries → Dead Letter Queue (DLQ)
```

### Timer ID Format
- Timer ID: `message_id-offset` (e.g., "msg-123-42")
- Allows same message_id to be scheduled multiple times with AllowDuplicate=true
- Each event gets unique timer in the timing wheel

## IV. Delivery Model (At-Least-Once)

### Dead Letter Queue (DLQ)
Events that fail delivery after max retries are sent to a Dead Letter Queue:
```
┌─────────────────────────────────────────────────────────┐
│                 Dead Letter Queue                       │
├─────────────────────────────────────────────────────────┤
│  - Failed events stored in PebbleDB                    │
│  - Captures: event data, error reason, retry count     │
│  - Configurable: -dlq-enabled, -dlq-max-retries        │
│  - Events can be replayed or inspected                 │
└─────────────────────────────────────────────────────────┘
```

### gRPC Streaming
- Bi-directional streaming for Subscribe
- Client sends Acks asynchronously
- Server pushes events based on subscription

### Backpressure Flow Control
```
┌─────────────┐      Credits: 1000      ┌─────────────┐
│   Server    │◄─────────────────────────│   Client    │
│             │                           │             │
│  Dispatch   │                           │  Process    │
│  Events     │                           │  Acks       │
│             │                           │             │
│ Credits--   │                           │ Credits++   │
│             │◄─────────────────────────│             │
└─────────────┘       Event+Ack          └─────────────┘
```

**Flow Control Mechanism**:
- Client advertises credit (buffer size)
- Server respects credit limit
- Each event decrements credit
- Each ack increments credit
- Throttle when credit = 0

### Delivery Guarantees

1. **At-Least-Once**:
   - Events guaranteed to be delivered ≥1 times
   - Duplicates handled by dedup store
   - Ack-based offset commits

2. **Offset Management**:
   - Track next offset to deliver per consumer group
   - Commit offset after ack
   - Retry on timeout → redeliver same offset

3. **Consumer Group Rebalancing**:
   - Notify subscribers of rebalance
   - Transfer offset state to new consumer
   - Resume from last committed offset

## V. Replay Engine

### Replay Modes

1. **Time-Range Replay**:
   ```
   Given: start_ts, end_ts, subscriber_id
        ↓
   Scan segment files using sparse index
        ↓
   Filter events by timestamp
        ↓
   Stream to subscriber
   ```

2. **Offset-Based Replay**:
   ```
   Given: start_offset, count, subscriber_id
        ↓
   Seek to segment containing start_offset
        ↓
   Read sequentially
        ↓
   Stream to subscriber
   ```

3. **Fast Replay** (compacted):
   - Use pre-compacted snapshots
   - Skip deleted/tombstoned events
   - Faster but loses history

### Replay State
- Track replay progress per subscriber
- Checkpoint every N events
- Resume on crash

## VI. Deduplication Strategy

### message_id Based Deduplication
```
┌─────────────────────────────────────────┐
│         Deduplication Check             │
├─────────────────────────────────────────┤
│                                         │
│  message_id → offset map (persistent)   │
│                                         │
│  [msg1] → offset: 100                   │
│  [msg2] → offset: 200                   │
│  [msg3] → offset: 300                   │
│                                         │
│  TTL: 7 days (automatic expiration)     │
│                                         │
└─────────────────────────────────────────┘
```

**Idempotent Publish**:
1. Check dedup store for message_id
2. If exists: return stored offset (success without append)
3. If not: append to WAL, store (message_id, offset)
4. Dedup store auto-expires old entries

**Store Choice**: Pebble (RocksDB-like) for Go
- LSM tree structure
- Built-in TTL support
- Good performance
- Used in CockroachDB

## VII. Retention & Compaction

### Retention Policies

1. **Size-Based**:
   - Max segment size: 512MB
   - Auto-rotate on threshold

2. **Time-Based**:
   - Delete segments older than N days
   - Safe only after all consumers pass that offset

3. **Offset-Based**:
   - Keep segments until min(consumer_offset) + safety_margin
   - Compute per segment using consumer state

### Compaction Strategy

```
Before Compaction:
┌──────────┐ ┌──────────┐ ┌──────────┐
│ Seg 1    │ │ Seg 2    │ │ Seg 3    │
│ 100MB    │ │ 100MB    │ │ 100MB    │
│ Active   │ │ Active   │ │ Compacting│
└──────────┘ └──────────┘ └──────────┘

After Compaction:
┌──────────┐ ┌──────────┐
│ Seg 1    │ │ Seg 2    │
│ 100MB    │ │ 50MB     │ (tombstones removed)
│ Active   │ │ Active   │
└──────────┘ └──────────┘
```

**Compaction Rules**:
1. Only compact closed segments
2. Never compact active (being written) segment
3. Read all segments
4. Filter: events before min_consumer_offset + tombstones
5. Write to new segment
6. Replace old segments
7. Update index

## VIII. Failure Recovery

### Crash Recovery

```
1. Start partition node
2. Check Raft metadata (leader/follower)
3. If follower:
   - Wait for leader replication
   - Build WAL from leader

4. If leader:
   - Scan all segments
   - Rebuild sparse index
   - Recover timing wheel from WAL
   - Resume scheduling loop
5. Notify Raft cluster of readiness
```

### Leader Failure

```
Leader Fails
    ↓
Raft detects leader unavailability
    ↓
Elect new leader
    ↓
New leader promotes follower
    ↓
Rebuild WAL from other followers if needed
    ↓
Resume operations
```

### Partition Reassignment

```
Add new node
    ↓
Raft updates partition ownership map
    ↓
Rebalance: move some partitions to new node
    ↓
Transfer partition data (stream from existing)
    ↓
Update consumer group partition assignments
    ↓
Consumers reconnect to new partition nodes
```

## IX. Performance Characteristics

### Throughput

- **Write**: ~100K events/sec per partition (single leader)
- **Read**: ~500K events/sec per partition (multiple followers)
- **Latency**: 5-10ms p99 for publish
- **Scheduling**: 100ms tick default (configurable), supports 10M+ scheduled events

### Scalability

- **Horizontal**: Add partition nodes, Raft cluster handles routing
- **Partition Count**: 100-1000 partitions per cluster (tradeoff)
- **Consumer Groups**: Unlimited (offset storage scales with groups)
- **Topics**: Unlimited (consistent hash routing)

### Durability

- **fsync**: Configurable (every event, batch, every N ms)
- **Replication**: Async (custom protocol), configurable ack quorum
- **WAL**: Always append-only before ack
- **Dedup**: Persistent with TTL (default 7 days)

### Consistency

- **Metadata**: Strong (Raft consensus)
- **WAL**: Eventually consistent (async replication)
- **Delivery**: At-least-once (not exactly-once)
- **Ordering**: Per partition, per consumer group (not global)

## X. Operational Considerations

### Monitoring

- Partition lag (events vs delivered)
- WAL disk usage per partition
- Scheduler delay (expected vs actual trigger time)
- Consumer group rebalances
- Replication lag

### Troubleshooting

- **High latency**: Check scheduler wheel, disk IO, network
- **Missing events**: Check dedup store, WAL integrity, consumer offsets
- **Consumer lag**: Check delivery worker, backpressure
- **Rebalances**: Check node health, Raft stability

### Config Tuning

- **Tick granularity**: 10ms (fast) to 1s (slow) - default 100ms
- **WAL segment size**: 64MB (fast) to 1GB (slow) - default 512MB
- **Replication batch size**: 100-10000 events
- **Dedup TTL**: 1-30 days - default 7 days
- **DLQ max retries**: 1-10 - default 3
- **Dispatcher retry delay**: 100ms-10s - default 1s

## XI. Security

### Authentication
- Mutual TLS between nodes
- Client authentication via certificates or tokens

### Authorization
- Topic-level ACLs
- Consumer group access control
- Admin operations (Raft cluster membership)

### Encryption
- At-rest: Full disk encryption (LUKS, EBS)
- In-transit: TLS 1.3
- In-memory: Optional field-level encryption


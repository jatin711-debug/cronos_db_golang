# CronosDB Technical Deep-Dive: Algorithms, Data Structures & Design Patterns

> **Purpose**: This document explains every technical concept, algorithm, and data structure used in CronosDB so you can understand the codebase before reading it. Think of this as the "theory manual" that makes the "code manual" readable.

---

## Table of Contents

1. [Core Concepts](#1-core-concepts)
2. [Storage Layer Deep-Dive](#2-storage-layer-deep-dive)
3. [Scheduling: Hierarchical Timing Wheels](#3-scheduling-hierarchical-timing-wheels)
4. [Deduplication: Bloom Filters](#4-deduplication-bloom-filters)
5. [Delivery & Flow Control](#5-delivery--flow-control)
6. [Distributed Systems Patterns](#6-distributed-systems-patterns)
7. [Consensus: Raft](#7-consensus-raft)
8. [Client-Side Patterns](#8-client-side-patterns)
9. [Performance Optimizations](#9-performance-optimizations)
10. [How to Read the Codebase](#10-how-to-read-the-codebase)

---

## 1. Core Concepts

### 1.1 What is a Time-Triggered Event Database?

**Traditional Message Queue** (Kafka, RabbitMQ):
- Publish message → Deliver immediately (or as soon as consumer is ready)
- No concept of "deliver at 3:00 PM tomorrow"

**CronosDB**:
- Publish message with `schedule_ts = 1719846000000` (some future timestamp)
- System stores it durably
- **Exactly** at that timestamp, it delivers to consumers
- Think: "distributed cron + message queue + database"

**Why this matters**: Enables scheduled tasks, delayed jobs, temporal workflows, event sourcing with time-based triggers.

### 1.2 Write-Ahead Log (WAL)

**Concept**: Before any data is considered "stored", it's written to an append-only log file.

**Why WAL?**
- **Durability**: Even if system crashes after write, data survives
- **Ordering**: Events have a strict sequence (offset 0, 1, 2, 3...)
- **Recovery**: On restart, replay the log to rebuild state
- **Replication**: Followers can replicate the log

**Analogy**: Like a bank ledger. Every transaction is written permanently before it's considered real.

**In CronosDB** (`internal/storage/wal.go`):
- Segmented into 512MB files
- Each record uses **WAL v2** format:
  `[Length(4B)][CRC32(4B)][Offset(8B)][Raft Term(8B)][ScheduleTS(8B)][MsgID][Topic][Payload][Meta][Trailing Checksum(4B)]`
- CRC32 + trailing checksum detect corruption; Raft term enables term-aware replication
- Sparse index for O(log N) lookups
- Upgrading from earlier builds requires a clean `--data-dir` (v2 is not backward-compatible)

### 1.3 Partitioning

**Concept**: Split data across multiple independent units. Each partition has its own WAL, scheduler, and delivery pipeline.

**Why partition?**
- **Parallelism**: 16 partitions = 16x throughput (in theory)
- **Isolation**: One partition's load doesn't affect others
- **Distribution**: Different nodes can own different partitions

**How routing works**:
```
topic "orders" → FNV-1a hash → hash % 16 = partition_id
```

All nodes use the same hash function, so they all agree on which partition owns "orders".

---

## 2. Storage Layer Deep-Dive

### 2.1 Segmented Log Files

**Problem**: If you have one giant log file:
- Deleting old data requires rewriting the entire file
- Corruption anywhere kills everything
- Hard to replicate (must send entire file)

**Solution**: Split into segments

```
segments/
  segment-0000000000000000000.log   (offsets 0-99999)
  segment-0000000000000100000.log   (offsets 100000-199999)
  segment-0000000000000200000.log   (offsets 200000+, ACTIVE)
```

**Benefits**:
- **Compaction**: Delete entire segments when all data is consumed
- **Replication**: Send just the segment files that changed
- **Parallel reads**: Different segments can be read concurrently
- **Memory mapping**: Each segment can be mmap'd independently

**In code**: `internal/storage/segment.go`

### 2.2 Sparse Index

**Problem**: To find event at offset 1,000,000, you could scan from offset 0. But that's O(N).

**Solution**: Keep an index that maps offsets → file positions.

**But** a full index for every event uses too much memory.

**Sparse Index**: Only index every Nth event (e.g., every 1000 events).

```
Index entries (every 1000 events):
  offset=0      → position=64      (after header)
  offset=1000   → position=52480
  offset=2000   → position=104896
  ...
```

**Lookup algorithm**:
1. Binary search index for largest offset ≤ target
2. Seek to that position in the segment
3. Scan forward until target offset found

**Complexity**: O(log N) for binary search + O(1000) scan = effectively O(log N)

**In code**: `internal/storage/index.go`

### 2.3 Memory-Mapped Files (mmap)

**Concept**: Ask the OS to map a file directly into process memory.

**Without mmap**:
```
Read file → Kernel copies to page cache → Kernel copies to user buffer → Use data
```
(2 copies, 2 context switches)

**With mmap**:
```
mmap file → Access memory directly → OS handles paging behind the scenes
```
(0 copies, 0 context switches for reads)

**Why use it?**
- Zero-copy reads (huge performance win)
- OS handles caching automatically
- Multiple processes can share the same mapped memory

**Trade-off**: Writes still need syscalls. So CronosDB uses mmap for reads, buffered writes for appends.

**In code**: `internal/storage/segment.go` (mmap on Linux/Windows, fallback to ReadAt)

### 2.4 CRC32 Checksums

**Concept**: Compute a 32-bit checksum of data. If a single bit flips, checksum changes.

**Why CRC32 (not SHA-256, MD5)?**
- CRC32: ~3-5 GB/s (hardware accelerated on modern CPUs)
- SHA-256: ~200-500 MB/s
- MD5: Broken for security, fine for integrity but CRC32 is faster

**Use case**: Detecting disk corruption, not preventing attacks. CRC32 is perfect for this.

**In code**: Every WAL record has a CRC32 field. `internal/storage/wal.go`

---

## 3. Scheduling: Hierarchical Timing Wheels

### 3.1 The Problem: Scheduling Millions of Timers

You have 10 million events, each scheduled for a different future time.

**Naive approach**: Check every event every tick.
```go
for _, event := range allEvents {
    if event.schedule_ts <= now {
        deliver(event)
    }
}
```
Problem: O(N) per tick. With 10M events and 100ms ticks = 100M checks/second. CPU dies.

**Priority queue (min-heap)**: O(log N) insert, O(1) peek, O(log N) pop.
Better, but still O(log N) per event. With 10M events, log2(10M) ≈ 24 comparisons per operation.

### 3.2 Timing Wheel: O(1) Timer Operations

**Concept**: A circular buffer (the "wheel") where each slot represents a time interval.

**Simple Timing Wheel**:
```
Wheel with 60 slots, 1-second tick:
  Slot 0  → events at :00, :60, :120...
  Slot 1  → events at :01, :61, :121...
  ...
  Slot 59 → events at :59, :119, :179...
  
  Current pointer moves one slot per tick.
  When pointer reaches a slot, all events in that slot are "ready".
```

**Why O(1)?**
- Add timer: `slot = (current_slot + delay) % 60` → O(1)
- Tick: Move pointer, process slot's list → O(events in slot), amortized O(1)
- Remove timer: Linked list removal → O(1)

**Problem**: What if delay is 1 hour? `slot = (0 + 3600) % 60 = 0`. But we don't want it firing immediately!

### 3.3 Hierarchical Timing Wheels

**Solution**: Multiple wheels, like how a clock has hours, minutes, seconds.

```
Level 0 (Root): 100ms tick, 60 slots = 6 second window
Level 1:        6s tick,    60 slots = 360 second (6 min) window
Level 2:        360s tick,  60 slots = 6 hour window
Level 3:        6hr tick,   60 slots = 15 day window
```

**How it works**:
1. Event scheduled for 5 seconds → Level 0, slot 50
2. Event scheduled for 5 minutes → Level 1, slot 50
3. Event scheduled for 5 hours → Level 2, slot 50

**Cascade**: When Level 0 completes a full rotation (60 ticks = 6 seconds), events from Level 1's current slot "cascade" down to Level 0.

```
Level 0 rotation complete → Take Level 1 slot 0 → Recalculate positions in Level 0 → Insert
```

**Why this is brilliant**:
- Near-future events: In Level 0, fired soon
- Far-future events: In higher levels, minimal memory
- All operations: O(1)!

**In CronosDB**: `internal/scheduler/timing_wheel.go`
- 100ms tick (configurable)
- 60 slots per wheel
- Up to 10 levels
- Handles millions of events with bounded memory

### 3.4 Absolute Time Tracking (No Drift)

**Problem**: If you use relative delays ("fire in 5 seconds"), cascading can accumulate rounding errors.

**Solution**: Store absolute timestamps.

```
Event.schedule_ts = 1719846000000 (exact millisecond)

delay = schedule_ts - now
tick = delay / tick_duration
slot = (current_tick + tick) % wheel_size
```

When cascading, recalculate from the absolute `schedule_ts`, not relative position. No drift!

**In code**: `internal/scheduler/scheduler.go`

### 3.5 Two-Tier Cold/Hot Storage

**Problem**: What if you have 100M events scheduled over the next year? Can't fit all in memory.

**Solution**: Two-tier architecture

```
Hot Tier (Timing Wheel): Events within next 60 minutes
  → In memory, fast access

Cold Tier (PebbleDB): Events beyond 60 minutes
  → On disk, only offsets stored (~16 bytes/event)
  → Full event stays in WAL (single source of truth)
```

**Hydrator**: Background process that scans cold store and moves events to timing wheel as their time approaches.

```
Hydrator loop (adaptive interval 5s-5min):
  Scan cold store for events with schedule_ts < now + hot_window
  Read full event from WAL using offset
  Add to timing wheel
  Remove from cold store
```

**Adaptive interval**: Scans more frequently under load, less when idle.

**Why PebbleDB for cold store?**
- LSM tree = excellent for range scans ("give me events between T1 and T2")
- Sorted by key = `[schedule_ts:be64][offset:be64]`
- Automatic compaction

**In code**: `internal/scheduler/cold_store.go`, `internal/scheduler/scheduler.go`

---

## 4. Deduplication: Bloom Filters

### 4.1 The Problem

Client publishes event with `message_id = "order-123"`. Network hiccups, client retries. System must not process the same event twice.

**Requirements**:
- Check if `message_id` was seen before
- Do it in < 1 microsecond (can't hit disk every time)
- Use minimal memory (100M IDs × 64 bytes = 6.4GB - too much!)

### 4.2 Bloom Filter: Probabilistic Set Membership

**Concept**: A space-efficient probabilistic data structure.

**Properties**:
- **Definitely NO**: If bloom says "not seen", it's definitely new (no false negatives)
- **Maybe YES**: If bloom says "seen", it might be new (false positive possible)

**How it works**:
```
Bit array of size M (e.g., 100M bits = ~12MB)
K hash functions (e.g., 7)

Add "order-123":
  h1("order-123") % M → set bit 42
  h2("order-123") % M → set bit 17
  ...
  h7("order-123") % M → set bit 99

Check "order-123":
  h1("order-123") % M → bit 42 set? ✓
  h2("order-123") % M → bit 17 set? ✓
  ...
  All 7 bits set? → "MAYBE seen" (could be false positive)
  Any bit not set? → "DEFINITELY new"
```

**Math**:
- 100M items, 1% false positive rate → ~12MB memory
- 0.1% FPR → ~17MB
- Check time: O(K) = ~7 hash operations = ~40 nanoseconds

**Why this is perfect for dedup**:
- 99% of checks: "definitely new" → skip disk entirely
- 1% of checks: "maybe" → check disk to confirm
- 100M items in 12MB instead of 6.4GB

### 4.3 Two-Tier Deduplication

```
Tier 1: Bloom Filter (in-memory, probabilistic)
  → "NO" (99%): Definitely new, store in PebbleDB + add to bloom
  → "MAYBE" (1%): Check Tier 2

Tier 2: PebbleDB (persistent, exact)
  → Found: Duplicate, return stored offset
  → Not found: False positive, store + add to bloom
```

**Why PebbleDB as fallback?**
- Bloom filter is lost on restart → need persistent store
- Bloom has false positives → need exact confirmation
- PebbleDB is fast enough for 1% of traffic

### 4.4 Rust Bloom Filter (FFI)

**Why Rust for bloom filter?**
- Go's goroutines + GC = overhead for lock-free bit operations
- Rust: `AtomicU64` with `fetch_or` = true lock-free operations
- Rayon: Parallel batch processing (1000 items at once)
- 5-10x faster than pure Go

**CGO FFI**: Go calls Rust functions directly.
```go
// Go side
// #cgo LDFLAGS: -lcronos_dedup
// extern void bloom_add(const char* key);
import "C"
C.bloom_add(C.CString(key))
```

**In code**: `internal/dedup/rust_bloom_unix.go`, `internal/dedup/rust/src/lib.rs`

---

## 5. Delivery & Flow Control

### 5.1 Credit-Based Flow Control

**Problem**: Server sends 10,000 events/second. Consumer processes 100/second. Consumer's memory explodes.

**Solution**: Credits (tokens).

```
Initial state: Consumer has 1000 credits

Server sends event → credits-- (999)
Server sends event → credits-- (998)
...
Consumer acks event → credits++ (999)
Consumer acks event → credits++ (1000)

Credits = 0? Server STOPS sending.
Credits > 0? Server resumes.
```

**Why credits?**
- Consumer controls its own pace
- Server doesn't need to know consumer's capacity
- Simple, stateful, backpressure works automatically

**In code**: `internal/delivery/dispatcher.go`

### 5.2 Circuit Breaker

**Problem**: Consumer crashes. Server keeps trying to send, wasting resources, filling logs.

**Solution**: Circuit breaker pattern (from electrical engineering).

```
States:
  CLOSED   → Normal operation, requests flow through
  OPEN     → Failure rate > threshold, reject all requests fast
  HALF-OPEN → After timeout, allow one trial request

Transitions:
  CLOSED → OPEN:  failure_rate > 50% AND attempts > 10
  OPEN   → HALF-OPEN: after 30 seconds
  HALF-OPEN → CLOSED: trial succeeds
  HALF-OPEN → OPEN: trial fails
```

**Why atomic state machine?**
- No locks = no contention under high load
- Instant decision: check state, allow/reject
- Per-subscription: one bad consumer doesn't affect others

**In code**: `internal/delivery/circuit_breaker.go`

### 5.3 Non-Blocking Retry Heap

**Problem**: Event delivery fails. When to retry? `time.Sleep()` blocks a goroutine.

**Solution**: Min-heap ordered by `retry_at` timestamp.

```
RetryHeap (min-heap by retry_at):
  [retry_at=10:00:01, event=A]
  [retry_at=10:00:05, event=B]
  [retry_at=10:00:30, event=C]

Process loop (every 100ms):
  Peek top: retry_at <= now?
    Yes → Pop and redispatch
    No → Do nothing, check again next tick
```

**Why min-heap?**
- O(1) peek at next retry
- O(log N) insert/pop
- No goroutines sleeping
- Responsive even under retry storms

**In code**: `internal/delivery/retry_queue.go`

### 5.4 Dead Letter Queue (DLQ)

**Problem**: Event fails after 5 retries. What now? Infinite retries = infinite pain.

**Solution**: After max retries, send to DLQ.

```
DLQ: Append-only binary segments (same format as WAL)
  → Can be inspected later
  → Can be replayed
  → Can be archived
```

**Why binary segments?**
- 10x smaller than JSON
- CRC32 integrity
- Easy to rotate and archive

**In code**: `internal/delivery/dlq_segment.go`

---

## 6. Distributed Systems Patterns

### 6.1 Consistent Hashing

**Problem**: You have 10 partitions, 3 nodes. Which node owns partition 5?

**Naive**: `partition_id % node_count`. But when a node joins, everything moves!

```
Node A owns: 0, 3, 6, 9
Node B joins → Node A owns: 0, 2, 4, 6, 8 (almost everything moved!)
```

**Consistent Hashing**: Map both nodes and partitions to a ring.

```
Hash Ring (0 to 2^64):
  Node A at position 1000
  Node B at position 5000
  Node C at position 9000
  
  Partition 5 hashes to position 3000 → belongs to Node A (next clockwise)
  Partition 7 hashes to position 7000 → belongs to Node B
```

**Virtual Nodes**: Each physical node gets 150 virtual positions on the ring.

```
Node A: positions 100, 1100, 2100, ..., 14900 (150 vnodes)
```

**Why virtual nodes?**
- Better load distribution (150 points vs 1)
- When Node B joins, it takes some vnodes from each existing node
- Minimal data movement (only affected vnodes move)

**In code**: `internal/cluster/hashring.go`

### 6.2 Gossip Protocol (SWIM)

**Problem**: In a cluster, how does every node know about every other node?

**Naive**: Central registry. Single point of failure.

**Gossip**: Like office gossip. You tell a few people, they tell a few people, soon everyone knows.

```
Node A: "I'm alive, here's my info"
→ Tells Node B and C
  Node B: "I heard A is alive, let me tell D and E"
    Node D: "I heard A and B are alive..."
```

**SWIM Protocol** (Scalable Weakly-consistent Infection-style Membership):
1. **Probe**: Pick random node, send ping
2. **Ack**: Node responds → it's alive
3. **No ack**: Ask 2 other nodes to ping it indirectly
4. **Still no ack**: Mark as suspect, then dead

**Why SWIM?**
- No central coordinator
- Fault-tolerant (messages can be lost)
- Converges quickly (O(log N) rounds)
- Used by HashiCorp Memberlist (same as Consul, Nomad)

**In code**: `internal/cluster/membership.go`, `internal/cluster/memberlist_adapter.go`

### 6.3 Replication: Leader-Follower

**Concept**: One leader accepts writes, followers copy them.

```
Leader (Node A):
  Accepts publish requests
  Writes to local WAL
  Replicates to followers

Follower (Node B):
  Receives replication stream
  Writes to local WAL
  Serves read requests (if configured)
```

**Why leader-follower?**
- Simple: all writes go to one place
- Consistent: no conflicting writes
- Available: if leader dies, promote follower

**In code**: `internal/replication/leader.go`, `internal/replication/follower.go`

### 6.4 ISR (In-Sync Replicas)

**Concept**: Only followers that are caught up are "in-sync".

```
Replication factor = 3 (1 leader + 2 followers)
ISR = [Leader, Follower1, Follower2] → all 3 are caught up

Follower2 falls behind → ISR = [Leader, Follower1]
Follower2 catches up → ISR = [Leader, Follower1, Follower2]
```

**Why ISR matters?**
- Acknowledgment: Can ack write when ISR size ≥ min_isr
- Durability: More ISR = more copies = safer
- Availability: Can still operate if some followers are down

**In code**: `internal/cluster/manager.go`

---

## 7. Consensus: Raft

### 7.1 The Problem

Multiple nodes need to agree on something (e.g., "who owns partition 5?").

**Without consensus**:
```
Node A: "I own partition 5"
Node B: "I own partition 5"  ← Split brain! Both think they own it!
```

### 7.2 Raft: Understandable Consensus

**Core idea**: Elect a leader. Leader handles all decisions. Everyone follows the leader.

**Three roles**:
- **Leader**: Handles all client requests, replicates to followers
- **Follower**: Replicates leader's log, votes in elections
- **Candidate**: Wants to become leader (during election)

**How leader election works**:
```
1. All nodes start as followers
2. If follower doesn't hear from leader for election_timeout → become candidate
3. Candidate votes for itself, asks others for votes
4. If candidate gets majority votes → becomes leader
5. New leader sends heartbeat to establish authority
```

**Log replication**:
```
Client: "Add Node D to cluster"
Leader: Appends to its log → sends to followers
Followers: Append to their logs → ack
Leader: Receives majority acks → commits → tells followers to commit
```

**Why Raft?**
- Easier to understand than Paxos (the famous "Paxos Made Simple" paper is 16 pages of dense math)
- Proven in production (etcd, Consul, TiKV)
- Strong consistency (linearizable reads)

**In CronosDB**: Used only for **metadata** (who owns what partition), not for data replication.
- Data replication: synchronous gRPC over a dedicated internal listener (`InternalGRPCServer`, default `:7947`), batch CRC32, quorum-acked via `--min-insync-replicas`; bulk install via `ReplicationService.Snapshot` streaming RPC with per-file CRC32
- Metadata: Raft (strong consistency needed)

**In code**: `internal/cluster/raft.go`

### 7.3 Raft FSM (Finite State Machine)

Raft doesn't just store logs. It applies them to a state machine.

```
Log entries:
  [1] AddNode(node=D)
  [2] AssignPartition(partition=5, node=D)
  [3] UpdatePartition(partition=5, leader=D)

FSM applies each entry:
  After [1]: Nodes = {A, B, C, D}
  After [2]: Partition 5 owner = D
  After [3]: Partition 5 leader = D
```

**Why FSM?**
- Log = history of changes
- FSM = current state derived from history
- Can replay log to rebuild state after crash

**In code**: `internal/cluster/raft.go` (ClusterFSM)

---

## 8. Client-Side Patterns

### 8.1 Connection Pooling

**Problem**: Creating a gRPC connection takes time (TCP handshake, TLS, HTTP/2 setup).

**Solution**: Pool connections, reuse them.

```
Pool:
  Node A: [conn1, conn2, conn3] (3 connections)
  Node B: [conn1, conn2, conn3]
  Node C: [conn1, conn2, conn3]

Publish: Pick connection from pool → Round-robin
```

**Why multiple connections per node?**
- HTTP/2 multiplexing is good, but not infinite
- Multiple connections = parallel streams
- If one connection hiccups, others keep working

**In code**: `pkg/client/internal/connpool/`

### 8.2 Metadata Cache

**Problem**: Every publish needs to know which node owns the partition. Asking a central server every time = bottleneck.

**Solution**: Cache partition → node mapping locally.

```
Metadata Cache:
  partition 0 → Node A (expires in 30s)
  partition 1 → Node B (expires in 30s)
  ...

Background refresh every 30s.
```

**Why cache?**
- 99% of requests use cached metadata (no network call)
- Cache miss → fetch from any node, update cache
- Stale cache? Request fails, refresh cache, retry

**In code**: `pkg/client/internal/metadata/`

### 8.3 Circuit Breaker (Client-Side)

Same concept as server-side, but for client → server connections.

```
Node A keeps failing → Circuit opens → Stop sending to Node A
→ Try Node B instead
→ After 30s, try Node A again (half-open)
```

**In code**: `pkg/client/internal/circuitbreaker/`

### 8.4 Hedging

**Concept**: Send request to primary. If it takes too long, send to backup. Use whichever responds first.

```
Send to Node A (primary)
Wait 50ms...
Node A hasn't responded? Send to Node B (hedge)
Node B responds first? Use B's response, cancel A
```

**Why hedge?**
- Tail latency reduction (P99 improves significantly)
- Doesn't increase load much (only slow requests get hedged)
- Works best with idempotent operations

**In code**: `pkg/client/internal/hedging/`

### 8.5 Retry with Exponential Backoff + Jitter

**Problem**: Server is overloaded. Client retries immediately. Server gets more overloaded.

**Solution**: Backoff + jitter.

```
Attempt 1: Wait 0ms
Attempt 2: Wait 100ms × random(0.5-1.5) = 120ms
Attempt 3: Wait 200ms × random(0.5-1.5) = 180ms
Attempt 4: Wait 400ms × random(0.5-1.5) = 350ms
...
```

**Why jitter?**
- Without jitter: All clients retry at exactly 100ms, 200ms, 400ms → thundering herd
- With jitter: Retries are spread out

**In code**: `pkg/client/internal/retry/`

---

## 9. Performance Optimizations

### 9.1 sync.Pool

**Problem**: Go's garbage collector. Creating/destroying objects = GC pressure.

**Solution**: `sync.Pool` = object reuse pool.

```go
var timerPool = sync.Pool{
    New: func() interface{} { return &Timer{} },
}

// Get from pool
timer := timerPool.Get().(*Timer)
// Use it...
// Return to pool
timerPool.Put(timer)
```

**Why?**
- Reuse objects instead of allocating
- GC has less work = lower pause times
- Critical for high-throughput systems

**Used in**: Timer objects, record buffers, transport buffers

### 9.2 Batch Operations

**Concept**: Do multiple things in one operation.

```
Instead of:
  for each event: write to disk
  for each event: check dedup
  for each event: schedule

Do:
  batch = [event1, event2, ..., event1000]
  write batch to disk (single syscall)
  check dedup for batch (parallel)
  schedule batch (single lock acquisition)
```

**Why?**
- Syscalls are expensive → batch amortizes cost
- Lock acquisition is expensive → single lock for 1000 items
- Network round-trip is expensive → batch 4000 events per RPC

**In code**: Everywhere! `PublishBatch`, `AppendBatch`, `ScheduleBatch`, `DispatchBatch`

### 9.3 FNV-1a Hashing

**Problem**: Need to hash strings for partition routing. SHA-256 is slow (~400ns).

**Solution**: FNV-1a (~5ns).

```go
h := fnv.New64a()
h.Write([]byte("orders"))
partition := h.Sum64() % numPartitions
```

**Why FNV-1a?**
- Fast: 5ns vs 400ns for SHA-256
- Good distribution for non-cryptographic use
- Zero allocations in Go

**Trade-off**: Not cryptographically secure. But we don't need security for partition routing.

**In code**: `pkg/utils/hash.go`

### 9.4 Atomic Operations (Lock-Free)

**Problem**: Locks are slow. Contended locks are very slow.

**Solution**: Atomic operations (CAS - Compare And Swap).

```go
// Instead of:
mu.Lock()
credits--
mu.Unlock()

// Do:
atomic.AddInt32(&credits, -1)
```

**Why?**
- No kernel context switch
- No waiting
- Hardware-level operation (single CPU instruction)

**Used in**: Credit tracking, in-flight counters, circuit breaker state, bloom filter bits

### 9.5 Pre-created Segments

**Problem**: When active segment fills up, creating a new segment takes time (file creation, header write).

**Solution**: Create the next segment in background when current segment is 90% full.

```
Segment 2 is active (at 90% capacity)
Background goroutine: Create Segment 3, write header
Segment 2 fills up → Instant swap to Segment 3 (already ready)
```

**Why?**
- Zero-latency rotation
- No pause in write path

**In code**: `internal/storage/wal.go`

---

## 10. How to Read the Codebase

### Suggested Reading Order

1. **Start here**: `cmd/api/main.go`
   - See how everything is wired together
   - Understand the startup sequence

2. **Core data structures**: `pkg/types/event.go`, `pkg/types/errors.go`
   - Understand what an Event looks like
   - Understand error codes

3. **Storage**: `internal/storage/wal.go`, `internal/storage/segment.go`
   - How events are persisted
   - Read the Append and Read paths

4. **Scheduling**: `internal/scheduler/scheduler.go`, `internal/scheduler/timing_wheel.go`
   - How events are scheduled for future delivery
   - Follow the Schedule → Tick → Ready → Dispatch flow

5. **Deduplication**: `internal/dedup/store.go`, `internal/dedup/bloom_store.go`
   - How duplicates are detected
   - Follow the CheckAndStore path

6. **Delivery**: `internal/delivery/dispatcher.go`, `internal/delivery/worker.go`
   - How events reach consumers
   - Follow the Subscribe → Dispatch → Ack flow

7. **API layer**: `internal/api/handlers.go`
   - How gRPC requests are handled
   - See how Publish, Subscribe, Ack are implemented

8. **Cluster**: `internal/cluster/manager.go`, `internal/cluster/raft.go`
   - How distributed mode works
   - Read after understanding single-node mode

9. **Client SDK**: `pkg/client/client.go`, `pkg/client/producer.go`, `pkg/client/consumer.go`
   - How clients interact with the system
   - Good for understanding the external API

### Key Questions to Ask While Reading

**For any file**:
1. What problem does this solve?
2. What would break if this didn't exist?
3. What are the invariants (things that must always be true)?
4. Where are the locks? What do they protect?
5. What happens on crash/restart?

**For data structures**:
1. What operations does it support? What's the complexity?
2. Is it thread-safe? How?
3. What's the memory footprint?

**For algorithms**:
1. What's the input? What's the output?
2. What's the time complexity? Space complexity?
3. What are the edge cases?

---

## Quick Reference: File → Concept Mapping

| File | Concept | Why It Matters |
|------|---------|---------------|
| `internal/storage/wal.go` | Segmented WAL, sparse index | Durability + fast recovery |
| `internal/scheduler/timing_wheel.go` | Hierarchical timing wheel | O(1) scheduling for millions of events |
| `internal/scheduler/cold_store.go` | PebbleDB cold storage | Bounded memory for far-future events |
| `internal/dedup/bloom_store.go` | Bloom filter + PebbleDB | Fast dedup with persistence |
| `internal/delivery/dispatcher.go` | 32-shard dispatcher + credits | Concurrent delivery with backpressure |
| `internal/delivery/circuit_breaker.go` | Atomic circuit breaker | Isolate failed consumers instantly |
| `internal/delivery/retry_queue.go` | Min-heap retry queue | Non-blocking retries |
| `internal/cluster/raft.go` | HashiCorp Raft | Metadata consensus |
| `internal/cluster/hashring.go` | Consistent hashing with vnodes | Minimal data movement on node changes |
| `internal/replication/protocol.go` | *(removed)* | Replaced by `ReplicationService.Append / Sync / Snapshot` over gRPC (`internal/replication/follower.go`, `internal/api/replication_server.go`) |
| `internal/replication/follower.go` | `Follower.InstallSnapshot`, `dialCredentials` | Bulk snapshot install + per-file CRC32 + atomic dir swap; mTLS-aware |
| `internal/replication/mtls.go` | `BuildClientTLSConfig`, `BuildServerTLSConfig` | Optional CA-pinned mTLS on the internal replication channel |
| `pkg/utils/hash.go` | FNV-1a, CRC32 | Fast hashing for routing/integrity |
| `pkg/client/internal/connpool/` | Connection pooling | Reuse expensive connections |
| `pkg/client/internal/hedging/` | Request hedging | Reduce tail latency |

---

## Summary: The "Why" of CronosDB

| Feature | Technology | Why This Choice |
|---------|-----------|----------------|
| WAL segments | Append-only binary files | Durability + ordering + recovery |
| Sparse index | Every 1000 events | O(log N) lookup without full index memory |
| Timing wheel | Hierarchical circular buffers | O(1) timer ops for millions of events |
| Cold store | PebbleDB LSM tree | Range scans + compaction + bounded memory |
| Bloom filter | Rust FFI + AtomicU64 | 40ns checks, lock-free, 12MB for 100M items |
| PebbleDB fallback | LSM tree with TTL | Exact dedup + automatic expiration |
| 32-shard dispatcher | Sharded hash maps | Lock contention ÷ 32 |
| Credit flow control | Atomic CAS counters | Lock-free backpressure |
| Circuit breaker | Atomic state machine | Instant isolation, no goroutine leaks |
| Retry heap | Min-heap by timestamp | O(log N) retries, responsive timeoutLoop |
| Raft consensus | HashiCorp Raft | Strong metadata consistency |
| Custom replication | Binary TCP protocol | Throughput over consistency for data |
| Consistent hashing | SHA-256 + 150 vnodes | Minimal data movement |
| FNV-1a routing | Fast non-cryptographic hash | 5ns vs 400ns for SHA-256 |
| sync.Pool | Object reuse | Near-zero GC pressure |
| Batch operations | Single lock/syscall per N items | Amortized overhead |

---

*Now you're ready to read the code! Start with `cmd/api/main.go` and follow the data flow.*

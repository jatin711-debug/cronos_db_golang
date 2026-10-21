# Partition Runtime Architecture

## Purpose

The partition module is the core execution unit that groups WAL, scheduler, dedup, consumer, and delivery state.

## Key Files

- [internal/partition/manager.go](../../../internal/partition/manager.go) — partition lifecycle, leader promotion/demotion, `SyncPartitionFromLeader` (bulk install trigger).
- [internal/partition/split.go](../../../internal/partition/split.go) — online partition split. **Experimental**: `PartitionService` refuses a split unless the node runs with `--dev --experimental-features`.
- [internal/partition/retention.go](../../../internal/partition/retention.go) — `PruneWAL` (compaction, retention and operator pruning; leader-only and log-start-only in a cluster) and `FollowLogStart` on followers.
- [internal/partition/disk_pressure.go](../../../internal/partition/disk_pressure.go) — disk-pressure monitoring, emergency compaction callback.
- [internal/partition/backpressure.go](../../../internal/partition/backpressure.go) — admission control across ready queue, timing wheel depth, and in-flight deliveries.
- [internal/partition/snapshot.go](../../../internal/partition/snapshot.go) — partition-recovery `snapshot.json` (HWM + consumer offsets); distinct from the gRPC `ReplicationService.Snapshot` install path used for follower bootstrap.
- [internal/partition/lifecycle.go](../../../internal/partition/lifecycle.go), [replicated_lifecycle.go](../../../internal/partition/replicated_lifecycle.go) — lifecycle index, mutation/delivery exclusion and replicated activation.
- [internal/partition/term_start.go](../../../internal/partition/term_start.go) — durable format marker and current-term leadership proof.
- [internal/partition/recovery_snapshot.go](../../../internal/partition/recovery_snapshot.go) — consistent lifecycle recovery-state capture and import across WAL generation swaps.

## Main Flow

1. Partition manager creates and starts partition runtime components.
2. Routing resolves partition ownership for publish/replay paths.
3. Leader/follower transitions attach replication responsibilities.
4. Compaction removes finished segments; online split is experimental (see above).

## Production Decisions

- Partition-level encapsulation limits blast radius: each partition owns WAL, scheduler, dedup, dispatcher, worker, consumer groups, **DLQ**, and optional leader/follower replication roles.
- **Standalone** mode creates all partitions at startup; **cluster** mode creates partition 0 for shared state and **lazy-creates** others on first use.
- Create path wires **`NewDeadLetterQueue` + `NewDispatcherWithDLQ`** so delivery failures are durable.
- Start path runs **dedup WAL recovery** and **WAL timer replay** (`replayWALTimers`): only events whose time has not come are scheduled again. Events already due stay in the log; each subscription reads what its group has not finished from the log at its consumers' pace (`redriveRetained` in [handlers.go](../../../internal/api/handlers.go)), so a restart does not load the undelivered backlog into memory.
- Partition storage uses WAL v2 and default fsync mode `batch`.
- Epoch handling fences stale leaders; `ReplicateMu` preserves offset order on the RF>1 write path only.
- Admission control and disk monitor prevent overload and trigger emergency compaction.
- `--event-lifecycle` enables future-event state queries, cancellation and
  rescheduling. The disk-backed index tracks heads without payloads and is
  rebuilt before readers run. A staged mutation suppresses its predecessor
  but does not make it removable; acceptance requires the current-term proof.
- Lifecycle snapshot install captures the accepted boundary, epoch, consumer
  progress and dead letters with the WAL generation. Startup resumes an
  interrupted state import before serving; suffix replacement clears stale
  completions and rebuilds the index before offsets can be reused.
- See the [lifecycle contract](../../RETENTION_AND_EVENT_LIFECYCLE.md) and
  [term-start protocol](../../REPLICATION_TERM_START.md) for rolling-upgrade
  requirements, internal offsets and unsupported combinations.

## Debug Pointers

- Missing partition errors: [internal/partition/manager.go](../../../internal/partition/manager.go)
- Ownership transitions: [internal/partition/manager.go](../../../internal/partition/manager.go)
- Split behavior (experimental): [internal/partition/split.go](../../../internal/partition/split.go)

## Related Diagrams

- [System overview](../README.md#system-overview)
- [Cluster rebalance](cluster.md#cluster-rebalance)

# Compliance and Retention Architecture

## Purpose

The compliance module enforces retention and storage lifecycle constraints while preserving protected runtime data paths.

## Key Files

- [internal/compliance/retention.go](../../../internal/compliance/retention.go)
- [internal/api/partition_handler.go](../../../internal/api/partition_handler.go)
- [cmd/api/main.go](../../../cmd/api/main.go)

## Main Flow

1. Startup creates a managed retention enforcer (`compliance.NewManagedEnforcer`) with `pm.RemoveRetainedSegment` as its remove function, and runs it once an hour.
2. The enforcer parses the 64-byte segment header (`firstOffset`, `createdTS`), never proposes the active segment of a partition, skips protected system directories, honors context cancellation, and picks segments older than `--retention-max-age-hours` or the oldest ones while the total is above `--retention-max-size-gb`.
3. It does not delete files itself. Each candidate goes to `PartitionManager.RemoveRetainedSegment`, which calls `Partition.PruneWAL` for that segment. A segment is removed only when, for every entry in it, at least one consumer group takes the entry and every group that does has finished it, and the change feed (when one runs) has exported it. On a replicated partition only the leader decides, and only the oldest segments go, so the log stays one unbroken range; a follower's call returns without removing anything, and followers drop what their leader has released (`FollowLogStart`). A segment of a partition not loaded on this node is kept.
4. The partition admin RPCs `RunRetention` and `Compact` go through the same `PruneWAL` boundary and report reclaimed segments and bytes.

With `--event-lifecycle`, accepted cancellations and superseded copies can be
terminal before their original schedule. The retained active successor still
has its own consumer obligation. Accepted term-start records need no consumer
completion. Neither an unaccepted mutation nor an uncommitted term-start record
can release a segment.

`--wal-relocation-bytes` optionally moves never-due work out of the oldest
selected closed segment. It is off by default, requires lifecycle mode, and
replicates the replacement before the predecessor can be pruned. It cannot
evacuate unfinished due work or the active segment. See
[retention and relocation](../../RETENTION_AND_EVENT_LIFECYCLE.md#retention-and-relocation).

## Production Decisions

- Age and size limits only propose segments; accepted lifecycle state, consumer
  completion, the committed boundary, change-feed progress and dead-letter
  replication decide. A pinned segment can keep a log above its size limit.
- Protected directories and system paths are excluded from destructive cleanup.
- Context cancellation is honored during enforcement scans.
- Retention operations are separated from hot request path.
- Admin operations return explicit status payloads rather than silent actions.

## Debug Pointers

- Policy implementation: [internal/compliance/retention.go](../../../internal/compliance/retention.go)
- Runtime admin retention logic: [internal/api/partition_handler.go](../../../internal/api/partition_handler.go)

## Related Diagrams

- [WAL lifecycle](storage.md#wal-lifecycle)
- [Observability feedback loop](slo.md#observability-feedback-loop)

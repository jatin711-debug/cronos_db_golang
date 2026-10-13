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

## Production Decisions

- Age and size limits only propose segments; consumer completion and the change feed decide. A segment that cannot go yet stays, which can keep a log above its size limit.
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

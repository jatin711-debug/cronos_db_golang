# Partition Runtime Architecture

## Purpose

The partition module is the core execution unit that groups WAL, scheduler, dedup, consumer, and delivery state.

## Key Files

- [internal/partition/manager.go](../../../internal/partition/manager.go)
- [internal/partition/split.go](../../../internal/partition/split.go)
- [internal/partition/disk_pressure.go](../../../internal/partition/disk_pressure.go)

## Main Flow

1. Partition manager creates and starts partition runtime components.
2. Routing resolves partition ownership for publish/replay paths.
3. Leader/follower transitions attach replication responsibilities.
4. Partition split and compaction operations maintain scalability.

## Production Decisions

- Partition-level encapsulation limits blast radius.
- Epoch handling supports leader fencing.
- Admission control prevents overload per partition.
- Disk monitor can trigger emergency compaction under pressure.

## Debug Pointers

- Missing partition errors: [internal/partition/manager.go](../../../internal/partition/manager.go)
- Ownership transitions: [internal/partition/manager.go](../../../internal/partition/manager.go)
- Split behavior: [internal/partition/split.go](../../../internal/partition/split.go)

## Related Diagrams

- [system_overview.mmd](../../mermaid/system_overview.mmd)
- [cluster_rebalance_flow.mmd](../../mermaid/cluster_rebalance_flow.mmd)

# Replication Architecture

## Purpose

Replication keeps partition data synchronized across followers and between regions for durability and geo resilience.

## Key Files

- [internal/replication/protocol.go](../../../internal/replication/protocol.go)
- [internal/replication/leader.go](../../../internal/replication/leader.go)
- [internal/replication/follower.go](../../../internal/replication/follower.go)
- [internal/replication/region.go](../../../internal/replication/region.go)
- [internal/api/replication_server.go](../../../internal/api/replication_server.go)
- [internal/api/crossregion_server.go](../../../internal/api/crossregion_server.go)

## Main Flow

1. Leader appends events and tracks follower progress.
2. Replication service Append enforces partition and offset expectations.
3. Sync endpoint streams catch-up batches for lagging followers.
4. Cross-region replicator batches outbound events to remote regions.

## Production Decisions

- Expected-next-offset checks prevent silent divergence.
- Streaming sync is used for efficient catch-up transfers.
- Cross-region replication is asynchronous to preserve local write latency.
- Conflict handling in cross-region path favors deterministic resolution.

## Debug Pointers

- Follower lag and append mismatch: [internal/api/replication_server.go](../../../internal/api/replication_server.go)
- Leader/follower state transitions: [internal/replication/leader.go](../../../internal/replication/leader.go)
- Cross-region behavior: [internal/api/crossregion_server.go](../../../internal/api/crossregion_server.go)

## Related Diagrams

- [cross_region_replication.mmd](../../mermaid/cross_region_replication.mmd)
- [cluster_rebalance_flow.mmd](../../mermaid/cluster_rebalance_flow.mmd)

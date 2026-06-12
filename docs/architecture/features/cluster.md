# Cluster Control Plane Architecture

## Purpose

The cluster module provides node membership, partition routing, leader assignment, and metadata consensus.

## Key Files

- [internal/cluster/manager.go](../../../internal/cluster/manager.go)
- [internal/cluster/router.go](../../../internal/cluster/router.go)
- [internal/cluster/hashring.go](../../../internal/cluster/hashring.go)
- [internal/cluster/membership.go](../../../internal/cluster/membership.go)
- [internal/cluster/memberlist_adapter.go](../../../internal/cluster/memberlist_adapter.go)
- [internal/cluster/raft.go](../../../internal/cluster/raft.go)
- [cmd/api/main.go](../../../cmd/api/main.go)

## Main Flow

1. Cluster manager starts Raft and membership services.
2. Router computes partition ownership via consistent hash ring.
3. Join/leave events trigger rebalance and state transfer actions.
4. Leader-only tasks monitor partition health and reconcile metadata into Raft.

## Production Decisions

- Configurable virtual nodes improve partition leadership balance.
- Consensus metadata is persisted with Raft and Bolt-backed storage.
- Rebalance path uses partition accessor hooks to avoid ownership without data.
- Explicit epoch usage supports leader fencing behavior.

## Debug Pointers

- Ownership/routing confusion: [internal/cluster/router.go](../../../internal/cluster/router.go)
- Metadata drift: [internal/cluster/manager.go](../../../internal/cluster/manager.go)
- Raft status and peers: [internal/cluster/raft.go](../../../internal/cluster/raft.go)

## Related Diagrams

- [cluster_rebalance_flow.mmd](../../mermaid/cluster_rebalance_flow.mmd)
- [system_overview.mmd](../../mermaid/system_overview.mmd)
- [startup_sequence.mmd](../../mermaid/startup_sequence.mmd)

# Cluster Control Plane Architecture

## Purpose

The cluster module provides node membership, partition routing, leader assignment, and metadata consensus.

## Key Files

- [internal/cluster/manager.go](../../../internal/cluster/manager.go) — `Manager.JoinCluster`, `Manager.reconcileLocalLeadership` (5s tick that wires up `PromoteToLeader` + `AddFollower` after formation).
- [internal/cluster/router.go](../../../internal/cluster/router.go)
- [internal/cluster/hashring.go](../../../internal/cluster/hashring.go)
- [internal/cluster/membership.go](../../../internal/cluster/membership.go)
- [internal/cluster/memberlist_adapter.go](../../../internal/cluster/memberlist_adapter.go)
- [internal/cluster/raft.go](../../../internal/cluster/raft.go)
- [internal/cluster/election.go](../../../internal/cluster/election.go) — partition leader election on failure.
- [internal/cluster/autoscaler.go](../../../internal/cluster/autoscaler.go) — partition-leader health monitoring and reconciliation into Raft.
- [internal/cluster/service.go](../../../internal/cluster/service.go), [internal/cluster/types.go](../../../internal/cluster/types.go)
- [cmd/api/main.go](../../../cmd/api/main.go)

## Main Flow

1. Cluster manager starts Raft and membership services.
2. Router computes partition ownership via consistent hash ring.
3. Join/leave events trigger rebalance and state transfer actions.
4. Leader-only tasks monitor partition health and reconcile metadata into Raft.

## Production Decisions

- Production clusters require `--replication-factor>=3` and `--min-insync-replicas>=2`.
- Replication traffic is secured with mTLS via `--replication-tls-*` in production (see [replication.md](replication.md)).
- Configurable virtual nodes improve partition leadership balance (default `2048`, raised from the older `150` because sparse vnode counts produced severe ownership skew in small clusters).
- Consensus metadata is persisted with Raft and Bolt-backed storage.
- Rebalance path uses partition accessor hooks to avoid ownership without data.
- Explicit epoch usage supports leader fencing behavior.
- New-node join path: `Manager.JoinCluster` iterates `router.GetLocalPartitions()` and calls `PartitionManager.SyncPartitionFromLeader(partitionID, leader.Address)`, which triggers `Follower.InstallSnapshot` (bulk `ReplicationService.Snapshot` install) for each partition the joining node owns.
- After formation, `reconcileLocalLeadership` runs every 5s on every node and idempotently wires up `PromoteToLeader` + `AddFollower` for every locally-led partition — this is what enables streaming `ReplicationService.Append` on a healthy cluster.

## Debug Pointers

- Ownership/routing confusion: [internal/cluster/router.go](../../../internal/cluster/router.go)
- Metadata drift: [internal/cluster/manager.go](../../../internal/cluster/manager.go)
- Raft status and peers: [internal/cluster/raft.go](../../../internal/cluster/raft.go)

## Related Diagrams

- [cluster_rebalance_flow.mmd](../../mermaid/cluster_rebalance_flow.mmd)
- [system_overview.mmd](../../mermaid/system_overview.mmd)
- [startup_sequence.mmd](../../mermaid/startup_sequence.mmd)

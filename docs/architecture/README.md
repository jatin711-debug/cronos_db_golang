# CronosDB Architecture By Feature

This folder contains the architecture split by feature so developers can read subsystem docs independently.

## Production-Hardening Highlights

Recent changes reflected across these docs:

- **WAL v2 record format** with Raft term and trailing checksum; upgrading requires a clean data dir.
- **Persistent consumer group metadata** (assignments, group state) and exactly-once commit IDs in `OffsetStore`.
- **Bounded CDC worker pool** (`DefaultCDCWorkers=4`, `DefaultCDCQueueSize=10000`) with non-blocking `Emit` and graceful `Close`.
- **Rewritten retention enforcer** that parses segment headers, protects active segments/system dirs, and removes aged/size-eligible segments plus their `.index` files.
- **Dedicated internal cluster listener** (`InternalGRPCServer`, default `:7947`) carries `ReplicationService` and `RaftService` only — replication traffic is fully isolated from the public API on `:9000`.
- **Replication mTLS** via `replication.MTLSConfig` + `BuildClientTLSConfig` / `BuildServerTLSConfig`; CA-pinned, `tls.RequireAndVerifyClientCert` on the server.
- **Bulk snapshot install** (`ReplicationService.Snapshot` streaming RPC) — segment + sparse-index files with per-file IEEE CRC32, atomic dir swap, `WAL.ReloadSegments()`. Used for new-node bootstrap and follower-wipe recovery.
- **`--snapshot-catchup-threshold`** is defined and exposed (default `10000`); currently invoked only by `PartitionManager.SyncPartitionFromLeader` on node join — automatic lag-driven trigger is not yet wired.
- **Production security requirements**: TLS, auth, encryption at rest, replication mTLS; disabled by the `--dev` flag.

## Start Here

- Composition root and runtime lifecycle: [cmd/api/main.go](../../cmd/api/main.go)
- Full narrative guide: [docs/DEVELOPER_ARCHITECTURE_GUIDE.md](../DEVELOPER_ARCHITECTURE_GUIDE.md)
- Mermaid source diagrams: [docs/mermaid](../mermaid)

## Feature Documents

- API Layer: [features/api.md](features/api.md)
- Audit: [features/audit.md](features/audit.md)
- Auth: [features/auth.md](features/auth.md)
- CDC: [features/cdc.md](features/cdc.md)
- Cluster Control Plane: [features/cluster.md](features/cluster.md)
- Compliance and Retention: [features/compliance.md](features/compliance.md)
- Config and Reload: [features/config.md](features/config.md)
- Consumer Groups: [features/consumer.md](features/consumer.md)
- Dashboard: [features/dashboard.md](features/dashboard.md)
- Deduplication: [features/dedup.md](features/dedup.md)
- Delivery Pipeline: [features/delivery.md](features/delivery.md)
- Partition Runtime: [features/partition.md](features/partition.md)
- Replay Engine: [features/replay.md](features/replay.md)
- Replication: [features/replication.md](features/replication.md)
- Scheduler and Timing Wheel: [features/scheduler.md](features/scheduler.md)
- Schema Registry: [features/schema.md](features/schema.md)
- SLO and Metrics: [features/slo.md](features/slo.md)
- Storage and WAL: [features/storage.md](features/storage.md)
- Tenant Accounting: [features/tenant.md](features/tenant.md)
- Tracing: [features/tracing.md](features/tracing.md)
- Transactions (2PC): [features/tx.md](features/tx.md)

## Recommended Reading Path

1. [features/api.md](features/api.md)
2. [features/partition.md](features/partition.md)
3. [features/storage.md](features/storage.md)
4. [features/scheduler.md](features/scheduler.md)
5. [features/delivery.md](features/delivery.md)
6. [features/consumer.md](features/consumer.md)
7. [features/cluster.md](features/cluster.md)
8. [features/replication.md](features/replication.md)
9. Remaining reliability and governance modules

## Diagram Index

- System overview: [system_overview.mmd](../mermaid/system_overview.mmd)
- Startup lifecycle: [startup_sequence.mmd](../mermaid/startup_sequence.mmd)
- Publish flow: [publish_flow.mmd](../mermaid/publish_flow.mmd)
- Delivery state model: [delivery_state_machine.mmd](../mermaid/delivery_state_machine.mmd)
- Scheduler hot/cold path: [scheduler_hot_cold_flow.mmd](../mermaid/scheduler_hot_cold_flow.mmd)
- WAL lifecycle: [wal_lifecycle.mmd](../mermaid/wal_lifecycle.mmd)
- Dedup decision path: [dedup_decision_flow.mmd](../mermaid/dedup_decision_flow.mmd)
- Cluster rebalance: [cluster_rebalance_flow.mmd](../mermaid/cluster_rebalance_flow.mmd)
- Cross-region replication: [cross_region_replication.mmd](../mermaid/cross_region_replication.mmd)
- Transaction state model: [transaction_state_machine.mmd](../mermaid/transaction_state_machine.mmd)
- Schema validation: [schema_validation_flow.mmd](../mermaid/schema_validation_flow.mmd)
- Observability feedback loop: [observability_feedback_loop.mmd](../mermaid/observability_feedback_loop.mmd)

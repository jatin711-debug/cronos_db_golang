# Config and Reload Architecture

## Purpose

The config module defines defaults, merges runtime settings, and supports safe in-process reload behavior.

## Key Files

- [internal/config/defaults.go](../../../internal/config/defaults.go)
- [internal/config/config.go](../../../internal/config/config.go)
- [internal/config/reload.go](../../../internal/config/reload.go)
- [pkg/types/event.go](../../../pkg/types/event.go)

## Main Flow

1. Startup loads flags and environment overrides into a typed config.
2. System components are constructed from this config in the composition root.
3. Reload wrapper listens for SIGHUP and updates active config references.

## Production Decisions

- `--dev` disables production security requirements and is intended for local development only.
- Production validation requires TLS, auth, encryption at rest, replication mTLS, `--replication-factor>=3`, and `--min-insync-replicas>=2`.
- Environment variables registered in `internal/config/config.go`: `CRONOS_NODE_ID`, `CRONOS_DATA_DIR`, `CRONOS_GRPC_ADDR`, `CRONOS_HTTP_ADDR`, `CRONOS_DEV`, `CRONOS_CLUSTER`, `CRONOS_CLUSTER_SEEDS`, `CRONOS_TLS_ENABLED`, `CRONOS_TLS_CA_FILE`, `CRONOS_TLS_CERT_FILE`, `CRONOS_TLS_KEY_FILE`, `CRONOS_REPLICATION_TLS_ENABLED`, `CRONOS_REPLICATION_TLS_CA_FILE`, `CRONOS_REPLICATION_TLS_CERT_FILE`, `CRONOS_REPLICATION_TLS_KEY_FILE`, `CRONOS_AUTH_ENABLED`, `CRONOS_AUTH_JWT_SECRET`, `CRONOS_MIN_IN_SYNC_REPLICAS`, `CRONOS_SNAPSHOT_CATCHUP_THRESHOLD`, `CRONOS_EXACTLY_ONCE_COMMITS`, `CRONOS_ENCRYPTION_ENABLED`, `CRONOS_ENCRYPTION_KEY_FILE`, `CRONOS_NODE_RACK`, `CRONOS_NODE_ZONE`, `CRONOS_NODE_REGION`, `CRONOS_TRACING_ENABLED`, `CRONOS_TRACING_EXPORTER`, `CRONOS_TRACING_OTLP_ENDPOINT`, `CRONOS_TRACING_SAMPLE_RATIO`, `CRONOS_TRACING_INSECURE`.
- New flags include retention settings, full TLS / auth / encryption / replication-mTLS surface, snapshot install threshold, exact-once commits, follower reads, load shedding, ingest and memory rate limits, and topology labels (`-node-rack/-zone/-region`).
- Helm `config.dev` defaults to `true`; `values-production.yaml` disables dev and requires TLS/auth/encryption/replication-mTLS secrets.
- Strong defaults favor stability and durability (e.g. `DefaultFsyncMode = "batch"`, `DefaultMinInSyncReplicas = 1` with production override ≥2, `DefaultVirtualNodes = 2048`).
- Feature flags gate potentially risky behavior changes (`--follower-reads`, `--exactly-once-commits`, tracing, TLS, auth).
- Reload support avoids full process restart for selected runtime updates.
- **Known configuration gap**: `--snapshot-catchup-threshold` (default `10000`) is defined and exposed but **not yet consumed** by any automatic lag-driven code path. It is currently invoked only by `PartitionManager.SyncPartitionFromLeader` on node join (see [replication.md](replication.md)). Flag is forward-compatible; treat as a documented-but-unwired knob until the automatic trigger lands.

## Debug Pointers

- Unexpected runtime behavior: [internal/config/config.go](../../../internal/config/config.go)
- Missing default assumptions: [internal/config/defaults.go](../../../internal/config/defaults.go)

## Related Diagrams

- [startup_sequence.mmd](../../mermaid/startup_sequence.mmd)
- [system_overview.mmd](../../mermaid/system_overview.mmd)

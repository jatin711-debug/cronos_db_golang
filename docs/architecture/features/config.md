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
- Production validation requires TLS, JWT auth **and** `--auth-policy-file`, encryption at rest, replication mTLS, `--replication-factor>=3`, and `--min-insync-replicas>=2` (unless `--dev`).
- Auth flags: `--auth-enabled`, `--auth-jwt-secret`, `--auth-jwt-public-key`, `--auth-policy-file` (there is **no** `--auth-token` static token flag).
- Key defaults (`internal/config/defaults.go`): fsync `batch`, tick **10ms**, wheel **600**, virtual nodes **2048**, retention max-age **168h**, minISR **1** (prod forces ≥2), snapshot catchup threshold **10000**.
- Environment variables registered in `internal/config/config.go` include `CRONOS_NODE_ID`, `CRONOS_DATA_DIR`, `CRONOS_GRPC_ADDR`, `CRONOS_HTTP_ADDR`, `CRONOS_DEV`, `CRONOS_CLUSTER`, `CRONOS_CLUSTER_SEEDS`, TLS/replication-TLS, auth JWT, minISR, snapshot threshold, encryption, topology, and tracing vars.
- Helm `config.dev` defaults to `true`; `values-production.yaml` disables dev and requires TLS/auth/encryption/replication-mTLS secrets.
- Feature flags gate risky behavior (`--follower-reads`, `--exactly-once-commits`, tracing, TLS, auth).
- SIGHUP reload updates selected safe runtime fields only; data-dir/partition-count/seeds still require restart.
- **Snapshot threshold:** `--snapshot-catchup-threshold` (default `10000`, `0` = disable) is used on join / `SyncPartitionFromLeader`. **Known limitation (documented):** automatic mid-flight lag-driven InstallSnapshot is not wired (connected followers use incremental catch-up only). Full write-up: [replication.md](replication.md), [ARCHITECTURE.md § Known Limitations](../../../ARCHITECTURE.md#known-limitations).

## Debug Pointers

- Unexpected runtime behavior: [internal/config/config.go](../../../internal/config/config.go)
- Missing default assumptions: [internal/config/defaults.go](../../../internal/config/defaults.go)

## Related Diagrams

- [startup_sequence.mmd](../../mermaid/startup_sequence.mmd)
- [system_overview.mmd](../../mermaid/system_overview.mmd)

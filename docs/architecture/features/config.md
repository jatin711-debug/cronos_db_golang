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
- Key defaults (`internal/config/defaults.go`): fsync `batch`, tick **10ms**, wheel **600**, virtual nodes **2048**, retention max-age **168h**, minISR **1** (prod forces ≥2).
- Only a subset of flags has `CRONOS_*` environment overrides (see table below); the rest are flag-only. This matters for container/Helm deployments that configure via env.
- Helm `config.dev` defaults to `true`; `values-production.yaml` disables dev and requires TLS/auth/encryption/replication-mTLS secrets. **Known gap:** the chart's ConfigMap sets env vars the binary does not read (e.g. `CRONOS_PARTITION_COUNT`, `CRONOS_FSYNC_MODE`), and there is no `CRONOS_AUTH_POLICY_FILE` env var, so `values-production.yaml` currently fails validation at boot.
- Feature flags gate risky behavior (`--follower-reads`, `--exactly-once-commits`, tracing, TLS, auth).
- SIGHUP reload updates selected safe runtime fields only; data-dir/partition-count/seeds still require restart. SIGHUP is a no-op on Windows.
- **Snapshot threshold:** `--snapshot-catchup-threshold` (default `10000`) is currently a **dead config key** — parsed but never read anywhere; `SyncPartitionFromLeader` installs snapshots unconditionally. **Known limitation (documented):** automatic mid-flight lag-driven InstallSnapshot is not wired (connected followers use incremental catch-up only). Full write-up: [replication.md](replication.md), [ARCHITECTURE.md § Known Limitations](../../../ARCHITECTURE.md#known-limitations).

## Full Flag Reference

Defaults from `internal/config/defaults.go` unless noted. Flags marked **env** have a `CRONOS_*` override (see next section).

### Core

| Flag | Default | Description |
|------|---------|-------------|
| `--node-id` | *(empty)* | Unique node identifier (**env**) |
| `--data-dir` | `./data` | Data directory for WAL, dedup, offsets, snapshots (**env**) |
| `--grpc-addr` | `:9000` | Public gRPC listen address (**env**) |
| `--http-addr` | `:8080` | HTTP health/metrics/dashboard address (**env**) |
| `--partition-count` | `1` | Number of partitions (standalone mode) |
| `--replication-factor` | `1` | Replicas per partition (prod requires ≥3) |
| `--dev` | `false` | Developer mode: bypasses production validation (**env**) |

### Storage

| Flag | Default | Description |
|------|---------|-------------|
| `--segment-size` | `536870912` (512MB) | WAL segment size before rotation |
| `--index-interval` | `1000` | Sparse index interval (events per entry) |
| `--fsync-mode` | `batch` | `every_event` \| `batch` \| `periodic` |
| `--flush-interval` | `1000` | Background flush interval (ms) for batch/periodic fsync |
| `--retention-max-age-hours` | `168` | Delete WAL segments older than this (0 = disable) |
| `--retention-max-size-gb` | `0` | Size-based WAL retention in GB (0 = disable) |

### Scheduler

| Flag | Default | Description |
|------|---------|-------------|
| `--tick-ms` | `10` | Timing wheel tick duration (ms) |
| `--wheel-size` | `600` | Slots per timing wheel level |
| `--hot-window-minutes` | `60` | Events beyond this window go to cold store (0 = disable) |
| `--hydrator-min-interval` | `5000` | Minimum adaptive hydrator scan interval (ms) |
| `--hydrator-max-interval` | `300000` | Maximum adaptive hydrator scan interval (ms) |

### Delivery & Admission

| Flag | Default | Description |
|------|---------|-------------|
| `--max-ready-queue` | `1000000` | Max ready-queue depth per partition |
| `--max-timing-wheel-size` | `10000000` | Max active timers in the hot timing wheel |
| `--max-in-flight` | `500000` | Max in-flight deliveries per partition |
| `--ack-timeout` | `30s` | Delivery ack deadline (**reload-only env**) |
| `--max-retries` | `5` | Max delivery retries before DLQ (**reload-only env**) |
| `--retry-backoff` | `1s` | Base retry backoff |
| `--max-credits` | `1000` | Per-subscription credit budget (**reload-only env**) |
| `--cb-failure-threshold` | `0.5` | Circuit breaker failure rate to trip (**reload-only env**) |
| `--cb-min-attempts` | `10` | Min attempts before the breaker evaluates |
| `--cb-open-duration-ms` | `30000` | Circuit breaker open duration (ms) |

### Dedup

| Flag | Default | Description |
|------|---------|-------------|
| `--dedup-ttl` | `168` | Dedup claim retention (hours) |
| `--bloom-capacity` | `100000000` | Bloom filter capacity per partition |

### Replication & Cluster

| Flag | Default | Description |
|------|---------|-------------|
| `--replication-batch` | `100` | Events per replication batch |
| `--replication-timeout` | `10s` | Replication RPC timeout |
| `--min-insync-replicas` | `1` | Min ISR **including leader** to ack a write (prod ≥2) (**env**) |
| `--snapshot-catchup-threshold` | `10000` | **Dead config key** — parsed, never read (**env**) |
| `--cluster` | `false` | Enable cluster mode (**env**) |
| `--cluster-seeds` | *(empty)* | Comma-separated seed gossip addresses (**env**) |
| `--cluster-gossip-addr` | `:7946` | Membership gossip listen address |
| `--cluster-grpc-addr` | `:7947` | Internal gRPC (replication/raft/cross-region) address |
| `--cluster-raft-addr` | `:7948` | Raft transport listen address |
| `--raft-dir` | `./raft` | Raft log/state directory |
| `--raft-join` | *(empty)* | Raft cluster join address |
| `--virtual-nodes` | `2048` | Virtual nodes per physical node on the hash ring |
| `--heartbeat-interval` | `1s` | Membership heartbeat period |
| `--failure-timeout` | `5s` | Heartbeat silence before marking a node failed |
| `--suspect-timeout` | `3s` | Time a node stays suspect before failure |
| `--use-memberlist` | `false` | Use HashiCorp Memberlist (SWIM) instead of custom TCP gossip |
| `--clock-skew-threshold-ms` | `5000` | Max allowed clock skew from leader (0 = disabled) |
| `--node-rack` | *(empty)* | Rack/AZ label for topology-aware placement (**env**) |
| `--node-zone` | *(empty)* | Zone label (**env**) |
| `--node-region` | *(empty)* | Region label (**env**) |

### Security

| Flag | Default | Description |
|------|---------|-------------|
| `--tls-enabled` | `false` | TLS for the public gRPC listener (**env**) |
| `--tls-ca-file` | *(empty)* | CA certificate path (**env**) |
| `--tls-cert-file` | *(empty)* | Server certificate path (**env**) |
| `--tls-key-file` | *(empty)* | Server private key path (**env**) |
| `--tls-client-auth` | `false` | Require client certificates (mTLS) |
| `--replication-tls-enabled` | `false` | mTLS for internal replication traffic (**env**) |
| `--replication-tls-ca-file` | *(empty)* | Internal CA path (**env**) |
| `--replication-tls-cert-file` | *(empty)* | Internal certificate path (**env**) |
| `--replication-tls-key-file` | *(empty)* | Internal private key path (**env**) |
| `--auth-enabled` | `false` | Enable JWT authentication (**env**) |
| `--auth-jwt-secret` | *(empty)* | HMAC secret (visible in `ps`; prefer the env var) (**env**) |
| `--auth-jwt-public-key` | *(empty)* | Ed25519/RSA public key file for asymmetric JWTs |
| `--auth-policy-file` | *(empty)* | RBAC policy JSON (required in production) — **flag-only, no env var** |
| `--encryption-enabled` | `false` | AES-256-GCM encryption at rest for WAL segments (**env**) |
| `--encryption-key-file` | *(empty)* | Path to 32-byte master key file (**env**) |

### Guardrails

| Flag | Default | Description |
|------|---------|-------------|
| `--load-shedding-threshold` | `0.0` | Load shedding threshold 0.0–1.0 (0 = disabled) (**reload-only env**) |
| `--topic-rate-limit` | `0.0` | Per-subject per-topic events/sec (0 = disabled) |
| `--topic-rate-burst` | `0.0` | Topic rate limiter burst (0 = disabled) |
| `--max-memory-percent` | `0.0` | Reject publishes above this process RSS ratio (0 = disabled) |
| `--memory-check-interval` | `5000` | Memory sampling interval (ms) |
| `--max-ingest-rate` | `0` | Max events/sec per partition (0 = unlimited) |
| `--ingest-burst-size` | `0` | Ingest token-bucket burst size |
| `--follower-reads` | `false` | Allow followers to serve Replay reads |
| `--exactly-once-commits` | `false` | Exactly-once consumer offset commits (**env**) |

### Tracing

| Flag | Default | Description |
|------|---------|-------------|
| `--tracing-enabled` | `false` | Enable OpenTelemetry tracing (**env**) |
| `--tracing-exporter` | `none` | `none` \| `stdout` \| `otlp` (**env**) |
| `--tracing-otlp-endpoint` | `127.0.0.1:4317` | OTLP gRPC endpoint (**env**) |
| `--tracing-sample-ratio` | `0.01` | Sampling ratio 0.0–1.0 (**env** + **reload-only env**) |
| `--tracing-insecure` | `true` | Plaintext OTLP (no TLS) (**env**) |

## Environment Variables

Registered in `internal/config/config.go` (startup): `CRONOS_NODE_ID`,
`CRONOS_DATA_DIR`, `CRONOS_GRPC_ADDR`, `CRONOS_HTTP_ADDR`, `CRONOS_DEV`,
`CRONOS_CLUSTER`, `CRONOS_CLUSTER_SEEDS`, `CRONOS_TLS_ENABLED`,
`CRONOS_TLS_CA_FILE`, `CRONOS_TLS_CERT_FILE`, `CRONOS_TLS_KEY_FILE`,
`CRONOS_REPLICATION_TLS_ENABLED`, `CRONOS_REPLICATION_TLS_CA_FILE`,
`CRONOS_REPLICATION_TLS_CERT_FILE`, `CRONOS_REPLICATION_TLS_KEY_FILE`,
`CRONOS_AUTH_ENABLED`, `CRONOS_AUTH_JWT_SECRET`,
`CRONOS_MIN_IN_SYNC_REPLICAS`, `CRONOS_SNAPSHOT_CATCHUP_THRESHOLD`,
`CRONOS_EXACTLY_ONCE_COMMITS`, `CRONOS_ENCRYPTION_ENABLED`,
`CRONOS_ENCRYPTION_KEY_FILE`, `CRONOS_NODE_RACK`, `CRONOS_NODE_ZONE`,
`CRONOS_NODE_REGION`, `CRONOS_TRACING_ENABLED`, `CRONOS_TRACING_EXPORTER`,
`CRONOS_TRACING_OTLP_ENDPOINT`, `CRONOS_TRACING_SAMPLE_RATIO`,
`CRONOS_TRACING_INSECURE`.

Reload-only (SIGHUP, `internal/config/reload.go`): `CRONOS_MAX_CREDITS`,
`CRONOS_ACK_TIMEOUT`, `CRONOS_MAX_RETRIES`, `CRONOS_LOAD_SHEDDING_THRESHOLD`,
`CRONOS_CB_FAILURE_THRESHOLD`, `CRONOS_TRACING_SAMPLE_RATIO`,
`CRONOS_STATS_PRINT_INTERVAL_MS`.

CDC sinks (read by the composition root, `cmd/api/main.go`):
`CRONOS_CDC_KAFKA_BROKERS`, `CRONOS_CDC_KAFKA_TOPIC`, `CRONOS_CDC_WEBHOOK_URL`.

## Debug Pointers

- Unexpected runtime behavior: [internal/config/config.go](../../../internal/config/config.go)
- Missing default assumptions: [internal/config/defaults.go](../../../internal/config/defaults.go)

## Related Diagrams

- [Startup lifecycle](../../DEVELOPER_ARCHITECTURE_GUIDE.md#42-startup-and-shutdown-sequence)
- [System overview](../README.md#system-overview)

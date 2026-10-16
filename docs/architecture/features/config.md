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
- Production validation (`ValidateConfig`, unless `--dev`) requires TLS, JWT auth **and** `--auth-policy-file`, encryption at rest with a key file, replication mTLS, `--replication-factor>=3`, `--min-insync-replicas>=2`, `--fsync-mode` `batch` or `every_event`, and `--cluster-seeds` when `--cluster` is set. It refuses `--experimental-features`, `--exactly-once-commits`, and a `--pprof-addr` that is not a loopback address.
- Auth flags: `--auth-enabled`, `--auth-jwt-secret`, `--auth-jwt-public-key`, `--auth-policy-file` (there is **no** `--auth-token` static token flag).
- Key defaults (`internal/config/defaults.go`): fsync `batch`, tick **10ms**, wheel **600**, virtual nodes **2048**, retention max-age **168h**, minISR **1** (prod forces ≥2).
- Every flag can also be set by an environment variable `CRONOS_<FLAG>` (flag name upper-cased, `-` replaced by `_`); an explicit command-line flag takes precedence. See [Environment Variables](#environment-variables).
- Helm `config.dev` defaults to `true`; `values-production.yaml` disables dev and requires TLS/auth/encryption/replication-mTLS secrets. The chart configures the binary through `CRONOS_*` variables (ConfigMap and StatefulSet), including `CRONOS_AUTH_POLICY_FILE`.
- Feature flags gate behavior that is off by default (`--follower-reads`, tracing, TLS, auth). `--experimental-features` enables transactions, online partition splitting and cross-region replication; it requires `--dev`. `--exactly-once-commits` is also refused in production.
- SIGHUP reload updates selected safe runtime fields only; data-dir/partition-count/seeds still require restart. SIGHUP is a no-op on Windows.
- **Snapshot threshold:** `--snapshot-catchup-threshold` (default `10000`) is currently a **dead config key** — parsed but never read anywhere; `SyncPartitionFromLeader` installs snapshots unconditionally. **Known limitation (documented):** automatic mid-flight lag-driven InstallSnapshot is not wired (connected followers use incremental catch-up only). Full write-up: [replication.md](replication.md), [ARCHITECTURE.md § Known Limitations](../../../ARCHITECTURE.md#known-limitations).

## Full Flag Reference

Defaults from `internal/config/defaults.go` unless noted. Every flag has a `CRONOS_*` environment variable (see [Environment Variables](#environment-variables)). Flags marked **reload-only env** are also re-read from the environment on SIGHUP.

### Core

| Flag | Default | Description |
|------|---------|-------------|
| `--node-id` | *(empty)* | Unique node identifier |
| `--data-dir` | `./data` | Data directory for WAL, dedup, offsets, snapshots |
| `--grpc-addr` | `:9000` | Public gRPC listen address |
| `--http-addr` | `:8080` | HTTP health/metrics/dashboard address |
| `--partition-count` | `1` | Number of partitions (standalone mode) |
| `--replication-factor` | `1` | Replicas per partition (prod requires ≥3) |
| `--dev` | `false` | Developer mode: bypasses production validation |
| `--experimental-features` | `false` | Enables transactions, online partition splitting and cross-region replication. Requires `--dev`; refused in production |

### Storage

| Flag | Default | Description |
|------|---------|-------------|
| `--segment-size` | `536870912` (512MB) | WAL segment size before rotation |
| `--index-interval` | `1000` | Sparse index interval (events per entry) |
| `--fsync-mode` | `batch` | `every_event` \| `batch` \| `periodic`. Production mode refuses `periodic`: it acknowledges before the data is on disk |
| `--flush-interval` | `1000` | Background flush interval (ms) for batch/periodic fsync |
| `--compaction-interval` | `10m` | How often a partition looks for log segments whose events are all finished and removes them |
| `--backup-interval` | `1h` | How often the node backs up its partitions, at wall-clock multiples of the interval. `0` = no scheduled backups |
| `--backup-retention` | `168h` | How long a backup is kept. `0` = for good |
| `--backup-dir` | *(empty)* | Where backups go; default `backups` under `--data-dir` |
| `--retention-max-age-hours` | `168` | Delete WAL segments older than this (0 = disable) |
| `--retention-max-size-gb` | `0` | Size-based WAL retention in GB (0 = disable) |

### Scheduler

| Flag | Default | Description |
|------|---------|-------------|
| `--tick-ms` | `10` | Timing wheel tick duration (ms) |
| `--wheel-size` | `600` | Slots per timing wheel level |
| `--hot-window-minutes` | `60` | Events beyond this window go to cold store (0 disables time-based routing; memory overflow still spills) |
| `--hydrator-min-interval` | `5000` | Minimum adaptive hydrator scan interval (ms) |
| `--hydrator-max-interval` | `300000` | Maximum adaptive hydrator scan interval (ms) |

### Delivery & Admission

| Flag | Default | Description |
|------|---------|-------------|
| `--max-ready-queue` | `1000000` | Max ready-queue depth per partition |
| `--max-timing-wheel-size` | `10000000` | Max active timers in the hot timing wheel |
| `--max-in-flight` | `500000` | Max in-flight deliveries per partition |
| `--max-disk-usage` | `0.95` | Refuse publishes once the filesystem holding `--data-dir` is this full (fraction, `0` = never); they are taken again two points below. Deliveries and retention continue; followers refuse log growth but may acknowledge entries already stored |
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
| `--dedup-ttl` | `168` | Dedup claim retention (hours). Env: `CRONOS_DEDUP_TTL_HOURS` |
| `--bloom-capacity` | `100000000` | Bloom filter capacity per partition |

### Replication & Cluster

| Flag | Default | Description |
|------|---------|-------------|
| `--replication-batch` | `100` | Events per replication batch |
| `--replication-timeout` | `10s` | Replication RPC timeout |
| `--min-insync-replicas` | `1` | Min ISR **including leader** to ack a write (prod ≥2). Env: `CRONOS_MIN_IN_SYNC_REPLICAS` |
| `--snapshot-catchup-threshold` | `10000` | **Dead config key** — parsed, never read |
| `--cluster` | `false` | Enable cluster mode |
| `--cluster-seeds` | *(empty)* | Comma-separated membership addresses of the cluster's nodes. Name every node, on every node; production mode requires it |
| `--cluster-bootstrap` | `false` | This node creates the cluster, if it has no state and no other node in `--cluster-seeds` belongs to one. Give it to exactly one node |
| `--cluster-expected-nodes` | `0` | Nodes a new cluster starts with; first partition leaders are assigned as soon as that many are up. `0` = unknown |
| `--cluster-formation-wait` | `5s` | When the expected nodes are unknown or not all up: how long membership must be unchanged before first leaders are assigned. `0` assigns at once |
| `--cluster-gossip-addr` | `:7946` | Membership gossip listen address |
| `--cluster-grpc-addr` | `:7947` | Internal gRPC (replication/raft/cross-region) address |
| `--cluster-raft-addr` | `:7948` | Raft transport listen address |
| `--raft-dir` | `./raft` | Raft log/state directory |
| `--raft-join` | *(empty)* | Raft cluster join address |
| `--virtual-nodes` | `2048` | Virtual nodes per physical node on the hash ring |
| `--heartbeat-interval` | `1s` | Membership heartbeat period |
| `--failure-timeout` | `5s` | Heartbeat silence before marking a node failed |
| `--suspect-timeout` | `3s` | Time a node stays suspect before failure |
| `--use-memberlist` | `false` | Not supported; the server refuses to start with it |
| `--clock-skew-threshold-ms` | `5000` | Max allowed clock skew from leader (0 = disabled) |
| `--node-rack` | *(empty)* | Rack/AZ label for topology-aware placement |
| `--node-zone` | *(empty)* | Zone label |
| `--node-region` | *(empty)* | Region label |

### Security

| Flag | Default | Description |
|------|---------|-------------|
| `--tls-enabled` | `false` | TLS for the public gRPC listener |
| `--tls-ca-file` | *(empty)* | CA certificate path |
| `--tls-cert-file` | *(empty)* | Server certificate path |
| `--tls-key-file` | *(empty)* | Server private key path |
| `--tls-client-auth` | `false` | Require client certificates (mTLS) |
| `--http-tls-enabled` | `false` | Serve the HTTP port (health, metrics, admin API, dashboard) over TLS, with `--tls-cert-file` and `--tls-key-file`. Callers are not asked for a certificate |
| `--replication-tls-enabled` | `false` | Mutual TLS for all traffic between nodes: replication, membership and Raft. Requires the three files below |
| `--replication-tls-ca-file` | *(empty)* | CA that signed the nodes' certificates |
| `--replication-tls-cert-file` | *(empty)* | This node's certificate, for server and client use |
| `--replication-tls-key-file` | *(empty)* | Private key of that certificate |
| `--auth-enabled` | `false` | Enable JWT authentication |
| `--auth-jwt-secret` | *(empty)* | HMAC secret (visible in `ps`; prefer the env var) |
| `--auth-jwt-public-key` | *(empty)* | Ed25519/RSA public key file for asymmetric JWTs |
| `--auth-policy-file` | *(empty)* | RBAC policy JSON (required in production) |
| `--encryption-enabled` | `false` | AES-256-GCM encryption at rest for WAL segments |
| `--encryption-key-file` | *(empty)* | Path to 32-byte master key file |

### Guardrails

| Flag | Default | Description |
|------|---------|-------------|
| `--load-shedding-threshold` | `0.0` | Load shedding threshold 0.0–1.0 (0 = disabled) (**reload-only env**) |
| `--topic-rate-limit` | `0.0` | Per-subject per-topic events/sec (0 = disabled) |
| `--topic-rate-burst` | `0.0` | Topic rate limiter burst (0 = disabled) |
| `--max-memory-percent` | `80` | Publishes are refused while the process holds this share of its memory limit or more (0 = never) |
| `--scheduler-memory` | `67108864` | Node-wide scheduler payload budget in bytes; minimum 32 MiB. Excess events wait as WAL references. Excludes other process memory |
| `--memory-limit` | `0` | Memory the process may use, in bytes. `0` = the container's (cgroup) limit, or the machine's memory when there is none |
| `--memory-check-interval` | `1000` | How long a memory measurement is used (ms) |
| `--max-ingest-rate` | `0` | Max events/sec per partition (0 = unlimited) |
| `--ingest-burst-size` | `0` | Ingest token-bucket burst size |
| `--follower-reads` | `false` | Allow followers to serve Replay reads |
| `--exactly-once-commits` | `false` | Exactly-once consumer offset commits. Refused in production |

### Tracing

| Flag | Default | Description |
|------|---------|-------------|
| `--tracing-enabled` | `false` | Enable OpenTelemetry tracing |
| `--tracing-exporter` | `none` | `none` \| `stdout` \| `otlp` |
| `--tracing-otlp-endpoint` | `127.0.0.1:4317` | OTLP gRPC endpoint |
| `--tracing-sample-ratio` | `0.01` | Sampling ratio 0.0–1.0 (**reload-only env**) |
| `--tracing-insecure` | `true` | Plaintext OTLP (no TLS) |

### Profiling

| Flag | Default | Description |
|------|---------|-------------|
| `--pprof-addr` | *(empty)* | Serve `net/http/pprof` runtime profiles on this address; empty disables. Production mode requires a loopback address |

## Environment Variables

At startup (`LoadConfig` in `internal/config/config.go`) every registered flag
reads the environment variable `CRONOS_` + the flag name upper-cased with `-`
replaced by `_` (for example `--fsync-mode` reads `CRONOS_FSYNC_MODE`,
`--auth-policy-file` reads `CRONOS_AUTH_POLICY_FILE`). Two flags use a different
name: `--dedup-ttl` reads `CRONOS_DEDUP_TTL_HOURS` and `--min-insync-replicas`
reads `CRONOS_MIN_IN_SYNC_REPLICAS`. Environment values are applied before the
command line is parsed, so an explicit flag wins. An invalid value fails startup.

Reload-only (SIGHUP, `internal/config/reload.go`): `CRONOS_MAX_CREDITS`,
`CRONOS_ACK_TIMEOUT`, `CRONOS_MAX_RETRIES`, `CRONOS_LOAD_SHEDDING_THRESHOLD`,
`CRONOS_CB_FAILURE_THRESHOLD`, `CRONOS_TRACING_SAMPLE_RATIO`,
`CRONOS_STATS_PRINT_INTERVAL_MS`.

CDC sinks (read by the composition root, `cmd/api/main.go`):
`CRONOS_CDC_KAFKA_BROKERS`, `CRONOS_CDC_KAFKA_TOPIC`, `CRONOS_CDC_WEBHOOK_URL`.

Cross-region replication (experimental, `cmd/api/main.go`): `CRONOS_REGIONS`.
The server refuses to start when it is set without `--dev --experimental-features`.

## Debug Pointers

- Unexpected runtime behavior: [internal/config/config.go](../../../internal/config/config.go)
- Missing default assumptions: [internal/config/defaults.go](../../../internal/config/defaults.go)

## Related Diagrams

- [Startup lifecycle](../../DEVELOPER_ARCHITECTURE_GUIDE.md#42-startup-and-shutdown-sequence)
- [System overview](../README.md#system-overview)

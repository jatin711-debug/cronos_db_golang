# Server configuration

Use this page to choose server settings. For Go application settings, see the
[SDK guide](CLIENT_GUIDE.md#sdk-settings); for operator commands, see
[administration](ADMINISTRATION.md). The defaults below are the binary's
defaults. Docker Compose and Helm supply their own values.

## Set a value

Every flag has an environment variable. Defaults are loaded first, then
nonempty environment values, then explicit command-line flags. A flag wins
over its environment variable. Invalid environment values fail startup even
if a flag would override them; an empty environment value keeps the default.

These Bash examples start the same local development node:

```bash
cronos-api --dev --node-id=local --ack-timeout=45s
CRONOS_DEV=true CRONOS_NODE_ID=local CRONOS_ACK_TIMEOUT=45s cronos-api
```

Durations take a unit, such as `250ms`, `30s`, `10m` or `1h`. Flags ending in
`-ms` and the interval flags whose tables say milliseconds take an integer
instead. Byte counts take integers, such as `67108864` for 64 MiB. Booleans
take `true` or `false`. Use `--flag=false` to turn a boolean off explicitly.

Most environment names are `CRONOS_` followed by the uppercase flag name with
dashes replaced by underscores. Two differ: `--dedup-ttl` uses
`CRONOS_DEDUP_TTL_HOURS`, and `--min-insync-replicas` uses
`CRONOS_MIN_IN_SYNC_REPLICAS`. Every exact name is in the tables below.

## Choose a deployment

For local development, `--dev --node-id=local` is enough. It bypasses the
production startup requirements. It does not turn off security settings you
explicitly enable.

For production, use the [deployment requirements](PRODUCTION_RELEASE.md)
and [Docker guide](../DOCKER.md#production-considerations). Startup requires
TLS, JWT authentication with a policy file, WAL encryption with a key file,
mutual TLS between nodes, at least three replicas, at least two write
acknowledgements, and `batch` or `every_event` fsync. Cluster nodes also need
seeds. Experimental features, exactly-once commits, and a profiling listener
outside loopback are refused. Passing validation alone does not create a
working cluster: configure `--cluster`, reachable node addresses, certificates
and persistent storage as described in the deployment guide.

Start with the defaults and change a setting for a measured need. For slow
handlers, increase the acknowledgement timeout. For memory pressure, inspect
the scheduler budget and admission limits. For disk pressure, inspect
retention and unfinished events before shortening retention limits. Limits
that refuse new work let clients retry; they do not delete accepted events.

## Node and listeners

| Flag | Environment variable | Default | What it does and when to change it |
|---|---|---|---|
| `--node-id` | `CRONOS_NODE_ID` | required | Stable, unique identity of this node. Keep it with the node's persistent data; give each node a different ID. |
| `--data-dir` | `CRONOS_DATA_DIR` | `./data` | Root for partition logs, offsets and other node data. Mount persistent storage here. |
| `--grpc-addr` | `CRONOS_GRPC_ADDR` | `:9000` | Public gRPC listen address for applications and the admin CLI. `:9000` listens on all interfaces. |
| `--http-addr` | `CRONOS_HTTP_ADDR` | `:8080` | Health, metrics, dashboard and HTTP admin API listen address. Use HTTP TLS below when this port needs TLS. |
| `--partition-count` | `CRONOS_PARTITION_COUNT` | `1` | Number of logical partitions. Partitions let work run in parallel; more partitions also need more memory and files. Keep the cluster's nodes consistent. |
| `--replication-factor` | `CRONOS_REPLICATION_FACTOR` | `1` | Copies of each partition, including its leader. Production requires at least 3; also choose the write acknowledgement count below. |
| `--dev` | `CRONOS_DEV` | `false` | Bypass production startup requirements for local tests. Use `true` only for development. |
| `--experimental-features` | `CRONOS_EXPERIMENTAL_FEATURES` | `false` | Expose unverified transactions, online partition splitting and cross-region replication. Requires `--dev`; incompatible with event lifecycle mode. |

## Storage, retention and backups

Retention proposes old closed log segments for removal. Completion,
replication and lifecycle safety checks can keep a segment longer. An event
scheduled far into the future can pin a segment. These are cleanup targets,
not guaranteed disk quotas. See the [retention contract](RETENTION_AND_EVENT_LIFECYCLE.md).

| Flag | Environment variable | Default | What it does and when to change it |
|---|---|---|---|
| `--segment-size` | `CRONOS_SEGMENT_SIZE` | `536870912` | WAL segment size in bytes before rotation (512 MiB). Smaller segments allow finer cleanup at the cost of more files. |
| `--index-interval` | `CRONOS_INDEX_INTERVAL` | `1000` | Events between sparse log index entries. A smaller value speeds seeking at the cost of a larger index. |
| `--fsync-mode` | `CRONOS_FSYNC_MODE` | `batch` | `batch` groups disk syncs and waits for durability; `every_event` syncs each event. `periodic` can acknowledge before disk sync and is refused in production. |
| `--flush-interval` | `CRONOS_FLUSH_INTERVAL` | `1000` | Background WAL flush interval in milliseconds. Must be positive. In periodic mode this affects the unsynced-data window. |
| `--compaction-interval` | `CRONOS_COMPACTION_INTERVAL` | `10m` | How often each partition removes closed segments whose events are finished. `0` disables scheduled compaction; an admin command can still run it. |
| `--retention-max-age-hours` | `CRONOS_RETENTION_MAX_AGE_HOURS` | `168` | Age in hours after which closed segments may be removed, subject to safety checks. `0` disables the age target. |
| `--retention-max-size-gb` | `CRONOS_RETENTION_MAX_SIZE_GB` | `0` | Size target for WAL cleanup in GiB. `0` disables it; pinned work can keep usage above the target. |
| `--event-lifecycle` | `CRONOS_EVENT_LIFECYCLE` | `false` | Enable inspecting, canceling and rescheduling future events. Standalone requires RF=1/minISR=1; a cluster requires majority acknowledgements and capable replicas/voters. Incompatible with CDC sinks and experimental features. Activation changes the persistent format; turning it off is not a supported downgrade. |
| `--wal-relocation-bytes` | `CRONOS_WAL_RELOCATION_BYTES` | `0` | Encoded future-event bytes that a retention pass may copy forward to release pinned old segments. Requires lifecycle mode. `0` disables copying. |
| `--backup-interval` | `CRONOS_BACKUP_INTERVAL` | `1h` | Back up partitions at wall-clock multiples of this duration. `0` disables scheduled backups. Choose an interval that matches how much recovery work you can tolerate. |
| `--backup-retention` | `CRONOS_BACKUP_RETENTION` | `168h` | How long backup generations are kept. `0` keeps them indefinitely, so monitor free space. |
| `--backup-dir` | `CRONOS_BACKUP_DIR` | empty | Backup destination; empty uses `backups` under the data directory. Use a separate volume/failure domain to survive loss of the data volume. |

## Scheduling

The hot timing wheel holds near-term work in memory. Distant events and
memory overflow wait on disk, and the hydrator brings them back toward their
delivery time. Reducing the tick improves timing granularity but increases
scheduler activity; it does not guarantee an exact delivery deadline.

| Flag | Environment variable | Default | What it does and when to change it |
|---|---|---|---|
| `--tick-ms` | `CRONOS_TICK_MS` | `10` | Scheduler tick in milliseconds. Use a smaller tick only when measurements show a timing benefit. |
| `--wheel-size` | `CRONOS_WHEEL_SIZE` | `600` | Slots per timing-wheel level. Changes how times are spread through the wheel; normally keep the default. |
| `--hot-window-minutes` | `CRONOS_HOT_WINDOW_MINUTES` | `60` | Keep events due beyond this many minutes in the cold store. `0` disables time-based spill; the memory budget can still spill events. |
| `--hydrator-min-interval` | `CRONOS_HYDRATOR_MIN_INTERVAL` | `5000` | Shortest adaptive cold-store scan interval, in milliseconds. Lower values scan more often. |
| `--hydrator-max-interval` | `CRONOS_HYDRATOR_MAX_INTERVAL` | `300000` | Longest adaptive cold-store scan interval, in milliseconds. Keep it at least as large as the minimum. |

## Delivery and acknowledgement

These settings govern server-to-consumer delivery. They differ from the
SDK producer's retries of a failed publish. An acknowledgement timeout or
failed handler can cause redelivery, so handlers must tolerate duplicates.
Zero values for timeout, retries, credits, backoff and breaker settings fall
back to dispatcher defaults; they do not disable those mechanisms.

| Flag | Environment variable | Default | What it does and when to change it |
|---|---|---|---|
| `--ack-timeout` | `CRONOS_ACK_TIMEOUT` | `30s` | Time a delivery may remain unacknowledged before retry. Allow enough time for normal handler work; longer timeouts delay recovery after a consumer stops. |
| `--max-retries` | `CRONOS_MAX_RETRIES` | `5` | Delivery attempt limit before dead-lettering (the initial attempt counts). Increase only when failures are expected to recover. |
| `--retry-backoff` | `CRONOS_RETRY_BACKOFF` | `1s` | Base delay before redelivery, multiplied by the attempt number. Increase to avoid repeatedly hitting a failing dependency. |
| `--max-credits` | `CRONOS_MAX_CREDITS` | `1000` | Maximum unacknowledged deliveries for a subscription that does not request its own limit. A requested buffer size wins, capped at 50000. The Go SDK requests 1024 by default. |
| `--max-consumer-groups` | `CRONOS_MAX_CONSUMER_GROUPS` | `10000` | Consumer groups a partition holds at most. Subscribe creates a group for any new name and groups are kept with their progress, so a subscription for a new group beyond the limit is refused with `RESOURCE_EXHAUSTED`. Groups that exist are kept, and a negative value removes the limit. Group names are limited to 255 bytes. |
| `--cb-failure-threshold` | `CRONOS_CB_FAILURE_THRESHOLD` | `0.5` | Delivery failure fraction at which the server's breaker pauses a failing subscription. `0.5` means 50 percent. Distinct from the SDK's node breaker. |
| `--cb-min-attempts` | `CRONOS_CB_MIN_ATTEMPTS` | `10` | Minimum delivery samples before evaluating that fraction. A larger value avoids reacting to a few early failures. |
| `--cb-open-duration-ms` | `CRONOS_CB_OPEN_DURATION_MS` | `30000` | Milliseconds the delivery breaker stays open before probing for recovery. |

## Admission, memory and rate limits

These limits refuse new publishes when the node is busy. Set client retry
backoff so rejected work does not immediately overwhelm it again. Counts are
per partition unless the table says otherwise. The scheduler budget covers
payloads held by the scheduler, not the whole process.

| Flag | Environment variable | Default | What it does and when to change it |
|---|---|---|---|
| `--max-ready-queue` | `CRONOS_MAX_READY_QUEUE` | `1000000` | Refuse publishes that would exceed this many ready events. `0` disables this admission check. Increase only when consumers can drain the backlog. |
| `--max-timing-wheel-size` | `CRONOS_MAX_TIMING_WHEEL_SIZE` | `10000000` | Refuse publishes that would exceed this many active hot timers. `0` disables this check. |
| `--max-in-flight` | `CRONOS_MAX_IN_FLIGHT` | `500000` | Admission limit based on active, unacknowledged deliveries. `0` disables this check; subscription credits remain separate. |
| `--load-shedding-threshold` | `CRONOS_LOAD_SHEDDING_THRESHOLD` | `0` | Refuse new work earlier, at this fraction of `--max-ready-queue`; `0.8` means 80 percent. `0` disables early shedding. Requires a positive ready-queue limit. |
| `--max-disk-usage` | `CRONOS_MAX_DISK_USAGE` | `0.95` | Refuse publishes once the data filesystem is 95 percent full; resume two percentage points below. Accepts `0` (disabled) or `0.05` to `1`. Delivery and cleanup continue; followers refuse growth but can acknowledge data already stored. |
| `--max-memory-percent` | `CRONOS_MAX_MEMORY_PERCENT` | `80` | Refuse publishes at this process memory usage as a percentage of its memory limit. `0` disables it. Unlike disk usage, this takes `80`, not `0.8`. |
| `--memory-limit` | `CRONOS_MEMORY_LIMIT` | `0` | Memory limit in bytes used by admission. `0` detects the container's cgroup limit or falls back to machine memory. Set it explicitly when detection does not match your deployment. |
| `--memory-check-interval` | `CRONOS_MEMORY_CHECK_INTERVAL` | `1000` | Milliseconds a memory measurement may be reused. A shorter interval responds sooner at extra measurement cost. |
| `--scheduler-memory` | `CRONOS_SCHEDULER_MEMORY` | `67108864` | Node-wide scheduler payload budget in bytes (64 MiB). Minimum 32 MiB; `0` uses the default. Overflow waits as log references. Leave headroom for other memory. |
| `--max-ingest-rate` | `CRONOS_MAX_INGEST_RATE` | `0` | Sustained publish events per second per partition. Requires a positive ingest burst size too; `0` disables this limiter. |
| `--ingest-burst-size` | `CRONOS_INGEST_BURST_SIZE` | `0` | Token-bucket capacity for ingest bursts. Both this and the rate must be positive to enable limiting. |
| `--topic-rate-limit` | `CRONOS_TOPIC_RATE_LIMIT` | `0` | Events per second per JWT subject and topic, with authentication enabled. `0` disables this limit. Use with a positive burst size. |
| `--topic-rate-burst` | `CRONOS_TOPIC_RATE_BURST` | `0` | Token-bucket capacity for that topic limit; nonpositive rate or burst disables the topic limiter. |

## Duplicate detection and replay

| Flag | Environment variable | Default | What it does and when to change it |
|---|---|---|---|
| `--dedup-ttl` | `CRONOS_DEDUP_TTL_HOURS` | `168` | Hours to retain message-ID deduplication records. Cover your longest publish retry/backfill window. After expiration, a repeated ID can be accepted again. |
| `--bloom-capacity` | `CRONOS_BLOOM_CAPACITY` | `100000000` | Expected IDs per partition for the Bloom filter that avoids disk lookups. Larger capacity uses more memory; the durable dedup store remains authoritative. |
| `--follower-reads` | `CRONOS_FOLLOWER_READS` | `false` | Allow followers to serve replay reads. Their view can lag the leader; leave off when replay must use the leader. |
| `--exactly-once-commits` | `CRONOS_EXACTLY_ONCE_COMMITS` | `false` | Experimental commit-ID storage. Refused in production and unused by the normal Ack path; enabling it does not provide end-to-end exactly-once delivery. |

## Cluster and replication

Applications connect to the public gRPC port. Nodes use membership, internal
gRPC and Raft addresses to communicate with each other. Make those addresses
reachable by the other nodes, and put the corresponding DNS names in the
certificates. Changing the replication factor does not create missing nodes.

| Flag | Environment variable | Default | What it does and when to change it |
|---|---|---|---|
| `--cluster` | `CRONOS_CLUSTER` | `false` | Enable cluster membership, placement and replication. Without it the server runs standalone. |
| `--cluster-seeds` | `CRONOS_CLUSTER_SEEDS` | empty | Comma-separated membership addresses, for example `node1:7946,node2:7946,node3:7946`. Name all nodes on every node, including itself. Required in production cluster mode. |
| `--cluster-bootstrap` | `CRONOS_CLUSTER_BOOTSTRAP` | `false` | Allow this node to create a new cluster if it has no state and no seed belongs to one. Give this to exactly one node. Existing state is reused on restart. |
| `--cluster-expected-nodes` | `CRONOS_CLUSTER_EXPECTED_NODES` | `0` | Number of nodes expected at first formation. Assign first partition leaders once that many are up; `0` leaves the count unknown. |
| `--cluster-formation-wait` | `CRONOS_CLUSTER_FORMATION_WAIT` | `5s` | If the expected count is unknown or not yet reached, wait this long with stable membership before first placement. `0` assigns immediately. |
| `--cluster-gossip-addr` | `CRONOS_CLUSTER_GOSSIP_ADDR` | `:7946` | TCP membership listen/advertised address. In a cluster use a host and port peers can reach. |
| `--cluster-grpc-addr` | `CRONOS_CLUSTER_GRPC_ADDR` | `:7947` | Internal RPC address for replication, Raft control and experimental cross-region RPCs. Keep separate from the public listener. |
| `--cluster-raft-addr` | `CRONOS_CLUSTER_RAFT_ADDR` | `:7948` | Raft transport address used by the cluster coordinators. Peers must reach it. |
| `--advertise-grpc-addr` | `CRONOS_ADVERTISE_GRPC_ADDR` | empty | Public `host:port` advertised to SDKs in partition metadata. Set when applications must discover nodes beyond bootstrap addresses. Must be reachable by clients and covered by the public TLS certificate. |
| `--raft-dir` | `CRONOS_RAFT_DIR` | `./raft` | Persistent Raft state directory. Put it on persistent storage; the container/chart use a path under the data volume. |
| `--raft-join` | `CRONOS_RAFT_JOIN` | empty | Parsed but unused. Join through `--cluster-seeds`; changing this value has no effect. |
| `--virtual-nodes` | `CRONOS_VIRTUAL_NODES` | `2048` | Hash-ring positions per node. More positions spread partition ownership more evenly; nodes should agree on this value. |
| `--heartbeat-interval` | `CRONOS_HEARTBEAT_INTERVAL` | `1s` | Time between membership heartbeats. Shorter intervals increase traffic and detect failures sooner. |
| `--failure-timeout` | `CRONOS_FAILURE_TIMEOUT` | `5s` | Heartbeat silence before failure detection escalates. Allow for expected network pauses to avoid unnecessary leadership changes. |
| `--suspect-timeout` | `CRONOS_SUSPECT_TIMEOUT` | `3s` | Time a suspected node remains suspect before it is considered dead. |
| `--use-memberlist` | `CRONOS_USE_MEMBERLIST` | `false` | Unsupported. Setting `true` fails startup; the server uses its own membership protocol. |
| `--clock-skew-threshold-ms` | `CRONOS_CLOCK_SKEW_THRESHOLD_MS` | `5000` | Parsed but unused; this setting currently enforces no clock-skew limit. Synchronize node clocks operationally. |
| `--node-rack` | `CRONOS_NODE_RACK` | empty | Rack or availability-zone label used by topology-aware placement. Label nodes to describe independent failure domains. |
| `--node-zone` | `CRONOS_NODE_ZONE` | empty | Zone label for placement. Keep labels consistent across nodes in the same zone. |
| `--node-region` | `CRONOS_NODE_REGION` | empty | Region label for placement and experimental cross-region identity. |
| `--replication-batch` | `CRONOS_REPLICATION_BATCH` | `100` | Events per replication batch. Larger batches trade request overhead for batch size and latency. |
| `--replication-timeout` | `CRONOS_REPLICATION_TIMEOUT` | `10s` | Time allowed for replication. A timeout can leave a write's outcome unknown; retry the same message/request ID. |
| `--min-insync-replicas` | `CRONOS_MIN_IN_SYNC_REPLICAS` | `1` | Replicas, including the leader, required before a write is acknowledged. `0` means 1; production requires at least 2. Lifecycle mode in a cluster requires a majority of the replication factor. Higher values tolerate fewer unavailable replicas. |
| `--snapshot-catchup-threshold` | `CRONOS_SNAPSHOT_CATCHUP_THRESHOLD` | `10000` | Parsed but unused. Startup synchronization installs snapshots; connected followers catch up incrementally. Changing this does not trigger snapshots based on lag. |

## Security

TLS protects transport, JWTs identify callers, and the policy file grants
permissions. They serve different purposes. Public gRPC, HTTP and internal
node connections have separate TLS switches. See [permissions](ADMINISTRATION.md#permissions).

| Flag | Environment variable | Default | What it does and when to change it |
|---|---|---|---|
| `--tls-enabled` | `CRONOS_TLS_ENABLED` | `false` | Enable TLS on public gRPC. Requires the certificate and key below. |
| `--tls-ca-file` | `CRONOS_TLS_CA_FILE` | empty | PEM CA bundle for verifying public gRPC client certificates when client authentication is enabled. |
| `--tls-cert-file` | `CRONOS_TLS_CERT_FILE` | empty | PEM server certificate; names must cover addresses clients use. Also used by HTTP TLS. |
| `--tls-key-file` | `CRONOS_TLS_KEY_FILE` | empty | Private key matching that certificate. Mount it as a secret. |
| `--tls-client-auth` | `CRONOS_TLS_CLIENT_AUTH` | `false` | Require a client certificate on public gRPC (mTLS). Applications must supply their own certificate and key. |
| `--http-tls-enabled` | `CRONOS_HTTP_TLS_ENABLED` | `false` | Serve health, metrics, dashboard and HTTP admin endpoints over TLS using the public certificate/key. HTTP callers are not asked for a client certificate. |
| `--replication-tls-enabled` | `CRONOS_REPLICATION_TLS_ENABLED` | `false` | Require mutual TLS for replication, membership and Raft. Requires all three replication TLS files. |
| `--replication-tls-ca-file` | `CRONOS_REPLICATION_TLS_CA_FILE` | empty | CA bundle trusted to identify cluster nodes. |
| `--replication-tls-cert-file` | `CRONOS_REPLICATION_TLS_CERT_FILE` | empty | This node's PEM certificate for both server and client use between nodes. |
| `--replication-tls-key-file` | `CRONOS_REPLICATION_TLS_KEY_FILE` | empty | Private key matching the node certificate. |
| `--auth-enabled` | `CRONOS_AUTH_ENABLED` | `false` | Require JWTs on public RPCs and protected HTTP endpoints. Requires a verification secret or public key. |
| `--auth-jwt-secret` | `CRONOS_AUTH_JWT_SECRET` | empty | Shared HMAC verification secret. Use a secret-backed environment variable rather than a command-line argument visible in process listings. |
| `--auth-jwt-public-key` | `CRONOS_AUTH_JWT_PUBLIC_KEY` | empty | PEM Ed25519, RSA or ECDSA public key for asymmetric JWT verification. |
| `--auth-policy-file` | `CRONOS_AUTH_POLICY_FILE` | empty | JSON permissions keyed by JWT subject. Required in production. Explicitly grant topic publish/subscribe or global admin as needed. |
| `--encryption-enabled` | `CRONOS_ENCRYPTION_ENABLED` | `false` | Encrypt WAL segments with AES-256-GCM. Requires the key file; this is separate from TLS. |
| `--encryption-key-file` | `CRONOS_ENCRYPTION_KEY_FILE` | empty | 32-byte master key file. Preserve it securely with the recovery plan; encrypted backups need the matching key. |

## Tracing and profiling

| Flag | Environment variable | Default | What it does and when to change it |
|---|---|---|---|
| `--tracing-enabled` | `CRONOS_TRACING_ENABLED` | `false` | Create OpenTelemetry traces. Choose an exporter as well; tracing has runtime cost. |
| `--tracing-exporter` | `CRONOS_TRACING_EXPORTER` | `none` | `none`, `stdout` or `otlp`. Use stdout for local inspection or OTLP to send to a collector. |
| `--tracing-otlp-endpoint` | `CRONOS_TRACING_OTLP_ENDPOINT` | `127.0.0.1:4317` | OTLP gRPC collector `host:port`; used with the OTLP exporter. |
| `--tracing-sample-ratio` | `CRONOS_TRACING_SAMPLE_RATIO` | `0.01` | Fraction of traces sampled, from `0` to `1`. `0.01` samples about one percent; raise briefly when debugging. |
| `--tracing-insecure` | `CRONOS_TRACING_INSECURE` | `true` | Send OTLP without TLS. Set `false` when the collector requires TLS. |
| `--pprof-addr` | `CRONOS_PPROF_ADDR` | empty | Separate Go runtime profiling listener, for example `127.0.0.1:6060`. Empty disables it. Production requires loopback. |

## Additional environment variables

These have no command-line flag. CDC captures accepted event changes; it is
not a substitute for consumer delivery acknowledgements. CDC sinks cannot be
combined with event lifecycle mode. See [CDC](architecture/features/cdc.md).

| Variable | Default | Use |
|---|---|---|
| `CRONOS_CDC_KAFKA_BROKERS` | empty | Comma-separated Kafka brokers; nonempty registers a CDC Kafka sink. |
| `CRONOS_CDC_KAFKA_TOPIC` | `cronos-cdc` | Destination topic when the Kafka sink is configured. |
| `CRONOS_CDC_WEBHOOK_URL` | empty | Nonempty registers an HTTP webhook CDC sink. |
| `CRONOS_REGIONS` | empty | Comma-separated `region=address` entries for experimental cross-region peers. Requires `--dev --experimental-features`. See [replication](architecture/features/replication.md). |

## Apply changes and SIGHUP limits

Restart the node to apply configuration changes consistently. Updating a
parent shell's environment does not update an already running process.
On Unix, the SIGHUP listener re-reads a small set of variables from the
process's existing environment. It updates the shared config object without
reconfiguring most running components; it is not a general live reload.
Windows has no usable SIGHUP reload path.

| Re-read variable | Effect in the running server |
|---|---|
| `CRONOS_MAX_CREDITS` | Existing dispatchers keep their startup value; partitions constructed later use the new config. |
| `CRONOS_ACK_TIMEOUT` | Same dispatcher limitation. Startup takes durations such as `30s`; the reload parser instead takes integer seconds. |
| `CRONOS_MAX_RETRIES` | Existing dispatchers keep their startup value; new partitions use the new config. |
| `CRONOS_CB_FAILURE_THRESHOLD` | Existing delivery breakers keep their startup value; new partitions use the new config. |
| `CRONOS_LOAD_SHEDDING_THRESHOLD` | Admission reads the shared field on each check, so this value can affect existing partitions. This does not provide coordinated reload of the other limits. |
| `CRONOS_TRACING_SAMPLE_RATIO` | The tracer was constructed at startup and keeps its original sampler. |
| `CRONOS_STATS_PRINT_INTERVAL_MS` | Reload-only variable; the running stats ticker keeps its startup interval. There is no startup flag for it. |

The listener registers no component-update callbacks in `cmd/api/main.go`.
Its parsers also differ from startup validation. Use a restart rather than
depending on SIGHUP to tune production behavior.

## Docker and Helm

With Docker, pass variables with `-e`/`--env-file` and mount any named files
inside the container. A host path is not automatically available in the
container. Inspect [Compose](../docker-compose.yml) and
[chart values](../charts/cronos-db/values.yaml) for deployment-specific defaults.

The Helm chart has named values for common settings, and `extraEnv` for any
remaining variable. Values are Kubernetes `EnvVar` entries, including
`valueFrom` for secrets. Explicit environment entries override ConfigMap
values. Avoid duplicate names in `extraEnv` and the chart's explicit `env`
entries; prefer the named chart setting for TLS, auth, addresses and storage.

```yaml
# Additional values file, used together with values-production.yaml.
config:
  minInSyncReplicas: 2
extraEnv:
  - name: CRONOS_EVENT_LIFECYCLE
    value: "true"
  - name: CRONOS_RETENTION_MAX_AGE_HOURS
    value: "72"
  - name: CRONOS_BACKUP_INTERVAL
    value: "6h"
```

Apply changed environment values by rolling/restarting pods. Use
`backupPersistence` for a separate backup PVC and `values-production.yaml`
for the production security settings. The chart's base values enable dev
mode; they are intended for testing.

## Check the effective setup

Use `cronos-api --help` to inspect the binary's accepted flags. Check both
explicit arguments and container/pod environment when a setting surprises
you; deployment defaults can differ from binary defaults. The
[administration guide](ADMINISTRATION.md) describes topology, health, lag and
log checks. The [configuration architecture](architecture/features/config.md)
links the implementation and explains how values reach components.

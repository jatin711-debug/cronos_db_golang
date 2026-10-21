# Dashboard and administration

Start a development node with the [quick start](../README.md#quick-start).
For production, follow the [release requirements](PRODUCTION_RELEASE.md) and
[Docker deployment guide](../DOCKER.md#production-considerations).

## Dashboard

Release binaries and the container include a React dashboard at `/ui/` on the
HTTP listener. With the quick-start node, open
[http://127.0.0.1:8080/ui/](http://127.0.0.1:8080/ui/).

The dashboard shows topology, partition health, replication lag, consumer
groups, schemas, and operations. Its retention and compaction actions use the
same admin service as the CLI. Tenant usage currently shows zeros because
tenant limits are not configured; the rebalance action reports that placement
is driven by membership rather than initiating a rebalance itself.

When authentication is enabled, sign in with a JWT whose subject has
`"admin": true` in the server's policy file (see [permissions](#permissions)).
`cronos-admin generate-token` can sign a token for testing; see
[test tokens](#test-tokens).

Source builds need `make build` to compile and embed the dashboard. See the
[dashboard architecture](architecture/features/dashboard.md) for routes,
authentication, and implementation details.

## Administration tool

`cronos-admin` is included in each release and at `/app/cronos-admin` in the
container. The examples below assume it is on `PATH`. To build it from source:

```bash
go build -o bin/cronos-admin ./cmd/admin
```

Use `bin/cronos-admin.exe` as the output on Windows. For the quick-start
container, an equivalent invocation is:

```bash
docker exec cronos-dev /app/cronos-admin --server=localhost:9000 admin topology
```

Each command asks one node and reports that node's view. The
[command reference](#cronos-admin-command-reference) lists every command and
flag.

### Connecting to a node

`--server` (default `localhost:9000`) names the node and port. The `admin`,
`partition`, `consumer` and `debug` commands use the public client port
(`--grpc-addr`, default `:9000`). `cluster voters` and `cluster remove-node`
use the internal cluster port (`--cluster-grpc-addr`, default `:7947`).
Match the connection flags to how the server was started:

| Server setting | What `cronos-admin` needs |
|---|---|
| `--tls-enabled` (public port) | `--tls`, and `--tls-ca` unless the server certificate chains to a CA the system trusts. The certificate must cover the host given in `--server`. |
| `--tls-client-auth` (public port) | Also `--tls-cert` and `--tls-key`: a client certificate signed by the CA in the server's `--tls-ca-file`. |
| `--auth-enabled` (public port) | `--jwt-token` on every command that uses the public port. See [permissions](#permissions). |
| `--replication-tls-enabled` (cluster port) | `--tls`, `--tls-ca`, `--tls-cert` and `--tls-key` with the cluster's certificates: the CA in `--replication-tls-ca-file` and a certificate it signed. The cluster port does not check `--jwt-token`. |

Production mode requires `--tls-enabled`, `--auth-enabled` and
`--replication-tls-enabled`; `--tls-client-auth` is optional. Without `--tls`
the CLI connects in plain text and sends any `--jwt-token` unencrypted.

```bash
cronos-admin --server=node1.example.com:9000 --tls --tls-ca=ca.crt \
  --jwt-token="$TOKEN" admin topology
```

### Permissions

With `--auth-enabled`, every RPC on the public port needs a token signed with
the server's `--auth-jwt-secret` or matching `--auth-jwt-public-key`. A
missing, expired or badly signed token is refused with `Unauthenticated`. The
server's `--auth-policy-file` then decides what the token's subject (its `sub`
claim) may do. The file is a JSON object keyed by subject:

```json
{
  "ops": {"admin": true},
  "orders-app": {"topics": {"orders": {"publish": true, "subscribe": true}}}
}
```

- `"admin": true` allows every `admin` command, `cluster status`, the
  dashboard, all consumer-group RPCs, and the compact, retention and split
  RPCs of the partition service. It does not
  allow publishing to or subscribing from any topic.
- `topics` allows `publish` and `subscribe` on each named topic; names must
  match exactly. A per-topic `"admin"` key is accepted, but no RPC checks it.
- The read-only `partition list`, `partition info` and `partition wal`
  commands need only a valid token. `consumer list` and `consumer lag`
  require global admin permission because they expose group progress.

A refused permission is reported as `PermissionDenied` with the subject's
name. With auth disabled (the default) no command needs a token. `--dev` does
not disable auth.

### Public admin commands

These commands connect to the public client gRPC listener:

```bash
cronos-admin --server=localhost:9000 admin topology
cronos-admin --server=localhost:9000 admin partition-health 0
cronos-admin --server=localhost:9000 admin replication-lag
cronos-admin --server=localhost:9000 admin consumer-groups list
cronos-admin --server=localhost:9000 admin consumer-group-lag my-group 0
cronos-admin --server=localhost:9000 admin schema-list
cronos-admin --server=localhost:9000 admin tenant-usage
cronos-admin --server=localhost:9000 admin retention-run --max-age-hours=168
cronos-admin --server=localhost:9000 admin compaction-run
```

Partition 0 is a valid target for health and consumer-group lag. For
retention, compaction and replication lag, `0` means every applicable
partition on this node, including partition 0.

### Cluster membership

`cluster voters` and `cluster remove-node` connect to the internal cluster
gRPC listener, whose production connections require the cluster's client
certificate:

```bash
cronos-admin --server=node1:7947 \
  --tls --tls-ca=ca.crt --tls-cert=tls.crt --tls-key=tls.key cluster voters
```

`cluster voters` shows whether the node asked coordinates the cluster, its
Raft term, and the Raft addresses of the nodes that count towards a majority.
Use the same connection options with `cluster remove-node <node-id>` for a
node that is permanently lost. A node that is still running is refused, and a
node cannot remove itself: ask another one. See
[node replacement and removal](PRODUCTION_RELEASE.md#configuration-and-installation)
before changing a production cluster's membership. A standalone node has no
Raft service and answers these commands with `Unimplemented`.

### Offline recovery tools

Run these against the data directory of a stopped node:

```bash
cronos-admin restore --from=/backups/<backup> --data-dir=/data
cronos-admin check-log --data-dir=/data
cronos-admin check-log --data-dir=/data --partition=3 --repair
```

Restore checks every file against the backup's manifest and refuses a data
directory that already holds any of the backup's partitions. `check-log`
reports damage; `--repair` cuts out damaged log content and reports what was
lost. Preserve backups before repair. Neither tool contacts a server or checks
that the node is stopped, so stop it first. See the
[maintenance validation record](MAINTENANCE_VALIDATION_2026-09-29.md) and
[backup and restore requirements](PRODUCTION_RELEASE.md#checks).

### Test tokens

`generate-token` signs a token on the machine it runs on. It is meant for
testing:

```bash
TOKEN=$(cronos-admin generate-token --secret="$(cat jwt-secret)" --subject=ops --ttl=1h)
cronos-admin --server=localhost:9000 --jwt-token="$TOKEN" admin topology
```

The token is signed with HS256 using `--secret`, which must equal the server's
`--auth-jwt-secret`. It holds only the subject (`sub` and `sub_id`), the time
it was issued and its expiry, `--ttl` later; permissions come from the policy
file. A server that verifies only with `--auth-jwt-public-key` refuses it.
Anyone who has the secret can sign a token for any subject, including an
admin, and a secret given on the command line can be kept in shell history and
seen in process listings. Issue production tokens from the system that manages
your JWTs.

## cronos-admin command reference

### Global flags

Every command accepts these flags; they only matter for commands that connect
to a node.

| Flag | Default | Meaning |
|---|---|---|
| `-s`, `--server` | `localhost:9000` | `host:port` of the node to ask |
| `--tls` | off | Connect with TLS instead of plain text |
| `--tls-ca` | system CAs | PEM file of the CA that signed the server's certificate |
| `--tls-cert`, `--tls-key` | none | Client certificate and key for mutual TLS. Give both: one alone is ignored |
| `--tls-skip-verify` | off | Do not verify the server's certificate (development only) |
| `--jwt-token` | none | Token sent as `authorization: Bearer <token>` with every RPC |

`--tls-ca`, `--tls-cert`, `--tls-key` and `--tls-skip-verify` take effect only
with `--tls`. `--help` shows the value of `--jwt-token` as
`authorization: Bearer <token>`; pass the token alone.

### Commands

"Port" is the listener the command uses. "Needs with auth" is what the command
needs when the server runs with `--auth-enabled`: `admin` means
`"admin": true` in the policy file, and "cluster certificate" means a client
certificate signed by the cluster's CA when the server runs with
`--replication-tls-enabled`.

| Command | What it does, and when to use it | Port | Needs with auth |
|---|---|---|---|
| `admin topology` | Nodes, and each partition's leader, replicas, in-sync replicas and high watermark. Check placement and replica health | public | `admin` |
| `admin partition-health <id>` | A partition's high watermark, segments, timers, ready events, cold-store entries and replay error on this node. Check one partition | public | `admin` |
| `admin replication-lag [<id>]` | Leader high watermark and each follower's offset, lag and in-sync state. Check that followers keep up | public | `admin` |
| `admin consumer-groups list` | Consumer groups on every partition of this node, with member count | public | `admin` |
| `admin consumer-group-lag <group> <id>` | A group's committed offset, the partition's high watermark, and the lag between them. Check that consumers keep up | public | `admin` |
| `admin schema-list` | Topics with a schema, their latest version and compatibility mode | public | `admin` |
| `admin schema-get <topic> <version>` | One schema version; `0` is the latest | public | `admin` |
| `admin tenant-usage [<tenant>]` | In-flight and storage usage of one tenant, or of all | public | `admin` |
| `admin retention-run` | Applies age or size retention once. Free disk now, or apply a limit once, without changing `--retention-max-age-hours` or `--retention-max-size-gb` | public | `admin` |
| `admin compaction-run [--partition-id <id>]` | Removes finished log segments without waiting for `--compaction-interval`; `0` or omitted means every partition on this node | public | `admin` |
| `admin cluster-rebalance` | Changes nothing: reports that on-demand rebalance is not available, since placement follows membership | public | `admin` |
| `partition list` | Partitions with their topic and leader | public | valid token |
| `partition info <id>` | A partition's leader, replicas, high watermark, last offset, segment count and disk usage, as JSON | public | valid token |
| `partition wal <id>` | Last offset, segment file count and total size of this node's log for a partition | public | valid token |
| `consumer list` | Consumer groups with their topic and partitions | public | `admin` |
| `consumer lag <group>` | A group's topic, partitions and committed offset per partition | public | `admin` |
| `debug publish <topic> <payload>` | Publishes one test event, due 5 seconds later, and prints its partition and offset. Check that a node accepts writes | public | `publish` on the topic |
| `cluster voters` | Whether the node asked coordinates the cluster, its Raft term, and the Raft addresses of the voters | cluster | cluster certificate |
| `cluster remove-node <node-id>` | Removes a stopped node that is gone for good from Raft and from every node's member list | cluster | cluster certificate |
| `cluster status` | Calls `AdminService.GetClusterTopology`, as `admin topology` does | public | `admin` |
| `generate-token` | Signs a test token; see [test tokens](#test-tokens) | none | none |
| `restore` | Restores a backup into a data directory | files | node stopped |
| `check-log` | Checks the partition logs for damage; `--repair` cuts it out | files | node stopped |
| `completion <shell>` | Prints a completion script for `bash`, `zsh`, `fish` or `powershell` | none | none |

A command waits 5 seconds for an answer; `admin retention-run` and
`admin compaction-run` wait 60, `cluster voters` 10 and `cluster remove-node`
30.

### Arguments and results

| Command | Details |
|---|---|
| `admin topology` | A partition whose leader does not answer within 2 seconds shows `isr=unknown`. A standalone node lists itself and no partitions |
| `admin partition-health <id>` | Only partitions on this node; ID `0` is valid. `disk` always prints `0`; `partition info` shows disk usage |
| `admin replication-lag [<id>]` | Omitted or `0`: every partition this node leads, with the follower lines merged and `hwm` summed. Only replicated partitions led here list followers; others print `hwm=0` |
| `admin consumer-group-lag <group> <id>` | ID `0` is valid. `-1` means the partition is not on this node or the group has not committed there |
| `admin schema-list`, `admin schema-get` | Schemas the node loaded from `<data-dir>/schemas` when it started. No RPC registers a schema, so the list is normally empty |
| `admin tenant-usage [<tenant>]` | Omitted: the sum over all tenants. Tenant limits cannot be set in this release, so it reports `limits_configured=false` and zero usage |
| `partition list` | In a cluster, every partition in the cluster's metadata; standalone, the node's partitions. `watermark` always prints `0`; `partition info` shows the high watermark |
| `partition info <id>` | Fields that are zero are left out of the JSON. In a cluster, a partition this node does not hold shows only its topic, leader and replicas |
| `partition wal <id>` | Only partitions on this node |
| `consumer list`, `consumer lag` | Read the groups of partition 0 only; see [limitations](#limitations). `consumer lag` prints committed offsets, not lag; `-1` means nothing committed |
| `debug publish <topic> <payload>` | Sends the payload as bytes with message ID `admin-<nanoseconds>`; the server picks the partition. `"admin": true` alone does not allow it |

`admin retention-run` flags:

| Flag | Default | Meaning |
|---|---|---|
| `--partition-id` | `0` | Partition to clean; `0` means every partition on this node |
| `--max-age-hours` | `0` | Remove segments older than this; `0` sets no age limit |
| `--max-size-bytes` | `0` | Remove the oldest segments until the log fits; `0` sets no size limit. For every partition, this is a budget for the node's total log size |

With both limits at `0` it removes nothing. It never removes a partition's
active segment, applies the same completion checks as background retention,
and on a replicated partition only the leader removes segments.

`admin compaction-run` flags:

| Flag | Default | Meaning |
|---|---|---|
| `--partition-id` | `0` | Partition to compact; `0` means every partition on this node |

It prints `segments_removed=N` with the number of removed segments. The
RPC's `bytes_reclaimed` remains `0`; compare `partition wal <id>` before and
after for disk usage. On replicated partitions, cleanup requires leadership;
a request over every local partition can fail on a follower. Use an explicit
partition and ask its leader when ownership is spread across nodes.

`generate-token` flags:

| Flag | Default | Meaning |
|---|---|---|
| `--secret` | none (required) | HMAC key; must equal the server's `--auth-jwt-secret` |
| `--subject` | `admin` | Subject of the token; it gets the permissions of that key in the policy file |
| `--ttl` | `24h` | How long the token is valid, as a duration such as `30m` or `1h` |

`restore` flags:

| Flag | Default | Meaning |
|---|---|---|
| `--from` | required | One backup generation: the `backup-*` directory that holds `backup.json`, under `--backup-dir` (default `backups` in the data directory) |
| `--data-dir` | required | Data directory of the stopped node |
| `--encryption-key-file` | none | Master key of an encrypted backup; checked against the backup before anything is copied |

`check-log` flags:

| Flag | Default | Meaning |
|---|---|---|
| `--data-dir` | required | Data directory of the stopped node |
| `--partition` | `-1` (all) | Check only this partition. Required with `--repair` |
| `--encryption-key-file` | none | Master key, when the logs are encrypted |
| `--repair` | off | Cut each damaged segment after its last valid record; the events it reports as unreadable are given up |

`check-log` exits with an error while damaged segments remain.

### Limitations

- `admin retention-run`, `admin compaction-run` and `admin replication-lag`
  use `0` for every applicable partition. They cannot target only partition 0.
  Use `PartitionService.Compact` or `PartitionService.RunRetention` below
  when cleanup must be scoped to partition 0 alone.
- `consumer list` and `consumer lag` read only partition 0's consumer groups.
  A group that subscribes only to other partitions is missing there, and
  `consumer lag` reports `consumer group not found`. Use
  `admin consumer-groups list` and `admin consumer-group-lag`, which read
  every partition on the node.

### Server RPCs without a command

The public port also serves these RPCs, which `cronos-admin` does not call.
Use any gRPC client; the port serves gRPC reflection. The contracts are in
[events.proto](../proto/events.proto) and [admin.proto](../proto/admin.proto).

| RPC | Use | Needs with auth |
|---|---|---|
| `PartitionService.GetSchedulerStatus` | Timers, ready events and timing-wheel settings of a partition | valid token |
| `PartitionService.Compact` | Compaction of any partition, including 0, optionally only before `before_ts`; reports what it removed | `admin` |
| `PartitionService.RunRetention` | Retention of one partition, including 0, also below `min_offset` | `admin` |
| `ConsumerGroupService.CreateConsumerGroup`, `RebalanceConsumerGroup` | Create a group; reassign a group's partitions to its members | `admin` |
| `EventService.GetEventState`, `CancelEvent`, `RescheduleEvent` | Inspect, cancel or move one scheduled event; use the [Go SDK lifecycle methods](CLIENT_GUIDE.md#cancel-or-reschedule-an-event) or see the [lifecycle contract](RETENTION_AND_EVENT_LIFECYCLE.md) | `publish` on the topic |
| `EventService.Replay` | Read a topic's events from an offset or a time | `subscribe` on the topic |

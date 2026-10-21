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

When authentication is enabled, sign in with a JWT for an authorized admin
subject. `cronos-admin generate-token --help` describes token generation;
the subject's permissions come from the server's policy file.

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
cronos-admin --server=localhost:9000 admin retention-run --partition-id=0
cronos-admin --server=localhost:9000 admin compaction-run --partition-id=0
```

Use `--tls`, `--tls-ca`, `--tls-cert`, and `--tls-key` for TLS connections.
`--jwt-token` authenticates when JWT auth is enabled. The endpoint's certificate
must cover the server address used by the CLI.

### Cluster membership

Cluster commands connect to the internal cluster gRPC listener, whose
production connections require the cluster's client certificate:

```bash
cronos-admin --server=node1:7947 \
  --tls --tls-ca=ca.crt --tls-cert=tls.crt --tls-key=tls.key cluster status
```

Use the same connection options with `cluster voters` to inspect Raft voters,
or `cluster remove-node <node-id>` for a node that is permanently lost. A node
that is still running is refused. See
[node replacement and removal](PRODUCTION_RELEASE.md#configuration-and-installation)
before changing a production cluster's membership.

### Offline recovery tools

Run these against the data directory of a stopped node:

```bash
cronos-admin restore --from=/backups/<backup> --data-dir=/data
cronos-admin check-log --data-dir=/data
cronos-admin check-log --data-dir=/data --partition=3 --repair
```

Restore verifies backup files and requires an empty destination. `check-log`
reports damage; `--repair` cuts out damaged log content and reports what was
lost. Preserve backups before repair. See the
[maintenance validation record](MAINTENANCE_VALIDATION_2026-09-29.md) and
[backup and restore requirements](PRODUCTION_RELEASE.md#checks).

# =============================================================================
# CronosDB Docker Setup Guide
# =============================================================================

## Quick Start

### Get the image

Each release is published on Docker Hub (linux/amd64):

```bash
docker pull johnny711dock/cronos-db:0.6.0-rc.4    # the current release; also :latest
```

Or build it from a checkout:

```bash
docker build -t cronos-db:latest .
```

The image holds the server at `/app/cronos-api`, built with cgo and the Rust bloom filter, and the administration tool at `/app/cronos-admin`. It runs as the non-root user `cronos`, and keeps its data in the volume `/data`. The examples below use the local tag `cronos-db:latest`; use `johnny711dock/cronos-db:0.6.0-rc.4` for the published image.

### Run single node

> **Note:** Without `--dev`, the server enforces production validation (TLS, auth + policy
> file, encryption, RF≥3, minISR≥2, replication mTLS) and refuses to boot. Use `--dev` for
> local runs; see the production example below for a hardened configuration.

```bash
docker run -d \
  --name cronos-single \
  -p 9000:9000 \
  -p 8080:8080 \
  -v cronos-data:/data \
  cronos-db:latest \
  --node-id=my-node \
  --data-dir=/data \
  --dev
```

### Run with docker-compose (single node)

```bash
docker compose up -d cronos-single
```

## Cluster Mode

### Start 3-node cluster

```bash
docker compose up -d cronos-node1 cronos-node2 cronos-node3
```

### Check health

```bash
curl http://localhost:8080/health   # node1
curl http://localhost:8081/health   # node2
curl http://localhost:8082/health   # node3
```

## Data Persistence

### Named volumes (recommended)

Docker Compose automatically creates named volumes:
- `cronos-data` - Single node data
- `node1-data`, `node2-data`, `node3-data` - Cluster node data

### Host bind mount (for development)

```bash
docker run -d \
  --name cronos-single \
  -p 9000:9000 \
  -p 8080:8080 \
  -v /path/on/host:/data \
  cronos-db:latest \
  --node-id=my-node \
  --data-dir=/data \
  --dev
```

### Inspect volumes

The compose project name defaults to the checkout directory (`cronos_db_golang`), so
volume names are prefixed accordingly:

```bash
docker volume inspect cronos_db_golang_cronos-data
```

## Environment Variables

Every flag of the server can be set through `CRONOS_` and its name in upper case with dashes as underscores (`--cluster-seeds` is `CRONOS_CLUSTER_SEEDS`); a flag on the command line wins. The ones the image and `docker-compose.yml` set:

| Variable | Image default | Compose default | Description |
|----------|----------------|-----------------|-------------|
| `CRONOS_NODE_ID` | `cronos-node-1` | per-service | Unique node identifier (the binary has no default and requires one) |
| `CRONOS_DATA_DIR` | `/data` | `/data` | Data directory (the binary's default is `./data`) |
| `CRONOS_GRPC_ADDR` | `:9000` | `0.0.0.0:9000` | gRPC listen address |
| `CRONOS_HTTP_ADDR` | `:8080` | `0.0.0.0:8080` | HTTP health check address |
| `CRONOS_CLUSTER` | `false` | `false` | Enable cluster mode |
| `CRONOS_CLUSTER_SEEDS` | (none) | (none) | Comma-separated seed addresses |
| `CRONOS_TRACING_ENABLED` | `false` | `false` | Enable OpenTelemetry tracing |
| `CRONOS_TRACING_EXPORTER` | `none` | `otlp` | Tracing exporter (`none`, `stdout`, `otlp`) |
| `CRONOS_TRACING_OTLP_ENDPOINT` | `127.0.0.1:4317` | `otel-collector:4317` | OTLP gRPC endpoint |
| `CRONOS_TRACING_SAMPLE_RATIO` | `0.01` | `0.01` | Trace sampling ratio (0.0-1.0) |
| `CRONOS_TRACING_INSECURE` | `true` | `true` | Disable TLS for OTLP exporter |

> "Image default" is what a plain `docker run` of the image gets; "Compose default"
> is what `docker-compose.yml` injects via `${VAR:-...}` substitutions.

## Observability Stack (Grafana + Prometheus + Tempo + OTEL Collector)

### Start observability services

```bash
make observability-up
```

### Start app tracing with low overhead sampling

Use 1% sampling to keep throughput impact minimal:

`make node1` passes every tracing flag itself, and a flag wins over the environment, so
tracing is set through make variables (defaults: exporter `otlp`, endpoint `127.0.0.1:4317`,
sample ratio `0.01`):

```bash
make node1 TRACING_ENABLED=true TRACING_SAMPLE_RATIO=0.01
```

### Open dashboards

- Grafana: `http://localhost:3000` (admin/admin)
- Prometheus: `http://localhost:9090`
- Tempo API: `http://localhost:3200`

### Stop observability services

```bash
make observability-down
```

## Common Commands

### View logs

```bash
docker logs -f cronos-single
```

### Exec into container

```bash
docker exec -it cronos-single /bin/sh
```

### Stop and remove

```bash
docker compose down
docker volume rm cronos_db_golang_cronos-data
```

### Run load test (from host)

```bash
# Build load test tool (add .exe on Windows)
go build -tags clustertest -o bin/cluster_loadtest cluster_loadtest.go

# Run test
./bin/cluster_loadtest -publishers=10 -events=1000 -batch -batch-size=100
```

## Production Considerations

1. **Use named volumes** - Data survives container restarts
2. **Non-root user** - Image runs as `cronos` user for security
3. **Health checks** - Built-in HTTP health endpoint; `/health/ready` answers 200 once the node serves publishes
4. **Resource limits** - Set a memory limit (`--memory`). The server reads it from the container and refuses publishes while it holds 80% of it (`--max-memory-percent`), instead of being killed
5. **Kubernetes** - The Helm chart in `charts/cronos-db` with `values-production.yaml` sets all of the below up; see [the release requirements](docs/PRODUCTION_RELEASE.md)

### Production deployment example

Production mode refuses to boot unless all of the following are configured:
`--replication-factor >= 3`, `--min-insync-replicas >= 2`, TLS, auth with a policy file,
encryption at rest, mutual TLS between nodes, `--fsync-mode` `batch` or `every_event`, and
`--cluster-seeds` naming every node, with `--cluster-bootstrap` on exactly one of them (see
`internal/config/config.go` `ValidateConfig`).

The nodes reach each other on three ports: membership (7946), cluster gRPC (7947) and Raft
(7948). Each node has to name, in `--cluster-gossip-addr`, `--cluster-grpc-addr` and
`--cluster-raft-addr`, an address the other nodes can reach, and its certificate must be valid
for it. The first of three nodes, on a host the others reach as `node1.cronos`:

```bash
docker run -d \
  --name cronos-node1 \
  --restart unless-stopped \
  -p 9000:9000 -p 8080:8080 -p 7946:7946 -p 7947:7947 -p 7948:7948 \
  --memory=2g \
  --cpus=2 \
  -v cronos-node1-data:/data \
  -v /etc/cronos/certs:/certs:ro \
  --env-file /etc/cronos/secrets.env \
  johnny711dock/cronos-db:0.6.0-rc.4 \
  --node-id=node1 \
  --data-dir=/data \
  --cluster \
  --cluster-bootstrap \
  --cluster-seeds=node1.cronos:7946,node2.cronos:7946,node3.cronos:7946 \
  --cluster-gossip-addr=node1.cronos:7946 \
  --cluster-grpc-addr=node1.cronos:7947 \
  --cluster-raft-addr=node1.cronos:7948 \
  --replication-factor=3 \
  --min-insync-replicas=2 \
  --fsync-mode=batch \
  --tls-enabled \
  --tls-cert-file=/certs/server.crt \
  --tls-key-file=/certs/server.key \
  --http-tls-enabled \
  --auth-enabled \
  --auth-policy-file=/certs/auth-policy.json \
  --encryption-enabled \
  --encryption-key-file=/certs/master.key \
  --replication-tls-enabled \
  --replication-tls-cert-file=/certs/node.crt \
  --replication-tls-key-file=/certs/node.key \
  --replication-tls-ca-file=/certs/ca.crt
```

The other two nodes differ in `--node-id`, their three addresses and their certificate, and
leave out `--cluster-bootstrap`. `--http-tls-enabled` serves health, metrics and the dashboard
over HTTPS, so health checks and Prometheus have to use `https://`.

> `secrets.env` holds `CRONOS_AUTH_JWT_SECRET=...`. Passing the JWT secret through the
> environment (an env file or an orchestrator secret) keeps it out of process listings,
> where a `--auth-jwt-secret` flag would show.

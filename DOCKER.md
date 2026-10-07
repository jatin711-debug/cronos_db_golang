# =============================================================================
# CronosDB Docker Setup Guide
# =============================================================================

## Quick Start

### Build the image

```bash
docker build -t cronos-db:latest .
```

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
docker-compose up -d cronos-single
```

## Cluster Mode

### Start 3-node cluster

```bash
docker-compose up -d cronos-node1 cronos-node2 cronos-node3
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

| Variable | Server default | Compose default | Description |
|----------|----------------|-----------------|-------------|
| `CRONOS_NODE_ID` | `cronos-node-1` | per-service | Unique node identifier |
| `CRONOS_DATA_DIR` | `./data` | `/data` | Data directory |
| `CRONOS_GRPC_ADDR` | `:9000` | `0.0.0.0:9000` | gRPC listen address |
| `CRONOS_HTTP_ADDR` | `:8080` | `0.0.0.0:8080` | HTTP health check address |
| `CRONOS_CLUSTER` | `false` | `false` | Enable cluster mode |
| `CRONOS_CLUSTER_SEEDS` | (none) | (none) | Comma-separated seed addresses |
| `CRONOS_TRACING_ENABLED` | `false` | `false` | Enable OpenTelemetry tracing |
| `CRONOS_TRACING_EXPORTER` | `none` | `otlp` | Tracing exporter (`none`, `stdout`, `otlp`) |
| `CRONOS_TRACING_OTLP_ENDPOINT` | `127.0.0.1:4317` | `otel-collector:4317` | OTLP gRPC endpoint |
| `CRONOS_TRACING_SAMPLE_RATIO` | `0.01` | `0.01` | Trace sampling ratio (0.0-1.0) |
| `CRONOS_TRACING_INSECURE` | `true` | `true` | Disable TLS for OTLP exporter |

> "Server default" is what a plain `docker run` (or bare binary) gets; "Compose default"
> is what `docker-compose.yml` injects via `${VAR:-...}` substitutions.

## Observability Stack (Grafana + Prometheus + Tempo + OTEL Collector)

### Start observability services

```bash
make observability-up
```

### Start app tracing with low overhead sampling

Use 1% sampling to keep throughput impact minimal:

```bash
# Linux/macOS
CRONOS_TRACING_ENABLED=true \
CRONOS_TRACING_EXPORTER=otlp \
CRONOS_TRACING_OTLP_ENDPOINT=127.0.0.1:4317 \
CRONOS_TRACING_SAMPLE_RATIO=0.01 \
make node1
```

```powershell
# Windows PowerShell
$env:CRONOS_TRACING_ENABLED="true"
$env:CRONOS_TRACING_EXPORTER="otlp"
$env:CRONOS_TRACING_OTLP_ENDPOINT="127.0.0.1:4317"
$env:CRONOS_TRACING_SAMPLE_RATIO="0.01"
make node1
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
docker-compose down
docker volume rm cronos_db_golang_cronos-data
```

### Run load test (from host)

```bash
# Build load test tool
go build -tags clustertest -o bin/cluster_loadtest.exe cluster_loadtest.go

# Run test
./bin/cluster_loadtest.exe -publishers=10 -events=1000 -batch -batch-size=100
```

## Production Considerations

1. **Use named volumes** - Data survives container restarts
2. **Non-root user** - Image runs as `cronos` user for security
3. **Health checks** - Built-in HTTP health endpoint
4. **Resource limits** - Add `--memory=2g` for production

### Production deployment example

Production mode refuses to boot unless all of the following are configured:
`--replication-factor >= 3`, `--min-insync-replicas >= 2`, TLS, auth with a policy file,
encryption at rest, and replication mTLS (see `internal/config/config.go` `ValidateConfig`).

```bash
docker run -d \
  --name cronos-prod \
  --restart unless-stopped \
  -p 9000:9000 \
  -p 8080:8080 \
  --memory=2g \
  --cpus=2 \
  -v cronos-prod-data:/data \
  -v /etc/cronos/certs:/certs:ro \
  cronos-db:latest \
  --node-id=prod-node \
  --data-dir=/data \
  --cluster \
  --cluster-seeds=seed1:7946,seed2:7946 \
  --replication-factor=3 \
  --min-insync-replicas=2 \
  --tls-enabled \
  --tls-cert-file=/certs/server.crt \
  --tls-key-file=/certs/server.key \
  --auth-enabled \
  --auth-jwt-secret="$CRONOS_AUTH_JWT_SECRET" \
  --auth-policy-file=/certs/auth-policy.json \
  --encryption-enabled \
  --encryption-key-file=/certs/master.key \
  --replication-tls-enabled \
  --replication-tls-cert-file=/certs/server.crt \
  --replication-tls-key-file=/certs/server.key \
  --replication-tls-ca-file=/certs/ca.crt
```

> Prefer passing the JWT secret via the `CRONOS_AUTH_JWT_SECRET` environment variable
> (e.g. `--env-file` or an orchestrator secret) rather than a CLI flag, which is visible
> in process listings.

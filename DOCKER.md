# =============================================================================
# CronosDB Docker Setup Guide
# =============================================================================

## Quick Start

### Build the image

```bash
docker build -t cronos-db:latest .
```

### Run single node

```bash
docker run -d \
  --name cronos-single \
  -p 9000:9000 \
  -p 8080:8080 \
  -v cronos-data:/data \
  cronos-db:latest \
  --node-id=my-node \
  --data-dir=/data
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
  --data-dir=/data
```

### Inspect volumes

```bash
docker volume inspect cronos_db_cronos-data
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CRONOS_NODE_ID` | `cronos-node-1` | Unique node identifier |
| `CRONOS_DATA_DIR` | `/data` | Data directory |
| `CRONOS_GRPC_ADDR` | `0.0.0.0:9000` | gRPC listen address |
| `CRONOS_HTTP_ADDR` | `0.0.0.0:8080` | HTTP health check address |
| `CRONOS_CLUSTER` | `false` | Enable cluster mode |
| `CRONOS_CLUSTER_SEEDS` | (none) | Comma-separated seed addresses |

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
docker volume rm cronos_db_cronos-data
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

```bash
docker run -d \
  --name cronos-prod \
  --restart unless-stopped \
  -p 9000:9000 \
  -p 8080:8080 \
  --memory=2g \
  --cpus=2 \
  -v cronos-prod-data:/data \
  cronos-db:latest \
  --node-id=prod-node \
  --data-dir=/data \
  --cluster \
  --cluster-seeds=seed1:7946,seed2:7946
```
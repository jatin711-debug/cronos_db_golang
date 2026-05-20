.PHONY: build clean proto rust-dedup test node1 node2 node3 cluster loadtest loadtest-batch loadtest-max help docker docker-build docker-single docker-cluster

# Build settings
BINARY=cronos-api
BUILD_DIR=bin
RUST_DEDUP_DIR=internal/dedup/rust

ifeq ($(OS),Windows_NT)
RUST_SHARED_LIB=cronos_dedup.dll
RUST_COPY_CMD=powershell -NoProfile -Command "Copy-Item -Force '$(RUST_DEDUP_DIR)/target/release/$(RUST_SHARED_LIB)' 'internal/dedup/'"
CLEAN_BUILD_CMD=powershell -NoProfile -Command "if (Test-Path '$(BUILD_DIR)') { Remove-Item -Recurse -Force '$(BUILD_DIR)' }"
CLEAN_DATA_CMD=powershell -NoProfile -Command "foreach ($$d in @('data/node1','data/node2','data/node3')) { if (Test-Path $$d) { Remove-Item -Recurse -Force $$d } }"
else
UNAME_S := $(shell uname -s 2>/dev/null)
ifeq ($(UNAME_S),Darwin)
RUST_SHARED_LIB=libcronos_dedup.dylib
RUST_COPY_CMD=cp -f $(RUST_DEDUP_DIR)/target/release/$(RUST_SHARED_LIB) internal/dedup/
else
RUST_SHARED_LIB=libcronos_dedup.so
RUST_COPY_CMD=cp -f $(RUST_DEDUP_DIR)/target/release/$(RUST_SHARED_LIB) internal/dedup/
endif
CLEAN_BUILD_CMD=rm -rf $(BUILD_DIR)
CLEAN_DATA_CMD=rm -rf data/node1 data/node2 data/node3
endif

# Throughput profile settings for node targets (override as needed)
PARTITION_COUNT?=16
REPLICATION_FACTOR?=1
FSYNC_MODE?=periodic
FLUSH_INTERVAL_MS?=100
INDEX_INTERVAL?=4096
SEGMENT_SIZE_BYTES?=1073741824
VIRTUAL_NODES?=2048
MAX_CREDITS?=50000
ACK_TIMEOUT?=60s

# Load test settings (can be overridden)
NODES?=3
PUBLISHERS?=24
EVENTS?=50000
PAYLOAD?=256
SCHEDULE_DELAY?=0
TOPIC?=cluster-loadtest
ROUND_ROBIN?=true
BATCH_SIZE?=4000

# Default target
help:
	@echo "CronosDB Makefile Commands:"
	@echo ""
	@echo "  make build          - Build the server binary"
	@echo "  make rust-dedup     - Build Rust dedup library (release)"
	@echo "  make test           - Build Rust dedup lib and run go tests"
	@echo "  make proto          - Regenerate protobuf code"
	@echo "  make clean          - Clean build artifacts and data"
	@echo ""
	@echo "  make node1          - Start node 1 (leader, port 9000)"
	@echo "  make node2          - Start node 2 (port 9001)"
	@echo "  make node3          - Start node 3 (port 9002)"
	@echo "  make cluster        - Show cluster startup commands"
	@echo ""
	@echo "  make loadtest       - Run cluster load test (single-event mode)"
	@echo "  make loadtest-batch - Run cluster load test (batch mode, fastest)"
	@echo "  make loadtest-max   - Run recommended max-throughput benchmark profile"
	@echo ""
	@echo "Examples:"
	@echo "  make loadtest-batch PUBLISHERS=24 EVENTS=80000 BATCH_SIZE=4000"
	@echo "  make node1 FSYNC_MODE=periodic FLUSH_INTERVAL_MS=50"
	@echo ""
	@echo "  make clean-data     - Remove data directories only"

# Build the server
build: rust-dedup
	go build -o $(BUILD_DIR)/$(BINARY).exe ./cmd/api/main.go
	go build -tags clustertest -o $(BUILD_DIR)/cluster_loadtest.exe cluster_loadtest.go

# Build Rust dedup library used by cgo bindings
rust-dedup:
	cd $(RUST_DEDUP_DIR) && cargo build --release
	$(RUST_COPY_CMD)

# Run tests (requires Rust dedup shared library)
test: rust-dedup
	go test ./...

# Generate protobuf code
proto:
	protoc --go_out=. --go-grpc_out=. proto/events.proto

# Clean everything
clean:
	$(CLEAN_BUILD_CMD)
	$(CLEAN_DATA_CMD)

# Clean data only
clean-data:
	$(CLEAN_DATA_CMD)

# Start Node 1 (Bootstrap/Leader)
node1:
	go run ./cmd/api \
		--node-id=node1 \
		--cluster \
		--partition-count=$(PARTITION_COUNT) \
		--replication-factor=$(REPLICATION_FACTOR) \
		--fsync-mode=$(FSYNC_MODE) \
		--flush-interval=$(FLUSH_INTERVAL_MS) \
		--index-interval=$(INDEX_INTERVAL) \
		--segment-size=$(SEGMENT_SIZE_BYTES) \
		--virtual-nodes=$(VIRTUAL_NODES) \
		--max-credits=$(MAX_CREDITS) \
		--ack-timeout=$(ACK_TIMEOUT) \
		--data-dir=./data/node1 \
		--grpc-addr=127.0.0.1:9000 \
		--http-addr=127.0.0.1:8080 \
		--cluster-gossip-addr=127.0.0.1:7946 \
		--cluster-grpc-addr=127.0.0.1:7947 \
		--cluster-raft-addr=127.0.0.1:7948

# Start Node 2 (joins Node 1)
node2:
	go run ./cmd/api \
		--node-id=node2 \
		--cluster \
		--partition-count=$(PARTITION_COUNT) \
		--replication-factor=$(REPLICATION_FACTOR) \
		--fsync-mode=$(FSYNC_MODE) \
		--flush-interval=$(FLUSH_INTERVAL_MS) \
		--index-interval=$(INDEX_INTERVAL) \
		--segment-size=$(SEGMENT_SIZE_BYTES) \
		--virtual-nodes=$(VIRTUAL_NODES) \
		--max-credits=$(MAX_CREDITS) \
		--ack-timeout=$(ACK_TIMEOUT) \
		--data-dir=./data/node2 \
		--grpc-addr=127.0.0.1:9001 \
		--http-addr=127.0.0.1:8081 \
		--cluster-gossip-addr=127.0.0.1:7956 \
		--cluster-grpc-addr=127.0.0.1:7957 \
		--cluster-raft-addr=127.0.0.1:7958 \
		--cluster-seeds=127.0.0.1:7946

# Start Node 3 (joins Node 1)
node3:
	go run ./cmd/api \
		--node-id=node3 \
		--cluster \
		--partition-count=$(PARTITION_COUNT) \
		--replication-factor=$(REPLICATION_FACTOR) \
		--fsync-mode=$(FSYNC_MODE) \
		--flush-interval=$(FLUSH_INTERVAL_MS) \
		--index-interval=$(INDEX_INTERVAL) \
		--segment-size=$(SEGMENT_SIZE_BYTES) \
		--virtual-nodes=$(VIRTUAL_NODES) \
		--max-credits=$(MAX_CREDITS) \
		--ack-timeout=$(ACK_TIMEOUT) \
		--data-dir=./data/node3 \
		--grpc-addr=127.0.0.1:9002 \
		--http-addr=127.0.0.1:8082 \
		--cluster-gossip-addr=127.0.0.1:7966 \
		--cluster-grpc-addr=127.0.0.1:7967 \
		--cluster-raft-addr=127.0.0.1:7968 \
		--cluster-seeds=127.0.0.1:7946

# Show cluster startup instructions
cluster:
	@echo "To start a 3-node cluster, run these in separate terminals:"
	@echo ""
	@echo "  Terminal 1: make node1"
	@echo "  Terminal 2: make node2"
	@echo "  Terminal 3: make node3"
	@echo ""
	@echo "Node 1 must be started first (bootstrap node)."

# Run standard load test
loadtest:
	go run -tags clustertest cluster_loadtest.go -nodes=$(NODES) -publishers=$(PUBLISHERS) -events=$(EVENTS) -payload=$(PAYLOAD) -delay=$(SCHEDULE_DELAY) -topic=$(TOPIC) -round-robin=$(ROUND_ROBIN) -partition-count=$(PARTITION_COUNT)

# Run batch mode load test (high throughput)
loadtest-batch:
	go run -tags clustertest cluster_loadtest.go -nodes=$(NODES) -publishers=$(PUBLISHERS) -events=$(EVENTS) -payload=$(PAYLOAD) -delay=$(SCHEDULE_DELAY) -topic=$(TOPIC) -round-robin=$(ROUND_ROBIN) -batch -batch-size=$(BATCH_SIZE) -partition-count=$(PARTITION_COUNT)

# Run recommended max-throughput profile
loadtest-max:
	go run -tags clustertest cluster_loadtest.go -nodes=3 -publishers=24 -events=40000 -payload=256 -delay=0 -topic=cluster-loadtest -round-robin=true -batch -batch-size=4000 -partition-count=16

# Run smaller test
loadtest-small:
	go run -tags clustertest cluster_loadtest.go -publishers=10 -events=1000

# Check cluster health
health:
	@echo "Checking node health..."
	@curl -s http://127.0.0.1:8080/health && echo " - Node 1 OK" || echo " - Node 1 FAIL"
	@curl -s http://127.0.0.1:8081/health && echo " - Node 2 OK" || echo " - Node 2 FAIL"
	@curl -s http://127.0.0.1:8082/health && echo " - Node 3 OK" || echo " - Node 3 FAIL"

# Docker commands
docker:
	docker build -t cronos-db:latest --no-cache .

docker-build:
	docker build -t cronos-db:latest --no-cache . && echo "Image built successfully"

docker-single:
	docker-compose up -d cronos-single

docker-cluster:
	docker-compose up -d cronos-node1 cronos-node2 cronos-node3

docker-logs:
	docker-compose logs -f

docker-down:
	docker-compose down

docker-clean:
	docker-compose down -v

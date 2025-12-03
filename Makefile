.PHONY: build clean proto node1 node2 node3 cluster loadtest loadtest-batch help

# Build settings
BINARY=cronos-api
BUILD_DIR=bin

# Load test settings (can be overridden: make loadtest-batch BATCH_SIZE=200)
PUBLISHERS?=30
EVENTS?=10000
BATCH_SIZE?=100

# Default target
help:
	@echo "CronosDB Makefile Commands:"
	@echo ""
	@echo "  make build          - Build the server binary"
	@echo "  make proto          - Regenerate protobuf code"
	@echo "  make clean          - Clean build artifacts and data"
	@echo ""
	@echo "  make node1          - Start node 1 (leader, port 9000)"
	@echo "  make node2          - Start node 2 (port 9001)"
	@echo "  make node3          - Start node 3 (port 9002)"
	@echo "  make cluster        - Show cluster startup commands"
	@echo ""
	@echo "  make loadtest       - Run standard load test"
	@echo "  make loadtest-batch - Run batch mode load test"
	@echo ""
	@echo "  make clean-data     - Remove data directories only"

# Build the server
build:
	go build -o $(BUILD_DIR)/$(BINARY).exe ./cmd/api/main.go
	go build -tags clustertest -o $(BUILD_DIR)/cluster_loadtest.exe cluster_loadtest.go

# Generate protobuf code
proto:
	protoc --go_out=. --go-grpc_out=. proto/events.proto

# Clean everything
clean:
	rm -rf $(BUILD_DIR)
	rm -rf data/node1 data/node2 data/node3

# Clean data only
clean-data:
	rm -rf data/node1 data/node2 data/node3

# Start Node 1 (Bootstrap/Leader)
node1:
	go run ./cmd/api \
		--node-id=node1 \
		--cluster \
		--partition-count=8 \
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
		--partition-count=8 \
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
		--partition-count=8 \
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
	go run -tags clustertest cluster_loadtest.go -publishers=$(PUBLISHERS) -events=$(EVENTS)

# Run batch mode load test (high throughput)
loadtest-batch:
	go run -tags clustertest cluster_loadtest.go -publishers=$(PUBLISHERS) -events=$(EVENTS) -batch -batch-size=$(BATCH_SIZE)

# Run smaller test
loadtest-small:
	go run -tags clustertest cluster_loadtest.go -publishers=10 -events=1000

# Check cluster health
health:
	@echo "Checking node health..."
	@curl -s http://127.0.0.1:8080/health && echo " - Node 1 OK" || echo " - Node 1 FAIL"
	@curl -s http://127.0.0.1:8081/health && echo " - Node 2 OK" || echo " - Node 2 FAIL"
	@curl -s http://127.0.0.1:8082/health && echo " - Node 3 OK" || echo " - Node 3 FAIL"

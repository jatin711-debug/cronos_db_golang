# CronosDB Makefile
# - Production-oriented defaults for local benchmarking and cluster bring-up.
# - Cross-platform support for Windows (PowerShell), Linux, and macOS.
# - Rust shared library build/staging for cgo integration.

.DEFAULT_GOAL := help

.PHONY: help print-config build ensure-build-dir rust-dedup test proto clean clean-data \
	verify-env lint ci node1 node2 node3 cluster loadtest loadtest-batch loadtest-max loadtest-small health \
	docker docker-build docker-single docker-cluster docker-logs docker-down docker-clean

# -----------------------------------------------------------------------------
# Tooling and build settings
# -----------------------------------------------------------------------------
BINARY ?= cronos-api
BUILD_DIR ?= bin
EXE_EXT :=

RUST_DEDUP_DIR ?= internal/dedup/rust
RUST_TARGET_DIR ?= $(RUST_DEDUP_DIR)/target/release
RUST_STAGE_DIR ?= internal/dedup
RUST_PROFILE ?= release
RUST_LIB_BASENAME ?= cronos_dedup

DOCKER_COMPOSE ?= docker compose

CARGO_PROFILE_FLAG := --release
ifneq ($(RUST_PROFILE),release)
CARGO_PROFILE_FLAG := --profile $(RUST_PROFILE)
endif

# Platform detection and OS-specific commands.
ifeq ($(OS),Windows_NT)
PLATFORM := windows
EXE_EXT := .exe
RUST_SHARED_LIB := $(RUST_LIB_BASENAME).dll

MKDIR_BUILD_CMD = powershell -NoProfile -Command "New-Item -ItemType Directory -Force '$(BUILD_DIR)' | Out-Null"
VERIFY_RUST_LIB_CMD = powershell -NoProfile -Command "if (!(Test-Path '$(RUST_TARGET_DIR)/$(RUST_SHARED_LIB)')) { throw 'Rust artifact missing: $(RUST_TARGET_DIR)/$(RUST_SHARED_LIB)' }"
STAGE_RUST_LIB_CMD = powershell -NoProfile -Command "New-Item -ItemType Directory -Force '$(RUST_STAGE_DIR)' | Out-Null; Copy-Item -Force '$(RUST_TARGET_DIR)/$(RUST_SHARED_LIB)' '$(RUST_STAGE_DIR)/'; Copy-Item -Force '$(RUST_TARGET_DIR)/$(RUST_SHARED_LIB)' './'"
CLEAN_BUILD_CMD = powershell -NoProfile -Command "if (Test-Path '$(BUILD_DIR)') { Remove-Item -Recurse -Force '$(BUILD_DIR)' }"
CLEAN_DATA_CMD = powershell -NoProfile -Command "foreach ($$d in @('data/node1','data/node2','data/node3')) { if (Test-Path $$d) { Remove-Item -Recurse -Force $$d } }"

# On Windows, placing the DLL in repo root and internal/dedup is generally
# sufficient for go run/go test dynamic loading in local workflows.
GO_RUNTIME_PREFIX :=

VERIFY_GO_CMD = where go >NUL 2>&1 || (echo [verify-env] Missing required tool: go && exit /b 1)
VERIFY_CARGO_CMD = where cargo >NUL 2>&1 || (echo [verify-env] Missing required tool: cargo && exit /b 1)
VERIFY_PROTOC_CMD = where protoc >NUL 2>&1 || (echo [verify-env] Missing required tool: protoc && exit /b 1)
VERIFY_DOCKER_CMD = where docker >NUL 2>&1 || (echo [verify-env] Missing required tool: docker && exit /b 1)
VERIFY_DOCKER_COMPOSE_CMD = docker compose version >NUL 2>&1 || (echo [verify-env] Missing required tool: docker compose && exit /b 1)

else
UNAME_S := $(shell uname -s 2>/dev/null)
PLATFORM := linux
RUST_RUNTIME_ENV := LD_LIBRARY_PATH
RUST_SHARED_LIB := lib$(RUST_LIB_BASENAME).so

ifeq ($(UNAME_S),Darwin)
PLATFORM := macos
RUST_RUNTIME_ENV := DYLD_LIBRARY_PATH
RUST_SHARED_LIB := lib$(RUST_LIB_BASENAME).dylib
endif

MKDIR_BUILD_CMD = mkdir -p "$(BUILD_DIR)"
VERIFY_RUST_LIB_CMD = test -f "$(RUST_TARGET_DIR)/$(RUST_SHARED_LIB)" || (echo "Rust artifact missing: $(RUST_TARGET_DIR)/$(RUST_SHARED_LIB)" && exit 1)
STAGE_RUST_LIB_CMD = mkdir -p "$(RUST_STAGE_DIR)" && cp -f "$(RUST_TARGET_DIR)/$(RUST_SHARED_LIB)" "$(RUST_STAGE_DIR)/"
CLEAN_BUILD_CMD = rm -rf "$(BUILD_DIR)"
CLEAN_DATA_CMD = rm -rf data/node1 data/node2 data/node3

# Ensure the Rust shared library is discoverable by dynamic linker for run/test.
GO_RUNTIME_PREFIX = $(RUST_RUNTIME_ENV)="$(abspath $(RUST_TARGET_DIR)):$$$(RUST_RUNTIME_ENV)"

VERIFY_GO_CMD = command -v go >/dev/null 2>&1 || (echo '[verify-env] Missing required tool: go' && exit 1)
VERIFY_CARGO_CMD = command -v cargo >/dev/null 2>&1 || (echo '[verify-env] Missing required tool: cargo' && exit 1)
VERIFY_PROTOC_CMD = command -v protoc >/dev/null 2>&1 || (echo '[verify-env] Missing required tool: protoc' && exit 1)
VERIFY_DOCKER_CMD = command -v docker >/dev/null 2>&1 || (echo '[verify-env] Missing required tool: docker' && exit 1)
VERIFY_DOCKER_COMPOSE_CMD = docker compose version >/dev/null 2>&1 || (echo '[verify-env] Missing required tool: docker compose' && exit 1)
endif

# -----------------------------------------------------------------------------
# Throughput profile settings (override at invocation time)
# Example: make loadtest-batch PUBLISHERS=32 EVENTS=100000 BATCH_SIZE=4000
# -----------------------------------------------------------------------------
PARTITION_COUNT ?= 16
REPLICATION_FACTOR ?= 1
FSYNC_MODE ?= periodic
FLUSH_INTERVAL_MS ?= 100
INDEX_INTERVAL ?= 4096
SEGMENT_SIZE_BYTES ?= 1073741824
VIRTUAL_NODES ?= 2048
MAX_CREDITS ?= 50000
ACK_TIMEOUT ?= 60s

# Load test settings
NODES ?= 3
PUBLISHERS ?= 24
EVENTS ?= 50000
PAYLOAD ?= 256
SCHEDULE_DELAY ?= 0
TOPIC ?= cluster-loadtest
ROUND_ROBIN ?= true
BATCH_SIZE ?= 4000

print-config:
	@echo Platform:            $(PLATFORM)
	@echo Binary output:       $(BUILD_DIR)/$(BINARY)$(EXE_EXT)
	@echo Rust shared library: $(RUST_TARGET_DIR)/$(RUST_SHARED_LIB)
	@echo Rust stage dir:      $(RUST_STAGE_DIR)

help:
	@echo CronosDB Makefile Commands
	@echo.
	@echo Build/Test
	@echo   make verify-env     - Validate go/cargo/protoc/docker toolchain presence
	@echo   make lint           - Run go vet + gofmt checks
	@echo   make ci             - Run rust-dedup, build, test, lint in sequence
	@echo   make build          - Build API and cluster load test binaries
	@echo   make rust-dedup     - Build and stage Rust dedup shared library
	@echo   make test           - Build Rust dedup library and run go tests
	@echo   make proto          - Regenerate protobuf code
	@echo   make clean          - Remove build artifacts and node data
	@echo   make clean-data     - Remove node data only
	@echo.
	@echo Cluster
	@echo   make node1          - Start node 1 (bootstrap)
	@echo   make node2          - Start node 2 (joins node1)
	@echo   make node3          - Start node 3 (joins node1)
	@echo   make cluster        - Print cluster startup order
	@echo   make health         - Check node health endpoints
	@echo.
	@echo Load Test
	@echo   make loadtest       - Run single-event mode cluster load test
	@echo   make loadtest-batch - Run batch mode cluster load test
	@echo   make loadtest-max   - Recommended max-throughput profile
	@echo.
	@echo Docker
	@echo   make docker         - Build image
	@echo   make docker-single  - Start single-node container
	@echo   make docker-cluster - Start 3-node container cluster

verify-env:
	@echo Verifying required tools...
	@$(VERIFY_GO_CMD)
	@$(VERIFY_CARGO_CMD)
	@$(VERIFY_PROTOC_CMD)
	@$(VERIFY_DOCKER_CMD)
	@$(VERIFY_DOCKER_COMPOSE_CMD)
	@echo Environment verification passed.

lint:
	@echo Running lint checks...
	$(GO_RUNTIME_PREFIX) go vet ./...
ifeq ($(OS),Windows_NT)
	@powershell -NoProfile -Command "$$u = gofmt -l .; if ($$u) { Write-Output 'Unformatted Go files:'; $$u; exit 1 }"
else
	@u="$$(gofmt -l .)"; if [ -n "$$u" ]; then echo "Unformatted Go files:"; echo "$$u"; exit 1; fi
endif
	@echo Lint checks passed.

ci: verify-env
	@echo [ci] Step 1/4: rust-dedup
	@$(MAKE) rust-dedup
	@echo [ci] Step 2/4: build
	@$(MAKE) ensure-build-dir
	$(GO_RUNTIME_PREFIX) go build -o $(BUILD_DIR)/$(BINARY)$(EXE_EXT) ./cmd/api/main.go
	$(GO_RUNTIME_PREFIX) go build -tags clustertest -o $(BUILD_DIR)/cluster_loadtest$(EXE_EXT) cluster_loadtest.go
	@echo [ci] Step 3/4: test
	$(GO_RUNTIME_PREFIX) go test ./...
	@echo [ci] Step 4/4: lint
	@$(MAKE) lint
	@echo CI checks passed.

ensure-build-dir:
	@$(MKDIR_BUILD_CMD)

# Build Rust dedup library used by cgo bindings, then stage runtime artifact.
rust-dedup:
	cd $(RUST_DEDUP_DIR) && cargo build $(CARGO_PROFILE_FLAG)
	@$(VERIFY_RUST_LIB_CMD)
	@$(STAGE_RUST_LIB_CMD)

# Build server binaries.
build: rust-dedup ensure-build-dir
	$(GO_RUNTIME_PREFIX) go build -o $(BUILD_DIR)/$(BINARY)$(EXE_EXT) ./cmd/api/main.go
	$(GO_RUNTIME_PREFIX) go build -tags clustertest -o $(BUILD_DIR)/cluster_loadtest$(EXE_EXT) cluster_loadtest.go

# Run all tests. Rust shared library is built/staged first.
test: rust-dedup
	$(GO_RUNTIME_PREFIX) go test ./...

proto:
	protoc --go_out=. --go-grpc_out=. proto/events.proto

clean:
	@$(CLEAN_BUILD_CMD)
	@$(CLEAN_DATA_CMD)

clean-data:
	@$(CLEAN_DATA_CMD)

# Start Node 1 (Bootstrap/Leader)
node1: rust-dedup
	$(GO_RUNTIME_PREFIX) go run ./cmd/api \
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
node2: rust-dedup
	$(GO_RUNTIME_PREFIX) go run ./cmd/api \
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
node3: rust-dedup
	$(GO_RUNTIME_PREFIX) go run ./cmd/api \
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

cluster:
	@echo To start a 3-node cluster, run these in separate terminals:
	@echo   Terminal 1: make node1
	@echo   Terminal 2: make node2
	@echo   Terminal 3: make node3
	@echo Node 1 must be started first (bootstrap node).

loadtest:
	go run -tags clustertest cluster_loadtest.go -nodes=$(NODES) -publishers=$(PUBLISHERS) -events=$(EVENTS) -payload=$(PAYLOAD) -delay=$(SCHEDULE_DELAY) -topic=$(TOPIC) -round-robin=$(ROUND_ROBIN) -partition-count=$(PARTITION_COUNT)

loadtest-batch:
	go run -tags clustertest cluster_loadtest.go -nodes=$(NODES) -publishers=$(PUBLISHERS) -events=$(EVENTS) -payload=$(PAYLOAD) -delay=$(SCHEDULE_DELAY) -topic=$(TOPIC) -round-robin=$(ROUND_ROBIN) -batch -batch-size=$(BATCH_SIZE) -partition-count=$(PARTITION_COUNT)

loadtest-max:
	go run -tags clustertest cluster_loadtest.go -nodes=3 -publishers=24 -events=40000 -payload=256 -delay=0 -topic=cluster-loadtest -round-robin=true -batch -batch-size=4000 -partition-count=16

loadtest-small:
	go run -tags clustertest cluster_loadtest.go -publishers=10 -events=1000

health:
	@echo "Checking node health..."
ifeq ($(OS),Windows_NT)
	@powershell -NoProfile -Command "$$targets=@(@{Name='Node 1'; Url='http://127.0.0.1:8080/health'}, @{Name='Node 2'; Url='http://127.0.0.1:8081/health'}, @{Name='Node 3'; Url='http://127.0.0.1:8082/health'}); foreach($$t in $$targets){ try { $$r=Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 $$t.Url; if($$r.StatusCode -eq 200){ Write-Output (' - '+$$t.Name+' OK') } else { Write-Output (' - '+$$t.Name+' FAIL ('+$$r.StatusCode+')') } } catch { Write-Output (' - '+$$t.Name+' FAIL') } }"
else
	@curl -fsS http://127.0.0.1:8080/health >/dev/null && echo " - Node 1 OK" || echo " - Node 1 FAIL"
	@curl -fsS http://127.0.0.1:8081/health >/dev/null && echo " - Node 2 OK" || echo " - Node 2 FAIL"
	@curl -fsS http://127.0.0.1:8082/health >/dev/null && echo " - Node 3 OK" || echo " - Node 3 FAIL"
endif

docker:
	docker build -t cronos-db:latest --no-cache .

docker-build:
	docker build -t cronos-db:latest --no-cache . && echo Image built successfully

docker-single:
	$(DOCKER_COMPOSE) up -d cronos-single

docker-cluster:
	$(DOCKER_COMPOSE) up -d cronos-node1 cronos-node2 cronos-node3

docker-logs:
	$(DOCKER_COMPOSE) logs -f

docker-down:
	$(DOCKER_COMPOSE) down

docker-clean:
	$(DOCKER_COMPOSE) down -v

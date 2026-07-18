# CronosDB Makefile
# - Production-oriented defaults for local benchmarking and cluster bring-up.
# - Cross-platform support for Windows (PowerShell), Linux, and macOS.
# - Rust shared library build/staging for cgo integration.

.DEFAULT_GOAL := help

.PHONY: help print-config build ensure-build-dir rust-dedup test test-unit test-integration test-chaos proto clean clean-data \
	verify-env verify-tag-env verify-release-env tag-preflight release-preflight lint ci tag tag-push release publish \
	node1 node2 node3 cluster loadtest loadtest-single loadtest-batch loadtest-max loadtest-small loadtest-throughput health \
	docker docker-build docker-build-no-cache docker-build-hub docker-push docker-push-hub docker-single docker-cluster docker-logs docker-down docker-clean \
	observability-up observability-down dashboard

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
DOCKER_IMAGE_NAME ?= cronos-db
DOCKER_HUB_USER ?= johnny711dock
DOCKER_TAG ?= latest
REMOTE ?= origin
VERSION ?=
RELEASE_TITLE ?= CronosDB $(VERSION)
RELEASE_NOTES_FILE ?=
RELEASE_DRAFT ?= false
RELEASE_PRERELEASE ?= false

GH_RELEASE_NOTES_ARGS := --generate-notes
ifneq ($(strip $(RELEASE_NOTES_FILE)),)
GH_RELEASE_NOTES_ARGS := --notes-file "$(RELEASE_NOTES_FILE)"
endif

GH_RELEASE_EXTRA_FLAGS :=
ifeq ($(RELEASE_DRAFT),true)
GH_RELEASE_EXTRA_FLAGS += --draft
endif
ifeq ($(RELEASE_PRERELEASE),true)
GH_RELEASE_EXTRA_FLAGS += --prerelease
endif

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
STAGE_RUST_LIB_CMD = powershell -NoProfile -ExecutionPolicy Bypass -File scripts/stage-rust-lib.ps1 -SourceDir '$(RUST_TARGET_DIR)' -BaseName '$(RUST_LIB_BASENAME)' -Destinations '$(RUST_STAGE_DIR);$(BUILD_DIR);.'
# Copy web/dashboard/dist/ into internal/api/web/dist/ so //go:embed
# picks up the SPA at compile time. internal/api/web_handler.go embeds
# web/dist/* (the relative path from the Go file's package directory).
STAGE_DASHBOARD_DIST_CMD = powershell -NoProfile -Command "if (Test-Path 'web/dashboard/dist') { if (Test-Path 'internal/api/web/dist') { Get-ChildItem 'internal/api/web/dist' -Exclude 'README.txt' -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force }; New-Item -ItemType Directory -Force 'internal/api/web/dist' | Out-Null; Copy-Item -Recurse -Force 'web/dashboard/dist/*' 'internal/api/web/dist/' }"
CLEAN_BUILD_CMD = powershell -NoProfile -Command "if (Test-Path '$(BUILD_DIR)') { Remove-Item -Recurse -Force '$(BUILD_DIR)' }; if (Test-Path 'internal/api/web/dist') { Get-ChildItem 'internal/api/web/dist' -Exclude 'README.txt' | Remove-Item -Recurse -Force }"
CLEAN_DATA_CMD = powershell -NoProfile -Command "foreach ($$d in @('$(DATA_ROOT)/node1','$(DATA_ROOT)/node2','$(DATA_ROOT)/node3')) { if (Test-Path $$d) { Remove-Item -Recurse -Force $$d } }"

# On Windows the cgo-linked cronos_dedup.dll must be on the DLL search path when
# go runs the test/server binaries. Their working directory is the package dir,
# which does not contain the DLL, so a repo-root copy alone is not found. Prepend
# the staged DLL directory to PATH — the Windows equivalent of the Unix
# LD_LIBRARY_PATH line above. cygpath converts make's mixed-form abspath (D:/...)
# into the MSYS form (/d/...) that a shell PATH list requires; the D:/... form is
# mis-parsed as two entries and the DLL fails to load (0xC0000135).
GO_RUNTIME_PREFIX = PATH="$$(cygpath -u '$(abspath $(RUST_STAGE_DIR))'):$$PATH"

VERIFY_GO_CMD = powershell -NoProfile -Command "if (-not (Get-Command go -ErrorAction SilentlyContinue)) { Write-Error '[verify-env] Missing required tool: go'; exit 1 }"
VERIFY_CARGO_CMD = powershell -NoProfile -Command "if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) { Write-Error '[verify-env] Missing required tool: cargo'; exit 1 }"
VERIFY_PROTOC_CMD = powershell -NoProfile -Command "if (-not (Get-Command protoc -ErrorAction SilentlyContinue)) { Write-Error '[verify-env] Missing required tool: protoc'; exit 1 }"
VERIFY_DOCKER_CMD = powershell -NoProfile -Command "if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { Write-Error '[verify-env] Missing required tool: docker'; exit 1 }"
VERIFY_DOCKER_COMPOSE_CMD = powershell -NoProfile -Command 'docker compose version *> $$null; if ($$LASTEXITCODE -ne 0) { Write-Error '[verify-env] Missing required tool: docker compose'; exit 1 }'
VERIFY_GIT_CMD = powershell -NoProfile -Command "if (-not (Get-Command git -ErrorAction SilentlyContinue)) { Write-Error '[verify-tag-env] Missing required tool: git'; exit 1 }"
VERIFY_GH_CMD = powershell -NoProfile -Command "if (-not (Get-Command gh -ErrorAction SilentlyContinue)) { Write-Error '[verify-release-env] Missing required tool: gh'; exit 1 }"
VERIFY_GH_AUTH_CMD = powershell -NoProfile -Command 'gh auth status *> $$null; if ($$LASTEXITCODE -ne 0) { Write-Error '[verify-release-env] Not authenticated. Run: gh auth login'; exit 1 }'

# Windows resolves `npm` via `npm.cmd`. .cmd shims are not directly executable
# from MSYS make, so we use the .cmd form. The invocation body itself is
# platform-portable (npm install + npm run build).
NPM_CMD = npm.cmd
VERIFY_NPM_CMD = powershell -NoProfile -Command "if (-not (Get-Command npm.cmd -ErrorAction SilentlyContinue)) { Write-Error '[verify-env] Missing required tool: npm'; exit 1 }"

REQUIRE_VERSION_CMD = powershell -NoProfile -Command "if ([string]::IsNullOrWhiteSpace('$(VERSION)')) { Write-Error 'VERSION is required. Example: make tag VERSION=v0.2.1'; exit 1 }"
VALIDATE_VERSION_CMD = powershell -NoProfile -Command "if ('$(VERSION)' -notmatch '^v\d+\.\d+\.\d+([.-][0-9A-Za-z.-]+)?$$') { Write-Error 'VERSION must look like vMAJOR.MINOR.PATCH'; exit 1 }"
VERIFY_GIT_CLEAN_CMD = powershell -NoProfile -Command '$$s = git status --porcelain; if ($$s) { Write-Error "Working tree not clean. Commit or stash changes before tagging."; exit 1 }'
VERIFY_TAG_EXISTS_LOCAL_CMD = powershell -NoProfile -Command 'git rev-parse -q --verify refs/tags/$(VERSION) 2>$$null >$$null; if ($$LASTEXITCODE -ne 0) { Write-Error "Missing local tag $(VERSION). Run make tag VERSION=$(VERSION) first."; exit 1 }'
VERIFY_TAG_ABSENT_LOCAL_CMD = powershell -NoProfile -Command 'git rev-parse -q --verify refs/tags/$(VERSION) 2>$$null >$$null; if ($$LASTEXITCODE -eq 0) { Write-Error "Tag already exists locally: $(VERSION)"; exit 1 }; exit 0'
VERIFY_TAG_EXISTS_REMOTE_CMD = powershell -NoProfile -Command '$$r = git ls-remote --tags $(REMOTE) refs/tags/$(VERSION); if (-not $$r) { Write-Error "Tag $(VERSION) not found on remote $(REMOTE). Run make tag-push VERSION=$(VERSION)."; exit 1 }'
VERIFY_TAG_ABSENT_REMOTE_CMD = powershell -NoProfile -Command '$$r = git ls-remote --tags $(REMOTE) refs/tags/$(VERSION); if ($$r) { Write-Error "Tag already exists on remote $(REMOTE): $(VERSION)"; exit 1 }'

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
STAGE_RUST_LIB_CMD = mkdir -p "$(RUST_STAGE_DIR)" && mkdir -p "$(BUILD_DIR)" && cp -f "$(RUST_TARGET_DIR)/$(RUST_SHARED_LIB)" "$(RUST_STAGE_DIR)/" && cp -f "$(RUST_TARGET_DIR)/$(RUST_SHARED_LIB)" "$(BUILD_DIR)/"
# Copy web/dashboard/dist/ into internal/api/web/dist/ so //go:embed
# picks up the SPA at compile time. internal/api/web_handler.go embeds
# web/dist/* (the relative path from the Go file's package directory).
STAGE_DASHBOARD_DIST_CMD = if [ -d web/dashboard/dist ]; then mkdir -p internal/api/web/dist && find internal/api/web/dist -mindepth 1 -not -name 'README.txt' -delete 2>/dev/null || true && cp -R web/dashboard/dist/. internal/api/web/dist/; fi
CLEAN_BUILD_CMD = rm -rf "$(BUILD_DIR)" && find internal/api/web/dist -mindepth 1 -not -name 'README.txt' -delete 2>/dev/null || true
CLEAN_DATA_CMD = rm -rf "$(DATA_ROOT)/node1" "$(DATA_ROOT)/node2" "$(DATA_ROOT)/node3"

# Ensure the Rust shared library is discoverable by dynamic linker for run/test.
GO_RUNTIME_PREFIX = $(RUST_RUNTIME_ENV)="$(abspath $(RUST_TARGET_DIR)):$$$(RUST_RUNTIME_ENV)"

VERIFY_GO_CMD = command -v go >/dev/null 2>&1 || (echo '[verify-env] Missing required tool: go' && exit 1)
VERIFY_CARGO_CMD = command -v cargo >/dev/null 2>&1 || (echo '[verify-env] Missing required tool: cargo' && exit 1)
VERIFY_PROTOC_CMD = command -v protoc >/dev/null 2>&1 || (echo '[verify-env] Missing required tool: protoc' && exit 1)
VERIFY_DOCKER_CMD = command -v docker >/dev/null 2>&1 || (echo '[verify-env] Missing required tool: docker' && exit 1)
VERIFY_DOCKER_COMPOSE_CMD = docker compose version >/dev/null 2>&1 || (echo '[verify-env] Missing required tool: docker compose' && exit 1)
VERIFY_GIT_CMD = command -v git >/dev/null 2>&1 || (echo '[verify-tag-env] Missing required tool: git' && exit 1)
VERIFY_GH_CMD = command -v gh >/dev/null 2>&1 || (echo '[verify-release-env] Missing required tool: gh' && exit 1)
VERIFY_GH_AUTH_CMD = gh auth status >/dev/null 2>&1 || (echo '[verify-release-env] Not authenticated. Run: gh auth login' && exit 1)

NPM_CMD = npm
VERIFY_NPM_CMD = command -v npm >/dev/null 2>&1 || (echo '[verify-env] Missing required tool: npm' && exit 1)

REQUIRE_VERSION_CMD = test -n "$(strip $(VERSION))" || (echo 'VERSION is required. Example: make tag VERSION=v0.2.1' && exit 1)
VALIDATE_VERSION_CMD = printf '%s' "$(VERSION)" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$$' || (echo 'VERSION must look like vMAJOR.MINOR.PATCH' && exit 1)
VERIFY_GIT_CLEAN_CMD = test -z "$$(git status --porcelain)" || (echo 'Working tree not clean. Commit or stash changes before tagging.' && exit 1)
VERIFY_TAG_EXISTS_LOCAL_CMD = git rev-parse -q --verify refs/tags/$(VERSION) >/dev/null 2>&1 || (echo 'Missing local tag $(VERSION). Run make tag VERSION=$(VERSION) first.' && exit 1)
VERIFY_TAG_ABSENT_LOCAL_CMD = git rev-parse -q --verify refs/tags/$(VERSION) >/dev/null 2>&1 || (echo 'Tag already exists locally: $(VERSION)' && exit 1)
VERIFY_TAG_EXISTS_REMOTE_CMD = test -n "$$(git ls-remote --tags $(REMOTE) refs/tags/$(VERSION))" || (echo 'Tag $(VERSION) not found on remote $(REMOTE). Run make tag-push VERSION=$(VERSION).' && exit 1)
VERIFY_TAG_ABSENT_REMOTE_CMD = test -z "$$(git ls-remote --tags $(REMOTE) refs/tags/$(VERSION))" || (echo 'Tag already exists on remote $(REMOTE): $(VERSION)' && exit 1)
endif

# -----------------------------------------------------------------------------
# Throughput profile settings (override at invocation time)
# Example: make loadtest-batch PUBLISHERS=32 EVENTS=100000 BATCH_SIZE=4000
# -----------------------------------------------------------------------------
PARTITION_COUNT ?= 32
REPLICATION_FACTOR ?= 1
FSYNC_MODE ?= periodic
FLUSH_INTERVAL_MS ?= 100
INDEX_INTERVAL ?= 4096
# 512MB matches the server default and avoids allocating 1GB per active
# partition. Override for large-disk benchmarks when rotation cost matters.
SEGMENT_SIZE_BYTES ?= 536870912
VIRTUAL_NODES ?= 2048
MAX_CREDITS ?= 50000
ACK_TIMEOUT ?= 60s
TRACING_ENABLED ?= false
TRACING_EXPORTER ?= otlp
TRACING_OTLP_ENDPOINT ?= 127.0.0.1:4317
TRACING_SAMPLE_RATIO ?= 0.01
TRACING_INSECURE ?= true
DATA_ROOT ?= ./data
NODE1_GRPC ?= 127.0.0.1:9000
NODE1_HTTP ?= 127.0.0.1:8080
NODE2_GRPC ?= 127.0.0.1:9001
NODE2_HTTP ?= 127.0.0.1:8081
NODE3_GRPC ?= 127.0.0.1:9002
NODE3_HTTP ?= 127.0.0.1:8082

# Load test settings
NODES ?= 3
PUBLISHERS ?= 24
EVENTS ?= 50000
PAYLOAD ?= 256
SCHEDULE_DELAY ?= 0
TOPIC ?= cluster-loadtest
ROUND_ROBIN ?= true
BATCH_SIZE ?= 4000
LOADTEST_BATCH ?= true
LOADTEST_ALLOW_DUPLICATE ?= true
THROUGHPUT_BATCH_SIZE ?= 2000
LOADTEST_NODE_FLAGS = -node1-grpc=$(NODE1_GRPC) -node1-http=$(NODE1_HTTP) -node2-grpc=$(NODE2_GRPC) -node2-http=$(NODE2_HTTP) -node3-grpc=$(NODE3_GRPC) -node3-http=$(NODE3_HTTP)

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
	@echo   make build-api      - Build only the cronos-api server binary
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
	@echo '  (set TRACING_ENABLED=true to export traces with low sample ratio)'
	@echo   make cluster        - Print cluster startup order
	@echo   make health         - Check node health endpoints
	@echo   make demo           - Run the publish/subscribe demo (start a node first)
	@echo.
	@echo Load Test
	@echo   make loadtest       - Run batch mode cluster load test (LOADTEST_BATCH=false for single-event mode)
	@echo   make loadtest-single - Run single-event mode cluster load test
	@echo   make loadtest-batch - Run batch mode cluster load test
	@echo   make loadtest-max   - Recommended max-throughput profile
	@echo   make loadtest-throughput - Aggressive throughput benchmark (compiled binary, large payload)
	@echo.
	@echo Docker
	@echo   make docker         - Build image (cached)
	@echo   make docker-build-no-cache - Build image (no cache)
	@echo   make docker-build-hub - Build Docker Hub image: johnny711dock/cronos-db:latest
	@echo   make docker-push      - Push Docker Hub image
	@echo   make docker-push-hub  - Build and push Docker Hub image
	@echo   make docker-single  - Start single-node container
	@echo   make docker-cluster - Start 3-node container cluster
	@echo.
	@echo Observability
	@echo   make observability-up   - Start Prometheus + Grafana + Tempo + OTEL Collector
	@echo   make observability-down - Stop/remove observability services
	@echo.
	@echo Release
	@echo   make tag VERSION=v0.2.1      - Create local annotated tag (requires clean git state)
	@echo   make tag-push VERSION=v0.2.1 - Push an existing local tag to REMOTE (default: origin)
	@echo   make release VERSION=v0.2.1  - Create GitHub Release from pushed tag using gh CLI
	@echo '  make publish VERSION=v0.2.1  - Run ci (includes rust-dedup), tag, push, and release'
	@echo   Optional overrides: REMOTE=origin RELEASE_NOTES_FILE=notes.md RELEASE_DRAFT=true RELEASE_PRERELEASE=true

verify-env:
	@echo Verifying required tools...
	@$(VERIFY_GO_CMD)
	@$(VERIFY_CARGO_CMD)
	@$(VERIFY_PROTOC_CMD)
	@$(VERIFY_DOCKER_CMD)
	@$(VERIFY_DOCKER_COMPOSE_CMD)
	@echo Environment verification passed.

verify-tag-env:
	@echo Verifying tag tooling...
	@$(VERIFY_GIT_CMD)
	@echo Tag tooling verification passed.

verify-release-env: verify-tag-env
	@echo Verifying release tooling...
	@$(VERIFY_GH_CMD)
	@$(VERIFY_GH_AUTH_CMD)
	@echo Release tooling verification passed.

tag-preflight: verify-tag-env
	@$(REQUIRE_VERSION_CMD)
	@$(VALIDATE_VERSION_CMD)
	@$(VERIFY_GIT_CLEAN_CMD)

release-preflight: verify-release-env
	@$(REQUIRE_VERSION_CMD)
	@$(VALIDATE_VERSION_CMD)

lint:
	@echo Running lint checks...
	$(GO_RUNTIME_PREFIX) go vet ./...
ifeq ($(OS),Windows_NT)
	@powershell -NoProfile -Command "if (gofmt -l .) { Write-Output 'Unformatted Go files:'; gofmt -l .; exit 1 }"
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
build: dashboard rust-dedup ensure-build-dir
	@$(STAGE_DASHBOARD_DIST_CMD)
	$(GO_RUNTIME_PREFIX) go build -o $(BUILD_DIR)/$(BINARY)$(EXE_EXT) ./cmd/api/main.go
	$(GO_RUNTIME_PREFIX) go build -tags clustertest -o $(BUILD_DIR)/cluster_loadtest$(EXE_EXT) cluster_loadtest.go

# Build only the cronos-api server binary (no dashboard, no loadtest).
build-api: rust-dedup ensure-build-dir
	$(GO_RUNTIME_PREFIX) go build -o $(BUILD_DIR)/$(BINARY)$(EXE_EXT) ./cmd/api/main.go

# Run all tests. Rust shared library is built/staged first.
test: rust-dedup test-unit

# Run unit tests only: excludes the integration (tests/integration) and chaos
# (tests/chaos) suites which require a running cluster or docker. The chaos
# suite is additionally gated by the `chaos` build tag, so it never compiles
# into a plain `go test` run.
test-unit: rust-dedup
	$(GO_RUNTIME_PREFIX) go test $$({ go list ./... 2>/dev/null || true; } | grep -vE '/tests/integration|/tests/chaos')

# Run the integration suite against a running server. If no server is reachable
# at CRONOS_TEST_ADDR / CRONOS_TEST_HTTP_ADDR, the script starts a temporary
# single-node instance, runs the tests, and tears it down.
test-integration: rust-dedup build
	@bash scripts/run-integration-tests.sh

# Run the chaos suite (requires docker + a running cronos cluster).
test-chaos: rust-dedup
	$(GO_RUNTIME_PREFIX) go test -tags chaos -v ./tests/chaos/...

# Benchmark targets. The gate compares the geometric mean MB/s of the primary
# WAL batch benchmark against bench/baseline.txt and fails on a >10% drop.
bench-baseline:
	@mkdir -p bench
	$(GO_RUNTIME_PREFIX) go test ./internal/storage \
		-bench=BenchmarkWAL_AppendBatch_Matrix/fsync=periodic/payload=256B/batch=1000/par=16 \
		-benchtime=3s -count=3 > bench/baseline.txt

bench-gate:
	@bash scripts/check-benchmark-gate.sh

# Remove stale generated files so protoc always produces a clean output.
proto:
	@rm -f pkg/types/events.pb.go pkg/types/events_grpc.pb.go pkg/types/admin.pb.go pkg/types/admin_grpc.pb.go
	@rm -rf cronos_db/pkg/types github.com
	protoc --go_out=. --go_opt=module=github.com/jatin711-debug/cronos_db_golang --go-grpc_out=. --go-grpc_opt=module=github.com/jatin711-debug/cronos_db_golang proto/events.proto proto/admin.proto

# Build the admin dashboard SPA at web/dashboard/. This target is
# opt-in (not part of the default `make build` chain) so Go-only builds
# don't pull in Node. Step 5 will copy the resulting dist/ into the Go
# binary via //go:embed.
dashboard:
	@echo "Building admin dashboard SPA..."
	@cd web/dashboard && $(NPM_CMD) install --no-audit --no-fund
	@cd web/dashboard && $(NPM_CMD) run build

tag: tag-preflight
	@$(VERIFY_TAG_ABSENT_LOCAL_CMD)
	@$(VERIFY_TAG_ABSENT_REMOTE_CMD)
	git tag -a $(VERSION) -m "release: $(VERSION)"
	@echo Created tag $(VERSION)

tag-push: verify-tag-env
	@$(REQUIRE_VERSION_CMD)
	@$(VALIDATE_VERSION_CMD)
	@$(VERIFY_TAG_EXISTS_LOCAL_CMD)
	@$(VERIFY_TAG_ABSENT_REMOTE_CMD)
	git push $(REMOTE) $(VERSION)
	@echo Pushed tag $(VERSION) to $(REMOTE)

release: release-preflight
	@$(VERIFY_TAG_EXISTS_LOCAL_CMD)
	@$(VERIFY_TAG_EXISTS_REMOTE_CMD)
	gh release create $(VERSION) --title "$(RELEASE_TITLE)" $(GH_RELEASE_NOTES_ARGS) $(GH_RELEASE_EXTRA_FLAGS)
	@echo Created GitHub release for $(VERSION)

publish: tag-preflight
	@echo '[publish] Step 1/4: ci (includes rust dedup build for $(PLATFORM))'
	@$(MAKE) ci
	@echo [publish] Step 2/4: tag
	@$(MAKE) tag VERSION=$(VERSION) REMOTE=$(REMOTE)
	@echo [publish] Step 3/4: tag-push
	@$(MAKE) tag-push VERSION=$(VERSION) REMOTE=$(REMOTE)
	@echo [publish] Step 4/4: release
	@$(MAKE) release VERSION=$(VERSION) REMOTE=$(REMOTE) RELEASE_TITLE="$(RELEASE_TITLE)" RELEASE_NOTES_FILE="$(RELEASE_NOTES_FILE)" RELEASE_DRAFT=$(RELEASE_DRAFT) RELEASE_PRERELEASE=$(RELEASE_PRERELEASE)
	@echo Published $(VERSION)

clean:
	@$(CLEAN_BUILD_CMD)
	@$(CLEAN_DATA_CMD)

clean-data:
	@$(CLEAN_DATA_CMD)

# Start Node 1 (Bootstrap/Leader)
node1: build
	$(GO_RUNTIME_PREFIX) $(BUILD_DIR)/$(BINARY)$(EXE_EXT) \
		--dev \
		--node-id=node1 \
		--auth-enabled=false \
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
		--tracing-enabled=$(TRACING_ENABLED) \
		--tracing-exporter=$(TRACING_EXPORTER) \
		--tracing-otlp-endpoint=$(TRACING_OTLP_ENDPOINT) \
		--tracing-sample-ratio=$(TRACING_SAMPLE_RATIO) \
		--tracing-insecure=$(TRACING_INSECURE) \
		--data-dir=$(DATA_ROOT)/node1 \
		--grpc-addr=$(NODE1_GRPC) \
		--http-addr=$(NODE1_HTTP) \
		--cluster-gossip-addr=127.0.0.1:7946 \
		--cluster-grpc-addr=127.0.0.1:7947 \
		--cluster-raft-addr=127.0.0.1:7948

# Start Node 2 (joins Node 1)
node2: build
	$(GO_RUNTIME_PREFIX) $(BUILD_DIR)/$(BINARY)$(EXE_EXT) \
		--dev \
		--node-id=node2 \
		--auth-enabled=false \
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
		--tracing-enabled=$(TRACING_ENABLED) \
		--tracing-exporter=$(TRACING_EXPORTER) \
		--tracing-otlp-endpoint=$(TRACING_OTLP_ENDPOINT) \
		--tracing-sample-ratio=$(TRACING_SAMPLE_RATIO) \
		--tracing-insecure=$(TRACING_INSECURE) \
		--data-dir=$(DATA_ROOT)/node2 \
		--grpc-addr=$(NODE2_GRPC) \
		--http-addr=$(NODE2_HTTP) \
		--cluster-gossip-addr=127.0.0.1:7956 \
		--cluster-grpc-addr=127.0.0.1:7957 \
		--cluster-raft-addr=127.0.0.1:7958 \
		--cluster-seeds=127.0.0.1:7946

# Start Node 3 (joins Node 1)
node3: build
	$(GO_RUNTIME_PREFIX) $(BUILD_DIR)/$(BINARY)$(EXE_EXT) \
		--dev \
		--node-id=node3 \
		--auth-enabled=false \
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
		--tracing-enabled=$(TRACING_ENABLED) \
		--tracing-exporter=$(TRACING_EXPORTER) \
		--tracing-otlp-endpoint=$(TRACING_OTLP_ENDPOINT) \
		--tracing-sample-ratio=$(TRACING_SAMPLE_RATIO) \
		--tracing-insecure=$(TRACING_INSECURE) \
		--data-dir=$(DATA_ROOT)/node3 \
		--grpc-addr=$(NODE3_GRPC) \
		--http-addr=$(NODE3_HTTP) \
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

loadtest: build
ifeq ($(LOADTEST_BATCH),true)
	$(BUILD_DIR)/cluster_loadtest$(EXE_EXT) $(LOADTEST_NODE_FLAGS) -nodes=$(NODES) -publishers=$(PUBLISHERS) -events=$(EVENTS) -payload=$(PAYLOAD) -delay=$(SCHEDULE_DELAY) -topic=$(TOPIC) -round-robin=$(ROUND_ROBIN) -batch -batch-size=$(BATCH_SIZE) -allow-duplicate=$(LOADTEST_ALLOW_DUPLICATE) -partition-count=$(PARTITION_COUNT)
else
	$(BUILD_DIR)/cluster_loadtest$(EXE_EXT) $(LOADTEST_NODE_FLAGS) -nodes=$(NODES) -publishers=$(PUBLISHERS) -events=$(EVENTS) -payload=$(PAYLOAD) -delay=$(SCHEDULE_DELAY) -topic=$(TOPIC) -round-robin=$(ROUND_ROBIN) -partition-count=$(PARTITION_COUNT)
endif

loadtest-single: build
	$(BUILD_DIR)/cluster_loadtest$(EXE_EXT) -nodes=$(NODES) -publishers=$(PUBLISHERS) -events=$(EVENTS) -payload=$(PAYLOAD) -delay=$(SCHEDULE_DELAY) -topic=$(TOPIC) -round-robin=$(ROUND_ROBIN) -partition-count=$(PARTITION_COUNT)

loadtest-batch: build
	$(BUILD_DIR)/cluster_loadtest$(EXE_EXT) $(LOADTEST_NODE_FLAGS) -nodes=$(NODES) -publishers=$(PUBLISHERS) -events=$(EVENTS) -payload=$(PAYLOAD) -delay=$(SCHEDULE_DELAY) -topic=$(TOPIC) -round-robin=$(ROUND_ROBIN) -batch -batch-size=$(BATCH_SIZE) -allow-duplicate=$(LOADTEST_ALLOW_DUPLICATE) -partition-count=$(PARTITION_COUNT)

loadtest-max: build
	$(BUILD_DIR)/cluster_loadtest$(EXE_EXT) $(LOADTEST_NODE_FLAGS) -nodes=3 -publishers=32 -events=200000 -payload=256 -delay=0 -topic=cluster-loadtest -round-robin=true -batch -batch-size=4000 -allow-duplicate=$(LOADTEST_ALLOW_DUPLICATE) -partition-count=$(PARTITION_COUNT)

loadtest-throughput: build
	$(BUILD_DIR)/cluster_loadtest$(EXE_EXT) $(LOADTEST_NODE_FLAGS) -nodes=3 -publishers=48 -events=200000 -payload=4096 -delay=0 -topic=cluster-loadtest -round-robin=true -batch -batch-size=$(THROUGHPUT_BATCH_SIZE) -allow-duplicate=$(LOADTEST_ALLOW_DUPLICATE) -partition-count=$(PARTITION_COUNT)

loadtest-small: build
	$(BUILD_DIR)/cluster_loadtest$(EXE_EXT) -publishers=10 -events=1000

health:
	@echo "Checking node health..."
ifeq ($(OS),Windows_NT)
	@powershell -NoProfile -Command "$$targets=@(@{Name='Node 1'; Url='http://127.0.0.1:8080/health'}, @{Name='Node 2'; Url='http://127.0.0.1:8081/health'}, @{Name='Node 3'; Url='http://127.0.0.1:8082/health'}); foreach($$t in $$targets){ try { $$r=Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 $$t.Url; if($$r.StatusCode -eq 200){ Write-Output (' - '+$$t.Name+' OK') } else { Write-Output (' - '+$$t.Name+' FAIL ('+$$r.StatusCode+')') } } catch { Write-Output (' - '+$$t.Name+' FAIL') } }"
else
	@curl -fsS http://127.0.0.1:8080/health >/dev/null && echo " - Node 1 OK" || echo " - Node 1 FAIL"
	@curl -fsS http://127.0.0.1:8081/health >/dev/null && echo " - Node 2 OK" || echo " - Node 2 FAIL"
	@curl -fsS http://127.0.0.1:8082/health >/dev/null && echo " - Node 3 OK" || echo " - Node 3 FAIL"
endif

# Run the publish/subscribe demo against a running local server.
# Start one first, e.g. `make node1` or `./bin/cronos-api --dev --node-id=node1 --data-dir=./data`.
demo:
	$(GO_RUNTIME_PREFIX) go run ./examples/pubsub_demo

docker: docker-build

docker-build:
	@echo Building Docker image: $(DOCKER_IMAGE_NAME):$(DOCKER_TAG)
	docker build -t $(DOCKER_IMAGE_NAME):$(DOCKER_TAG) .

docker-build-no-cache:
	@echo Building Docker image (no cache): $(DOCKER_IMAGE_NAME):$(DOCKER_TAG)
	docker build --no-cache -t $(DOCKER_IMAGE_NAME):$(DOCKER_TAG) .

docker-build-hub:
	@echo Building Docker Hub image: $(DOCKER_HUB_USER)/$(DOCKER_IMAGE_NAME):$(DOCKER_TAG)
	docker build -t $(DOCKER_HUB_USER)/$(DOCKER_IMAGE_NAME):$(DOCKER_TAG) .

docker-push:
	@echo Pushing Docker Hub image: $(DOCKER_HUB_USER)/$(DOCKER_IMAGE_NAME):$(DOCKER_TAG)
	docker push $(DOCKER_HUB_USER)/$(DOCKER_IMAGE_NAME):$(DOCKER_TAG)

docker-push-hub: docker-build-hub docker-push
	@echo Pushed $(DOCKER_HUB_USER)/$(DOCKER_IMAGE_NAME):$(DOCKER_TAG)

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

observability-up:
	$(DOCKER_COMPOSE) --profile observability up -d prometheus tempo otel-collector grafana

observability-down:
	$(DOCKER_COMPOSE) --profile observability stop grafana prometheus tempo otel-collector
	$(DOCKER_COMPOSE) --profile observability rm -f grafana prometheus tempo otel-collector

# Contributing to CronosDB

Thank you for considering a contribution to CronosDB.

## Where to Start

1. **Issues:** Check the issue tracker for bugs or requested features. Issues labeled `good first issue` are a great place to start.
2. **Discussions:** For new features or architectural changes, open an issue first so design trade-offs can be reviewed.

## Setup Environment

Prerequisites:

- **Go 1.26.10** (the version `go.mod` names)
- **Rust** / Cargo (optional: the bloom-filter library used through cgo; without cgo the server uses one written in Go)
- **protoc** (only to regenerate the gRPC code with `make proto`)
- **Docker** with `docker compose` (the container tests, and `make verify-env`)
- **npm** (admin dashboard SPA build)
- **Make** (optional but recommended; Windows and Unix targets are supported)

Build and local run:

```bash
make verify-env   # checks go, cargo, protoc, docker and docker compose
make build        # rust-dedup + dashboard stage + cronos-api
./bin/cronos-api --dev -node-id=node1 -data-dir=./data
```

Testing:

```bash
make test-unit          # unit suite (excludes live integration/chaos)
make lint               # gofmt + go vet

# Acceptance tests: three real server processes, a fault campaign with a ledger
# of every acknowledged event. They build the server and take several minutes.
go test -tags acceptance -count=1 -timeout 25m ./tests/acceptance/
```

### Windows helpers

After `make build`, these PowerShell helpers start a development node and run
the core unit suites with the staged Rust DLL on `PATH`:

```powershell
.\scripts\run-node-windows.ps1 -NodeID node1 -GRPCAddr localhost:9000
.\scripts\run-tests-windows.ps1
```

### Local cluster and load testing

Run `make node1`, `make node2`, and `make node3` in separate terminals, then
`make health`. These are development and benchmark presets. For a container
cluster, follow the [Docker guide](DOCKER.md#cluster-mode).

```bash
make loadtest-batch PUBLISHERS=20 EVENTS=50000 BATCH_SIZE=1000
make loadtest-max
```

The throughput presets use settings that differ from production. Use the
[measured profiles and their limits](docs/CLUSTER_PERFORMANCE_VALIDATION_2026-10-16.md)
when comparing results; the full recipes are in the [Makefile](Makefile).

### Documentation guidance

For documentation changes, build the complete site and fail on warnings:

```sh
python -m pip install -r requirements-docs.txt
python -m mkdocs build --strict --site-dir build/docs-check
```

Keep root docs and feature guides consistent with runtime behavior. Preserve
dated audits, benchmark results and probe output as evidence; update their
closure notes or add a clearly dated qualification instead of turning old
results into current claims. Protocol changes must document mixed-version
behavior, format activation and downgrade limits as well as the happy path.

Architecture and developer walkthrough:

- [README.md](README.md) — product overview and quick start
- [ARCHITECTURE.md](ARCHITECTURE.md) — full system architecture
- [docs/DEVELOPER_ARCHITECTURE_GUIDE.md](docs/DEVELOPER_ARCHITECTURE_GUIDE.md) — composition root and flows
- [docs/architecture/README.md](docs/architecture/README.md) — per-feature docs index
- [docs/PRODUCTION_RELEASE.md](docs/PRODUCTION_RELEASE.md) — what a release promises and the test behind each promise
- [docs/RETENTION_AND_EVENT_LIFECYCLE.md](docs/RETENTION_AND_EVENT_LIFECYCLE.md) — lifecycle identity, acceptance, retention and recovery
- [docs/REPLICATION_TERM_START.md](docs/REPLICATION_TERM_START.md) — current-term commitment and rolling upgrades

## Pull Request Process

1. **Fork & Branch:** Create a focused branch for your change, from `developement` (the development branch; `main` is fast-forwarded to it when CI passes).
2. **Code Style:** Run `gofmt` (or `make lint`). Prefer clear GoDoc on exported types and fields.
3. **Tests:** Add or update tests. Unit tests must pass; add integration/chaos coverage when changing cluster/replication paths.
4. **Docs:** Update architecture or feature docs when behavior or defaults change.
5. **Commits:** Use clear, conventional-style messages when possible (`fix:`, `docs:`, `feat:`).
6. **Submit PR:** Describe the problem, the approach, and how you verified it. CI (`production-checks`) runs on every pull request: build, vet, the suite under the race detector and without cgo, the acceptance fault campaign, container partition tests and an image scan, the production Helm chart in Kubernetes, and `govulncheck`.

## Code Review

All submissions require review. Please be open to feedback and iterate promptly.

Thank you for contributing!

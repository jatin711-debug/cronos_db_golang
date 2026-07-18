# Contributing to CronosDB

Thank you for considering a contribution to CronosDB.

## Where to Start

1. **Issues:** Check the issue tracker for bugs or requested features. Issues labeled `good first issue` are a great place to start.
2. **Discussions:** For new features or architectural changes, open an issue first so design trade-offs can be reviewed.

## Setup Environment

Prerequisites:

- **Go 1.25+** (module currently pins `go 1.25.12`)
- **Rust** / Cargo (bloom-filter FFI library via cgo)
- **protoc** (Protocol Buffers compiler)
- **npm** (admin dashboard SPA build)
- **Make** (optional but recommended; Windows and Unix targets are supported)

Build and local run:

```bash
make verify-env   # checks go, cargo, protoc, npm where available
make build        # rust-dedup + dashboard stage + cronos-api
./bin/cronos-api --dev -node-id=node1 -data-dir=./data
```

Testing:

```bash
make test-unit          # unit suite (excludes live integration/chaos)
make lint               # gofmt + go vet
```

Architecture and developer walkthrough:

- [README.md](README.md) — product overview and quick start
- [ARCHITECTURE.md](ARCHITECTURE.md) — full system architecture
- [docs/DEVELOPER_ARCHITECTURE_GUIDE.md](docs/DEVELOPER_ARCHITECTURE_GUIDE.md) — composition root and flows
- [docs/architecture/README.md](docs/architecture/README.md) — per-feature docs index

## Pull Request Process

1. **Fork & Branch:** Create a focused branch for your change.
2. **Code Style:** Run `gofmt` (or `make lint`). Prefer clear GoDoc on exported types and fields.
3. **Tests:** Add or update tests. Unit tests must pass; add integration/chaos coverage when changing cluster/replication paths.
4. **Docs:** Update architecture or feature docs when behavior or defaults change.
5. **Commits:** Use clear, conventional-style messages when possible (`fix:`, `docs:`, `feat:`).
6. **Submit PR:** Describe the problem, the approach, and how you verified it.

## Code Review

All submissions require review. Please be open to feedback and iterate promptly.

Thank you for contributing!

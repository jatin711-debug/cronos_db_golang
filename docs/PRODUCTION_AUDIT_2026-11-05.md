# Production readiness audit — 2026-11-05

Reviewed HEAD: `579fd79a15b84cf4bcc7bda878aee6fa9d665fc6`.

**Remediation follow-up:** The findings below describe the original audit of HEAD.
The working tree now fixes CDC cursor recovery and standalone production readiness,
returns an explicit unsupported response for manual rebalance, and wires validated
per-subject, per-node publish-rate configuration. See
[the validation record](PRODUCTION_VALIDATION.md) for the current scope and results.
The original findings are retained as the before-change record.

**Verdict: not ready for an unconditional production sign-off.** The supported core has substantial implementation and regression coverage, but this review reproduced a CDC recovery defect, found a production configuration/readiness inconsistency, and confirmed incomplete operator features. Production capacity and disaster-recovery evidence remain open. Passing tests do not establish that all code paths are correct.

This is a source and local execution audit, not a certification of a deployed cluster. Production code was not changed. The pre-existing untracked `docs/INTERVIEW_PREPARATION.md` was left untouched.

## Findings, in priority order

### 1. High: corrupted CDC cursor skips pending exports

Location: `internal/partition/changefeed.go:107–120`; retention consequence: `internal/partition/retention.go:77–125`.

`openChangeFeed` treats malformed existing `changefeed.json` like a newly enabled feed: it sets the cursor to the WAL's last offset and persists that position. Pending exports are thereby treated as completed. This is relevant when recovery metadata is damaged; it is not a claim that ordinary clean restarts corrupt this file.

**Reproduced:** keep a sink unavailable, publish three durable events, close the partition, replace its saved cursor with malformed JSON, and reopen it with the sink still unavailable. All three events remain in the WAL, but the cursor advances from -1 to 2. The new correctness probe fails. Because retention checks this cursor, it can subsequently regard these unexported records as exported if the other deletion conditions are satisfied.

Fix: distinguish missing state for a genuinely new feed from damaged existing state. Refuse startup or conservatively replay from a retained safe position on corruption; validate cursor fields and bounds. Add recovery tests covering malformed, missing and out-of-range metadata. Exclude CDC from production scope until addressed if a narrower release is needed.

### 2. Medium: production validation accepts an unusable standalone replication configuration

Locations: `internal/config/config.go:411–466`, `cmd/api/main.go:312`, `internal/api/health.go:126`, `internal/api/handlers.go:667`.

Production validation requires RF >= 3 and minISR >= 2 but does not require clustering to be enabled. `--cluster` defaults to false. A configuration with the required security fields and replication numbers therefore passes validation without a cluster manager to establish replication leadership. The publish path correctly refuses writes without that leader; this is an availability/configuration failure, not a demonstrated loss of acknowledged data.

The readiness check only checks leadership when clustering is enabled and a manager is present. It can consequently report readiness for this unusable configuration. The production Helm chart explicitly enables clustering, so this primarily affects direct binary/custom deployments.

**Reproduced:** one probe confirms that production validation accepts this configuration; a second creates/starts its partition and observes a refused publish (`replication leader for partition 0 is not ready`) alongside HTTP 200 readiness.

Fix: reject incompatible standalone replication settings at startup and make readiness require the prerequisites for accepting publishes. If standalone production is intentionally supported, give it an explicit, consistent durability contract.

### 3. Medium: manual rebalance reports success without performing the operation

Locations: `internal/api/admin_handler.go:695–712`, `web/dashboard/src/views/OperationsView.tsx:164`.

`TriggerRebalance` returns `Success: true`, zero partitions moved, and an error string saying that the trigger is not exposed. The dashboard takes the success branch. Membership-driven reconciliation exists, but this operator request does not initiate a rebalance. Automation relying on the success field receives a misleading result.

Fix: implement and verify the operation, or return an explicit unsupported/failure response and disable the dashboard action. This is an existing documented gap, not a newly discovered regression.

### 4. Scope blocker for shared tenancy: tenant quota configuration is not wired

Locations: `cmd/api/main.go:145`, `internal/tenant/tenant.go:103–120`, `internal/api/handlers.go:627`.

The server creates a tenant accountant, but there is no production call site for `SetLimits`. Admission accounting is guarded by `HasConfiguredLimits`, which remains false without those calls. The quota implementation therefore does not provide configured per-tenant resource isolation in the shipped server. Global backpressure is separate and does not establish tenant fairness.

Fix: add validated, persistent operator configuration and integration coverage, or explicitly exclude quota-isolated multi-tenancy from the supported product. This is also acknowledged in existing release documentation.

## Release evidence still needed

`docs/PRODUCTION_RELEASE.md:3–8` explicitly leaves multi-host soak, recovery/upgrade drills and machine/power-loss testing open. This audit did not close those gates. Establish workload-specific event/payload/backlog limits, scheduling lateness and recovery objectives on the intended machines and disks. Exercise growth, replacement, drains, restore and upgrade boundaries while accounting for acknowledged events.

The Kubernetes smoke script changes the production profile to three pods with soft anti-affinity (`scripts/production-chart-smoke.sh:35–36`). It is useful startup/restart coverage but does not establish the five-node production profile's behavior during real host failures or drains. The rendered chart's disruption budget and hard anti-affinity assertions do pass.

Other documented limits remain relevant: automatic lag-triggered snapshot selection is unwired; unfinished work can pin WAL retention; dead-letter journal history is not compacted. These need tested operating bounds and runbooks. Transactions, online splitting and cross-region replication are explicitly rejected in production mode; they are excluded features, not capabilities this review assumes are supported.

## Verification performed in this audit

- Windows amd64 target, Go 1.26.10, `CGO_ENABLED=0`: `go test -count=1 -timeout 300s ./internal/... ./pkg/... ./cmd/...` passed: **35 packages passed; 3 had no test files**.
- `go vet ./internal/... ./pkg/... ./cmd/...` passed under the same target settings.
- Dashboard: **21 tests in 6 files passed**; TypeScript/Vite production build passed.
- `npm audit --json`: zero reported vulnerabilities in the response obtained during this audit. This does not cover Go, Rust or the container image.
- `python scripts/check-production-chart.py`: Helm lint and production chart assertions passed.
- Publication tooling: **8 Python unit tests passed**.
- `cargo test --locked --manifest-path internal/dedup/rust/Cargo.toml`: compiled successfully, but executed **zero Rust tests**. Do not count this as Rust behavior coverage; separate Go integration tests exercise that boundary in cgo builds.
- Additional isolated Go overlay probes exposed the CDC and configuration/readiness findings. They are separate from the passing ordinary suite. Sources and outputs are preserved under `docs/audit-probes/2026-11-05/`.

Not run here: Linux/cgo race suite, full acceptance/chaos campaigns, Docker image build/scan, live Kubernetes tests, current Go/Rust vulnerability scans, or multi-host/power-loss tests. Docker was not available on this host's PATH. Existing CI definitions and historical reports were inspected, but remote CI status and release artifacts were not independently verified in this audit.

Before sign-off, resolve the reproduced recovery and configuration defects; make the supported operator/API scope honest; then run the full candidate checks and deployment-specific failure/recovery campaign against an immutable image digest. A controlled staging pilot is reasonable. A claim that the complete codebase is production-ready with no issues is not supported by the evidence.

# Deployment validation and remaining release gates

## Release candidate v0.12.0-rc.1 — 2026-11-06

PRs #25 and #26 are merged. The candidate is pinned to
`08be93b61f8e4f510c61ed2708c7ebf665335143`. It remains a **draft prerelease**;
passing these checks does not close the target-environment gates below.

- [Merged-source CI](https://github.com/jatin711-debug/cronos_db_golang/actions/runs/42214183750)
  passed Go/race/pure-Go checks, dependency security, the fault campaign,
  dashboard/chart checks and production chart startup.
- [Release workflow](https://github.com/jatin711-debug/cronos_db_golang/actions/runs/42214272737)
  built all five binary targets and staged the tested Linux amd64 image archive.
  Trivy passed the fixable HIGH/CRITICAL threshold. Network partitions passed
  in 135.90 s: all 2,682 acknowledged events were delivered. The memory-limited
  restart passed in 60.86 s: all 16,331 acknowledged events were delivered;
  another 53 publish attempts failed and were not counted as acknowledged.
- Candidate-source recovery tests passed on Windows amd64 without cgo:
  partition recovery contracts (18.708 s), storage recovery contracts (1.222 s),
  and the three-process backup-restore acceptance drill (169.117 s). These
  include pending CDC exports, DLQ records, consumer progress, deduplication,
  scheduled work and refusal of invalid backup/format inputs. CDC/DLQ coverage
  here is component-level, not a production-sized external-sink drill.
- Checksum-verified Windows release binaries for v0.11.0 and v0.12.0-rc.1
  passed a rolling upgrade, rollback and second upgrade under traffic. The
  finalized harness passed in 125.570 s (test body 124.53 s), acknowledging
  628, 625 and 654 events in its three phases. Every acknowledged event was
  delivered, none early, and replica logs agreed after each phase. There were
  8, 4 and 103 events delivered more than once respectively, permitted by the
  at-least-once contract. This small local drill uses development mode,
  replication TLS, RF=3/minISR=2 and batch fsync. It establishes compatibility
  for that binary pair and exercised data, not arbitrary downgrade safety.

The staged image's recorded `IMAGE_ID` is
`sha256:aadd7babcb58ee04a302a34ed5babf6883cd19279ab76e587251bcde02d4b457`.
Its compressed archive SHA-256 is
`cdf2260577ba76b4adcd13bdc2002573542c6532e7460d05e3041734d0b94f6e`.
This identifies the staged artifact; no registry digest or signed publication
provenance is claimed. Release assets contain checksums and the image scan.
Local recovery/upgrade logs and an evidence manifest are attached to the draft.

The reusable drill is `TestRollingUpgradeAndRollback` in `tests/acceptance`.
Set `CRONOS_BINARY` to the candidate executable and
`CRONOS_UPGRADE_FROM_BINARY` to the compatible prior executable, then run:

```sh
go test -tags acceptance -run '^TestRollingUpgradeAndRollback$' -count=1 -v -timeout 15m ./tests/acceptance/
```

The drill skips when the prior binary is unspecified. The candidate tag is
unchanged: this test and evidence record are a follow-up to its source.
Still open: the independent-host 24-hour soak, production-sized CDC/DLQ/upgrade
drills, disk/backlog operating bounds, target-storage power-loss evidence and
signed publication. The next environment step is three disposable hosts with
the intended storage; none has been connected in this session.

## Dedicated production candidate — closure criteria, 2026-11-05

The first production deployment is dedicated to one application/operator.
Schemas are managed as configuration. Shared-tenant storage/in-flight quotas,
live schema registration and forced manual rebalance are excluded from this
release scope; membership-driven reconciliation remains supported. This scope
decision does not claim those excluded features were implemented.

Schema files must be distributed identically to every node before startup.
Malformed/unreadable files, unknown types and duplicate versions now stop startup
instead of silently removing validation. A topic with no schema still accepts
arbitrary payloads. Deployment preflight must compare the expected topic/version
inventory on every node using `schema-list`/`schema-get`, and verify a known-invalid
payload is refused. Use only the documented schema keyword subset.

Product fixes in this candidate (not yet released):

- Publish response failures include `error_code`. Updated SDKs report internal
  failures as `ErrorKindInternal`, exhausted leadership refusals as
  `ErrorKindLeaderChange`, and authentication/permission failures as
  `ErrorKindAuth`. Old servers retain the legacy response fallback; an internal
  error can occur after append, so retries must preserve message ID and payload.
- A valid non-admin dashboard token receives HTTP 403 and "Admin access required";
  missing/invalid credentials still receive HTTP 401.
- Schema loading fails closed and a failed reload preserves the active registry.

### Reference test profile

These are acceptance targets to establish, not measured capacity or a hardware
purchase recommendation. Use disposable staging instances on three separate Linux
amd64 hosts/failure domains, initially 4 vCPUs, 8 GiB RAM and 100 GiB persistent SSD
per node. Record actual disk model/storage service, filesystem and cache semantics.
Use replication factor 3, minimum in-sync replicas 2, TLS, JWT/RBAC, encryption at
rest and `every_event` fsync. Pin the exact commit and image digest throughout.

- Run for 24 hours at 100 events/s; once per hour burst to 500 events/s for 60 s.
  Use 1 KiB payloads normally and 64 KiB for 1% of events, unique message IDs,
  dedup enabled, and a mix of immediate and delayed schedules. Seed 100,000
  delayed events before injecting failures. Increase this envelope only after
  it passes and disk/memory headroom is measured.
- Require zero lost acknowledged events and zero early deliveries. Track every
  acknowledged ID through delivery and, when enabled, CDC export. Record
  duplicates separately under the at-least-once contract.
- Under healthy steady load, target publish p99 <= 250 ms and scheduling
  lateness p99 <= 1 s; no unexpected refusals. Report the same metrics separately
  for bursts and disruptions instead of hiding them in whole-run averages.
- After losing one host, target resumed publish/delivery service within 60 s.
  After a full backup restore, target service recovery within 30 minutes and
  zero loss of events acknowledged before the backup's recorded cut. Measure
  actual backup age separately; later acknowledgements are outside that cut.
- Include slow consumers, a failed CDC destination, disk pressure, 3-to-5 node
  growth, drain/replacement and leader loss during catch-up. Do not proceed to
  another disruption until the ledger and replica state reconcile. Record
  memory, disk, pinned retention and DLQ journal growth; unexplained unbounded
  growth or inability to recover within the envelope fails the gate.
- Test the intended upgrade from v0.11.0 on copied data. Verify the documented
  on-disk format boundary before rollback; never assume a downgrade is safe.
  Test controlled host/power loss only on disposable infrastructure using the
  intended storage. A process kill or VM reboot is not a physical power-cut test.

### Current evidence and remaining work

Local Windows amd64, CGO-disabled checks passed for the SDK/API and the schema/server
packages. Regression coverage includes post-append replication failure followed by
same-ID recovery, SDK failure classification and old-server fallback, HTTP 401/403,
and corrupt/duplicate schema loading. Dashboard tests/build passed.
`TestClusterRestoredFromBackups` passed in 142.121 s on this workstation during this
change, before the subsequent schema-startup hardening. That test covers scheduled
work and consumer progress; it does not close pending-CDC/DLQ recovery, upgrade,
physical-host failure, or final-image gates. The raw local run is recorded under
`build/production-contract-restore.log` (an ignored local artifact).

The independent-host soak, target-storage power-loss campaign, complete
CDC/DLQ/upgrade drills, bounded-operation measurements and final candidate
CI/security/provenance evidence remain required. No separate staging hosts have
been provided in this session. Local evidence and explicit scope exclusions do
not close these environment gates.

## Local remediation validation — 2026-11-05

Candidate: working-tree changes on `579fd79a15b84cf4bcc7bda878aee6fa9d665fc6`.
Local image `cronos-db:audit-fixes-final` (also tagged `cronos-db:ci`):
`sha256:22ee76ee3f088cf433bcd4628081f2538856d105a9aa56d22290ca67053c4632`.
This image was not published. Evidence is in
[`audit-probes/2026-11-05/remediation/`](audit-probes/2026-11-05/remediation/).

- Fixed CDC cursor recovery, production cluster validation/readiness, misleading
  manual rebalance success, and missing per-subject publish-rate configuration.
  The changed Windows Go packages passed their regression suite. Earlier dashboard
  tests/build passed; chart assertions and `git diff --check` also pass.
- The final Docker image builds successfully. Trivy 0.75.0 with a freshly downloaded
  database reports zero fixable HIGH/CRITICAL findings in Debian packages and both
  Go binaries. The previous image had two APT package findings. Runtime-layer cache
  bypass, package upgrades and a minimum APT version check close those findings.
  This thresholded result does not claim zero vulnerabilities of every severity.
- `cargo audit` passes on the updated Rust lockfile (23 dependencies).
- Final-image `TestNetworkPartitions` passes (258.97 s): isolated nodes, asymmetric
  link failure and total partition, followed by replica-log agreement.
- Final-image `TestRestartInContainersWithABacklogLargerThanTheirMemory` passes
  (172.96 s). Of 16,384 attempts at 64 KiB each, 16,370 were acknowledged and 14
  failed quorum admission. All acknowledged events were delivered after killing
  and restarting every node with 1,023 MiB pending. Peak sampled memory was 209 MiB
  per node against a kernel-enforced 384 MiB limit; no node was OOM-killed.
- Five-worker kind smoke passes on Kubernetes v1.37.0: five Cronos pods placed on
  separate workers, TLS/auth/key-permission checks, rolling restart and recovery
  with the first pod's volume emptied. This script sends no events; event recovery
  evidence comes from the separate container campaign. The local Docker engine
  uses cgroup v1, so the initial bootstrap failed. A disposable-cluster-only
  `failCgroupV1: false` compatibility override allowed the run; the workflow does
  not apply that override. The cluster was deleted after evidence collection.

The earlier Linux race run had two package timeouts and a Raft TLS leadership
failure; five isolated reruns of that TLS test passed. Those original failures
remain part of the record and are not proof of a data race. The resumed full suite
passed all 35 test-bearing packages (three others have no test files), with no
reported races, using Linux amd64 Go 1.27.3, cgo enabled, one package at a time and
a 600-second package timeout:
`go test -race -p 1 -count=1 -timeout 600s ./internal/... ./pkg/... ./cmd/...`.
This local run is not a claim that remote CI's parallel 180-second profile passed.

These checks close local container/deployment gaps. Separate-host soak, physical
power loss, workload-specific capacity, and upgrade/rollback validation remain
open. They cannot be inferred from a single Docker host.

## Configured tenant rates

Create a persistent JSON file, for example:

```json
{"publisher-a":{"max_events_per_second":1000},"publisher-b":{"max_events_per_second":100}}
```

Start the authenticated server with `--tenant-limits-file=/etc/cronos/tenant-limits.json`
or `CRONOS_TENANT_LIMITS_FILE`. Subjects are verified JWT subjects, not client-supplied
event metadata. Limits apply per node across the subject's topics, with a one-second
burst. A batch must fit that burst. Unlisted subjects are unlimited. Restart to load
changes; bucket state resets on restart. These are not cluster-wide rate limits.

With Helm, put that JSON into the existing auth Secret and set
`auth.tenantLimitsKey` to its key name. Restart the pods after updating the Secret.
Invalid files and unsupported quota fields fail startup. Storage and in-flight
quotas remain unsupported: their counters do not yet have durable recovery or
cluster-wide accounting. Do not advertise quota-isolated shared tenancy.

## CDC recovery contract change

Missing, malformed, null, negative-invalid or beyond-log CDC positions now start
from the retained log's beginning. First enabling CDC on existing data also exports
retained history. Destinations must tolerate duplicates. This cannot restore records
already removed by retention; restore them from an appropriate backup if needed.
All nodes must use consistent CDC configuration.

## Executable topology coverage

The manual `five-node-production-topology` workflow uses five kind workers and
five Cronos pods with hard anti-affinity. It exercises startup, rolling restart and
loss of the first pod's data volume using the production security settings. The
ordinary CI smoke profile remains three pods. Both use reduced resource/partition
settings for CI. Kind workers share one physical host: this is scheduling/topology
coverage, not physical multi-host fault or capacity evidence.

## Gates requiring the target environment

Record the commit and immutable image digest, machine/storage topology, workload,
configuration, timestamps and raw logs for each run. Agree on throughput, p99
scheduling lateness, recovery time and acceptable duplicate delivery before testing.

1. Run a 24–72 hour representative soak on separate hosts, with actual payload
   sizes and scheduled backlog, slow consumers, a failed CDC sink and disk pressure.
   Account for every accepted event through eventual delivery/export; measure
   memory, disk growth, lateness and refusal rates against the agreed limits.
2. Grow from three to five nodes, replace nodes, drain one at a time, and fail a
   leader during handoff/catch-up. Verify acknowledged-event conservation and
   replica agreement before proceeding to the next disruption.
3. Restore backups containing scheduled events, pending CDC, consumer progress and
   DLQ records. Measure recovery time and recovery point. Test the intended upgrade
   path and only supported rollback boundaries on copies of real-sized data.
4. Perform controlled host/power-loss tests on disposable infrastructure using the
   intended disks and durability settings. Process kills and kind restarts do not
   prove that storage honors power-loss durability.
5. Run candidate CI, race and acceptance suites and fresh Go/Rust/image scans.
   Preserve failures and diagnose intermittent results. Publish the tested digest.

Automatic snapshot selection for large replication lag and DLQ journal compaction
remain separate engineering work. Bound supported refill/backlog sizes and alert on
pinned retention/disk pressure until those paths have measured operating limits.

## Dashboard and feature journey, 2026-11-05

Run against a Windows amd64 build of the working tree. A single authenticated node
(real JWTs, RBAC policy, per-subject limits file, persisted schema) and a three-node
TLS cluster (the acceptance harness, with a workload) were driven through the
dashboard in a browser and through the public RPCs.

Verified end to end: sign-in with an admin, a non-admin and an expired token (admin
APIs refuse the last two); publish, deduplication, schema rejection, delivery no
earlier than the scheduled time, acknowledgement and replay; cluster topology with
leaders, replicas and in-sync sets; partition health; replication lag; consumer
groups and committed-offset lag; schemas; tenant usage with configured limits;
retention and compaction actions; manual rebalance reported as unsupported.

Defects found and fixed:

- JSON schema validation ignored `required`, so `{}` was accepted by a schema that
  requires `order_id`. It is now enforced; other keywords (enum, pattern, ranges,
  additionalProperties) are still not enforced.
- The dashboard's replication view could not show partition 0 (the HTTP API treated
  `partition_id=0` as "all partitions") and titled partition 0 "All Partitions". The
  HTTP API now treats an explicit 0 as partition 0; the RPC keeps 0 as "all".
- The dashboard labelled the per-node leader count "Leader Partitions" beside the
  cluster-wide total, so a node leading nothing showed 0 while every partition had a
  leader. It is now "Led by this node".
- `AckRequest.next_offset` was documented as optional, but a successful ack with any
  value other than the highest delivered offset plus one is rejected. The comment now
  says so; the client library already sends it.

Limits left as they are: replication lag is only reported by a partition's leader
(the cluster view has the same information from any node), while partition health is
local to each node and answers for any partition the node holds;
retention and compaction take 0 as "all local partitions", so partition 0 cannot be
targeted alone; a node serves the dashboard assets compiled into it, and the files
checked in under `internal/api/web/dist` are older than the source (the Docker build
stages a fresh build, a plain `go build` does not).

Acceptance campaign on this host: every test passed except
`TestOverloadIsRefusedAndSurvived`, which failed in 1 of 4 runs of this tree and in 3
of 7 runs of an unmodified HEAD worktree, and in 2 of 2 runs of the cgo build in a
Linux container. In failing runs the nodes took the whole 624 MiB flood without
refusing. The evidence points at the test, not the nodes: its events fell due 45 seconds
into the flood, and the failing runs were the slow ones (about 90 events/s instead of
200), which outlast that window; the mechanism, that an event which is due no longer
counts toward the memory the guard measures, is inferred, not observed. With the due
time at 20 minutes the nodes refused as designed (one run). The test now schedules
its events 150 seconds out. With that change it passed 4 of 4 runs in the Linux
container and 4 of 4 on Windows (about 160 s each, up from about 55 s).

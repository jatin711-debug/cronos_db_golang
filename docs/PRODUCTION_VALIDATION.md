# Deployment validation and remaining release gates

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

Limits left as they are: replication lag and per-partition health are only reported
by a partition's leader (the cluster view has the same information from any node);
retention and compaction take 0 as "all local partitions", so partition 0 cannot be
targeted alone; a node serves the dashboard assets compiled into it, and the files
checked in under `internal/api/web/dist` are older than the source (the Docker build
stages a fresh build, a plain `go build` does not).

Acceptance campaign on this host: every test passed except
`TestOverloadIsRefusedAndSurvived`, which failed in 1 of 4 runs of this tree and in 3
of 7 runs of an unmodified HEAD worktree. In failing runs the nodes took the whole
624 MiB flood without refusing. On Windows the process memory gauge comes from Go
runtime counters, not the Linux resident-memory read the test is designed around, so
treat this test as Linux-only evidence; its Linux CI result is the one that counts.

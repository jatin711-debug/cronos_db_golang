# CronosDB v0.11.0

This release closes the defects found by the
[2026-11-05 production audit](PRODUCTION_AUDIT_2026-11-05.md) and by a dashboard
and feature journey run after it (see the
[validation record](PRODUCTION_VALIDATION.md)). It adds one feature, per-subject
publish-rate limits, and changes four behaviours that an upgrade can notice.
It keeps the version-2 WAL format: v0.10.0 data directories are used as they are.
It is not 1.0: the release gates the
[1.0 readiness review](RELEASE_READINESS_1.0.0_2026-10-28.md) lists (a multi-host
soak, recovery and upgrade drills, machine or power-loss testing) are still open.

## Read before upgrading

- **Production mode requires cluster mode.** Validation used to accept a
  standalone node with RF >= 3 and minISR >= 2; it then refused every publish
  while `/health/ready` reported ready. A production configuration without
  `--cluster` is now refused at startup, and readiness is down when replication
  is configured but no cluster manager exists. `--dev` is unchanged. The Helm
  chart already enables clustering.
- **JSON schemas now enforce `required`.** A payload without a property a
  schema lists as required used to be accepted; it is now refused with a schema
  validation error. Producers that relied on the gap will start to see refusals.
  `enum`, `pattern`, ranges and `additionalProperties` are still not enforced.
- **Manual rebalance returns `Unimplemented`.** `TriggerRebalance` used to
  return `Success=true` with an explanatory error and did nothing. Leadership
  still reconciles automatically after membership changes; the dashboard action
  is disabled. Automation that checked the success field must handle the error.
- **A missing or damaged change-feed cursor replays retained history.** It used
  to jump to the end of the log, which made pending exports look finished and
  let retention remove them. Enabling a feed on a partition that already holds
  events now also exports its retained history, and restoring a backup without a
  cursor exports everything still retained. Sinks must tolerate duplicates, and
  history that retention already removed cannot be recovered.

## Changes

- **Change feed (CDC).** A malformed, unreadable or out-of-range
  `changefeed.json` is no longer treated as a newly enabled feed. Recovery tests
  cover malformed, missing and out-of-range cursors.
- **Per-subject publish-rate limits.** `--tenant-limits-file`
  (`CRONOS_TENANT_LIMITS_FILE`) loads `max_events_per_second` for each JWT
  subject (at least 1), enforced as a per-node token bucket with one second of
  burst. It requires authentication, so that limits identify the caller, and is
  read at startup only. The chart mounts it from the existing auth Secret with
  `auth.tenantLimitsKey`. Storage, in-flight and cluster-wide quotas remain
  unsupported, so this is not tenant isolation.
- **Dashboard and admin API.** `partition_id=0` names partition 0 on the HTTP
  API; the RPC keeps 0 as "all partitions". The replication view no longer
  titles partition 0 "All Partitions" and says that only a partition's leader
  reports follower offsets. The per-node leader count is labelled "Led by this
  node". Partition health accepts partition 0.
- **`AckRequest.next_offset`** is documented as required on a successful ack. The
  server already rejected any other value; only the comment changed.
- **Release pipeline.** Image builds refresh the runtime stage every time and
  fail fast on apt retries, the Rust lockfile moves `rand` to 0.8.6, and a
  five-worker kind workflow runs the chart smoke test with hard anti-affinity.
- **Acceptance test.** `TestOverloadIsRefusedAndSurvived` scheduled its events 45
  seconds into the flood, so a slow flood never reached the memory limit and the
  test failed about half the time. Events are now due after 150 seconds.

## Verification

CI on the pull request passed all five required jobs and the secret scan. On
Windows and in a Linux container the unit suites, `go vet`, the dashboard tests
and build, and the acceptance campaign passed, and a journey through every
dashboard view and the publish, deduplication, schema, scheduled delivery,
acknowledgement and replay path ran against an authenticated node and a three-node
TLS cluster. [The validation record](PRODUCTION_VALIDATION.md) lists what was
and was not exercised.

## Known limits

Unchanged from v0.10.0, and still open: multi-host soak, recovery and upgrade
drills and power-loss testing; automatic lag-triggered snapshot selection; unfinished
work can pin WAL retention; the dead-letter journal is not compacted. Replication
lag and per-partition health are reported by a partition's leader only; retention
and compaction take partition 0 as "all local partitions". The dashboard assets in
a binary are those compiled into it: a plain `go build` serves the files checked in
under `internal/api/web/dist`, which the container build replaces with a fresh build.
This release makes no new capacity claim.

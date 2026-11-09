# CronosDB v0.12.0

This release fixes two dependency vulnerabilities in the image, classifies
publish failures so that clients can tell a refusal from a server failure,
makes schema loading fail closed, and fixes a replay request that ignored
`Count`. It keeps the version-2 WAL format: v0.11.0 data directories are used as
they are. It is not 1.0: the release gates of the
[1.0 readiness review](RELEASE_READINESS_1.0.0_2026-10-28.md) and the
[deployment validation record](PRODUCTION_VALIDATION.md) (a multi-host soak,
production-sized recovery and upgrade drills, physical power-loss testing) are
still open.

The supported scope is a dedicated deployment: one application and operator,
schemas managed as configuration on every node, membership-driven rebalancing,
and no shared-tenant quota guarantees.

## Read before upgrading

- **Schema files now stop startup when they are invalid.** A file in
  `<data-dir>/schemas/` that cannot be read or decoded, lacks a topic, a positive
  version or a definition, has an invalid JSON or Avro definition or an unknown
  type, or repeats a topic's version used to be skipped silently, which removed
  validation for that topic. The node now refuses to start and names the file.
  Check the `schemas/` directory on every node before upgrading, and keep the
  same files on every node. A topic without a schema still accepts any payload.
- **The Go SDK classifies publish failures.** The server sets
  `PublishResponse.error_code`, and the updated SDK returns `ErrorKindInternal`
  for a write or replication failure after the event reached the log (it used to
  be `ErrorKindValidation`), `ErrorKindLeaderChange` when retries end on a
  leadership refusal, and `ErrorKindAuth` for authentication and permission
  failures. Code that switches on `Error.Kind` should handle these. Retry an
  internal failure with the same message ID and payload: it does not prove the
  event was never stored. Against a v0.11.0 server the SDK keeps the old
  classification.
- **The HTTP admin API answers a valid non-admin token with 403.** It used to
  answer 401. Missing or invalid credentials still get 401. Both now carry a JSON
  body, `{"error": "..."}`, instead of plain text; the dashboard says "Admin
  access required".
- **`Replay` with only `Count` returns `Count` events.** It used to return one
  event, because the zero `StartOffset` and `EndOffset` looked like a one-event
  range. Explicit ranges and `ReplayByOffsetRange` are unchanged.

The supported upgrade from v0.11.0 is unchanged: stop every node, replace the
binary or image, and start them again; data directories are kept. A local
three-node drill also upgraded a running cluster from v0.11.0 one node at a time,
rolled it back to v0.11.0 and upgraded it again under traffic, with every
acknowledged event delivered (`TestRollingUpgradeAndRollback`). That is evidence
for this pair of versions on a development-mode cluster, not a support statement
for rolling upgrades or for downgrades in general.

## Changes

- **Security.** `golang.org/x/net` v0.62.0 fixes CVE-2026-78661, flagged in the
  v0.11.0 image by the image scan; `github.com/klauspost/compress` v1.20.2 fixes
  GO-2026-6865. `golang.org/x/sys`, `x/sync`, `x/text` and `x/tools` move with
  them.
- **Publish error contract.** `PublishResponse` has a new field, `error_code`, the
  gRPC status code of a failed publish (0 from older servers).
- **Schema loading** validates every file and replaces the active registry only
  when all of them load; a failed reload keeps the previous registry. A schema of
  an unknown type refuses publishes instead of accepting them.
- **Dashboard.** A non-admin token sees "Admin access required" instead of a
  sign-in prompt.
- **Replay.** The `Count` fix above, with a regression test.
- **Documentation.** The documentation and the Go SDK documentation were
  reviewed against the code: refusals and the error kind each maps to, the
  acknowledgement `next_offset` rule, replay semantics, partition 0 on each API,
  every flag, RPC and metric.
- **Tests.** `TestRollingUpgradeAndRollback` (opt-in: set
  `CRONOS_UPGRADE_FROM_BINARY` to a compatible earlier release) upgrades, rolls
  back and upgrades a three-node cluster under traffic, checking delivery and
  replica agreement after each phase.

## Verification

The candidate for this release, v0.12.0-rc.2, was built from the same source
apart from this release's documentation and chart version. The release itself is
checked again rather than inheriting the candidate's results: CI, the release
workflow's image scan and container fault tests on the exact image, and the
rolling upgrade drill with its binaries. The release page records the results
and the image digest.

## Known limits

Unchanged from v0.11.0: replication lag is reported by a partition's leader only;
retention and compaction take partition 0 as "all local partitions"; schemas
cannot be registered at run time; manual rebalance returns `Unimplemented`;
tenant limits cover publish rate only; automatic lag-triggered snapshot selection
and dead-letter journal compaction are not implemented. This release makes no new
capacity claim.

# CronosDB v0.14.0

This release makes catching up a consumer group far behind cheap: on the
catch-up that failed v0.13.1's compressed soak, publishes now hold their rate
and the group is back in step in a third of the time. A follower far behind
is copied a snapshot of its leader's log instead of having it replayed. Events
scheduled far ahead can be bounded and their cost on disk is reported, disks
get a free-space floor and a ballast, and images are published for Linux amd64
and arm64 with signed provenance. It is not 1.0:
the release gates of the
[1.0 readiness review](RELEASE_READINESS_1.0.0_2026-10-28.md) are still open
(a passing 24-hour soak, a soak on separate hosts, power-loss testing on real
disks, recovery and upgrade drills at production size).

## Read before upgrading

Two changes behave differently from v0.13.1 for operators:

- **Partition 0 is no longer "every partition"** for retention, compaction
  and replication lag. Up to v0.13.1 the admin RPCs, the CLI and the HTTP
  admin API read `partition_id=0` (or `--partition-id=0`) as "every partition
  of the node", so partition 0 could not be targeted alone, and
  `POST /api/admin/retention/run?partition_id=0` or `.../compaction/run?partition_id=0`
  ran on every partition. Now `0` is partition 0, and leaving the partition
  out means every partition. **A script that passes `0` to mean "all" must
  leave it out**, or it will act on partition 0 only. Clients built against
  v0.13.1 never send `0` on the wire (the field was not optional), so they
  keep meaning "all".
- **Rolling back to v0.13.1 redelivers some finished events.** Completions
  above a group's floor are stored in a new form (see below). v0.13.1 does
  not read it, so after a rollback each group is given again the events it
  had finished above its floor. Nothing is skipped or lost: v0.13.1's own
  deletions remove the new records whenever its log is cut or a floor rises,
  so none can outlive the entries it was about. Upgrading again converts
  v0.13.1's records back.

And one default that now does something: `--snapshot-catchup-threshold` (10000)
was read by nothing up to v0.13.1. A follower far behind its leader now copies
the leader's log as a snapshot instead of having it replayed (see below). Set
it to `0` for the old behaviour.

The supported upgrade from v0.13.1 is unchanged: stop every node, replace the
binary or image, and start them again; data directories are kept. The first
start converts each partition's completion records, which takes a moment per
million records.

## Changes

- **Catching up a consumer group far behind costs much less.** A group's
  completions above its floor were one Pebble record per event. A floor held
  back by an event scheduled hours ahead kept a record for every event
  delivered since, each looked up in the store by every completion check and
  deleted again when the floor passed it; v0.13.1's compressed soak saw
  publishes fall from 450 to about 180 per second while a group caught up.
  Completions are now one bit per offset, in blocks of 4,096 offsets stored
  as one record each and held in memory. With acks of 50 events behind a held
  floor, the offset store writes 6.4 instead of 35.5 bytes of WAL and 6.5
  instead of 43.6 bytes of flushes and compactions per event, and a
  completion check takes 150 ns instead of 40 µs. A group whose floor keeps
  up with its acks stores no blocks at all. Repeating the compressed soak's
  catch-up (a group stopped for 14 minutes at 450 publishes per second, about
  390,000 events behind) on one GitHub runner with each server: v0.13.1 took
  50 minutes from the start of the disruption to reconcile, with publishes at
  a median of 405 per second (28 at the worst) and a median latency of 63 ms;
  this release took 17 minutes, about 3 of them catching up, with publishes
  held at 450 per second and a median latency of 13 ms. Neither lost,
  refused or delivered early anything.
- **A follower far behind is copied a snapshot.** When an append from the
  leader shows that a follower lacks at least `--snapshot-catchup-threshold`
  entries and at least half of the leader's log, the follower copies the log
  as files (segments, consumer progress, dead letters) instead of having it
  replayed batch by batch; a snapshot sends the whole log, so it then sends
  at most twice what replay would. The copy holds the partition's
  replication lock until it is installed, so no acknowledged entry can be
  lost by it. Nodes also check when they start and every 30 seconds.
  `cronos_replication_snapshot_catchups_total{outcome}` counts the copies.
- **`--max-schedule-ahead`** refuses publishes, batch publishes and
  reschedules due further ahead than a duration (off by default). The log is
  removed from its start only, so an event due far ahead keeps every segment
  written after it.
  **`cronos_retention_blocked_bytes{partition,reason}`** reports the bytes of
  closed segments a pruning pass had to keep behind its oldest blocker, and
  the chart's new `CronosDBRetentionHeldByFutureEvent` alert warns when an
  event not yet due keeps more than `alerts.retentionHeldByFutureEventBytes`
  (10 GiB) for 30 minutes.
- **Disk space.** `--min-free-disk-bytes` refuses publishes while less space
  is free than that, whatever `--max-disk-usage` says. `--disk-ballast-bytes`
  keeps a file, `disk-ballast` in the data directory, that holds space in
  reserve: delete it to recover a full disk, and the node creates it again
  at its next start. `cronos_disk_used_ratio` and `cronos_disk_free_bytes`
  report each sample, and the chart's new `CronosDBDiskFillingUp` alert warns
  at `alerts.diskUsedWarnRatio` (85 percent) for 10 minutes, before publishes
  are refused.
- **Deleting a consumer group removes its progress.** A group created again
  under the same name used to find the old group's floor and completions,
  and skip what they covered.
- **Images.** The release workflow publishes each release to
  `ghcr.io/jatin711-debug/cronos-db` for Linux amd64 and arm64 behind one tag,
  with attested build provenance, an SPDX SBOM and a cosign signature, using
  only the workflow's own token. It copies the image to Docker Hub when the
  repository has `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` secrets. See
  [Docker](../DOCKER.md) for how to verify an image.
- **Testing tools.** `TestPowerLoss` simulates power cuts with LazyFS, one
  node at a time and all three at once, and `scripts/soak/netem.sh` adds
  latency, loss and partitions between hosts; see
  [fault testing](FAULT_TESTING.md).

## Compatibility

- **Completion records:** new keys under each group's existing completion
  prefix (`done:<len>:<group>:<partition>:`), ending in a block's last offset
  and `b`. Records of v0.13.1 are converted when a store is opened. See
  "Read before upgrading" for a rollback.
- **Wire:** `partition_id` became `optional` in `ReplicationLag`,
  `RunRetentionRequest` and `RunCompactionRequest`, which keeps the field
  number and type. The replication position response gains `log_start`
  (field 6); v0.13.1 nodes do not send it and the periodic check then copies
  nothing, while the append-time check uses the leader's log end, which
  v0.13.1 already sends. Snapshots use the existing transfer, so v0.13.1 and
  new nodes work together during an upgrade.
- **Flags:** new `--max-schedule-ahead`, `--min-free-disk-bytes` and
  `--disk-ballast-bytes`, all off by default; `--snapshot-catchup-threshold`
  now takes effect.

## Verification

Before the release:

- **Release workflow, dry run** (run 44516011602, 2026-11-19): every job
  passed. The amd64 image passed the container fault tests; the arm64 image
  was built on an arm64 runner, scanned and started; the two were published
  as one index (`ghcr.io/jatin711-debug/cronos-db-dryrun:0.0.0-dryrun.19`,
  linux/amd64 and linux/arm64). `gh attestation verify` found its SLSA
  provenance, signed by `release.yml` on the branch, and its SPDX SBOM, and
  `cosign verify` with the identity pattern in [Docker](../DOCKER.md) accepted
  its signature.
- **Power cuts:** `TestPowerLoss` passed with `every_event` (3 runs) and
  `batch` (2 runs), and failed with `periodic` as a control; see
  [fault testing](FAULT_TESTING.md).
- **Catch-up comparison** (nightly workflow, `catchup_ab`, run 44698712329):
  the numbers under Changes. Its first run (2026-11-20) failed this branch: a
  bug in the new completion blocks dropped completions of blocks past the
  floor's block, so finished events were delivered again, and a group with 4
  credits fell 1.6 million events behind. The fix came with a test that
  checks the blocks against a plain model through random acks, floor raises,
  replicated progress, log cuts and restarts. In the passing run both groups
  received every accepted event, with no more repeats than v0.13.1 (16, from
  the soak resubscribing its slow group).
- **CI** (run 44711035735): every job passed, including the race suite, the
  acceptance suite and the container tests.
- **Nightly jobs** (run 44721002444): the acceptance and container suites
  passed, with as many repeated deliveries per fault test as the last nightly
  run of v0.13.1 on `main`; the rolling upgrade from v0.13.1, rollback and
  upgrade again passed, the rollback phase delivering 60 events twice as
  described under "Read before upgrading"; `TestPowerLoss` passed on a
  GitHub runner with `every_event` and `batch`; and the soak's remote mode
  passed against three nodes driven by command (a leader lost, a leader
  frozen, a node replaced by an empty one).

The release workflow (run 44736155746) built both images from the tag, scanned
them (no fixable HIGH or CRITICAL findings), ran the network-partition and
memory-limited restart tests on the amd64 image and started the arm64 one,
then published them as one index, `sha256:c913c3fa52f2d20b9069e618721f34a55dcd977c6537cfcdeaef808791604669`, with attested provenance, an SPDX
SBOM and a cosign signature. The index was copied unchanged, signature and
attestations included, to `johnny711dock/cronos-db:0.14.0` and `:latest`.
`gh attestation verify` and `cosign verify` accept it under both names.

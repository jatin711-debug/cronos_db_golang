# CronosDB v0.12.2

This release fixes a defect that made a restarted node deliver again events a
consumer group had already acknowledged. Nothing was lost: delivery is
at-least-once and every acknowledged event was still delivered. It keeps the
version-2 WAL format and changes no API, flag or metadata format: v0.12.1 data
directories and cluster state are used as they are. It is not 1.0: the release
gates of the [1.0 readiness review](RELEASE_READINESS_1.0.0_2026-10-28.md) and
the [deployment validation record](PRODUCTION_VALIDATION.md) (a multi-host soak,
production-sized recovery and upgrade drills, physical power-loss testing) are
still open.

## Who should upgrade

Deployments where consumers act on a delivery more than once at a cost, and
deployments where several topics share a partition, which is where the defect
redelivered most. The supported upgrade from v0.12.1 is unchanged: stop every
node, replace the binary or image, and start them again; data directories are
kept.

## Fix

- **A restarted node delivered again what its consumer groups had completed.**
  A group's progress on a partition is a floor, below which every event is
  complete, and a record for each completed event above it. A node keeps the
  highest completed offset in memory so that new events, which are above it,
  are answered without a lookup; after a restart that value is read from the
  stored records the first time it is needed. But a partition that started
  raised its groups' floors to the start of its log before anything had read
  them, and the floor was taken as the highest completed offset. Every event a
  group had completed above its floor then counted as not complete, and the
  partition's leader delivered it again. An acknowledgement that arrived first
  did the same with its own offset.

  A group's floor cannot pass entries of another topic in the same partition,
  so a group that started after another topic's entries keeps all its progress
  in per-event records until those entries are removed from the log. The
  rolling upgrade drill of v0.12.1 showed it: a leader restarted under such a
  group delivered again every event the group had completed in that phase,
  74 to 137 redeliveries across four runs. The defect is present from v0.9.0
  through v0.12.1. A raise made before the stored value has been read is now
  kept and taken into account when it is read.
  `TestCompletionsAboveTheFloorSurviveRestart` fails without the fix.

## Verification

`TestRollingUpgradeAndRollback` from the v0.12.0 release binary to the fixed
build, back, and to the fixed build again, run twice on one Windows machine:
every acknowledged event was delivered, none early, and the replica logs agreed
after each phase. Events delivered more than once in the three phases:

| Build | Upgrade | Rollback | Upgrade again |
| --- | --- | --- | --- |
| v0.12.1, run 1 | 2 | 7 | 74 |
| v0.12.1, run 2 | 0 | 10 | 101 |
| v0.12.2 build, run 1 | 2 | 12 | 3 |
| v0.12.2 build, run 2 | 7 | 3 | 5 |

What remains is the progress a partition's new leader has not yet received
when it takes over; the rollback phase runs v0.12.0 nodes, which still have the
defect. The release itself is checked as before: CI, the release workflow's
image scan and container fault tests on the exact image, and the rolling
upgrade drill from v0.12.1 with the release binaries. The release page records
the results and the image digest.

## Known limits

A group's completion floor still does not pass other topics' entries in its
partition: such a group's per-event records, and the range its redelivery scan
reads, grow until those entries leave the log. Otherwise unchanged from v0.12.1:
replication lag is reported by a partition's leader only; retention and
compaction take partition 0 as "all local partitions"; schemas cannot be
registered at run time; manual rebalance returns `Unimplemented`; tenant limits
cover publish rate only; automatic lag-triggered snapshot selection and
dead-letter journal compaction are not implemented. This release makes no new
capacity claim.

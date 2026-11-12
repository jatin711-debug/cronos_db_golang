# CronosDB v0.12.2

This release fixes two defects that made CronosDB deliver again events a
consumer group had already acknowledged: one when a node restarted, one while a
group's progress advanced. Nothing was lost: delivery is at-least-once and
every acknowledged event was still delivered. It keeps the version-2 WAL format
and changes no API, flag or metadata format: v0.12.1 data directories and
cluster state are used as they are. It is not 1.0: the release gates of the
[1.0 readiness review](RELEASE_READINESS_1.0.0_2026-10-28.md) and the
[deployment validation record](PRODUCTION_VALIDATION.md) (a multi-host soak,
production-sized recovery and upgrade drills, physical power-loss testing) are
still open.

## Who should upgrade

Deployments where consumers act on a delivery more than once at a cost, and
deployments where several topics share a partition, which is where both
defects redelivered most. The supported upgrade from v0.12.1 is unchanged: stop
every node, replace the binary or image, and start them again; data directories
are kept.

## Fixes

A group's progress on a partition is a floor, below which every event is
complete, and a record for each completed event above it. A group's floor
cannot pass entries of another topic in the same partition, so a group that
started after another topic's entries keeps all its progress in per-event
records until those entries are removed from the log. Both defects hit such
groups hardest. Both are present from v0.9.0 through v0.12.1.

- **A restarted node delivered again what its consumer groups had completed.**
  A node keeps the highest completed offset in memory so that new events, which
  are above it, are answered without a lookup; after a restart that value is
  read from the stored records the first time it is needed. But a partition
  that started raised its groups' floors to the start of its log before
  anything had read them, and the floor was taken as the highest completed
  offset. Every event a group had completed above its floor then counted as not
  complete, and the partition's leader delivered it again. An acknowledgement
  that arrived first did the same with its own offset. The rolling upgrade
  drill of v0.12.1 showed it as 74 to 137 redeliveries in its last phase across
  four runs. A raise made before the stored value has been read is now kept and
  taken into account when it is read.
  `TestCompletionsAboveTheFloorSurviveRestart` fails without the fix.

- **A leader delivered again events whose acknowledgement had just raised a
  group's floor.** When acknowledgements closed a gap below a group's
  completion records, the floor rose over them in one durable write that also
  removed the records, and in memory only afterwards. A redelivery scan that
  came in between found neither, took the events for unfinished and sent them
  again: bursts of 5 to 13 events, often milliseconds after their first
  delivery and seconds after they had been acknowledged. Raising the log start
  when old segments were removed, and a follower applying its leader's floor,
  had the same order. Once compaction removed the other topic's entries in
  front of a waiting floor, the next acknowledgement raised it over everything
  the group had recorded since. The new floor is now stored first and the
  records it covers are removed after it is raised in memory.
  `TestCompletionsStayCompleteWhileTheFloorRises` fails without the fix.

## Verification

`TestRollingUpgradeAndRollback` kills and restarts each node of a three-node
cluster in turn, in three phases: upgrade, rollback, and upgrade again, under
traffic. Every run below delivered every acknowledged event, none early, and the
replica logs agreed after each phase. An event delivered just before its node
was killed is delivered again, because its acknowledgement died with the node;
that is the at-least-once guarantee. The other redeliveries are the defects'.
Runs on one Windows machine, the later ones traced per event:

| Build | Runs | Most redeliveries in one phase | Redeliveries not explained by a kill |
| --- | --- | --- | --- |
| v0.12.1 | 4 | 137 | not traced |
| first fix only | 16 | 38 | 30 in 6 of 10 traced runs |
| both fixes | 6 | 4 | none |

Three of the six runs with both fixes went from the v0.12.1 release binary to
the fixed build and back, three used the fixed build in every phase. The release
itself is checked as before: CI, the release workflow's image scan and
container fault tests on the exact image, and the rolling upgrade drill from
v0.12.1 with the release binaries. The release page records the results and the
image digest.

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

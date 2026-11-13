# CronosDB v0.13.0

This release stops two kinds of state from growing for the life of a cluster:
a consumer group's per-event progress records, and the cluster's metadata log.
A node that starts now also waits until its copy of the cluster metadata has
caught up before it takes up or gives up any partition. It keeps the version-2
WAL format and changes no API, flag or metadata format: v0.12.2 data
directories and cluster state are used as they are. It is not 1.0: the release
gates of the [1.0 readiness review](RELEASE_READINESS_1.0.0_2026-10-28.md) and
the [deployment validation record](PRODUCTION_VALIDATION.md) (a multi-host
soak, production-sized recovery and upgrade drills, physical power-loss
testing) are still open.

## Who should upgrade

Every clustered deployment. Each change of a partition's leader writes a record
to the partition's log that no consumer group takes, so in v0.12.2 every group's
progress stopped growing compactly at the first failover, not only where several
topics share a partition. Deployments that restart nodes often benefit from the
metadata snapshots. The supported upgrade from v0.12.2 is unchanged: stop every
node, replace the binary or image, and start them again; data directories are
kept.

## Changes

- **A group's progress no longer piles up behind entries it does not take.** A
  group's progress on a partition is a floor, below which every event is
  complete, and a record for each completed event above it. The floor moved
  over completed offsets only, so it stopped at the first entry of another
  topic in the partition, or at the record a new leader writes when it starts
  its term, until that entry left the log with its segment. Everything the
  group completed above it was kept as a record of its own, and the records,
  the progress sent to followers and the range the redelivery sweep reads grew
  for as long as the entry stayed. The redelivery sweep, which reads the log
  from the floor anyway, now raises the floor over every offset that holds an
  entry the group does not take or one of the group's completed events, up to
  what may be delivered, and the records it covers are removed. A consumer
  group belongs to one topic, so the floor never passes an event of the group
  that is not complete. After the upgrade the first sweep of each group raises
  its floor over what has piled up. `TestSharedPartition_GroupFloorPassesTheOtherTopic`
  fails without the change.

- **The cluster's metadata log is snapshotted.** Raft's default of a snapshot
  every 8,192 entries meant a cluster's metadata log, which grows by an entry
  for each change of placement or leadership, was never snapshotted: every
  start replayed the whole log, and the log only grew. It is now snapshotted
  every 128 entries (checked every 60 seconds) and when a node stops, keeping
  the last 1,024 entries for followers that are briefly behind. The snapshot
  holds the nodes and one record per partition, in the format earlier versions
  use for their snapshots, so a node rolled back to v0.12.2 starts from it.

- **A starting node acts on the cluster metadata only once its copy is
  current.** While a node replayed the metadata log, its leadership loop acted
  on each assignment it went through: it took up partitions it had led long
  before and released others, and v0.12.1's take-up guard declined the stale
  ones. A follower now waits until it has heard from the leader, its commit
  index has reached the end of its log and everything up to it is applied,
  settled for a second; a node that wins an election first applies everything
  committed in earlier terms. Until then the node neither takes up nor gives up
  partitions, makes no placement decision as the Raft leader, and its
  `/health/ready` reports `not_ready` with "this node's copy of the cluster
  metadata is still catching up". That needs a Raft quorum: a node that starts
  alone stays not ready until enough of its peers are up. Startup and liveness
  (`/health`) are unaffected, and the Helm chart starts its pods in parallel
  with not-ready addresses published, so a whole-cluster restart forms as
  before. A snapshot sent to a running node that fell behind now reports the
  assignments and node removals it carries, as applying their entries would
  have. Each new test in `internal/cluster/metadata_test.go` fails with its
  part of the change removed.

## Verification

The unit suites, and the acceptance suite on processes on one Windows machine
(fault campaign, certificate rotation, CDC through failovers, growth from three
to five nodes, whole-cluster restart, overload, lost and replaced nodes, a
leader replaced by an empty node, a backlog larger than memory, restore from
backups), passed with the changes. In that suite's node logs, the build before
the readiness condition was complete had restarted nodes decline 3 stale epoch-1
assignments met while replaying the metadata log; the release build declined
none. Its 3 remaining declines were at current epochs, where another replica
held more of the log. That suite kills its nodes rather than stopping them, and
none reached 128 entries, so every restart in it replayed the metadata log; the
snapshot at a clean stop and the start from it are covered by the unit tests
(`TestMetadata_SnapshotAtShutdownServesTheNextStart`). The release itself is
checked as before: CI, the release workflow's image scan and container fault
tests on the exact image, and the rolling upgrade drill from v0.12.2, whose
rollback phase runs v0.12.2 nodes on data the new build has written. The
release page records the results and the image digest.

## Known limits

Unchanged from v0.12.2, apart from the completion floor: replication lag is
reported by a partition's leader only; retention and compaction take partition
0 as "all local partitions"; schemas cannot be registered at run time; manual
rebalance returns `Unimplemented`; tenant limits cover publish rate only;
automatic lag-triggered snapshot selection and dead-letter journal compaction
are not implemented. This release makes no new capacity claim.

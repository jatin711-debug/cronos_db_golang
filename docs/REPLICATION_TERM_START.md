# Current-term commitment and term-start records

Released in [v0.8.0](RELEASE_V0.8.0.md). The coordinator activates this protocol
after capability agreement; it does not require `--event-lifecycle`. A mixed
cluster retains legacy commitment accounting, including the old-term defect,
until agreement. Complete the upgrade before relying on this guarantee.

An entry written by an earlier leader is not committed just because a later
leader copies it to enough replicas. A conflicting replica whose last log term
is newer can still win the next election and replace it. The regression in
`internal/api/old_term_quorum_test.go` reproduces this with three real WALs:
term 2 leaves `y` only on A, term 3 leaves conflicting `z` only on B, and A leads
again in term 4 and copies `y` to C. Replica counting alone previously delivered
`y`, although B could then lead and remove it.

The fix follows the current-term commitment restriction in
[the Raft paper, section 5.4.2](https://raft.github.io/raft.pdf): confirming an
entry of the leader's current term also commits the prefix before it.

## Commitment boundary

A partition in term-start mode appends an internal record when a local leader
activates a term, including after a restart. The marker has no message ID, topic,
payload or schedule; its only metadata is `cronos.internal.term_start=1`.
The WAL assigns its physical offset and term. The leader fsyncs it before
publishes can follow, and a follower fsyncs any replicated batch containing it
before acknowledging. Catch-up transfers it on an idle partition too.

The replication leader remembers the offset of that durable local marker.
A replica-count watermark below the marker proves no new commitment. Once
enough replicas acknowledge the marker or a later entry, the confirmed prefix
is committed. This proof stays valid when a follower disconnects or is removed;
delivery, held-write resolution, retention and export use the same boundary.
A new epoch installs a fresh commit generation so a delayed calculation or
cached acknowledgement cannot act as its predecessor's proof. Quorum checks
use memory and add no per-event disk reads or fsyncs.

Repeated reconciliation of one running leader writes no extra marker. A new
epoch, or a restart that reactivates leadership, writes one. The record consumes
a physical WAL offset but does not represent a user publish or consumer ACK.

## Rolling upgrades and durable format requirements

This build advertises `term-start/1`. The coordinator adds that requirement to
a replicated partition only when every assigned replica, handoff target and
Raft voter is alive and advertises support. Requirements are one-way Raft
metadata: placement, elections and replacement voters must continue honoring
them. A membership advertisement alone is not permission to change a log.

Before agreement, the partition uses the legacy format and commit accounting,
including its old-term limitation. Mixed clusters do not receive new records.
After agreement, reconciliation switches the leader and persists
`term-start.version` before its first control append. Downgrading a required
partition or a voter to a build that ignores requirements is unsupported.
Replicated lifecycle mutations additionally require this commit protocol.
Lifecycle activation is opt-in, and also waits for every assigned replica and
Raft voter to advertise the lifecycle append and recovery snapshot protocol.

Every append, catch-up request and empty probe of a term-start log uses the
dedicated `AppendTermStart` RPC. It names the term-start format and, independently,
the lifecycle format when required. An older server returns `Unimplemented`;
the sender never falls back to ordinary append. Updated receivers also reject
plain/lifecycle-only appends once the partition requires term-start support.
Malformed control records and unsupported versions are rejected before appending.

Snapshots advertise and verify the term-start format before installing files.
The source captures the format and WAL checkpoint under the replication lock,
so a concurrent format switch cannot put controls in a legacy snapshot.
The receiver persists its marker before installing the WAL. Full backups retain
the marker, while recovery recognizes retained controls when rebuilding stores.

## Reader behavior

- Delivery and scheduling discard term-start records. Public replay also waits
  behind the committed boundary and never emits a control record.
- Dedup recovery does not claim their empty message IDs. Consumer progress
  continues to refer to the physical offsets of actual delivered events.
- Retention treats an accepted control as terminal without requiring a matching
  consumer group. An uncommitted control cannot release a segment.
- Change feeds skip controls but advance through their physical offsets. Remote
  fetch reads the committed prefix and returns `next_offset` so even a page
  containing only controls can advance its cursor. Older clients can ignore
  this additive response field and still receive no internal events.
- Partition splitting skips controls; a leadership proof belongs to its source
  log and is not a publish to copy into another partition.

The regression tests cover old-prefix catch-up without its term-start record,
idle commitment once the record arrives, epoch changes, quorum loss, incompatible
receivers, capability agreement, election preference, malformed records, snapshot
refusal/install/restart, interrupted snapshot retries, lost success replies followed
by leader death, change-feed cursors, retention and backup recovery.

The three-node acceptance campaign compares authenticated physical WAL
snapshots, including control records, for contiguous offsets and identical
events on every replica. Public replay cannot establish that invariant because
it omits controls and serves only a leader's committed prefix. Restoring the
old replica-count-only rule makes the old-term regression fail even with the
new versioned transport still enabled.

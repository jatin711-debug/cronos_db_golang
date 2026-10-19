# WAL retention, cancellation, and rescheduling

Status: standalone implementation, the accepted-state index prerequisite,
replica capability gating (step 1 of the replicated protocol), the versioned
append and snapshot transport (step 2) and quorum-accepted mutations on a
replicated leader (step 3) on `retention-and-cancel`. The opt-in implementation is limited to standalone nodes.
Cluster rollout is a separate, gated step.

## Problem and invariants

Replicated logs must retain a contiguous offset range. One event scheduled a
month ahead can therefore pin a closed segment and every later segment, even
when consumers have finished the later events. Consumer completion refers to a
physical WAL offset; moving an event must not make it look completed or deliver
both copies.

The WAL remains authoritative. A mutation appends a record; it never edits the
old record. A replacement carries its complete payload and logical identity.
The replacement is fsynced before the old copy becomes removable. Derived
indexes and scheduler references may be discarded and rebuilt from retained WAL.
All scans and copying use byte and record limits, not backlog-sized allocations.

An event's stable reference is `(partition, original offset, original term,
message ID, topic)`. The term and message ID prevent offset reuse after a cut
from selecting another event. Deduplication TTL is not an identity boundary.
An enabled standalone partition persists a new generation term on every open,
above both its fencing file and retained WAL terms. Thus even reuse of the same
message ID at a lost periodic-fsync offset cannot match the previous handle.
Rescheduling and relocation preserve the reference. The latest user mutation
defines a revision `(offset, term)`; relocation changes the physical offset but
preserves the revision. A caller must supply the expected revision.

## Mutation contract

`GetEventState`, `CancelEvent`, and `RescheduleEvent` require publish permission
for the reference's topic. A reference is checked against the actual stored
event, including topic and partition. Mutations require a request ID and an
expected revision. Retrying the latest identical request succeeds without
appending again; a changed request with the same ID is rejected, and an older
revision conflicts. Request IDs are scoped to an event, not global publish IDs.

The first implementation changes only events whose current schedule is still
in the future when the mutation obtains its lock. Cancellation is terminal;
rescheduling requires another future timestamp. Due events are rejected. This
is not a recall protocol for events already sent to consumers. A partition
delivery gate covers eligibility checking through the actual stream send, and
mutations hold the exclusive side of that gate through durable commit.

An append followed by a failed fsync has an unknown outcome. The WAL fails
closed, and no further delivery or mutation is allowed until recovery. A retry
after recovery resolves the WAL record. A successful mutation means the local
WAL has been fsynced, including when ordinary publishes use periodic fsync.
State lookup flushes a just-published buffered tail before reading a handle;
it does not return a spurious missing-event error while periodic fsync waits.

## Record format and recovery

Version 1 uses a reserved `cronos.internal.*` metadata envelope in the existing
CRC-protected WAL record format. Publish and batch publish reject this namespace.
Records identify `cancel`, `reschedule`, or `relocate`, their original reference,
their predecessor, request ID, and the revision preserved by relocation.
Cancel records have no payload. Reschedule and relocation records contain the
complete event. Internal envelopes are removed from public event responses.

A small disk-backed index stores the latest physical offset for each mutated
reference, the retained mutation offsets and terms for that reference, and
offset-ordered cleanup keys. It contains no payloads. An accepted-state lookup
seeks directly to the newest retained mutation at or below a supplied boundary;
it does not scan payloads or earlier versions.

The local head and the accepted head have different purposes. A staged successor
freezes delivery of its predecessor while the outcome is unresolved. It cannot
make that predecessor removable. Only an accepted successor proves that an old
copy is terminal for retention; an accepted cancellation is itself terminal.
An active reschedule or relocation keeps its consumer obligation. Replay checks
the delivery boundary too, and state/mutation retries return `Unavailable` while
the current physical head is unaccepted instead of reporting a successful change.

Opening an enabled partition rebuilds the index before timers or subscribers can
run. Recovery reads at most 1,024 offsets with a 4 MiB target per chunk; a read
may exceed that byte target by one valid WAL record. Readers cannot see
a partially rebuilt generation. A fresh open, failed/canceled rebuild, conflicting
observation, or partial cleanup leaves the index unavailable until a complete
rebuild. Index errors fail delivery and retention closed. A suffix cut rebuilds
under the delivery gate before offsets can be reused; cached mutation records
must also match the indexed term. The caller reconstructs timers before promotion.

Prefix pruning removes obsolete index keys in batches of 128 records and advances
a retained-offset floor. Old worker/replay references stay inactive after the last
head is collected. Following a leader's log start cleans the derived history too.
A reference expires once no retained WAL record represents its state.

## Bounded retention

With this feature enabled, standalone pruning also removes only a prefix.
Before ordinary pruning, a pass may move active future events out of the oldest
closed segment. The pass has a byte budget and never moves due work, so consumer
completion does not need translating between physical offsets. A second pass
can continue an incomplete evacuation without copying an event twice. The
active segment is never evacuated. Canceled and superseded records are terminal
for retention, even if their original schedule is in the future.

Copying is disabled by default and has a separate per-pass byte budget. It does
not bypass disk admission. A slow consumer, an unavailable change feed, or a
replication requirement remains a retention blocker. Relocation is not a disk
quota: the destination can retain unused space until a later pass. A future
retention candidate is not copied unless the selected pruning policy can reach
its oldest segment. Cancellation and rescheduling work with relocation disabled.

## Rollout boundary

`--event-lifecycle` is off by default and requires standalone mode with RF=1.
Change feeds, experimental features, and partition splitting are rejected for enabled partitions until
their logical-event contracts are implemented. Existing deployments keep their
current write and delivery paths. A durable format marker and WAL inspection
prevent this build from reopening lifecycle data with the flag disabled. Do not
downgrade lifecycle data to v0.7 or an older executable that cannot read it.
Backups retain the control records and rebuild the derived index on restore.

Replicated enablement must add a dedicated versioned append RPC, capability
checks for every assigned replica, and a snapshot capability. An additive flag
on the existing append request is insufficient: an older receiver would ignore
it and persist a control record as an event. The leader must retain the original
until the replacement is accepted by the configured quorum and required readers
have advanced. Catch-up, suffix cuts, promotion, snapshot install, CDC export,
and delayed acknowledgements all need fault tests before lifting this restriction.

## Replicated protocol to implement next

The index now distinguishes the latest local head from history through an accepted
boundary. Retention and replay use that distinction, with held-range tests modeling
an unresolved mutation. This prerequisite does not enable replicated lifecycle
records. The cluster protocol still needs the following state and transitions:

1. **Persist the partition's format requirement in cluster metadata.**
   Implemented; see [Replica capabilities](#replica-capabilities) below. Every
   assigned replica must advertise support before enabling mutations, and
   placement and elections must exclude unsupported nodes afterwards. Checking
   just the currently connected quorum would leave an assigned older replica
   unable to catch up and potentially eligible for leadership. Enabling the
   format is a one-way operation for that partition, not a per-request hint.
2. **Append through a dedicated versioned RPC.** Implemented, with the
   snapshot capability; see [Versioned append](#versioned-append) and
   [Snapshots of a log in lifecycle mode](#snapshots-of-a-log-in-lifecycle-mode).
   Normal catch-up must select it
   whenever a batch contains lifecycle records. The receiver validates the
   version and envelope before changing the WAL. A legacy receiver returns
   `Unimplemented` without writing anything. Snapshot requests also carry a
   capability; a source refuses a receiver that cannot read its retained format.
   Empty probes, reconnects, replacement replicas, and bulk catch-up must obey
   the persisted requirement too.
3. **Separate staged state from accepted state.** Implemented for mutations on
   a replicated leader; see [Mutations on a replicated leader](#mutations-on-a-replicated-leader).
   Serialize changes with the
   partition's append/replicate lock, fsync locally, and replicate in WAL order.
   A staged successor freezes delivery of its predecessor while its outcome is
   unresolved; it is not permission to prune the predecessor. The replacement
   is deliverable, and the predecessor reclaimable, only after the accepted
   watermark reaches the successor. Mutation success requires that durable
   quorum, not just local append. Timeout has an unknown outcome and retries
   use the same request ID and expected revision.
4. **Keep enough versions to resolve the accepted boundary.** Recovery rebuilds
   mutation history from the retained log, including tentative records, in
   byte-bounded chunks. A suffix cut removes staged successors and their
   derived state before offsets can be reused. Promotion establishes the new
   leader's quorum boundary before releasing frozen work. A retained
   self-contained copy remains resolvable when its original offset was pruned;
   message-ID TTL is never used as the recovery frontier.
5. **Retain until every obligation is durable.** Evacuation still targets only
   future work and copies within a pass budget. The leader first confirms the
   replacement at quorum, then verifies consumer completion for other records,
   CDC position, and replicated dead-letter state, then removes a contiguous
   prefix. It advertises the new start only after that decision. A follower
   below the start resets there and catches up the retained replacement records;
   raising its consumer floor must not complete those newer physical offsets.
6. **Define the change-feed protocol explicitly.** A relocation advances the
   feed cursor without exporting a second publish. A cancel/reschedule is an
   ordered logical operation if the sink opts into that version; it must not
   masquerade as a new ordinary event. An original publish that the sink has
   not exported remains a retention obligation. Partition migration also needs
   an identity mapping rather than copying raw envelopes into another partition.
7. **Snapshot a consistent recovery boundary.** The manifest names the format
   and accepted boundary alongside WAL, consumer progress, and dead letters.
   Snapshot install validates the capability and files before switching
   generations, then rebuilds the derived index. A crash during install chooses
   a complete old or new generation; it cannot combine old mutation state with
   new offsets.

### Replica capabilities

Each node advertises the capabilities of its build in every membership message
it sends: join requests and replies, heartbeats, and the broadcast announcing a
new node. A message without them means none, which is what builds of v0.7.0 and
earlier send; unlike a missing address, a heartbeat without capabilities replaces
what the sender advertised before. This build advertises `lifecycle-records/1`:
it reads version 1 lifecycle records and refuses them on the legacy append path.
`lifecycle-replication/1` names the versioned append, catch-up and snapshot
protocol of step 2. No build advertises it yet, so no replicated partition can
require the lifecycle format (`cluster.LifecycleRequirement`).

A partition's requirement is `PartitionInfo.Requires` in the Raft state. A
dedicated command adds to it and nothing removes it: an assignment update from a
coordinator that predates requirements carries none and leaves it as it was, and
the change does not raise the partition's epoch. The coordinator commits it
(`Manager.RequirePartitionCapabilities`) only when every node the partition is
assigned to and every Raft voter is alive and advertises every capability. A
voter matters because any voter can become the Raft leader, which places
partitions and elects their leaders, and a voter of an older build would do so
without knowing the requirement. No admin command exposes it until step 2 can
be enabled.

Once a partition requires capabilities:

- An election chooses only among replicas that advertise them. When a replica
  without them holds more of the log than every replica with them, nobody is
  elected: its extra entries may be acknowledged. The election resumes when it
  advertises them or a capable replica catches up.
- Placement follows the hash ring only onto nodes that advertise them;
  otherwise the partition stays on its replicas, and the refusal is logged when
  it changes.
- Leadership is handed over only to a replica that advertises them. A handoff
  under way is abandoned when its target stops advertising them.
- A node that lacks a capability some partition requires is not added to the
  Raft group as a voter.

A node counts as supporting nothing while membership does not count it alive,
since what it will run when it returns is unknown. The checks cannot reach a
voter that is downgraded after a requirement is committed: its older build
ignores the requirement, and as Raft leader would place and elect without it.
Downgrading a cluster whose partitions require capabilities is unsupported.

### Versioned append

Lifecycle records travel between replicas only in `AppendLifecycle`, whose
request names the record format and wraps an ordinary append request. A build
before it answers `Unimplemented`, and the plain `Append` refuses lifecycle
records, as it did before.

A leader whose partition is in lifecycle mode sends every request through
`AppendLifecycle`: new entries, catch-up from the WAL, and the empty probes
that carry the log start. A follower without it therefore receives nothing
more of that log, not even the ordinary events between lifecycle records,
holds no copy and counts towards no quorum; elections and placement already
keep it out (step 1). A leader whose partition is not in lifecycle mode refuses
to send a batch with a lifecycle record at all.

The receiver checks everything before it writes anything: the format version,
that the cluster has committed the partition's requirement as this node knows
it, and every lifecycle record (`lifecycle.Decode`, and its partition). The
first accepted request switches the partition to lifecycle mode
(`PartitionManager.EnableReplicatedLifecycle`): the marker is written durably,
then the index is built from the retained WAL and published. Nothing produces
lifecycle records in a cluster before the switch, so that index is empty, but
building it reads the retained log once, under the partition's replication
lock. In lifecycle mode the plain `Append`, which only a leader of an older
build sends, is refused. A replicated batch with lifecycle records is fsynced
before it is acknowledged, whatever the fsync mode, and its records are added
to the index as tentative ones. Which of them the leader accepted is settled
when a replica is promoted (step 4).

In a cluster, a partition with a lifecycle marker opens in lifecycle mode
without `--event-lifecycle`, except with a change feed or experimental
features, which it refuses. A standalone node still requires the flag. Because
a partition of a cluster can switch while it runs, the dispatcher's lifecycle
hooks are installed for every partition of a cluster; until the switch they
pass every event through after one atomic load.

### Mutations on a replicated leader

`GetEventState`, `CancelEvent` and `RescheduleEvent` on a partition of a cluster
are served by its leader; a follower answers `Unavailable` and writes nothing.
The leader switches the partition to lifecycle mode on the first such call once
the cluster has committed its requirement, and from then on replicates through
`AppendLifecycle`; without the requirement the calls answer `FailedPrecondition`
as on a node without `--event-lifecycle`. A mutation announces itself like a
publish (`BeginPublish`), so that a leadership handoff waits for it.

Under the partition's replication lock and then the delivery gate, the leader
checks the request, appends the successor, fsyncs it and adds it to the index,
which freezes the event it replaces. It releases the delivery gate, since the
staged successor already keeps the predecessor from being delivered, and
replicates the successor like a publish. Success is reported once the replicas
the partition requires hold it, each of which fsyncs it before acknowledging.
Otherwise the successor stays in the leader's log, held as unaccepted, and the
call answers `Unavailable` with an unknown outcome. While it is unresolved the
event is delivered in neither version, its state reads `Unavailable`, and
another change is refused the same way. Catch-up that brings the successor to a
quorum accepts it, and the unchanged retry then reports success without
appending again; a new leader whose log lacks it removes it, and the retry
appends it anew there.

A cancellation releases the tenant's delivery credit when its call succeeds. A
cancellation accepted later through catch-up does not, because the retry that
reports it cannot tell whether the credit was released.

How a newly promoted leader settles the tentative records it holds is step 4;
until then, this is reachable only in tests: no build advertises
`lifecycle-replication/1`, so no replicated partition can require the format.

### Snapshots of a log in lifecycle mode

A snapshot request says which lifecycle record format the receiver reads. A
source whose partition is in lifecycle mode refuses a receiver that does not
read its format, before it sends anything, and names the format in the
snapshot's trailer. The receiver installs such a snapshot only when the cluster
has committed the partition's requirement as it knows it. It switches the
partition to lifecycle mode, marker first, before the snapshot replaces its log,
and rebuilds the index from the installed log afterwards. A crash between the
two leaves a partition that reopens in lifecycle mode and rebuilds its index
from whichever log generation is complete. A receiver already in lifecycle mode
rebuilds its index after any snapshot, since the index described the log that
was replaced.

This is the transport only. Which tentative records a snapshot's source had
accepted, and a manifest that names that boundary together with consumer
progress and dead letters, are step 7.

Before enabling this protocol, fault tests must cover leader death before and
after quorum, lost success replies, an unaccepted replacement becoming due,
replica reconnect to a legacy server, suffix replacement with offset reuse,
follower reset after prefix deletion, and interrupted snapshot installation.
Assert one logical pending event after every recovery, no delivery of controls,
no resurrection after a successful cancellation, and no consumer completion
inherited by a relocated event. Run these first on a bounded three-node fixture,
then on the separate fault rig; a peak-throughput run is not a substitute.

## Validation

Cover future cancellation, rescheduling earlier/later, stale revisions, exact
retries, forged metadata, wrong topic/term/message ID, due-event rejection,
delivery-vs-mutation exclusion, crash/restart reconstruction, prefix deletion
with a pending relocated event, bounded evacuation, encrypted WAL, periodic
fsync, backup/restore, and unchanged default retention. Replicated configurations
must reject enablement and ordinary replication must reject internal records.
Run focused tests with low Go concurrency on the development laptop; do not run
a throughput load as part of this change.

Acceptance-boundary regression tests additionally cover staged cancel/reschedule/
relocate records, later tentative heads hiding earlier accepted versions, reordered
observations, reused offsets with different terms, partial or canceled recovery,
partial index cleanup, concurrent rebuild readers, and follower prefix resets.
They assert that an unresolved mutation cannot release a prefix, report success,
reach replay, or cause redundant evacuation of a segment that must remain pinned.

Capability tests (`internal/cluster/capabilities_test.go`) cover a requirement
surviving an update from an older coordinator and a snapshot, its refusal while
an assigned replica lacks it or a voter is not alive, an election that skips an
incapable replica holding the same log and waits while one holds a longer log,
placement and handoffs that stay off incapable nodes, a handoff abandoned when
its target is downgraded, voter admission, and capabilities carried and replaced
through membership. Each fails with the corresponding check removed.

Versioned append tests (`internal/api/replication_lifecycle_test.go`,
`internal/replication/lifecycle_append_test.go`) cover refusals that write
nothing (no requirement, unknown format, malformed record, record of another
partition), the switch, storage and indexing of a cancellation, refusal of the
plain append afterwards, reopening in lifecycle mode after a restart, refusal of
lifecycle data by a standalone node without the flag, a leader that uses only
`AppendLifecycle`, refuses lifecycle records without a format, and sends nothing
to a follower that lacks the call. Snapshot tests cover a source that refuses a
receiver which does not read the format and names it in the trailer, and a
receiver that refuses without the requirement, switches before the install,
and keeps the cancellation after it and after a restart. Thirteen negative
controls, one per check, make them fail.

Replicated mutation tests (`internal/api/replicated_lifecycle_test.go`) cover a
cancellation accepted at quorum and held by both replicas, one whose outcome is
unknown while the follower refuses appends (the event frozen, its state and
further changes refused, no second append on the retry that succeeds after
catch-up), a reschedule whose replacement is the active copy on both replicas,
a follower that refuses changes, and a leader without the requirement. Five
negative controls make them fail.

## Using the standalone implementation

Start an isolated node with `--event-lifecycle --replication-factor=1`; optionally
set `--wal-relocation-bytes=8388608` to allow at most 8 MiB of encoded records per
compaction pass. The default is zero copying. Matching environment variables
are `CRONOS_EVENT_LIFECYCLE` and `CRONOS_WAL_RELOCATION_BYTES`.

Use the generated `types.EventServiceClient`. Save `partition_id`, `offset`,
and `term` from `PublishResponse`, together with the message ID and topic, as an
`EventReference`. Batch callers can request `return_references=true`; the
response's unordered `references` contains receipts for accepted events, matched
by message ID and partition. Already-accepted duplicates have no new receipt.
The Go producer's single-message `SendResult` also exposes `Term`.

Call `GetEventState` with that reference, then send `CancelEvent` or
`RescheduleEvent` with the returned revision offset/term and a unique request
ID. Rescheduling also supplies the new Unix millisecond timestamp. Save the
reference unchanged, and use the returned revision for the next user change.
On an ambiguous transport error, retry the identical request. `Aborted` means
another mutation changed the revision; read the state before deciding again.
`NotFound` means the reference is invalid or its state has expired from retention.
Due events and already-canceled events return `FailedPrecondition` for a new
mutation. Reusing the latest request ID with changed arguments is `InvalidArgument`.

`Replay` returns the currently active physical records, preserving its existing
offset-range behavior. Canceled and superseded versions are filtered; relocation
does not preserve the original physical replay position. Live consumer ACKs
continue to refer to the physical records actually delivered. Replicated
rollout, CDC semantics, and partition migration remain outside this first slice.

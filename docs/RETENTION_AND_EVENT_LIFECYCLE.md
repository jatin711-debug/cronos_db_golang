# WAL retention, cancellation, and rescheduling

Status: standalone implementation and the accepted-state index prerequisite on
`retention-and-cancel`. The opt-in implementation is limited to standalone nodes.
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

1. **Persist the partition's format requirement in cluster metadata.** Every
   assigned replica must advertise support before enabling mutations, and
   placement and elections must exclude unsupported nodes afterwards. Checking
   just the currently connected quorum would leave an assigned older replica
   unable to catch up and potentially eligible for leadership. Enabling the
   format is a one-way operation for that partition, not a per-request hint.
2. **Append through a dedicated versioned RPC.** Normal catch-up must select it
   whenever a batch contains lifecycle records. The receiver validates the
   version and envelope before changing the WAL. A legacy receiver returns
   `Unimplemented` without writing anything. Snapshot requests also carry a
   capability; a source refuses a receiver that cannot read its retained format.
   Empty probes, reconnects, replacement replicas, and bulk catch-up must obey
   the persisted requirement too.
3. **Separate staged state from accepted state.** Serialize changes with the
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

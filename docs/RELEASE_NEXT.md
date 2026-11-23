# CronosDB, next release (unreleased)

These notes collect what changed since v0.14.1. The version number is chosen
when the release is cut. It is not 1.0: the release gates of the
[1.0 readiness review](RELEASE_READINESS_1.0.0_2026-10-28.md) are still open
(a passing 24-hour soak, a soak on separate hosts, power-loss testing on real
disks, recovery and upgrade drills at production size).

It fixes the two limits the v0.14.1 notes list: events delivered a second
time when a partition's leadership moves, and the copy a starting node made
and then refused. Neither lost or skipped an event.

## Fixes

- **A node that returns leads again only once it knows what was finished
  while it was away.** A partition's leadership was handed back to a returning
  node as soon as its log ended where the leader's did. What each consumer
  group has finished is copied to a follower separately from the log, and the
  handoff did not wait for it: the node led with its log caught up and its
  record of the groups' progress where it had been when the node stopped, and
  delivered everything since a second time. In the v0.14.1 soak a leader away
  for 286 seconds at 450 publishes per second repeated about 64,000 events
  per group twelve seconds after it returned. A handoff now neither starts
  nor completes until the leader reports that its target holds the consumer
  progress. In the acceptance test that loses a leader for 1,600 publishes
  and takes it back, 230 events were delivered twice after its return before
  and 7 are now.

- **Followers keep up with what the groups finish, however much lies above
  their floors.** A follower is sent the full copy of the progress when its
  connection is made, and every 30 minutes; in between it is sent what
  changed. The full copy is every completion above each group's floor, and an
  event scheduled hours ahead holds a floor for those hours. Three things
  kept followers behind in that case:

  - The copy went a page of 4,096 completions per half second, so half a
    million completions took a minute per group. It now goes page after page
    in one pass.
  - A copy of several pages was judged against where the follower's log had
    ended when the copy began. Under load consumers had finished later events
    by its end, so the copy always counted as cut short and was begun again
    at the next pass, without end: followers were never nearer than a copy's
    length to what their leader knew, and the leader spent its time sending
    the copy. Each page is now judged against the follower's log as that page
    is sent; a few completions beyond it wait for the next delta, and only a
    follower that is far behind is sent the copy again.
  - When more than 65,536 completions had been recorded since a follower's
    version, it was given the full copy instead of a delta. A delta now
    carries about that many and the next one goes on from there.

  The soak showed what this cost: its first failover, 20 minutes in, repeated
  nothing; one 77 minutes in, to a node that had never been away, repeated
  15,920 events for one group; the restart of the whole cluster after three
  hours repeated 35,313 and 68,115.

- **A node that starts with a log of its own does not copy it again.** A node
  that already knew its partitions' leaders when it started asked each for a
  copy of the whole log, however little it lacked, and then refused to
  install it (`Failed to sync partition N: ... does not require the
  term-start format`), because the check of the partition's log format was
  put in place only later in the start. The start waited for the download and
  the files stayed in the partition's `snapshot-staging` directory. A
  joining node now copies a partition only when it holds nothing of it and
  has read the cluster state; otherwise its leader catches it up as a
  follower, by replay or by a copy as its distance calls for. The format
  check is in place before the node joins. Files left in `snapshot-staging`
  by an earlier version are replaced or removed by the next snapshot of that
  partition; they can be deleted while the node is stopped.

## Compatibility

- **On disk:** nothing changes.
- **Wire:** the replication position reply gains `progress_reported` (field
  7) and `progress_followers` (field 8). A leader of v0.14.1 or older does
  not send them, and a handoff from it goes ahead when the logs are equal, as
  before; so do handoffs decided by a node of an older build. The full copy
  uses the same message as before, sent more often, and deltas are unchanged
  for a follower.
- **Flags:** none added, removed or changed.

## Verification

- New tests: the full copy goes in one pass and continues from a failed
  page; a copy during which the log grew is not begun again; a few
  completions beyond a follower's log go with the next delta and many send
  the copy again; which followers hold the progress; a large backlog of
  completions reaches a follower in several deltas, checked against the
  leader's state, and the journal against a plain model in bounded pieces; a
  handoff waits for its target to hold the progress, before it starts and
  before it completes, and goes ahead when the leader does not report it;
  and a joining node copies only an empty partition once the cluster state
  is read.
- `TestLeaderThatReturnsDoesNotDeliverAgainWhatWasFinished` (acceptance,
  three nodes as processes): fails on v0.14.1 with 230 events delivered twice
  after the leader's return, passes with 7.

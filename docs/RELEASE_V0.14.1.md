# CronosDB v0.14.1

This release takes costs off a leader's publish, acknowledgement and read
paths. In the soak at 450 publishes per second the nodes that lead partitions
use about a fifth of a core where v0.14.0 used a third to two fifths. A
partition with sixteen publishers takes four times as many publishes per
second, a group with a small credit window is delivered three times as many
events or more, and forty idle client connections hold 4 MiB of memory instead
of 161. It changes no API, flag, wire message or on-disk format: v0.14.0 data
directories and cluster state are used as they are, and v0.14.0 reads what
v0.14.1 has written. It is not 1.0: the release gates of the
[1.0 readiness review](RELEASE_READINESS_1.0.0_2026-10-28.md) are still open
(a passing 24-hour soak, a soak on separate hosts, power-loss testing on real
disks, recovery and upgrade drills at production size).

## Who should upgrade

Clustered deployments with more than one publisher on a partition, with
several consumers on a partition, or with consumer groups whose credit window
is small: those are where a partition took one publish, or one stream's
acknowledgements, per disk sync. Also nodes with many client connections and
little memory, and encrypted deployments with change feeds or consumers that
follow the end of the log, where reads cost a leader the most CPU.

The supported upgrade from v0.14.0 is unchanged: stop every node, replace the
binary or image, and start them again; data directories are kept. Nothing is
converted at the first start, and going back to v0.14.0 is the same procedure.

## What behaves differently

- **`every_event` shares syncs on a replicated partition.** A publish is
  still acknowledged only after a sync that began after its append, on the
  leader and on enough followers. Publishes that arrive while one sync runs
  now share the next one, as they already did with `batch`; before, the
  partition took one publish per sync.
- **A leader sends a publish to its followers while its own sync runs.** A
  follower can therefore hold an entry durably before its leader does. The
  leader counts itself among the replicas that hold an entry only as far as
  its own log is durable, so an entry is acknowledged to its publisher, handed
  to consumers and exported no earlier than before: when enough replicas,
  the leader among them as far as it has synced, hold it durably.
- **Credits come back when an acknowledgement is read**, not when its
  completion is on disk. The `AckResponse` is still sent only once the
  completion is durable, and the events of a delivery are not handed out again
  in between. A node that loses power between the two delivers both windows
  again; before, it delivered one.
- **A connection's buffers are 64 KiB**, not 4 MiB, for reading and for
  writing. Publishing batches of about 4 MB over loopback without TLS took 8.0
  ms instead of 7.4; batches of 400 KB took the same. Behind TLS a read
  returns one record of at most 16 KiB whatever the buffer.
- **New log segments get an index entry at least every 256 KiB**, as well as
  every `--index-interval` events. An entry is 24 bytes held in memory, at
  most 10 MiB for 100 GiB of log. Segments written before keep the index they
  have, so reads of old segments cost what they did.

## Changes

- **Publishes to one partition share the leader's sync, and the leader's disk
  works while the followers' do.** A replicated publish appended under the
  partition's publish lock and waited there for its sync, and the followers
  were sent an event only once the leader's disk had finished with it. The
  append now writes its records out and gives the lock back; the publish waits
  for its sync and for its followers outside it. A leader and two followers on
  one disk, Linux, `every_event`: one publisher, 11 to 17 ms a publish before
  and 5.3 ms now.
- **A publish is told it is replicated when its follower says so.** A send to
  a follower carries the batches queued behind it. The sends of those batches
  still took whatever had been queued since and sent that before they
  finished, so with sixteen publishers on a partition a publish waited about
  ten round trips for an answer its follower had given in the first. With the
  change above: 187 to 245 publishes a second before, 962 now, and a request
  to a follower carries 8 events instead of 1.7.
- **The acknowledgements of every stream that waits are recorded in one
  write.** Each consumer stream brought its acknowledgements in a call of its
  own, and each call made its own durable write, one after the other. Calls
  that arrive during a write now wait and are recorded together by the next.
  Sixteen consumers acknowledging four events at a time on one partition: 561
  batches a second before, 3,346 now.
- **A consumer's next window does not wait for the disk.** See "Credits come
  back" above. One node, a group with a window of 64: 5,100 to 17,900 events a
  second from publish to acknowledgement before, 55,600 now; from a backlog,
  22,200 to 23,300 before and 71,200 now. With a window of 2,000 nothing
  changes (about 76,000 and 235,000).
- **A read finds its first record without decrypting the ones before it.** A
  range read found its first record from the nearest index entry by reading,
  and with encryption decrypting, every record in between: five hundred on
  average for a reader following the end of the log. It now passes over them
  by their lengths, which are stored in the clear, as a point read already
  did, and the denser index leaves fewer to pass over. On an encrypted log,
  reading the newest event took 1.1 ms and takes 22 µs; reading one event
  anywhere took 234 µs and takes 42 µs. In the v0.13.1 soak these reads were
  15 to 23 percent of a leader's CPU for the change feed and about a tenth for
  events the scheduler brought back from disk.
- **The progress journal drops its oldest entries without moving the rest.**
  A partition keeps its recent completions, up to 262,144 offsets, for
  followers that need only what changed. Once it held that many, every
  acknowledgement copied all of them to drop the oldest: 22 to 24 percent of a
  leader's CPU in the v0.13.1 soak. An acknowledgement recorded in a full
  journal took 1.2 ms and takes 147 ns.
- **Idle connections hold little memory.** Both gRPC servers gave each
  connection a 4 MiB read buffer and a 4 MiB write buffer for as long as it
  lasted: forty idle client connections held 161 MiB of heap, and hold 3.7 MiB
  now.

The publish, acknowledgement and delivery figures are from Linux (WSL2, ext4)
and the read, journal and memory figures from Windows, each with the
benchmark named in its commit.

## Compatibility

- **On disk:** nothing changes format. Index files of new segments hold more
  entries in the same format, which v0.14.0 reads.
- **Wire:** unchanged. v0.14.0 and v0.14.1 nodes work together during an
  upgrade: a v0.14.1 leader sends to its followers earlier than a v0.14.0
  leader does, and followers of either version answer as before.
- **Flags:** none added, removed or changed. `--index-interval` still gives
  the spacing in events; the spacing in bytes is fixed.
- **Clients:** unchanged. A consumer with a small credit window is sent its
  next window sooner.

## Verification

Before the release:

- **CI** (run 44997241670): every job passed, including the race suite, the
  fault campaign and the container tests.
- **Nightly jobs** (run 44997241289): the acceptance suite passed three times
  in a row and the container suite once; the rolling upgrade from v0.14.0,
  rollback and upgrade again passed; `TestPowerLoss` passed on a GitHub runner
  with `every_event` and `batch`; and the soak's remote mode passed against
  three nodes driven by command. The run before it (44899803697) failed in one
  test, `TestFollowerFarBehindCatchesUpFromASnapshot`, on the third pass of
  the suite: the test waited for a snapshot copy, and the leaders had dropped
  a finished segment 40 ms before the node returned, which left it fewer
  entries behind than the threshold, so it was caught up by replay, as it
  should be. The test now has its leaders keep their logs.
- **Soak comparison with v0.14.0** (nightly workflow, `catchup_ab` with
  profiles, run 44899806945): 40 minutes at 450 publishes per second with each
  server, three nodes of 2 CPUs and 384 MiB on one GitHub runner. Both passed,
  with nothing refused, early or of unknown outcome.

  | | v0.14.0 | v0.14.1 |
  |---|---|---|
  | CPU of each node that leads partitions, at minutes 15, 26 and 32 (share of one core) | 30%, 42%, 38% | 21%, 19%, 24% |
  | CPU of the third node | 8–9% | 7–8% |
  | Publish latency, median of the steady samples: p50 / p99 | 2.8 / 332 ms | 2.5 / 280 ms |
  | Slowest publish of the run | 1,445 ms | 724 ms |
  | Publishes the generator could not send on time, of 1.13 million | 10,712 | 1,777 |

  The run ends before the consumer group it stops has caught up, so it does
  not compare catch-up times.
- **Compressed soak** (RunPod, three nodes as processes on one 4-vCPU Linux
  host, 384 MiB each, encrypted WAL, `every_event`, 8 MiB segments, 4 hours at
  450 publishes per second with bursts of 1,200; the configuration v0.13.1's
  compressed soak failed in): passed all seven disruptions. It accepted
  6,690,058 events, and every one was delivered to both consumer groups and
  exported; none was refused, delivered early or left with an unknown outcome.

  | Disruption | Reconciled after | v0.13.1, same soak |
  |---|---|---|
  | Leader lost, its node down for 286 s | 304 s | 303 s |
  | Leader frozen for 20 s | 23 s | 27 s |
  | Leader lost while a follower catches up | 344 s | 479 s |
  | CDC destination down for 857 s | 877 s | 940 s |
  | Consumer group slow, then offline, for 857 s; 387,051 events behind | 902 s | not within 15 minutes; the run ended here |
  | Node replaced by an empty one (7.5 GiB lost) | 47 s | not reached |
  | Whole cluster lost | 67 s | not reached |

  The consumer group was back in step 44 seconds after it returned, with
  publishes held at 450 per second (median 1.1 ms; 99th percentile 60 ms in
  the minute of the catch-up). In v0.13.1 publishes fell to about 180 per
  second there and the group did not catch up. The soak allowed a disruption
  45 minutes to reconcile instead of its default 15, as the nightly comparison
  does; none needed more than the 902 seconds above, 857 of which are the
  outage itself.

  Over the steady minutes the median publish took 1.1 ms and the 99th
  percentile 5.3 ms. Publishes fell below 450 per second only in the minutes
  that lost a leader: 229 per second at the worst, when a leader was lost
  during catch-up, and 254 when the whole cluster was. The 5,341 failed
  publish attempts and every delivery more than 5 seconds late (57 seconds at
  the worst, after the whole cluster was lost) fall in those minutes too,
  apart from 33 failed attempts while the cluster started. The generator could
  not send 71,373 publishes on time, 1.1 percent of its target.

  Each node's heap in use stayed under 250 MiB of its 384 MiB limit (v0.13.1:
  258 MiB) and its resident memory under 277 MiB. Each node's data grew to
  9.3 GiB before the seeded events fell due, never more than 13 MiB above its
  file sizes, and was pruned to under 150 MiB by the end.

  Both groups were delivered some events twice: 245,686 for the main group
  and 266,049 for the slow one, all when leadership moved (a leader lost,
  frozen or replaced, the cluster restarted) and none in steady running; see
  Known limits.

The release workflow (run 45157279687) built both images from the tag, scanned
them, ran the network-partition and memory-limited restart tests on the amd64
image and started the arm64 one, then published them as one index,
`sha256:73e18d84b53e9649600dd67b95b38141853e53562e4e324ab9b1451bdb8fed0d`, with attested provenance, an SPDX SBOM and a cosign signature. The
index was copied unchanged, signature and attestations included, to
`johnny711dock/cronos-db:0.14.1` and `:latest`. `gh attestation verify` accepts
its provenance and its SBOM under both names. A dry run of the workflow on the
release commit (run 45146486432) had passed before the tag was made.

## Known limits

Unchanged from v0.14.0, and this release makes no new capacity claim. The
soaks ran on one host, so the separate-host soak gate stays open.

Measured for the first time in this release's soak, and also in v0.14.0: when
a partition's leadership moves, its new leader can deliver again events that
its consumer groups had already finished. Nothing is lost or skipped, and
consumers must handle repeats in any case, but the numbers are larger than a
change of leader should cost.

- **A node that returns is handed its partitions back before it knows what
  was finished while it was away.** The handoff waits for the node's log to
  end where the leader's does. The record of what each group has finished is
  copied to it separately, a page of 4,096 completions every half second, and
  the handoff does not wait for that. In the soak a leader was away for 286
  seconds at 450 publishes per second; 12 seconds after it returned it led its
  two partitions again and delivered about 64,000 events to each group a
  second time, which is what was appended while it was away. The nightly soak,
  with a 30-second outage at a tenth of the rate, repeats about 880 events per
  group on v0.14.0, and 800 and 220 on this release.
- **Later in a long run, a follower that takes over repeats more.** The first
  failover of the soak, 20 minutes in, repeated nothing. One 77 minutes in, to
  a node that had never been away, repeated 15,920 events for one group and
  1,945 for the other, and the restart of the whole cluster after three hours
  repeated 35,313 and 68,115. The followers' copies of the groups' progress
  were behind by then; why is not established yet. The code that copies them
  is the same in v0.14.0.

Both are the next release's work.

Found while testing this release, and also in v0.14.0: a node that already
knows its partitions' leaders when it starts asks each for a copy of its log
and then refuses to install it, because the check of the partition's log
format is not in place that early in the start. Its log shows
`Failed to sync partition N: ... does not require the term-start format`. The
node's own log is untouched and it catches up as usual, but the start waits
for the download, and the downloaded files stay in the partition's
`snapshot-staging` directory. Most starts do not know the leaders that early
and skip the copy. The next release removes it.

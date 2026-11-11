# CronosDB v0.12.1

This release fixes three defects found by a local soak test of v0.12.0. A node
that came back with an empty disk while it led a partition could have the other
replicas delete their logs; change data capture (CDC) could skip events after a
failover; and every replicated publish read the log back from disk. It also moves
to Go 1.26.11 for a `net/http` fix. It keeps the
version-2 WAL format and changes no API, flag or metadata format: v0.12.0 data
directories and cluster state are used as they are. It is not 1.0: the release gates
of the [1.0 readiness review](RELEASE_READINESS_1.0.0_2026-10-28.md) and the
[deployment validation record](PRODUCTION_VALIDATION.md) (a multi-host soak,
production-sized recovery and upgrade drills, physical power-loss testing) are
still open.

## Who should upgrade

Every replicated deployment should upgrade, and every deployment that enables
change data capture (`CRONOS_CDC_WEBHOOK_URL`) in particular.

Until it has, do not start a node with a new or wiped data directory under the
name of a node that may still lead partitions. Wait until `cronos-admin admin
topology` shows other leaders for all of its partitions, or remove the node
first (`cronos-admin cluster remove-node`).

The supported upgrade from v0.12.0 is unchanged: stop every node, replace the
binary or image, and start them again; data directories are kept.

## Fixes

- **A leader that came back with an empty disk could erase a partition.** The
  cluster names a partition's leader in its committed metadata, and a node led
  whatever that named it leader of. A node that lost its disk and was started
  again under its name before the others had seen it go, which takes a few
  seconds, was still named leader of what it led, and led it with an empty
  log. Its followers, which make their logs match the leader's, then removed
  everything they held, events acknowledged and not yet delivered among them;
  the soak lost about 5,400 such events this way. When the terms happened to
  match, the followers refused instead, and the partition took no publishes
  until an operator intervened. The defect is present from v0.6.0 through
  v0.12.0. A node now asks the other replicas where their logs end before it
  takes up a leadership, and does not lead while one of them holds more than it
  does; a node that has never held the partition also waits until another
  replica has answered. The node that coordinates the cluster holds a new,
  fenced election for a partition whose leader is alive, does not lead it, and
  holds less of the log than another replica, as it already did for a leader
  that is dead. `TestLeaderReplacedByAnEmptyNode` reproduces the loss on
  v0.12.0 and passes with the fix, and `TestFirstNodeReplaced` now checks that
  every acknowledged event is still in the log, not only that the replicas agree.
- **CDC could skip events after a failover.** A partition's leader sends dead
  letters to its followers in a request of their own, which carries no
  change-feed position. A follower took that request for word from a leader
  without a feed and moved its own feed position to the end of its log. When
  the leader was then lost before its export had caught up, the follower that
  took over continued from that position and never exported the events in
  between. Any delivery that ended in the dead-letter queue could start this.
  The defect is present from v0.7.0 through v0.12.0; a soak of v0.12.0 lost four
  acknowledged events from the CDC destination this way. A dead-letter request
  now leaves the follower's feed position alone unless it names one.
  `TestChangeFeed_DeadLettersDoNotMoveTheFeedPosition` fails without the fix.
- **Each replicated append read the log back.** Before appending a batch, a
  follower checks the term of the entry the batch follows, and the leader looks
  up the same term. Both read it from the segment, which means reading, and with
  encryption decrypting, up to an index interval of records. On the soak
  machine (Windows, encrypted WAL, `every_event` fsync) that was 94% of a node's
  CPU and about 33 ms per event, which held a three-node cluster to about 50
  publishes per second. The WAL now remembers the terms of the entries appended
  since it was opened, as runs that change only with leadership, and reads the
  segment only for older entries. New tests compare the remembered terms with
  the ones on disk through truncation, compaction, reopening, a reset of the log
  and encryption. On the soak machine a three-node cluster then held its target
  of 100 publishes per second with a median publish latency of 37 ms, against
  about 65 per second at 400 to 650 ms before. The gain on Linux has not been
  measured.
- **Go 1.26.11 replaces 1.26.10** in the module and the image builder, fixing
  `GO-2026-6859`, an HTTP/1 server crash from a header race in `net/http`'s
  `MaxBytesReader`. `govulncheck` reports it reachable from the server's HTTP
  listeners on v0.12.0.

## Tests

- `TestSoak` (opt-in: set `CRONOS_SOAK_DURATION`, at least `20m`) runs a
  three-node cluster with encryption, CDC, a slow consumer group and a
  dead-lettering topic under a steady workload with bursts, and does one
  disruption at a time: a leader lost, a leader frozen, a leader lost while a
  follower catches up, the CDC destination down, a consumer group behind, a node
  replaced by an empty one and the whole cluster lost. At the end every
  acknowledged event has to have reached both consumer groups and the CDC
  destination. It writes CSV and JSON reports of throughput, latency, lag and
  each node's memory and disk.
- `TestChangeFeedExportsEveryAcknowledgedEventThroughFailovers` exports a
  workload to a webhook while partition leaders are killed, frozen and lost
  during catch-up, and requires every acknowledged event at the destination.
- `TestLeaderReplacedByAnEmptyNode` wipes a partition leader's disk and starts it
  again at once, after a failover so that the log spans terms, and requires every
  acknowledged event to survive.
- `TestClusterGrowsFromThreeToFive` grows a cluster from three nodes to five
  under traffic, kills a leader while a replica on a new node is still being
  filled, restarts nodes one at a time, replaces one with an empty node, and
  removes one for good, then requires every acknowledged event delivered and
  each partition's replicas in agreement.

## Verification

A 25-minute `TestSoak` of the fixed build on one Windows machine (three node
processes, 384 MiB and 2 CPUs each, encrypted WAL, `every_event` fsync, 100
publishes per second with bursts of 500) passed every disruption and ended with
every acknowledged event delivered to both consumer groups and exported:

| Disruption | Publishes resumed | Reconciled |
| --- | --- | --- |
| Leader lost | 11.1 s | 55.8 s |
| Leader frozen for 20 s | — | 28.1 s |
| Leader lost while a follower catches up | 15.2 s | 365.5 s |
| CDC destination down for 1 min | — | 62.2 s |
| Consumer group slow, then offline | — | 84.3 s |
| Leader of two partitions replaced by an empty node | — | 6.4 s |
| Whole cluster lost | 48.0 s | 253.1 s |

In the same run the replaced node held back from the two partitions it led and
another replica was elected about a second later; in the run before the fix the
same disruption had the followers delete 31,662 entries each. The two long
reconciliations came while one node led most partitions, on a machine shared with
other work that left as little as 189 MiB of memory free; nothing was lost, and
they have not been measured on separate hosts. These are single-host results:
the nodes share one kernel, disk and clock, and the latencies are not those of
production hosts.

On the same machine and build, `TestClusterGrowsFromThreeToFive` passed in 88 s:
replicas reached the two new nodes 13.8 s after they were added, publishes
resumed 10.7 s after a leader was lost during that handoff, and all 1,195
acknowledged events were delivered. `TestRollingUpgradeAndRollback` from the
v0.12.0 release binary to this build, back, and to this build again under
traffic passed in 125 s.

The release itself is checked as before: CI, the release workflow's image scan
and container fault tests on the exact image, and the rolling upgrade drill from
v0.12.0 with the release binaries. The release page records the results and the
image digest.

## Known limits

Unchanged from v0.12.0: replication lag is reported by a partition's leader only;
retention and compaction take partition 0 as "all local partitions"; schemas
cannot be registered at run time; manual rebalance returns `Unimplemented`;
tenant limits cover publish rate only; automatic lag-triggered snapshot selection
and dead-letter journal compaction are not implemented. This release makes no new
capacity claim.

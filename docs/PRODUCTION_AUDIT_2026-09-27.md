# CronosDB production readiness audit

Date: 2026-09-27. Audited commit: `1de04d6575a810c79f716dfe48b54e1744b42a33`.

## Working-tree follow-up — 2026-09-28

### Finding closure review

**The remediation program is incomplete. Do not treat the passing local suite as
production approval.** This table compares the current working tree with each
finding's full required outcome. "Core fix implemented" means the specific code
defect has been addressed; it does not mean its production release validation is
complete. "Partial" means material requirements remain. This is a source and
existing-test review, not a new fault-injection or deployment campaign.

| Finding | Status | Implemented work and remaining requirement |
|---|---|---|
| F01: topic isolation | Partial | Replay and recipient topic filters, group topic checks, and ACK principal validation added. Two principals are now tested through the public API with authentication on, each with one topic and both topics in one partition: neither can publish to, subscribe to or replay the other's topic, a replay of one's own topic over the shared log returns only its events, both consume at once and receive exactly their own, one cannot join the other's consumer group, and one cannot acknowledge a delivery made to the other. Remaining: consumer groups are named in one space shared by all principals, so a principal can take a group name before another does; there is no tenant scope beyond topics. |
| F02: policy loading | Fixed with startup regressions | Invalid/missing policy and public-key loading fail before data stores/listeners are initialized. Subprocess tests cover missing, empty, malformed, empty-subject, and null-subject policies. |
| F03: ACK correctness | Partial | Tracked-delivery validation, per-event completion, and partition-local manager use added. Acks on a stream are recorded in batches with one durable write, outside the group lock. A group's completed work is a durable floor plus the completions above it, and the partition leader sends that progress to its followers, so a follower that takes over does not redeliver finished work, including work finished out of order (tested end to end over the replication channel). Progress reaches followers on the leader's maintenance tick, so what completed in the last moments before a failover can be delivered again; a replication-factor-1 handoff does not move it; an ACK concurrency test through the public RPC remains. |
| F04: delivery/backpressure | Partial | Worker and scheduler ready-slice ownership, worker count/64 MiB estimated-byte budget, WAL redrive, and unsent-group reservation cleanup added. Held-back, dropped, and abandoned deliveries are queued for redrive by offset range and follow acks; a connecting consumer drains its backlog at credit pace instead of a fixed 512 events/s; dead-lettered events are recorded complete. Regressions cover exhausted credits, worker overflow, disconnect, and backlog. Publishes are now refused while the process holds 80% of its own memory limit (the container's, when there is one), and the Go client reports that as `overloaded` instead of opening its circuit breaker. The guard that was there compared the whole machine's memory use with a percentage, which says nothing inside a container, and was off by default. Byte bounds on the queues themselves and an accepted-ID conservation ledger remain. Recovery no longer reads the undelivered backlog into memory at a start, and the reads of the log are bounded by bytes (eighth batch). |
| F05: timer recovery | Partial | WAL reconstruction, overdue cold hydration, and promotion scheduling added. Pending-timer preservation now tested through three consecutive snapshot/restart cycles; complete recovery/completion behavior remains to be validated. |
| F06: replication retries | Core fix implemented | A replica accepts one leader per epoch and records it durably before applying any of its entries; entries carry their term on the wire; an existing entry is replaced only when its term differs from the leader's, never within a term; a superseded leader steps down. Tests cover two leaders claiming one term, replacement of a divergent tail, same-term conflicts, step-down, and a leader change concurrent with appends. A same-term content conflict is rejected and needs a snapshot to reconcile. Every append now names the entry it follows and that entry's term; a replica that holds a different entry there refuses the append and says where its log stops agreeing, and the leader resends from that point. Before this a replica that had kept entries of an older leader got the new entries appended after them. An old leader that returns with a tail it never replicated is brought in line without anyone publishing (tested with a tail shorter and longer than what the new leader wrote, and with a new leader that wrote nothing: every request now says where the leader's log ends, and a follower drops what it holds beyond that from another term. Before, such a tail stayed until the next publish, and a leadership handoff that waited for equal logs blocked that publish; the restore test found it). The fault campaign kills and freezes leaders with producers running and compares the replicas' logs entry for entry, and the partition test does the same with links cut between running nodes. A follower's answer now counts as replication only for what it acknowledged of this leader's log, and a leader that a follower tells of a newer term stops leading at once. |
| F07: quorum/idempotency | Core fix implemented | A publish that reaches the log and then fails (replication, requested fsync, or scheduling) is held: its events are not scheduled and its message IDs are recorded as appended at their offsets, distinct from accepted. A retry finishes that publish instead of appending again; so does the next publish that reaches the replicas, or follower catch-up, if nobody retries. After a restart or promotion the log position is recorded, so a retry is answered from the log; claims left by a crash are dropped when the store opens. Change data capture and cross-region replication now read a per-partition change feed behind the accepted watermark: accepted events only, in log order, from the partition's leader, with a position that survives restarts and is handed to followers (tested through the publish path and a failover). Pruning now waits for the feed: an entry is kept until the feed has handed it over. Production mode refuses `--fsync-mode=periodic`, the one mode in which a replica acknowledges an entry it has not put on disk. Remaining: dedup recovery covers the last 500,000 log entries; a sink that keeps failing holds back that partition's feed for every sink, and with it that partition's pruning. |
| F08: leadership/handoff | Core fix implemented | The Raft-committed assignment is the only source of leadership: a node leads, and reports that it leads, only what is committed to it, never what its hash ring alone suggests. Failover elects the reachable replica with the most complete log and waits for enough replicas to answer that no acknowledged write can be missing. A rebalance moves leadership by a staged handoff: the leader stops accepting publishes, the target's log is shown to be equal, and only then is the change committed. New clusters assign leaders once membership has settled; a node unloads partitions it no longer holds; `/health/ready` reports whether publishes are served. Exercised with three nodes on one machine (one node killed; nothing accepted was lost and the survivors' logs matched). A node now keeps trying its seeds until it has reached every one, instead of joining through its own entry in the list and staying outside the cluster. A node reports the position of a partition it holds on disk but has not loaded, so a restarted replica is not taken for an empty one. The partition epoch and leader flag are safe to read while an append or a promotion changes them. Nodes that lost contact with each other find each other again, and a node restarted without seeds is found by the others; both used to stay apart until a restart. A fault campaign now runs in CI against three server processes: leaders and followers killed, a leader frozen and released, every node restarted including the one that created the cluster, a replica rebuilt from an empty disk, and the whole cluster killed at once. An election no longer writes the replica list it started from over the ring's placement, which left a partition on one replica, refusing publishes, when a node had come back just before its leader failed; and a node that returns is put back in the ring at once instead of up to ten seconds later. The fault campaign found both in CI. The network between the nodes is now failed in a test (`TestNetworkPartitions`, three containers of the image with links cut between them). It found two ways to lose an event, both closed (seventh batch): a leader returning from a network failure could acknowledge a publish that no other replica held, and an entry that only a cut-off leader held could be delivered and leave a completion record behind that later hid another event. Remaining: loss of the Raft leader during a handoff, slow or lossy links, and multi-machine runs. |
| F09: snapshot transfer | Partial | Stable checkpoint copy, per-file validation, filename checks, and install journal added. The checkpoint now copies files without holding the WAL lock, so appends continue; every crash point of an install recovers to one complete generation (tested, including a crash during recovery); a snapshot from a node behind the replica's accepted epoch is refused and the installed epoch is persisted; a replica brought up by snapshot rebuilds timers and dedup records when promoted (tested). An interrupted transfer resumes: the files that arrived stay staged and the leader sends only the rest (tested, including a damaged staged file and one the leader no longer has). Remaining: large-partition bootstrap measurements, and detection of interior damage in a staged checkpoint (see F10). |
| F10: key mismatch/corruption | Core fix implemented | Wrong-key recovery and unreadable-segment skipping now fail closed; wrong-key regression passes. Bytes that are not valid records are now judged by where they are: at the end of the log they are an interrupted write, cut off with a warning and cleared; inside a segment that later segments follow they are damage, and the partition refuses to open, naming the segment and the offsets that cannot be read. Opening used to take them for the end of the segment and silently drop the events behind them. `cronos-admin check-log` reports damage offline and, for a partition without another replica, cuts it out and says which offsets are given up; a replica of a cluster is repaired by removing its partition directory and letting the leader fill it again (all tested; see [storage](architecture/features/storage.md)). Remaining: a region of a closed segment overwritten with zeros looks like unwritten space and is not detected; records are checked when a log is opened, not continuously while a node runs. |
| F11: DLQ | Partial | Record-length/reopen defect and batch payload handling addressed. Entries are encrypted with the partition's key when encryption is on; entries written before that are still read, and an entry that cannot be opened with the configured key stops the partition from starting instead of being skipped. Records that fail their checksum are counted, logged with the file they were in, and shown in the queue's stats; the rest of the queue is kept. Retention of dead-lettered entries and a repair procedure for a damaged queue file remain. |
| F12: unordered time queries | Fixed with regressions | Correct full scans replace invalid timestamp pruning/index assumptions. Tests cover shuffled records across segment rotation and reopening. Full-scan cost remains a performance consideration under F17. |
| F13: SDK consumption | Partial | Stream infrastructure is canceled before waiting; cancellation cause also preserved for producer RPCs. An unpinned SDK consumer opens one stream per partition, with a regression that publishes across four partitions. Against a real cluster the client had four more defects, all fixed and tested: a publish ended at the first node that answered "not the leader" instead of going on to the leader, so with three replicas on three nodes most publishes failed unless the caller mapped node IDs to addresses by hand; a batch refused that way was reported as failed; consumers that left the subscription ID to the client chose the same ID in every process, and the second one was refused; and a subscription ended for good when the server closed its stream without an error. The server now ends the subscriptions of a partition it stops leading, so consumers move to the new leader instead of waiting on the old one. The client remembers which node served a partition, and notices a node that stopped answering after 15 seconds instead of 40. The fault campaign runs two consumers in one group through the client and has one leave. Remaining: the server shares a partition's events between a group's consumers by credit, with no assignment a consumer can observe or control. |
| F14: deployment | Partial | CLI precedence fixed; chart now wires bootstrap seeds, routable addresses, persistent Raft path, auth policy, private key staging, probes, placement, and shutdown grace. Image builds dashboard and uses Go 1.26.9 (eighth batch)/locked Rust dependencies. Render assertions pass; actual Linux image/chart startup still requires the new CI smoke job. The membership and Raft ports now speak mutual TLS with the replication certificates; they were plain TCP, and anyone who could reach the membership port could join the cluster as a node. The production chart's startup job and the fault campaign both run that way. The HTTP port remains plain HTTP and needs TLS terminated in front of it. `--use-memberlist`, which was accepted and did nothing, is refused. The node that creates the cluster (pod 0) no longer creates a second one when it comes back with an empty volume: every pod names every pod as a seed, and pod 0 creates a cluster only when it has no state and no other pod belongs to one. Tested with three processes (`TestFirstNodeReplaced`) and on the chart in the kind job, which also restarts the pods one at a time. |
| F15: backup/retention | Partial | Backups now hold each partition's log, consumer groups with offsets and completions, dedup store, dead-letter queue and epoch, captured in an order that is safe to restore, with the size and CRC of every file in the manifest. `cronos-admin restore` checks every file and restores into an empty data directory, all partitions or none; tests cover damaged, incomplete and misplaced backups, and one backup was restored and served by the real binary. Completion-aware live pruning has regressions. The logs of all partitions on a node are cut at one instant; scheduled backups start at wall-clock multiples of the interval on every node; the manifest identifies the encryption key without containing it, and restore can check a key before writing anything (all tested). A restored cluster needs no Raft metadata: a partition's first leader is the replica with the most complete log, at an epoch above any a replica accepted (tested with stand-in replicas). A cluster now deletes from its logs: the partition's leader removes finished segments from the start of the log, tells the other replicas where its log starts, and they follow; a replica whose log ends before that restarts its log there, which is also how an emptied replica rejoins (tested through the replication channel, across a leader change, and in the fault campaign). Before this, nothing was ever deleted in a cluster. A whole three-node cluster has now been restored from its backups in CI: backups taken while publishes arrive, every node destroyed, each restored from its own backup, and everything acknowledged before the backups present and delivered at its time (`TestClusterRestoredFromBackups`). The backup interval, retention and directory are settings now. Remaining: backups go to the data volume unless `--backup-dir` names another, and the chart mounts no other volume; the backups of different nodes are separate copies that agree only as closely as their clocks; backups are full copies and hold no keys or credentials, the latter by design; in a cluster one event that cannot go yet keeps every later segment; restore exercises on Linux. See the [maintenance validation](MAINTENANCE_VALIDATION_2026-09-29.md). |
| F16: transactions/splits | Contained for first release | Transactions and splitting are disabled by default and require --dev --experimental-features. Production rejects this opt-in and exactly-once commits. Public-RPC tests verify disabled endpoints; underlying experimental state machines remain unsupported. |
| F17: overload/latency | Partial | Worker event-count and estimated-byte caps added. Replication returns on quorum instead of waiting for every follower; sends to a slow follower stay ordered and bounded, and a skipped follower is caught up from the WAL; `--replication-timeout` is the Append deadline. `batch` fsync no longer syncs under the WAL lock: writers that append during a sync share the next one. An empty partition no longer allocates its bloom filter and consumer queue up front. CDC no longer runs on the append path or queues in memory: a slow sink makes its feed lag. The delivery settings (ack timeout, retries, backoff, credits, circuit breaker) now reach the dispatcher; they were parsed and ignored. An overload test runs in CI: producers flood three nodes that have a small memory limit and no consumer; the nodes refuse publishes, none dies, they hold less than their limit, and everything acknowledged is delivered once a consumer starts (`TestOverloadIsRefusedAndSurvived`). Byte-bounded queues and measurements of sustained load at production sizes remain; the reads of the log are bounded by bytes at a restart and on the replication, redrive and feed paths (eighth batch). |
| F18: accounting/metrics | Core fix implemented | SLO interceptor counts semantic failure responses. Counters now follow an event per partition: accepted, answered as duplicate, delivered (first attempt or retry), acknowledged (success or failure), timed out, dead-lettered; a histogram records how long after its scheduled time an event was first delivered; gauges show where each log starts and how far its change feed has got (tested through publish, subscribe and acknowledge). The chart's alert rules use them, and the rule that compared one node's leader count with the partition total, which fired on every node of a healthy cluster, is replaced. Remaining: nothing counts events that are never delivered because no consumer group takes their topic, and there is no per-tenant view. |
| F19: dependencies | Partial | Go module/toolchain at 1.26.9, which fixes the standard-library advisories CI reported, gRPC and dashboard lockfile updates present; Docker Go builder aligned. The archived Avro codec was replaced with a patched fork and explicit decoder limits after the first CI security scan failed. CI now scans the image it builds and fails on a high or critical vulnerability that has a fix. The first scan, of the `v0.6.0-rc.2` image, found seven, all in the base image's `perl-base` package and none in the server binary; the image build now applies Debian's security updates. An audit of the Rust dependencies remains. |
| F20: release gates | Partial | Linux cgo/race/vet/build/format checks, Rust and dashboard tests, Go/npm scans, chart assertions, and production-mode kind startup workflow added. CI runs for pull requests and for pushes to `main`; a commit of the development branch is checked on request, which is done for the commit of a release. A fault campaign (`tests/acceptance`) now runs there too: three server processes, producers and a consumer group through the client library, a ledger of every acknowledged event, and a comparison of the replicas' logs at the end. The job that builds the image also cuts links between three containers of it. Remaining: branch protection that makes the jobs required. |

The first pushed production-checks run found reachable decoder advisories in the
archived `hamba/avro` module. The follow-up migrates schema validation to
`iskorotkov/avro/v2` v2.34.0 and freezes the decoder configuration with a
4 MiB byte-field limit and 65,536-element array/map limits. A regression rejects
an oversized map block before allocation. A local `govulncheck` rescan found no
called-symbol vulnerabilities; it reported one imported-package finding without
a reachable call. The historical F19 findings below describe the original audited
dependency; the new CI scan must verify closure.

The next release-blocking work should establish the supported feature scope and
acceptance/loss contract, then finish F03/F06–F09 and F13/F15, wire and exercise
F14/F20, and measure F04/F17/F18 with a ledger of accepted event IDs. F01's public
authorization tests and F19's release scans also remain release gates.

### Eighth hardening batch — 2026-10-09

This batch is on the development branch and in no release. The verdict above
is unchanged. The table rows for F04, F14, F17 and F19 describe what moved.

- **The Go toolchain is 1.26.9.** govulncheck and the image scan reported
  reachable standard-library advisories in 1.26.7 (`html/template`,
  `net/http`, `crypto/tls`), each fixed in 1.26.9. Both jobs failed on them,
  and the image job stopped before its partition step, so the partition test
  had not run in CI on the commits that came before. `go.mod` and the Docker
  builder now name 1.26.9.
- **A restart no longer holds the undelivered backlog in memory.** A start
  walked the whole log and queued every event that was due, so the backlog
  sat in memory at every start, with nothing to bound it. The walk now
  schedules the events not yet due and leaves the rest in the log for their
  subscriptions to read at their consumers' pace (`replayWALTimers`). A
  promotion walks the log only when its start has not, and recovers the dedup
  store only for a partition that was already running. A test restarts a
  partition with 40 due events and 3 future ones: the 3 are timed and none of
  the 40 is queued. The test fails with the old behaviour.
- **Reads of the log are bounded by bytes where events can be large.**
  `WAL.ReadEventsWithin` stops once its records reach a byte bound, after at
  least one event, and the caller continues after the last event it was given.
  Catch-up sends a follower at most 8 MiB a request, plus the one event that crosses the bound. A request of 500 events of
  a megabyte each was over the 64 MiB transport limit, and such a follower
  never caught up; the test sends 40 MiB and fails on the old single request
  of 39 MiB. The redrive of a subscription and the change feed read 4 MiB at a
  time; the bulk sync of a partition stops at the byte limit of its request,
  10 MiB by default.

Verified here: the whole Go suite on 1.26.9. Not verified: an acceptance run
with a backlog larger than memory at a restart, which is still to be written;
the replay RPC, cross-region and dead-letter reads still take a whole range
with `ReadEvents`, and are the next to bound; the redrive and feed bounds have
no test of their own beyond the WAL test and the existing suites.

### Seventh hardening batch — 2026-10-07

This batch is on the development branch and in no release. The verdict above
is unchanged. The table rows for F06, F08 and F20 describe what moved.

Until now every fault in the tests stopped a process. `TestNetworkPartitions`
leaves all three nodes running and reachable by applications, and drops what
chosen nodes send each other: the leader of a partition on its own, one link
down, each node on its own in turn, every link down. Some runs failed, with
one event lost each time, in two different ways. Both are fixed, with a test
for each that fails without the fix; the list below is in the order the
faults were found.

- **A leader that was cut off kept taking publishes.** It could not
  acknowledge them, but it wrote each to its own log and held the sender until
  replication timed out, for as long as the network was down. A leader that
  does not count enough replicas as alive now says at once that the publish
  has to go elsewhere.
- **Nodes stayed dead to each other after the network returned.** A heartbeat
  written into a connection that carries nothing succeeds, so neither side
  opened a new one, and the old one stays silent for minutes after a failure.
  A connection is now used again only while the node at its other end is
  heard from, and a replication call that gets no answer gives up its
  connection.
- **An acknowledged publish was lost.** A leader that returns from a network
  failure finds followers that follow its successor and hold entries the
  successor wrote. A refused append told it where such a follower's log ends
  now, and that was taken for how much of its own log the follower holds: the
  next publish was at an offset the follower had passed, so nothing was sent
  and the follower counted as having confirmed it. The publish was
  acknowledged, and removed a moment later when the node took the
  successor's log. Only what a follower has acknowledged of this leader's log
  counts now. And a leader stops by itself when a follower answers for a
  newer term: it used to go on until the cluster's records reached it,
  seconds later.
- **An event was acknowledged, stayed in every replica's log, and was never
  delivered.** A leader that had lost its followers still delivered what was
  in its own log, including entries no other replica held, and recorded them
  as finished. Completion is recorded by offset. When the node took its
  successor's log those entries were replaced, the record stayed, and once the
  node led again the event that had taken the offset counted as finished.
  Three things changed:
  - A leader hands its consumers only entries that are on the replicas the
    partition requires (`Partition.DeliverableThrough`). This holds for every
    path to a consumer, including the one that reads the log for a
    subscription. A leader that loses its followers still delivers what was
    replicated before it lost them.
  - A replica keeps no completion at or beyond the end of its own log. It is
    removed when a follower's log is cut, when a partition starts, and when a
    replica is made leader; and a follower takes its leader's consumer
    progress only as far as its own log reaches, and is sent it again when it
    has caught up. Starting a partition is the case that needs no network
    failure: a node that crashed and lost the entries it had not synced used
    to count the events that took their offsets as finished.
  - What is scheduled is dropped when a node stops leading and built from the
    log again when it next leads. A timer keeps the event it was set for, and
    one kept across a change of the log would have fired with an event that
    was no longer there.
- **A delivery nobody acknowledged could end in the dead-letter queue because
  its consumer left.** A delivery that times out waits to be sent again to
  the same consumer. If that consumer disconnected in the meantime, the
  repeats went to a stream nobody read, and after the last one the event was
  dead-lettered and recorded as finished, with the rest of the group ready to
  take it. Such a delivery now goes back to the group, as does one that
  cannot be sent again because the stream failed. Only a consumer's own
  failures, and timeouts with the consumer still connected, count towards
  dead-lettering.

The first run of the test in CI failed where none on a developer's machine
had. The link that was cut there was the one between the leader of the
partitions and the node that decides who leads (it leads the cluster's Raft
group). That node replaced a leader which was still running, still reached
the third node, and was still where the applications sent their publishes.
Two faults came out of that, and a third came up in the same run. The test
now cuts that link whenever a partition is led by another node:

- **An election could lose what the old leader was still having confirmed.**
  The deciding node asked the replicas it could reach where their logs
  ended, and chose by the answers. The old leader went on writing to the
  third node, which confirmed it, until the new leader's first request told
  that node of the new epoch, about half a second later. What was
  acknowledged in between was not in the log that had been chosen, and was
  removed. An election now has two steps. It first commits that the
  partition has no leader, at a new epoch, so that an election stays owed
  whoever decides next and whether or not the old leader shows up again.
  Then each replica it asks stops accepting leaders below that epoch,
  durably, before it answers with where its log ends
  (`ReplicationPositionRequest.fence_epoch`), and the leader is committed at
  a higher one.
- **A leader that had been replaced kept the publishers that were sending to
  it.** Stepping down on a follower's word, as described above, left the
  node answering publishes with "replication leader is not ready", which a
  client takes for a final refusal: the publishers came back to the same
  node for as long as it stayed cut off from the cluster's records. A node
  whose partition has taken a newer epoch than its records of the cluster
  show now answers that it does not lead and that the publish belongs with
  the leader, which is what sends a client to look for it. A replaced leader
  also answers at once, where it used to wait out the replication timeout
  for a node it could not reach.
- **A node about to lead a partition tried to copy it from the node it had
  just counted dead.** The ring moves a partition when a node leaves, and
  the node it moves to fetched a copy from the current leader, which was the
  node that had left. Nothing came of it, but the attempt held the
  partition's log locked until the connection timed out, twenty seconds
  during which the node could neither take publishes for the partition nor
  be written to. It no longer tries.

Verified here: the unit tests that come with each change; the whole Go suite
(`go test ./...`); the race detector on the four packages the election and
publish paths are in; the four process-based acceptance tests; and
`TestNetworkPartitions`, six runs in a row against an image of this tree. The
image as pushed did not pass a local run of that test: once node3 was cut
off, 5 of the 150 publishes were accepted within 90 seconds. (In CI the
link failed earlier, as above.) Before the CI run, ten runs in a row had
passed, three of them with every publish and every delivery logged: a leader
that was cut off refused 25 publishes in them, and none of those reached a
consumer before it was replicated, where 6 of 9 had in a run logged the same
way before the change. No offset was acknowledged for two different events.

Not verified: links that are slow or lose some packets, as opposed to all;
outages longer than a minute; more than one machine. With
`--min-insync-replicas=1` the leader alone is enough to acknowledge a
publish, so it also delivers what only it holds; what such a leader loses
when it is replaced is lost as before, and the completion it recorded for
those entries is removed when its log is cut. A node that is cut off still
serves the subscriptions it has, from what was replicated before the
failure, until the links return; consumers connected to it see nothing new
for that long.

### Sixth hardening batch — 2026-10-07

This batch is released as `v0.6.0-rc.3`. The verdict above is unchanged. The
table rows for F01, F04, F06, F07, F08, F10, F11, F13, F14, F15, F17, F18 and
F20 describe what moved.

Most of it came from one new test. `tests/acceptance` starts three server
processes, publishes and consumes through the client library, kills, freezes
and restarts the nodes while that runs, and then checks that every publish
that was acknowledged was delivered, that nothing arrived before its time, and
that the three replicas of every partition hold the same log with each
acknowledged event exactly once at the offset its producer was told. It is the
first test that uses the product the way an application does, and before it
passed it needed these fixes, none of which the existing tests had found:

- **The client could not publish to most of a real cluster.** A node that does
  not lead a partition says so with an error code the producer did not treat
  as "try the next node". With three replicas on three nodes every node holds
  every partition, so two nodes in three answer that way, and the publish
  ended there. Batches refused the same way were reported as failed.
- **Two consumers of one group could not both connect.** The client numbered
  its subscriptions from 1 in every process, so the second program to start
  asked for an ID that was taken and was refused, repeatedly.
- **A consumer stayed on a node that had stopped leading.** The server kept
  the subscription open, and nothing was delivered on it again.
- **A subscription ended when the server closed its stream cleanly**, which is
  what a server that shuts down does.
- **Nodes that lost each other never met again.** A node stopped sending
  heartbeats to a node it suspected. After a network fault longer than the
  failure timeout both sides had gone quiet, and they stayed apart until one
  was restarted. A process that was frozen for a while came back and counted
  everyone else as failed.
- **The node that created the cluster stayed alone after a restart.** It names
  no seeds, so it introduced itself to nobody, and nobody was still trying it.
  In the production chart this is pod 0.
- **Reading a consumer group could end the process.** The snapshot timer and
  the admin API read a group's offset map while acknowledgements wrote to it.
  Go stops a program that does that. CI's race detector caught it.

Also in this batch, from the list of what a stable release needs:

- **Replicas check where an append continues from** (see F06).
- **The dead-letter queue is encrypted and reports damage** (see F11).
- **Pruning waits for the change feed** (see F07).
- **Production mode requires `batch` or `every_event` fsync.**
- **Replication to other regions is experimental.** It is asynchronous, drops
  events when a region stays unreachable and settles conflicts by last write.
  A node with `CRONOS_REGIONS` set refuses to start outside
  `--dev --experimental-features`. Change data capture to Kafka and webhooks
  is supported.
- **The image is scanned in CI** (see F19).

Verified here: the Go suite passes on Windows/amd64 pure-Go, and the changed
packages with cgo and `-race`. The fault campaign passed on Windows with a
pure-Go server: about 2,400 events, every one acknowledged and delivered,
identical logs on all three replicas, about a hundred events delivered more
than once after failovers. Each fix has a test that was run without the fix
and fails there.

- **A cluster deletes from its logs** (see F15). The fault campaign runs with
  small segments and a pass every two seconds, so its nodes remove entries
  while they are being killed and restarted, and the replica that is replaced
  with an empty one joins logs that no longer start at offset 0.
- **A retention pass no longer stops the partition.** It read whole segments
  while holding the lock that appends take and the lock that acknowledgements
  take.

- **Damage inside a log is no longer taken for its end** (see F10).
- **Two tenants were tested against each other through the public API** (see
  F01). Nothing had to be fixed.
- **Event accounting metrics and corrected alert rules** (see F18).

- **Membership and Raft speak mutual TLS** (see F14). The fault campaign runs
  with it.
- **The first node, replaced, created a second cluster** (see F14). It
  created one whenever it started without state.
- **A cluster was restored from its backups** (see F15): three processes,
  backups taken under publishes, all three destroyed and restored.
- **Overload is refused, not fatal** (see F04, F17). The memory guard was off
  by default and measured the wrong thing; the client hid the refusal behind
  its circuit breaker.
- **A replica kept entries its leader never had** (see F06). One run of the
  restore test left a partition without a leader that took publishes for
  thirty seconds at a time.
- **A node that came back was left out of a partition for good** (see F08).
  The campaign failed on this once in CI: a follower was restarted, the leader
  of a partition was frozen three seconds later, and the partition's new
  leader never got a second replica.

Not verified: network partitions (the campaign stops and kills processes; it
does not cut links); more than one machine; the campaign under load, since it
publishes about 60 events a second.

Known limits of log deletion in a cluster: only the start of a log is removed,
so one event that cannot go yet, such as one scheduled a month ahead or one
that no consumer group takes, keeps everything published after it in that
partition. A partition that carries several topics keeps a completion record
per finished event until the log start passes it, because a group's floor
does not move across events of other topics; the records a leader sends to
its followers are capped at 100,000 per group, so after a failover in such a
partition a group can be given events again that it had finished.

### Fifth hardening batch — 2026-10-07

The first three defects below were fixed in `v0.6.0-rc.2`; everything else in
this batch changes the working tree only. The verdict above is unchanged. The
table rows for F07, F08, F09, F15 and F17 describe what moved. Defects found
and fixed that were not in the original findings:

- **A node could start outside its cluster.** Nodes that share one seed list
  tried each seed once and stopped at the first answer, which could be their
  own listener. A node that started before the bootstrap node stayed alone for
  good. This is what kept the production Helm chart from starting. Also in
  v0.5.0.
- **Events could be delivered before their scheduled time.** A timer more than
  one wheel rotation away (6 s by default) could fire exactly one rotation
  early, and in a rarer case one second-level rotation late. Also in v0.5.0.
- **A partition's epoch and leader flag were read without the locks they are
  written under.** The race detector caught a position request reading the
  epoch during a replication append.
- **Elections could not see a restarted replica's log.** A node answered
  "nothing here" for a partition it held on disk but had not loaded, which is
  the state of every partition after a restart. With the leader gone after a
  full restart, the election had nothing to compare and could have chosen a
  replica that lacks acknowledged writes.
- **A restored cluster could not lead its partitions.** A new cluster counts
  epochs from 1, and a replica restored from a backup refuses to lead below
  the epoch stored with its data.
- **An idle partition stayed degraded after a failover.** A follower that had
  not acknowledged anything since the leader changed was neither caught up nor
  counted towards the quorum until somebody published to the partition.
- **Reading the log raced with appends.** `WAL.ReadEvents` looked at where the
  active segment ends after releasing the lock an append holds while moving
  it. Redrive, catch-up and replay all read that way.
- **Seven delivery settings were parsed and ignored.** `--ack-timeout`,
  `--max-retries`, `--retry-backoff`, `--max-credits` and the three
  circuit-breaker flags never reached the dispatcher, which ran on built-in
  defaults.

Verified here: the Go suite passes on Windows/amd64 both pure-Go and with cgo
and `-race`. For `v0.6.0-rc.2`, GitHub CI passed all four jobs on the release
commit, including the production chart starting in a disposable Kubernetes
cluster, and the published image was started as three nodes in production
mode. The tests for the timer wheel, the seed join, the epoch race, the read
race and the idle follower were run without their fixes and fail there.

Not verified: the change feed against a real Kafka broker or webhook, or under
sustained load; a restore of a real multi-node cluster; the working-tree
changes on Linux, where CI has not run on them yet; the three-node failover
check, which was not repeated after this batch; network partitions and any
fault campaign.

### Fourth hardening batch — 2026-10-06

This batch also changes the working tree only, and the verdict above is
unchanged. The table rows for F03, F06, F07, F08, F09, F15, and F17 describe
what moved. It also fixed these defects, which were not in the original
findings:

- **Log end reported as -1 after a segment rotation.** Until the next append,
  `WAL.GetLastOffset` described the new, empty active segment. Replica
  positions, elections, follower catch-up and timer replay all read it, and a
  checkpoint taken in that state announced last offset -1, so installing it
  failed.
- **An empty follower was never caught up.** A follower with nothing in its
  log reports next offset 0, which the leader ignored. A replica added after
  the leader had data could only be brought up by snapshot.
- **Dedup checkpoints were missing recent IDs.** The dedup store runs without
  Pebble's own WAL, so its checkpoint held only what had reached an sstable.
- **A crash could block a message ID until its TTL expired.** A claim written
  before its event reached the log was never completed or released, and every
  retry was refused as still pending.
- **A WAL checkpoint stopped appends for the whole copy.**
- **146 MB of memory per empty partition.** Every partition allocated a bloom
  filter for the configured capacity (100 million IDs by default) and a
  one-million-entry consumer queue when it was created. Both are now allocated
  on first use, which leaves 12 MB.

Verified here: the Go suite passes on Windows/amd64 both pure-Go and with cgo
and `-race`. With three nodes on one machine, replication factor 3 and
`min-insync-replicas=2`: 60,000 events were published, one node was killed,
every partition got a new leader holding exactly the accepted events, 40,000
more were accepted, and the two survivors' logs were identical. A backup was
restored with `cronos-admin restore` and the server started on the restored
directory. The tests for the empty-follower catch-up, the dedup checkpoint and
the log end after a rotation were run without their fixes and fail there.

Not verified: Linux runtime behavior, more than one machine, network
partitions, loss of the Raft leader during a handoff, and any fault campaign.
Cluster runs were kept to at most 720,000 events because of memory on the test
host; see the [throughput follow-up](CLUSTER_PERFORMANCE_VALIDATION_2026-09-29.md#second-follow-up--2026-10-06).

### Third hardening batch — 2026-10-06

This batch changes the working tree only; the verdict above is unchanged. The
table rows for F03, F04, F13, and F17 describe what moved. It also fixed five
defects that were not in the original findings:

- **Publish stall on every leadership reconcile.** The epoch file was rewritten
  and fsynced every 5 seconds per led partition under the partition manager's
  write lock. See the [throughput follow-up](CLUSTER_PERFORMANCE_VALIDATION_2026-09-29.md#follow-up--2026-10-06).
- **WAL flush I/O under the append lock.** The periodic flush wrote mapped pages
  to disk while holding the WAL lock, and a flush waiting behind an fsync held
  the segment read lock, parking the next append.
- **Two system calls per record on every WAL read.** Range reads, replay,
  follower catch-up, and recovery scans read each record's length and body
  separately; measured at about 20,000 events/s. Reads now use block reads.
- **Poison messages redelivered after dead-lettering.** A dead-lettered event had
  no completion record, so the WAL redrive handed it out again on every pass.
- **Consumer store use after close.** An ack or redelivery scan still running at
  shutdown called into a closed Pebble store, which panics.

Verified here: the Go suite passes on Windows/amd64 both pure-Go and with cgo and
`-race`; each fix has a regression test, and the tests for the epoch stall,
quorum wait, WAL flush lock, and all-partition consumption were run against the
previous revision and fail there. Not verified: Linux runtime behavior (the
changed packages only cross-compile and vet), multi-node failover, and any
fault campaign. Completion state is still local to the partition leader.

### Second hardening batch

The deployment/feature-scope changes and their operating requirements are in
[production release requirements](PRODUCTION_RELEASE.md). There is no local Docker
or Kubernetes runtime in this session, so image builds and the production-mode
chart smoke test have **not** been executed here. CI configuration is reviewable
code, not evidence of a completed CI run.

This batch also reproduced scheduler ready-slice reuse overwriting an already
drained event, then fixed the ownership handoff. Worker byte accounting includes
payload backing capacity and metadata; it bounds that worker queue's estimated
retention only, not process RSS or dispatcher/scheduler/CDC queues.

The findings below describe the audited commit. The working tree now contains
remediation changes and regression tests; it is not the original audited revision.
This follow-up does not change the production-readiness verdict.

Validation of the pending changes found and fixed two additional dispatcher
defects: exhausting the in-flight limit left later unsent groups' credits and
pending markers reserved, and topic filtering checked only the first group
member before selecting a recipient. Regression tests now cover recovery for
all unsent groups and per-recipient topic isolation. The capacity test failed
before its fix. The existing client cancellation test also failed; publish now
preserves the request context error when gRPC returns a cancellation status.

The dashboard's 6 test files / 21 tests and production build passed. Go checks
use `GOARCH=amd64`; this shell defaults to `386`, whose storage build fails on
integer constants. The final amd64 validation passed:

- `CGO_ENABLED=0 go test ./internal/... ./pkg/... -timeout 120s`
- `CGO_ENABLED=0 go vet ./internal/... ./pkg/...`
- `CGO_ENABLED=1 go test -race ./internal/delivery -run TestAudit -count=1 -timeout 60s`

The race check covers the delivery audit regressions, not the full repository.

Remaining release work includes public-RPC isolation/concurrency coverage,
durable acceptance outcomes across quorum failures and retries, completion-state
retention and replication, byte-bounded backpressure, and production-like
failover/restore tests. In particular, releasing a dedup claim after replication
failure permits retry but can append another copy of a retained event; it does
not establish stable idempotent acceptance. Passing local tests does not close
these findings or establish throughput and recovery guarantees.

Workload: high-throughput telemetry, with some loss and delay acceptable. This is a code and targeted runtime audit, not a production certification or capacity benchmark.

## Verdict

**Do not deploy the current revision as a production telemetry database.** The implementation has useful foundations: separate WAL, scheduler, delivery, replication and client packages; checksummed storage; configurable durability; substantial unit coverage; and an operational dashboard. However, several essential guarantees fail in ordinary retry, restart, reconnect and authorization paths.

The principal problem is uncontrolled loss and misleading success. A telemetry service may deliberately discard data under a documented overload or retention policy. It still needs isolation, bounded memory, accurate acceptance responses, recoverable retained data and measurable loss. The current implementation does not consistently provide those properties.

Fifteen focused functional probes reproduced defects, and one additional probe triggered the Go race detector. Normal Go tests passed; those tests do not establish the advertised production guarantees. Reproduction sources, runner and captured evidence are under [audit-probes](audit-probes/README.md). Application code was not changed during this audit.

## Scope and evidence

Reviewed the public gRPC and HTTP services, authorization, WAL and encryption, scheduler recovery, delivery and offsets, SDK, replication, membership and Raft integration, snapshots, backups, retention, transactions, partition splitting, CDC, metrics, dashboard, Docker and Helm packaging, and test/release setup. The checkout contains approximately 209 Go files, including 85 existing Go test files.

Evidence labels used below:

- **Reproduced:** a focused executable check demonstrated the stated behavior.
- **Code trace:** identified directly in the implementation; broader operational consequences remain to be exercised in an integration environment.
- **Scanner:** tool-reported dependency findings, with exposure qualifications where inspected.

Severity is relative to this workload. **Critical** means an access boundary can be bypassed. **High** means likely silent loss, corruption, prolonged unavailability or failure to deploy an advertised production path. **Medium** covers hardening and operational gaps. Optional features can be excluded from an initial release, but must actually be disabled if their findings remain open.

| Check | Result |
|---|---|
| `GOARCH=amd64 CGO_ENABLED=0 go test ./internal/... ./pkg/... -timeout 120s` | Passed |
| `go vet ./internal/... ./pkg/...`, pure Go | Passed |
| Existing full Go suite with cgo and `-race -count=1` | Two test failures; no race report in existing tests |
| Focused functional audit probes | All 15 failed their desired-behavior assertions |
| Focused delivery ownership probe with `-race` | Data race detected |
| Dashboard tests | 6 files, 21 tests passed |
| Dashboard TypeScript/Vite build | Passed |
| Helm lint with production values and template rendering | Passed syntax/template checks |
| Rust `cargo test --locked` | Passed, but contained zero tests |
| Go vulnerability scan | 13 symbol-level vulnerability reports; see F19 |
| npm audit | 9 affected packages: 5 high, 4 moderate; see F19 |

The race-suite failures were `TestHealthChecker_Deep_ReadOnlyWALCheck` (temporary directory cleanup failed while partition background work was active) and `TestSendAsyncContextCancellation` (gRPC cancellation wrapped/classified as transport error instead of the expected context error). These failures should be fixed, but they are distinct from the race reproduced by the new probe.

Execution was on Windows with Go 1.26.4 targeting amd64. The module declares Go 1.25.12. The cgo/race runs used an available GCC and the existing Rust dedup DLL. Docker was unavailable. No Linux container, real Kubernetes cluster, multi-node network fault, disk-full/power-cut campaign, sustained capacity test, or restore into a fresh production-like environment was run. No throughput or RPO/RTO guarantee is established by this audit.

## Findings

### F01 — Critical: topic authorization does not constrain returned events

**Evidence: reproduced replay leak; code trace of subscribe exposure.**

Publish routes by message ID or partition key, so one partition contains multiple topics. `Subscribe` checks permission for the requested topic, but constructs a delivery subscription without a topic filter. The dispatcher routes by partition and consumer group. Replay likewise authorizes the requested topic and then streams partition records without filtering by it. `ReplayRequest.Topic` is treated as informational.

The probe inserted an event for `secret`, requested replay for `allowed`, and received the secret event. A caller authorized for one topic can select a shared partition and receive another topic's data. Authentication alone does not prevent this.

Sources: [handlers.go:842](../internal/api/handlers.go#L842), [handlers.go:890](../internal/api/handlers.go#L890), [handlers.go:1015](../internal/api/handlers.go#L1015), [dispatcher.go:135](../internal/delivery/dispatcher.go#L135), [dispatcher.go:650](../internal/delivery/dispatcher.go#L650), [replay/engine.go:125](../internal/replay/engine.go#L125).

**Required:** carry an authorized topic/tenant scope through replay, subscription, dispatch and group state. Apply it before returning each record. Add public-RPC tests using two principals and two topics that share a partition. Define whether group IDs are scoped by tenant/topic; an arbitrary global group name must not confer access.

### F02 — Critical: a policy loading failure leaves allow-all permissions

**Evidence: code trace.**

Startup initializes auth with `AllowAllPolicy()`. Failure to read or parse the configured policy logs a warning and continues with that policy. An empty file or `{}` also creates the same empty-subject policy that permission checks interpret as allow-all. Production validation checks that a filename was supplied, not that a restrictive policy was loaded.

An invalid mount or malformed policy can grant every authenticated subject all topic and administrative permissions. This is an authorization failure; it does not by itself bypass JWT authentication.

Sources: [main.go:318](../cmd/api/main.go#L318), [main.go:331](../cmd/api/main.go#L331), [auth.go:79](../internal/auth/auth.go#L79), [auth.go:281](../internal/auth/auth.go#L281), [auth.go:340](../internal/auth/auth.go#L340), [config.go:472](../internal/config/config.go#L472).

**Required:** fail startup on missing, invalid or unintended empty production policies. Make permissive development mode a separate explicit state. Validate the policy before binding listeners.

### F03 — High: ACKs can commit fabricated or unsuccessful deliveries

**Evidence: reproduced manager behavior; code trace of public ACK path and offset persistence.**

The ACK handler ignores the dispatcher's validation error and then calls the consumer manager. The manager parses group and partition from the caller-supplied delivery ID and commits `NextOffset`; it does not check `Success`, subscription ownership, membership or an actual in-flight delivery. The probe sent an unsuccessful ACK for a never-delivered ID and advanced an existing group's offset to 1,000,000.

The public path has no corresponding per-delivery topic/owner authorization. This allows a principal to interfere with another known group, and ordinary negative acknowledgments can also advance its offset. `CommitOffset` permits regressions. Simply replacing it with `max()` would not solve holes caused by out-of-order scheduled delivery.

There is also a state ownership mismatch: main injects partition 0's group manager into the shared API, while other partitions have separate managers used by snapshots, compaction and metrics. Consumer offsets are not transferred by the WAL replication/snapshot protocol. The exactly-once storage helper is not called by the public commit path. Pending offset flush failures are logged after the pending map has been cleared; failed updates are not restored to the retry queue.

Sources: [handlers.go:931](../internal/api/handlers.go#L931), [group.go:321](../internal/consumer/group.go#L321), [group.go:426](../internal/consumer/group.go#L426), [offset_store.go:298](../internal/consumer/offset_store.go#L298), [main.go:380](../cmd/api/main.go#L380), [main.go:412](../cmd/api/main.go#L412).

**Required:** bind ACKs to authenticated delivery ownership, validate successful processing and offset bounds, track contiguous progress or explicit holes, and give each partition a consistent state owner. Specify the tolerated offset rollback/duplicate window and replicate durable group progress if failover must preserve it. Remove exactly-once claims until the complete path is proven.

### F04 — High: delivery backpressure loses work; the worker also races on its batch

**Evidence: code trace of loss paths; reproduced data race.**

The worker removes a ready batch before dispatch and only logs a dispatch failure. The dispatcher returns successfully when no subscribers exist, skips events when credits run out, and continues after a stream send failure. Those events remain in the WAL, but there is no automatic read path to deliver them later: `Subscribe` stores `StartOffset`/`NextOffset` without replaying the retained history. The comment promising replay on reconnect is not implemented by that path.

This can lose all delivery during a consumer outage, not just a small fsync window. Some no-credit skips are counted, but an event's eventual outcome is not accounted for. The worker's queue is unbounded; the scheduler queue limit does not include events already drained into the worker. Slow stream sends can therefore move backlog into memory outside the checked limit.

Separately, `processBatch` takes the queue slice and resets its length to zero without giving dispatch separate backing storage. `AddReadyEvent` can overwrite elements while the dispatcher reads them. The race probe reports precisely this concurrent access.

Sources: [worker.go:50](../internal/delivery/worker.go#L50), [worker.go:95](../internal/delivery/worker.go#L95), [dispatcher.go:650](../internal/delivery/dispatcher.go#L650), [handlers.go:890](../internal/api/handlers.go#L890), [manager.go:489](../internal/partition/manager.go#L489).

**Required:** separate queue ownership, bound every queue by bytes and events, and use durable per-group progress to redrive failed/unsent work. If telemetry is intentionally dropped, make the policy explicit and count the affected records. Test exhausted credits, no subscribers, disconnect during send, slow consumers and recovery without republishing.

### F05 — High: snapshot restart, cold hydration and follower promotion strand scheduled events

**Evidence: three reproduced defects.**

1. A partition snapshot records the WAL tail as `LastScheduledOffset`, but does not persist the corresponding pending hot timers. Restart skips replay through that offset. A future event present in both WAL and scheduler before snapshot/restart returned with zero timers afterward.
2. The cold hydrator scans starting at `now + hotWindow`. A cold entry already inside that window after downtime or a delayed scan is outside the scan range and can remain stranded. The targeted probe hydrated zero matching entries.
3. Replication append writes the follower WAL without scheduling the events. Promotion of an already-existing follower creates its replication leader but does not replay those records into its scheduler. A replicated future event remained unscheduled after promotion.

The WAL record can survive while the scheduling contract is lost. Timer recovery checkpoints have the same general requirement: a replay frontier is safe only if all earlier pending work is durably represented elsewhere.

Sources: [snapshot.go:62](../internal/partition/snapshot.go#L62), [manager.go:597](../internal/partition/manager.go#L597), [scheduler.go:555](../internal/scheduler/scheduler.go#L555), [scheduler.go:651](../internal/scheduler/scheduler.go#L651), [manager.go:1191](../internal/partition/manager.go#L1191).

**Required:** choose one coherent recovery model: a durable pending-timer index with a proven replay frontier, or WAL reconstruction with safe completion filtering. Scan overdue/eligible cold entries from a persisted frontier. Rebuild scheduling and dedup state before making a promoted replica writable. Verify multiple consecutive restarts, not only the first replay.

### F06 — High: repeating a replication append truncates accepted history

**Evidence: two reproduced RPC-handler defects.**

When a batch begins before the follower's next offset, the handler unconditionally truncates to that start. It does not distinguish an identical retry from a conflicting log. The probe appended offsets 0 and 1 at term 2, then repeated only offset 0 at term 2; next offset became 1 and the previously accepted offset 1 disappeared.

Term zero bypasses fencing entirely. The second probe accepted term 0 at local epoch 10. The handler does not validate the provided previous-log/expected-offset/checksum fields, and epoch changes plus truncation plus append are not one serialized partition operation. There is no committed-prefix protection in this reconciliation path.

Source: [replication_server.go:59](../internal/api/replication_server.go#L59).

**Required:** make identical retries idempotent, compare the existing prefix before reconciliation, never truncate a committed prefix, persist and serialize fencing state, reject invalid/obsolete epochs, and enforce one append authority per partition. Test delayed duplicates, overlapping batches, concurrent appends and leader changes.

### F07 — High: a failed quorum write can become a successful duplicate without recovery

**Evidence: reproduced batch publish behavior.**

Publish claims dedup state and appends locally before waiting for replicas. Quorum failure returns before scheduling, but leaves the dedup claim. Retrying the same batch can then filter out the event as a duplicate and return success without retrying replication or scheduling.

With minimum ISR 2 and no follower, the first publish failed; its retry reported success with zero published events, one duplicate and zero scheduled timers. A client can stop retrying even though its requested operation never completed.

CDC hooks run on local append, before the publish path has completed quorum and scheduling. A downstream observer can consequently see an event whose producer received failure. Also, a requested durable ACK forces the leader's flush but does not independently prove that periodic-mode followers durably flushed the same batch.

Sources: [handlers.go:735](../internal/api/handlers.go#L735), [handlers.go:819](../internal/api/handlers.go#L819), [handlers.go:422](../internal/api/handlers.go#L422), [replication_server.go:96](../internal/api/replication_server.go#L96).

**Required:** distinguish pending, accepted and completed idempotency states; a retry must finish or accurately report the prior operation. Define local-buffered, local-durable and replicated-durable acceptance separately. Give CDC a documented committed frontier if it is intended to observe accepted events only.

### F08 — High: routing, handoff and epoch authority are inconsistent

**Evidence: code trace; a multi-node split-brain/data-loss scenario was not executed.**

Membership changes update live router assignments before asynchronous transfer completes. The destination then asks that already-updated router for the current leader, which can identify the destination itself as the source for its first copy. Failed transfer does not roll the routing update back or reliably gate the new owner from serving.

Other inconsistencies compound this: syncing changed leaders into Raft retains the existing epoch; election updates the router without passing the new epoch; reconciliation checks committed leader identity but promotes using the router epoch; and reconciliation does not demote an existing leader merely because its assignment changed. Public write ownership checks use the router view. Raft initialization failure can continue into fallback behavior rather than failing closed.

Replica-offset refreshing runs from `performLeaderTasks`, called only by the Raft leader, yet inspects locally led partitions. The comment that all remote partition leaders report through this path does not match that call structure. This weakens the evidence used for lag-aware elections.

Sources: [router.go:185](../internal/cluster/router.go#L185), [router.go:276](../internal/cluster/router.go#L276), [manager.go:282](../internal/cluster/manager.go#L282), [manager.go:456](../internal/cluster/manager.go#L456), [manager.go:551](../internal/cluster/manager.go#L551), [manager.go:817](../internal/partition/manager.go#L817).

**Required:** use committed assignment plus a monotonic persisted epoch as the authority for every write and replication request. Make handoff a state machine: prepare destination, copy/catch up, commit ownership, fence old owner, enable new owner. Fail closed when that authority is unavailable. For telemetry, allowing an out-of-sync election can be a deliberate availability choice, but its possible loss must be measured and configured explicitly.

### F09 — High: snapshot transfer is not a verified, recoverable generation swap

**Evidence: code trace.**

The follower resets its running checksum at every file header but verifies only the last file. Earlier WAL payload files can therefore be installed without checksum validation. It also accepts a peer-supplied filename directly into `filepath.Join`, without confinement validation; a malicious or compromised peer can use traversal components.

Snapshot generation flushes the WAL but does not pin a consistent generation across hashing, streaming and compaction. Installation closes the old WAL, renames segment and index directories separately, deletes the old directories, and then reloads. There is no durable installation manifest/recovery procedure for a crash or failure between those operations. A sequence of renames is not an atomic replacement of the whole partition state.

The server's default transfer limit is 1 GiB; the follower requests that default for a whole-partition snapshot. A partition beyond that size cannot complete this bootstrap path as currently requested. Consumer progress, dedup and scheduler state are not included in the transferred segment/index pair.

Sources: [follower.go:139](../internal/replication/follower.go#L139), [follower.go:189](../internal/replication/follower.go#L189), [follower.go:221](../internal/replication/follower.go#L221), [replication_server.go:203](../internal/api/replication_server.go#L203).

**Required:** pin a generation, manifest every file and its size/hash, confine filenames, fsync staged data, and atomically publish a durable generation pointer with restart recovery. Support bounded chunks/resume for large partitions. Test corruption of a non-final file, interrupted install at each transition and bootstrap beyond 1 GiB.

### F10 — High: the wrong encryption key opens existing data as an empty writable WAL

**Evidence: reproduced.**

Segment recovery treats decryption failure as an invalid tail, resets its logical write position to the last good position and succeeds. With the wrong key, the first encrypted record fails and the populated WAL opens at next offset 0. Subsequent writes can overwrite the old record area. The probe verified the writable empty state; it did not intentionally overwrite the retained ciphertext.

WAL segment loading also skips some unreadable segments rather than failing startup. Tail repair needs to distinguish an incomplete final append from a wrong key or corruption inside previously valid history.

Sources: [segment.go:1281](../internal/storage/segment.go#L1281), [segment.go:1347](../internal/storage/segment.go#L1347), [wal.go:231](../internal/storage/wal.go#L231).

**Required:** fail closed on key mismatch and interior corruption, preserve/quarantine original files, and expose a repair procedure with an explicit lost-offset range. Verify key identity before any writable recovery. This is essential even when telemetry loss is allowed: a configuration mistake must not silently reset the database.

### F11 — High when DLQ is enabled: entries disappear on reopen and batches lose payloads

**Evidence: reproduced persistence failure; code trace of batch handling.**

The DLQ writer's record length/allocation includes four extra bytes compared with its CRC payload. The reader includes those trailing bytes in the checksum calculation and rejects the record. Adding one entry, closing and reopening produced zero entries.

Separately, the retry-exhaustion path passes `Delivery.Event` to the DLQ. For a multi-event delivery, that field is nil and records live in `Delivery.Batch`, so the full failed payload is not preserved. The completion callback has a related single-event assumption.

Sources: [dlq_segment.go:123](../internal/delivery/dlq_segment.go#L123), [dlq_segment.go:223](../internal/delivery/dlq_segment.go#L223), [dispatcher.go:1103](../internal/delivery/dispatcher.go#L1103).

**Required:** correct and version the record format; test close/reopen, CRC corruption and tail repair; preserve every event from failed batches. Include DLQ in any promised payload encryption and retention policy. Current WAL encryption does not by itself encrypt DLQ JSON.

### F12 — High for telemetry replay: time-range queries assume ordered schedule timestamps

**Evidence: reproduced.**

Segment pruning uses the timestamps of the first and last appended events as if they were minimum and maximum values. The sparse time index also binary-searches timestamps accumulated in append order. Valid out-of-order data can be omitted.

The probe appended timestamps `[5000, 1000, 4000]`; a query for `[1000, 2000]` returned zero events instead of one. Different future scheduling times and late-arriving telemetry naturally exercise this condition.

Sources: [wal.go:1059](../internal/storage/wal.go#L1059), [segment.go:1018](../internal/storage/segment.go#L1018), [index.go:190](../internal/storage/index.go#L190).

**Required:** track actual segment time bounds and use an index that supports unsorted ingestion, or fall back to a correct scan. Test shuffled timestamps across segment boundaries and clarify that current replay indexes schedule time, not an arbitrary timestamp inside a telemetry payload.

### F13 — High: SDK consumers can hang after stream failure and default routing misses partitions

**Evidence: reproduced reconnect blocker; code trace of routing.**

After `recvLoop` exits, `consumeFromNode` waits for infrastructure goroutines before cancelling their context. Heartbeat/drain loops need that cancellation to exit. The deferred cancel cannot execute until after the wait, so normal reconnect logic is blocked. The probe returned only after cancelling the outer context.

The SDK's default partition `-1` resolves subscription by topic to one partition. Producers distribute by message ID/partition key across partitions. There is no complete default assignment loop subscribing this consumer to every relevant partition of a topic. Single-partition tests hide the mismatch.

Sources: [consumer.go:562](../pkg/client/consumer.go#L562), [consumer.go:640](../pkg/client/consumer.go#L640), [consumer.go:198](../pkg/client/consumer.go#L198), [handlers.go:283](../internal/api/handlers.go#L283), [handlers.go:850](../internal/api/handlers.go#L850).

**Required:** cancel failed stream infrastructure before waiting, define how pending ACKs drain, and test automatic reconnection with retained data. Unify producer/consumer partition semantics; use real group assignments and subscriptions across partitions for scalable telemetry consumption.

### F14 — High: the supplied production deployment is not wired to runtime configuration

**Evidence: reproduced ignored configuration; rendered chart and code trace.**

The chart sets `CRONOS_PARTITION_COUNT`, `CRONOS_REPLICATION_FACTOR`, `CRONOS_FSYNC_MODE` and cluster address variables, but `LoadConfig` does not read those variables. The probe requested 32 partitions, replication factor 3, periodic fsync and a specific cluster address; it got 1, 1, batch and `:7947`.

Production values set minimum ISR 2 through a supported variable while replication factor stays at its default 1, violating configuration validation. The chart mounts an auth secret but does not provide the required policy filename. Fixing only one of these startup failures will not make the deployment correct.

Additional deployment gaps:

- All pods receive seed addresses, while Raft bootstrap is selected when the seed list is empty; the first-cluster bootstrap procedure is not correctly expressed by these manifests.
- Bind addresses such as wildcard/port-only values flow into advertised peer addresses; the manifests need explicit routable per-pod addresses.
- The encryption secret has no restricted file mode, while Unix master-key loading rejects group/world permissions. Ownership must also permit the non-root process to read it.
- The chart lacks a startup probe and an explicit termination grace period matching the application's 60-second shutdown window. Scheduling-affinity/node-selector/toleration values are not rendered into the pod specification.
- The HTTP admin listener uses plain `ListenAndServe`; enabling gRPC TLS does not protect that listener. Raft and the custom membership transport also require their own network trust controls; replication mTLS is not universal transport encryption.
- The Dockerfile does not build dashboard assets; a clean build depends on what embedded assets have been staged beforehand.

Sources: [config.go:257](../internal/config/config.go#L257), [statefulset.yaml](../charts/cronos-db/templates/statefulset.yaml), [configmap.yaml](../charts/cronos-db/templates/configmap.yaml), [manager.go:99](../internal/cluster/manager.go#L99), [crypto.go](../internal/storage/crypto.go), [main.go:544](../cmd/api/main.go#L544), [raft.go](../internal/cluster/raft.go), [Dockerfile](../Dockerfile).

**Required:** one validated configuration schema, a startup dump of effective nonsecret settings, and an installation test that boots the actual production chart on Linux. Make bootstrap, advertised addresses, key permissions, transport boundaries, probes and shutdown budgets explicit. Helm lint alone cannot validate these behaviors.

### F15 — High when enabled: backups and retention do not match the live partition model

**Evidence: code trace.**

The backup scheduler is initialized with `DataDir/wal`, while live WALs are under `DataDir/partitions/<id>/segments`. Its source does not match `BackupWAL`'s expected partition layout. Every scheduled backup also creates a new destination directory, so destination-local incremental state does not carry between runs. Active segments are excluded, leaving low-volume data outside backups until rotation. The configured local backup location does not by itself protect against disk/node loss.

Retention deletes segment files directly instead of through the live WAL manager. This leaves stale in-memory segment/index references and platform-dependent deletion behavior. The index deletion path appends `.index` to the full log filename rather than using the actual index filename. Retention/compaction do not consistently protect the earliest pending scheduled event or replica catch-up requirement. A later-offset ACK is not proof that all earlier scheduled events are dispensable. Size accounting excludes active segment bytes from its retained-segment calculation.

Sources: [main.go:165](../cmd/api/main.go#L165), [backup.go:31](../internal/storage/backup.go#L31), [backup_scheduler.go](../internal/storage/backup_scheduler.go), [retention.go:165](../internal/compliance/retention.go#L165), [retention.go:197](../internal/compliance/retention.go#L197), [manager.go:914](../internal/partition/manager.go#L914).

**Required:** back up consistent partition generations to an independent failure domain and prove restoration. Route deletion through WAL ownership and explicit retention watermarks. Define whether retention deliberately expires undelivered telemetry; expose that loss separately from successful processing.

### F16 — High if exposed: transactions and partition splitting are not safe production features

**Evidence: two reproduced transaction defects; code trace of split behavior.**

`Prepare` holds participant locks and calls abort handling on failure; that handling reacquires the same non-reentrant locks. The probe remained blocked beyond its context deadline. Duplicate participant IDs can also attempt to acquire the same lock twice. `Commit` accepts an already-aborted transaction in the second probe. Recovery treats prepared state as grounds to commit without an independently durable coordinator decision. The API does not establish a complete transactional relationship between user publishes, replicated decisions and participant state.

Splitting copies records and schedules them on the new partition while source records/timers remain active. Updating key bounds does not replace the hash-modulo routing scheme with a persistent range map. Moved keys can still route to the old partition and fail bounds checks; timers can exist in both places. Split bounds/ownership need a recoverable consensus transition, not just local field updates.

Sources: [coordinator.go:415](../internal/tx/coordinator.go#L415), [coordinator.go:467](../internal/tx/coordinator.go#L467), [coordinator.go:562](../internal/tx/coordinator.go#L562), [coordinator.go:667](../internal/tx/coordinator.go#L667), [split.go:36](../internal/partition/split.go#L36).

**Required for the first telemetry release:** disable these endpoints/features and remove their production guarantees. Fixed partitions and explicit at-least-once/best-effort semantics are a smaller supportable surface. If retained later, implement and fault-test their full state machines before exposing them.

### F17 — High under load: several paths defeat latency and memory limits

**Evidence: code trace; magnitude not benchmarked.**

Replication waits for all follower goroutines before evaluating results, while publishes are serialized per partition. A lagging follower can hold a healthy quorum's response until its timeout. `ReplicationTimeout` is passed as the leader constructor's flush interval, while the actual replicate timeout remains separately hard-coded. Catch-up work is also invoked in the foreground replication path.

CDC emits sequentially through append hooks and can block for up to five seconds per event/sink on a full queue. Large batches can therefore tie up ingestion far beyond one bounded batch timeout. Failed asynchronous sink retries have no durable replay cursor. Hooks are installed by iterating startup partitions, leaving lazily created partitions outside that initial wiring.

Combined with F04's unbounded worker queue, this prevents a credible high-throughput overload story. The memory monitor uses host virtual-memory information rather than a demonstrated process/cgroup budget. Several dispatcher settings are created from defaults instead of the exposed application configuration.

Sources: [leader.go](../internal/replication/leader.go), [manager.go:1191](../internal/partition/manager.go#L1191), [cdc](../internal/cdc), [main.go:265](../cmd/api/main.go#L265), [worker.go:50](../internal/delivery/worker.go#L50), [partition manager](../internal/partition/manager.go).

**Required:** make quorum completion independent of the slowest unnecessary replica while retaining safe async catch-up; wire real deadlines; put byte-bounded queues and explicit shedding at every boundary. Give CDC a durable cursor if delivery is promised, or a clearly measured best-effort contract. Measure container RSS, retained bytes, queue age and ingress rejection under burst and sink failure.

### F18 — Medium, release gate: metrics cannot explain accepted-event outcomes

**Evidence: code trace.**

The worker counts a dispatched batch even when the dispatcher skipped unsent records. Ready-queue metrics do not include all worker backlog. SLO interceptors classify success using the returned Go error, while many API failures return `Success:false` and a nil error. This can report healthy request success during quota, storage or replication failures.

Replication offsets, consumer state, scheduler depth and WAL acceptance must be connected to one accounting model. An operator currently cannot reliably distinguish intentionally expired telemetry, delivery lag, scheduling loss, quorum failure and permanent loss from the visible counters alone.

Sources: [worker.go:114](../internal/delivery/worker.go#L114), [slo](../internal/slo), [API interceptors](../internal/api), [handlers.go:735](../internal/api/handlers.go#L735), [main.go:637](../cmd/api/main.go#L637).

**Required:** count offered, rejected, accepted, delivered, expired and dropped records at clear boundaries, with bounded reason labels. Expose oldest pending age, consumer lag, replica lag, fsync latency, disk headroom, scheduler recovery failures and queue bytes. Make semantic API failures visible in availability metrics and client error handling.

### F19 — High/Medium by exposure: dependency security updates are overdue

**Evidence: scanner results dated 2026-09-27, with selected upstream advisories checked.**

`govulncheck` reported 13 vulnerabilities at called-symbol level across `google.golang.org/grpc@v1.81.1`, `github.com/hamba/avro/v2@v2.31.0` and the local Go 1.26.4 standard library. It additionally reported four imported-package and two required-module findings without called-symbol matches. Static call reachability is not proof of exploitability; some generated traces through interfaces are broad.

The highest-priority verified advisory is gRPC HTTP/2 fragmentation memory exhaustion, affecting the server transport in use and fixed upstream in 1.83.1. Another gRPC finding has an xDS-routing prerequisite, so it should not be presented as a demonstrated exploit of this ordinary server. Choose a supported patched version covering all findings, then rescan. See the [gRPC fragmentation advisory](https://github.com/grpc/grpc-go/security/advisories/GHSA-vp52-pcj8-j9qc) and [Go database entry for the xDS-dependent panic](https://pkg.go.dev/vuln/GO-2026-6443).

The local toolchain has relevant standard-library reports, including TLS post-handshake CPU exhaustion; that advisory lists fixes in Go 1.25.13 and 1.26.6. The actual release image toolchain must be inspected separately because Docker uses a floating Go 1.25 tag. See [GO-2026-6090](https://pkg.go.dev/vuln/GO-2026-6090).

The Avro scanner reports unbounded allocation, integer overflow and CPU exhaustion, with no fixed version in the scan's database entries (GO-2026-5048, GO-2026-5047, GO-2026-5046). Validation calls `avro.Unmarshal` on event data. Bound untrusted decoding and consider disabling this format until a reviewed patched path is available; validate actual triggerability in a separate controlled test.

`npm audit` reported nine affected packages: five high and four moderate, with fixes available. These counts include transitive and development dependencies and are not nine demonstrated browser/server exploits. In particular, the React Router advisory requires unstable RSC APIs, which were not found in the inspected static SPA. See the [upstream React Router advisory](https://github.com/remix-run/react-router/security/advisories/GHSA-qwww-vcr4-c8h2).

Full local scan output is saved under [audit evidence](audit-probes/evidence/). **Required:** patch and rescan the actual release artifacts; record affected surface, mitigation and expiry for accepted findings; add Go/npm/container/Rust dependency scanning to release checks. A Rust advisory audit and container scan were not performed here.

### F20 — Medium, release gate: existing tests and release automation miss the failure boundaries

**Evidence: repository inspection and test results.**

Only documentation publishing is configured under `.github/workflows`. A local Makefile target is not an enforced release gate. Existing tests frequently check individual components without verifying accepted-event conservation through restart or failover. A DLQ test expecting an empty result after removal can pass even if all entries fail to load. Snapshot metadata tests do not establish restoration of pending timers. Rust currently has no executed tests.

The chaos material does not establish an end-to-end ledger of accepted IDs and consumed/expired/dropped IDs. Applying a fake clock to a helper command does not demonstrate skew in the running database process. Existing performance results cannot substitute for throughput measured with the intended auth, encryption, replication, consumers and recovery behavior enabled.

Sources: [.github/workflows](../.github/workflows), [Makefile](../Makefile), [tests](../tests), [dedup Rust](../internal/dedup/rust), [audit probe results](audit-probes/README.md).

**Required:** CI for Go checks, cgo/race on Linux, dashboard tests/build, actual chart startup, dependency scans, and deterministic failure tests. Gate releases on recovery of identified accepted records, bounded resource use and a successful independent restore.

## Hardening order

1. **Constrain the first supported product.** Fixed partitions, one defined telemetry delivery policy, explicit loss/retention limits. Keep transactions, online split, and optional CDC/cross-region paths disabled until separately verified. Decide whether scheduled delivery is essential; if enabled, all F05 paths are mandatory fixes.
2. **Fix isolation and local correctness.** F01–F05, F10–F13, and F02's startup behavior. This includes ACK ownership, queue ownership, consumer replay/reconnect, timer recovery, DLQ persistence and unordered time queries. Convert each reproduction into a normal regression test as its fix lands.
3. **Establish a coherent replication protocol.** Address F06–F09 together: durable terms, idempotent append, truthful retry results, authoritative leadership, staged handoff and verified snapshot generations. Do not optimize replication before these invariants have executable tests.
4. **Make the shipped deployment and operations real.** F14–F15, dependency updates, CI, backup/restore and the accounting metrics in F18. Exercise the actual image/chart, not an alternative developer invocation.
5. **Measure and optimize the complete telemetry path.** Address F17 using profiles and controlled saturation only after correctness tests pass. Compare configurations by accepted-and-accounted-for events per second, not just append calls per second.

This is a hardening program across several subsystems, not a configuration-only production promotion. It does not require implementing financial-grade exactly-once transactions for the selected workload.

## Suggested release acceptance contract

Numerical targets cannot be chosen from the current prompt alone. Set an explicit sustained rate, burst rate/duration, payload distribution, partition count, retention period, maximum tolerable delay, and crash-loss window before capacity testing. The following behaviors should hold regardless of those numbers:

| Scenario | Required observable result |
|---|---|
| Two tenants/topics share a partition | Neither principal can read or ACK the other's records |
| Policy/key configuration is wrong | Startup fails without granting permissions or rewriting data |
| Consumers disconnect or run out of credits | Retained work resumes, or an explicit configured drop counter accounts for it |
| Quorum times out and producer retries | Stable idempotent result; no success for an unfinished operation |
| Process restarts twice with pending timers | Retained pending work is reconstructed both times |
| Leader fails / old leader returns | One valid append authority per epoch; no truncation from duplicate RPCs |
| Snapshot is interrupted or exceeds 1 GiB | Resume/retry safely; last good generation stays recoverable |
| Disk fills or a segment is corrupt | Bounded failure, truthful API response, preserved evidence and an explicit repair path |
| Out-of-order timestamps are ingested | Replay returns every retained matching record |
| Retention runs with delayed events and slow replicas | Documented expiry policy; no unexplained deletion |
| Sustained overload / slow CDC sink | Memory and disk stay bounded; backpressure or measured shedding occurs |
| Production pod rolls or node is replaced | Correct effective configuration, catch-up and readiness before serving |
| Backup is restored into a fresh environment | Event inventory and group/scheduling state meet the documented RPO/RTO |

For a first performance campaign, use realistic payload percentiles, skewed keys, multiple consumer groups, a slow consumer, delayed/out-of-order events and one lagging replica. Run long enough to include segment rotation, retention, snapshots and resource steady state; include a separate burst phase. Collect p50/p95/p99 publish and delivery latency, unique accepted IDs, unique delivered IDs, duplicates, explicit drops/expiry, queue bytes, RSS/cgroup usage, disk growth and recovery time.

For each bounded test run after pending work has settled, reconcile accepted IDs against delivered, intentionally expired/dropped and still-retained pending IDs. Loss within a stated crash window can be accepted for telemetry; unexplained missing IDs outside that contract remain release blockers.

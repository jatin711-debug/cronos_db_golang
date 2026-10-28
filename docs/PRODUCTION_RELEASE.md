# Production release requirements

**v0.10.0 fixes the thirteen defects of the
[1.0 readiness review](RELEASE_READINESS_1.0.0_2026-10-28.md)** and keeps
v0.9.0 data directories. Message IDs are unique per topic and v0.9.0
duplicate records are not carried over; see the
[v0.10.0 notes](RELEASE_V0.10.0.md). The review's release gates (a multi-host
soak, recovery and upgrade drills, machine or power-loss testing) are open.

**v0.9.0 changes the WAL segment format to version 2.** It requires new data
directories on every node; version-1 data and backups are not migrated, older
binaries cannot read the new files, and mixed-version clusters are
unsupported. The published v0.8.1 and earlier artifacts are unchanged. See the
[v0.9.0 notes](RELEASE_V0.9.0.md) and
[closed WAL segment boundaries](STORAGE_SEGMENT_BOUNDARIES.md).

The `v0.8.0` source line adds the lifecycle and replication changes described
in the [release notes](RELEASE_V0.8.0.md). Available artifacts and publication
status are on the [releases page](https://github.com/jatin711-debug/cronos_db_golang/releases).
Hardening goes on: the
[audit closure table](PRODUCTION_AUDIT_2026-09-27.md#finding-closure-review)
tracks unresolved guarantees, and
[current release limits](#what-v080-was-released-without) are listed
below. A green CI run is necessary but does not establish the complete release
acceptance contract.

## v0.10.0 release

The [v0.10.0 notes](RELEASE_V0.10.0.md) describe the fixes for the thirteen
defects of the [1.0 readiness review](RELEASE_READINESS_1.0.0_2026-10-28.md),
among them two ways to lose acknowledged writes (replica placement changes
and retried publishes after a change of leader), scheduler drift, duplicate
detection that forgot accepted IDs, and an encrypted node that could not
restart after a crash. The chart and default image tag are `0.10.0`.
Publication status, checksums and the exact deployment digest are on the
[release page](https://github.com/jatin711-debug/cronos_db_golang/releases/tag/v0.10.0).
Use its `IMAGE_DIGEST` and publication evidence to identify the tested image.
v0.9.0 data directories are kept; duplicate records are not. This release
makes no new capacity claim.

Published on 2026-10-28 from `315532780d117a3260758c8034a6504cee9c8037`. The
fixes passed the required checks on
[PR #17](https://github.com/jatin711-debug/cronos_db_golang/pull/17), and the
release revision on
[PR #18](https://github.com/jatin711-debug/cronos_db_golang/pull/18). The
[run on `main`](https://github.com/jatin711-debug/cronos_db_golang/actions/runs/40758628646)
passed on its second attempt; the first failed the discovery test, which
published before leadership had settled after start-up, while the fault
campaign in the same attempt passed. The
[release staging run](https://github.com/jatin711-debug/cronos_db_golang/actions/runs/40758708786)
built the binaries, started the Linux binary, and scanned and fault-tested the
exact image archived on the release, including container network faults and a
restart with a backlog larger than memory. The scan reported no findings.

The Linux amd64 image is `johnny711dock/cronos-db:0.10.0`, manifest digest
`sha256:9e21824133d9dd76aabe00d4dbb96b1fa2219014cd3fdc8ba11d19b58c575011`.
It was loaded from the staged archive and pushed locally; its configuration and
all nine filesystem layers match the scanned image, and anonymous registry
requests select the same manifest for `0.10.0` and `latest`. Pin the manifest
for deployments. This publication has no signed GitHub workflow provenance.
`latest` previously selected v0.9.0
(`sha256:b597796916f59408801e37577ea591e38bac312828d7bbc6bf06b15e07b3abf4`); a
node started from `latest` on a v0.9.0 data directory keeps it.

## v0.9.0 release

The [v0.9.0 notes](RELEASE_V0.9.0.md) describe the version-2 WAL segment
format, which detects erased areas of closed segments, and a fix for a node
crash when a scheduled backup overlapped the release of a partition. The chart
and default image tag are `0.9.0`. Publication status, checksums and the exact
deployment digest are on the
[release page](https://github.com/jatin711-debug/cronos_db_golang/releases/tag/v0.9.0).
Use its `IMAGE_DIGEST` and publication evidence to identify the tested image.
This release breaks on-disk compatibility and makes no new capacity claim.

Published on 2026-10-27 from `12507c7fb43e93b0812d987ccb916e02f14a7042`. The
required source checks passed on
[PR #15](https://github.com/jatin711-debug/cronos_db_golang/pull/15) (second
attempt of [the run](https://github.com/jatin711-debug/cronos_db_golang/actions/runs/40526717149);
the first failed a cluster start-up timing test and passed when re-run on the
same revision). The
[release staging run](https://github.com/jatin711-debug/cronos_db_golang/actions/runs/40531400344)
built the binaries, started the Linux binary, and scanned and fault-tested the
exact image archived on the release, including container network faults and a
restart with a backlog larger than memory.

The Linux amd64 image is `johnny711dock/cronos-db:0.9.0`, manifest digest
`sha256:b597796916f59408801e37577ea591e38bac312828d7bbc6bf06b15e07b3abf4`.
It was loaded from the staged archive and pushed locally; its configuration and
all nine filesystem layers match the scanned image, and anonymous registry
requests select the same manifest for `0.9.0` and `latest`. Pin the manifest
for deployments. This publication has no signed GitHub workflow provenance.
`latest` previously selected v0.8.1
(`sha256:f2839385386b2f44b6a07669fcb001dcab31273a3e7ba14f1f0510f6a9594883`); a
node started from `latest` on a v0.8.1 data directory now refuses it, because
the WAL segment format changed.

## v0.8.1 patch release

The [v0.8.1 notes](RELEASE_V0.8.1.md) describe the security, subscription
registration and deduplication lookup fixes. The chart and default image tag
are `0.8.1`. Publication status, checksums and the exact deployment digest are
on the [release page](https://github.com/jatin711-debug/cronos_db_golang/releases/tag/v0.8.1).
Use its `IMAGE_DIGEST` and publication evidence to identify the tested image.
This patch adds no WAL format and makes no new capacity claim.

## v0.8.0 publication record

Published on 2026-10-19 from `cfba215208b3a50c830020636a417f7aaeaf3845`.
The required source checks passed before promotion to `main`; the
[release staging run](https://github.com/jatin711-debug/cronos_db_golang/actions/runs/39277781872)
also built the binaries, started the Linux binary, and scanned and fault-tested
the exact image archived on the release.

The Linux amd64 image is `johnny711dock/cronos-db:0.8.0`, manifest digest
`sha256:428690215148b97eb9b6aaed5a3afb7c7a3227613c8fee731ab922e2276d4928`.
It was loaded from that archive and pushed locally, then verified through
anonymous registry requests. At publication, `latest` was an OCI index selecting
that same manifest; an index digest need not equal the image manifest digest. Pin the
manifest for deployments. This publication has no signed GitHub workflow
provenance. Evidence and binary checksums are attached to the
[published release](https://github.com/jatin711-debug/cronos_db_golang/releases/tag/v0.8.0).

Development follow-up on 2026-10-21 raises the build toolchain to Go 1.26.10,
the dashboard's Vite floor to 7.3.7, and `github.com/klauspost/compress` to
v1.18.7 after current CI found `GO-2026-6809` in `crypto/x509`, the bundled
`launch-editor` advisories in Vite 7.3.6, and `CVE-2026-63209` in the image's
compression dependency. See the [Go patch release](https://go.dev/doc/devel/release#go1.26.10),
[Vite advisory](https://github.com/advisories/GHSA-r825-w8mm-5pxw), and
[compression advisory](https://github.com/klauspost/compress/security/advisories/GHSA-259r-337f-4rfw).
These source updates are included in v0.8.1; the v0.8.0 binaries and image
above retain their original contents and scan date.

The same development follow-up initializes consumer-group membership before a
new subscription can receive live events. This closes a registration window in
which the first acknowledgement could be refused because its group did not yet
exist. This change is included in v0.8.1.

Deduplication lookups also keep buffered IDs visible until their Pebble batch
has committed. Previously, a lookup during the handoff from the buffer to
Pebble could briefly report an existing ID as missing. Failed flushes now leave
those buffered IDs visible while they wait for another attempt. These fixes
are included in v0.8.1.

The major correctness work in this release is current-term commitment after
leader changes, opt-in future-event cancellation/rescheduling, bounded
relocation, and consistent lifecycle recovery snapshots/backups. See the
[lifecycle contract](RETENTION_AND_EVENT_LIFECYCLE.md) and
[term-start protocol](REPLICATION_TERM_START.md). Current-term activation waits
for every assigned replica, handoff target and Raft voter to be capable.
Mixed clusters retain legacy accounting and its old-term defect until then.
Lifecycle operations additionally require explicit opt-in and majority
acknowledgements. Once formats activate, incompatible downgrade is unsupported.

## What v0.7.0 adds

`v0.7.0` implemented fixes for the runtime findings of the
[October audit](PRODUCTION_AUDIT_2026-10-16.md):
records that could not be read back, terms chosen by producers, append and
fsync errors that released message IDs, quota refusals that stranded them,
scheduler memory, the dedup recovery cutoff, completion and dead-letter
replication, disk admission on followers and client discovery of new nodes.
The workflow for building, scanning, testing, pushing and attesting one image
was also implemented; actual local publications lack that attestation. See
[hardening and upgrade behavior](HARDENING_V0.7.0.md), including what a
rolling upgrade from `v0.6.0` holds back, the Docker Hub secret names and the
optional separate backup PVC.

## Supported scope under validation

The first release targets fixed partitions and telemetry with explicit loss,
retention, and duplicate-delivery limits. Transactions and online partition
splitting are disabled by default. Both require `--dev --experimental-features`;
production configuration rejects that flag. Production also rejects
`--exactly-once-commits`: commit-ID storage is not an end-to-end delivery guarantee.

Change data capture to Kafka and webhooks is supported: it exports accepted
events in log order from each partition's leader, and a partition keeps its
log entries until they have been exported. Replication to other regions is
experimental and part of the same opt-in: a node with `CRONOS_REGIONS` set
refuses to start without `--dev --experimental-features`.

Production mode accepts `--fsync-mode=batch` and `every_event`. In both, a
replica has an entry on disk before it acknowledges it, so a publish
acknowledged with `min-insync-replicas=2` is on two disks. `periodic` is
refused: it acknowledges first and syncs later, and loses acknowledged
publishes when the machines of a quorum lose power together.

## Configuration and installation

Settings are applied in this order: defaults, matching `CRONOS_*` environment
variables, explicit command-line flags. Invalid environment values fail loading.
An explicit empty `--cluster-seeds=` clears environment-provided seeds. Startup
logs effective partition, replication, durability, and address settings without
logging secret values. Invalid authorization files fail before data directories
or listeners are initialized.

The production chart requires these existing secrets:

| Default secret | Required keys |
|---|---|
| `cronos-db-auth` | `jwt-secret`, `policy.json` |
| `cronos-db-tls` | `tls.crt`, `tls.key`, `ca.crt` |
| `cronos-db-replication-tls` | `tls.crt`, `tls.key`, `ca.crt` |
| `cronos-db-encryption` | `master.key` |

The policy file is a JSON object keyed directly by JWT subject, for example
`{"operator":{"admin":true}}`. Supply topic-specific permissions for application
subjects. The encryption key must meet `LoadMasterKey`'s 32-byte requirement.
The chart's non-root init container copies the projected key into a memory volume
with mode `0600`; the application reads that private copy. Keep the default
user/group/fsGroup relationship or adapt key staging to your security policy.

Use certificates whose SANs cover the advertised pod DNS addresses, including
`<pod>.<headless-service>.<namespace>.svc`. Replication certificates must support
client and server authentication. Public client certificates depend on the
configured public TLS policy.

The `cronos-db-replication-tls` secret secures all traffic between nodes:
replication, membership and Raft. Each of the three ports reads nothing from a
caller that has not presented a certificate signed by the CA in `ca.crt`, and a
node checks that the certificate of the node it calls is valid for the address
it called. `ca.crt` may hold more than one CA certificate.

Certificates are replaced without a restart (since `v0.6.0-rc.4`; earlier
releases read them once, at a start). A node reads a certificate, its key and its CA
file again when they change, and uses what they hold for the next connection
it makes or accepts, on every port: the client port, the HTTP port,
replication, membership and Raft. It looks at the files when a connection is
made, at most every two seconds, so nothing has to be signalled. Connections
that are open keep the certificate they were made with. A certificate whose
key has not arrived yet, or a file that cannot be read, leaves the one read
before in use and is reported in the log once.

To replace the CA that issues the nodes' certificates: put the old and the
new CA in every node's `ca.crt`; then give each node a certificate of the new
CA; then take the old CA out of `ca.crt`. Every step can be made on running
nodes and in any order of nodes.
`TestCertificatesAndTheirCAReplacedWhileRunning` does it to three running
nodes that producers and a consumer group are using, and then restarts one of
them. That node comes back with the new files only, and has to be let in by
the two that were never restarted, and let them in, on the membership, Raft
and replication ports; the cluster is then made to depend on it by killing
one of the two.

A cluster that runs 0.6.0-rc.2 or earlier with replication TLS cannot be
upgraded one pod at a time: those versions speak plain TCP on the membership
and Raft ports, and a node of this version does not talk to them. Stop all its
nodes and start them on the new version together. The data stays.

The chart starts pods in parallel, and every pod names every pod as a seed.
Ordinal zero is the one that creates the cluster (`--cluster-bootstrap`), and
it does so only when it has no Raft state and no other pod belongs to a
cluster. Raft state is on the data volume under `raft/`.

A pod whose volume is replaced comes back empty, finds the cluster through the
other pods and is filled from it; that holds for ordinal zero too. Replace one
volume at a time and wait for the pod to be ready: with two of three volumes
gone, events that were acknowledged by two replicas can be on neither of the
two that are left. Production mode refuses to start a node without
`--cluster-seeds`, because such a node creates a cluster whenever it has no
state.

A node that is lost for good is removed from the cluster by hand
(since `v0.6.0-rc.4`). Until then it stays a member,
which is what lets a node come back, and it keeps counting towards what a
majority is in Raft: a cluster of four with one node lost needs three of
four, which is every node that still runs. Once the node is certain not to
return, from a machine that has the cluster's certificates:

```
cronos-admin cluster remove-node <node-id> --server <any-node>:<cluster-grpc-port> \
  --tls --tls-ca ca.crt --tls-cert tls.crt --tls-key tls.key
```

Any node can be asked; one that does not coordinate the cluster passes the
request on. A node that is running is refused: it would join again at once.
`cronos-admin cluster voters` lists the nodes that count. A removed node that
is started again with its data joins as a member again. In
`TestLostNodeIsRemoved` one of four nodes is lost and removed, and the
cluster then loses a second node and carries on with two of three.

In `v0.6.0` and before, a node cannot be added to a running cluster for applications that are
already connected. Partition metadata names a leader by its node ID, and a
client turns that into an address only for the nodes it was started with or
was given in `NodeIDToAddress`. A node that joins later takes over leadership
of some partitions, and clients that do not know its address cannot publish
to those. Replace a node under the name and address of the one it replaces,
as a StatefulSet does, and restart or reconfigure clients before growing a
cluster.

Since `v0.7.0` a node started with `--advertise-grpc-addr` gives clients
that address in partition metadata, and updated clients refresh their address
map when membership changes; the Helm chart advertises each pod's DNS name
with `service.advertisePodAddress=true`. Explicit `NodeIDToAddress` overrides
still take precedence. Upgrade clients as well as servers to use discovery,
and advertise addresses reachable from those clients, which the client port's
certificate covers when TLS is on. A node started without the flag advertises
nothing.

The HTTP port (health, metrics, dashboard and the admin API) is served over
TLS with `--http-tls-enabled`, with the certificate of the client port
(`--tls-cert-file`, `--tls-key-file`). The production values of the chart
turn it on (`tls.http`), and the probes and the ServiceMonitor then use HTTPS.
Callers are not asked for a certificate on that port: the admin API checks
the bearer token. Prometheus does not verify the pods' certificate unless
`metrics.serviceMonitor.tlsConfig` names the CA and the server name. Without
the setting the port is plain HTTP, and the token and the answers cross the
network unencrypted; leave it off only when something in front of the pods
ends TLS for that port. The setting is there since `v0.6.0-rc.4`.

The image builds dashboard assets from the npm lockfile and the Rust library from
the Cargo lockfile. Its Go builder matches the module's minimum version. Pin the
resulting application image digest for a release deployment.

## Checks

`.github/workflows/ci.yml` defines Linux cgo/race tests, Go vet/build/formatting,
Rust tests, dashboard tests/build, Go/npm vulnerability checks, Helm assertions,
a scan of the built image for known vulnerabilities, a fault campaign against
three server processes, and a disposable kind cluster running the production
chart with security enabled. It runs for pull requests and for pushes to
`main`. Work on the development branch is not checked as it is pushed; a
commit there can be checked on request with
`gh workflow run ci.yml --ref developement`.
The five jobs (`go`, `fault-campaign`, `dashboard-and-chart`, `go-security`,
`production-chart-startup`) are required checks on `main` since 2026-10-13, for
administrators as well: `main` moves only to a commit on which all five have
passed in a run for a pull request or a push. A run on request does not count
for this, because GitHub does not hold its results against the commit. A
release therefore goes through a pull request from the development branch to
`main`, which runs the five jobs, and `main` moves when they are green. A
vulnerability finding fails its job; it is not silently waived.

Run chart assertions locally with `python scripts/check-production-chart.py`
(Helm and PyYAML required). `scripts/production-chart-smoke.sh` requires the
disposable `kind-cronos-ci` context and an already-loaded `cronos-db:ci` image. It
creates ephemeral test credentials, installs three production-mode pods, and
checks readiness, embedded dashboard assets, key permissions, and rejection of
unauthenticated HTTP admin requests. It then restarts the pods one at a time,
and starts the cluster again with the volume of ordinal zero emptied, checking
that the pod joins the cluster instead of creating one. It sends no events: it
is not a proof of quorum acceptance, failover, or restore. Tooling follows the
[kind quick-start workflow](https://kind.sigs.k8s.io/docs/user/quick-start/).

The `fault-campaign` job runs `tests/acceptance`: three server processes on the
runner, producers and a two-member consumer group working through the client
library, and a sequence of faults (a leader killed, a follower killed, a leader
frozen and released, each node restarted, a replica replaced with an empty one
and killed while it is refilled, the whole cluster killed). It fails if an
acknowledged event was not delivered, if one arrived before its time, or if
the replicas of a partition do not end with the same log. Its nodes speak
mutual TLS with each other, as production nodes do. Run it locally with
`go test -tags acceptance -count=1 -timeout 25m ./tests/acceptance/`; it needs
about 500 MB of memory. It stops and kills processes, and it publishes about
60 events a second, so it says nothing about behavior under load.

Processes on one machine cannot be cut off from one another, so the network
is failed in a test of its own. `TestNetworkPartitions` runs three containers
of the image on a network of their own and drops what chosen nodes send each
other, without a word, as a failed link does, while all three keep running
and applications still reach every one of them: the leader of a partition on
its own, one link down between two nodes that both still reach the third,
each node on its own in turn, and every link down. The same checks apply as
for the other faults. It runs in the `production-chart-startup` job, which
has built the image, and locally with
`CRONOS_IMAGE=<image> go test -tags acceptance -run TestNetworkPartitions ./tests/acceptance/`.

The `fault-campaign` job also restores a whole cluster from backups
(`TestClusterRestoredFromBackups`, on processes): three nodes back up while publishes
arrive, all three are destroyed, each is restored from its own backup, and
what was acknowledged before the backups is there and is delivered at its
time. Backups go to `backups` under the data directory unless `--backup-dir`
names another place. A backup on the volume it copies does not survive the
loss of that volume. The chart can provision a separate per-pod backup PVC with
`backupPersistence.enabled=true`, mounted at `/backups` by default. Choose a
separate failure domain and preserve the claims during recovery; another PVC
on the same storage backend does not protect against that backend's loss.

`cronos-admin` is in the image at `/app/cronos-admin`. `restore` and
`check-log` work on the files of a stopped node: scale the StatefulSet down
and run them from a pod of the same image that mounts the node's volume.

The acceptance tests make faults that show once in a few runs, and a change
gets one run. `.github/workflows/nightly.yml` runs them three times over
every night on `main`, on processes and on containers, and keeps the node
logs of a failure for two weeks. It runs on request for any branch:
`gh workflow run nightly.yml --ref developement`.

### Binaries

A release carries the server and `cronos-admin` for Linux and macOS (amd64
and arm64) and Windows (amd64), one archive for each, with `SHA256SUMS`.
`.github/workflows/release.yml` builds them from the tag when the release is
published, starts the Linux server it built and waits for it to say it is
ready, and attaches them. Manual staging can attach them to a draft before
publication. To build equivalent binaries locally, build and stage the dashboard
first (`make dashboard`, requires npm), then run
`bash scripts/build-release-binaries.sh <version> <output-directory>` with
Go, Bash, `tar` and `zip` available. Running the script alone from an unstaged
checkout leaves the dashboard placeholder embedded.

They are built without cgo, which is what lets one machine build all five
and lets each run without a library beside it. A build without cgo uses the
bloom filter written in Go; the image is built with cgo and uses the one in
Rust, which is faster. Neither keeps anything on disk, so a data directory
can go from one to the other. The `go` job of CI runs the suite once without
cgo and builds the five archives, so that a release is not the first to find
out that one of them does not build. The first release with binaries is
`v0.6.0-rc.4`: before it the server did not build for macOS, and needed cgo
and the Rust library everywhere but on Windows.

## Memory and overload

The scheduler shares a node-wide payload/accounting budget, 64 MiB by default
(`--scheduler-memory`, minimum 32 MiB). Far-future work and overflow within the
hot window (`--hot-window-minutes`, 60 by default) remain WAL references in the
cold store. Hydration reads bounded pages and can evict later timers for earlier
ones. This is an allocation budget, not an RSS cap: worker queues, Pebble caches,
in-flight requests and temporary WAL reads consume additional memory.

Publishes are refused, with `ResourceExhausted`, while the process holds
`--max-memory-percent` (80) of its memory limit. The limit is the container's
when there is one, otherwise the machine's memory, or `--memory-limit`. The
Go client reports the refusal as an error of kind `overloaded` and does not
retry it: the application should send less. A node at that point keeps
delivering, and takes publishes again when consumers have worked the backlog
off. `cronos_memory_held_bytes` and `cronos_memory_limit_bytes` show where a
node stands, and the chart alerts when a node has been refusing for two
minutes. The `fault-campaign` job tests this (`TestOverloadIsRefusedAndSurvived`):
producers flood a cluster that has a small limit and no consumer; the nodes
refuse, none dies, and everything that was acknowledged is delivered once a
consumer starts.

A node that restarts leaves on disk what is due and not delivered: a
subscription reads it from the log at the pace its consumers take it, a few
megabytes at a time. Two tests hold it to that. In
`TestRestartWithABacklogLargerThanMemory` three processes take in a backlog a
quarter larger than the memory limit they are restarted with, and must stay
below it until a consumer has received everything. In
`TestRestartInContainersWithABacklogLargerThanTheirMemory` three containers of
the image, which the kernel kills above 384 MiB, take in a gigabyte, are
killed, come back and deliver it. Recorded peaks are specific to each run;
see [v0.7.0 validation](HARDENING_V0.7.0.md) for its measured results. This holds
since `v0.6.0-rc.4`. `v0.6.0-rc.3` and everything before it reads the backlog
into memory at a start, and a node there whose backlog exceeds its memory is
killed on every start.

Queue admission also has event-count caps (`--max-ready-queue`,
`--max-timing-wheel-size`, `--max-in-flight`). These and sampled process memory
admission do not prove that all workloads avoid an OOM kill. A replay/redrive
page can exceed its byte target by one valid record. Backlogs beyond the tested
gigabyte, large future schedules and sustained production-size loads remain
unmeasured. The harness must not override a node's memory policy with the test
runner's `GOMEMLIMIT`.

## What a release promises, and what that rests on

Each promise below is checked by the test named with it, in CI: for every pull
request, for every push to `main`, and for the commit of a release. The
tests run three server processes on one machine at about 60 small events a
second; the last column says what they do not show.

| Promise | Checked by | Not shown |
|---|---|---|
| A publish that was acknowledged survives the loss of any one node (replication factor 3, two in-sync replicas, fsync `batch` or `every_event`) | `TestFaultCampaign`: a leader killed, a follower killed, a leader frozen and released, every node restarted, a node replaced with an empty one and killed while it is refilled, the whole cluster killed | Two nodes' disks lost together; loss of power on a real machine (the tests kill processes) |
| The same holds when the network between the nodes fails while all of them keep running, and a node that is cut off acknowledges nothing | `TestNetworkPartitions`: links cut between three containers of the image: a leader alone, one link down, each node alone in turn, every link down | Slow or lossy links, as opposed to dead ones; outages longer than a minute; more than one machine |
| Every acknowledged event is delivered at least once, and never before its time | The ledger of the same test, and of the restore and overload tests | Exactly once: after a failover some events are delivered again |
| The replicas of a partition hold the same log | `TestFaultCampaign` compares them entry for entry | |
| A partition takes publishes again within 90 seconds of its leader's death | The bound the campaign enforces | Typical times; behavior under load |
| Any node, the first included, can be replaced with an empty one | `TestFaultCampaign`, `TestFirstNodeReplaced`, the kind job on the chart | Two at once |
| A node that is lost for good is removed, and no longer counts towards a majority (since `v0.6.0-rc.4`) | `TestLostNodeIsRemoved`: one of four nodes lost and removed through `cronos-admin`, then a second node killed | Growing the cluster with connected clients is not covered by this test; `TestClientsFindTheOtherNodesThroughPartitionMetadata` shows a client given one node publishing to the leaders of every partition through the addresses metadata gives it |
| A cluster that is lost entirely comes back from its nodes' backups with everything acknowledged before the earliest of them | `TestClusterRestoredFromBackups` | Restore to a point between backups; getting backups off the node, which is the operator's to arrange |
| Producers that outrun the cluster are refused, and no node dies of it | `TestOverloadIsRefusedAndSurvived` | Load at production sizes |
| A node comes back with more waiting for consumers than it has memory, and delivers it (since `v0.6.0-rc.4`) | `TestRestartWithABacklogLargerThanMemory`; `TestRestartInContainersWithABacklogLargerThanTheirMemory`, with a limit of 384 MiB that the kernel enforces and a backlog of 1 GiB | Backlogs beyond a gigabyte; future events under production load. `v0.7.0` adds a separate scheduler payload budget for future events |
| Traffic between nodes is encrypted and only nodes that hold the cluster's certificate take part | `TestTransportTLS_*`, the kind job | |
| Certificates and their CA are replaced without a restart, and the HTTP port is served over TLS (since `v0.6.0-rc.4`) | `TestCertificatesAndTheirCAReplacedWhileRunning` on three running nodes; `TestNodes_ReplaceCertificatesAndTheirCAWhileRunning`, `TestCredentials_VerifyAgainstTheCAOfTheMoment`, `TestBuildHTTPTLSConfig_ServesTheHTTPPortOverTLS`; the kind job asks the HTTP port over TLS and checks that it does not answer without | The certificate of the client port and of the HTTP port replaced under load; a certificate that expires while a connection is open |
| One tenant cannot read, replay or acknowledge another's events | `TestIsolation_*` | |

No throughput, payload-size or latency figure is promised. None has been
measured on production hardware.

## What `v0.7.0` was released without

Releases are cut from the development branch when CI is green on the
commit. `v0.7.0` was published as the latest release and as the `latest`
image; the five CI jobs are required checks on `main` (see [Checks](#checks)).

`v0.7.0` was released without the following. These are limits of what has
been shown, not defects that are known; a deployment that depends on one of
them has to show it for itself first:

- A run on more than one machine. Everything above was shown on one host,
  with processes or with containers; links were cut between containers.
- A replica refilled from a partition larger than 1 GiB. The transfer has no
  size limit and resumes file by file, and has a ten minute deadline; it has
  only been run on megabytes.
- Throughput and latency measured on the hardware a deployment will use, with
  the memory that the backlog it allows needs. The
  [measurements of 2026-10-16](CLUSTER_PERFORMANCE_VALIDATION_2026-10-16.md)
  ran three processes on one Windows laptop; none was made on Linux.
- A cut of power. Recovery is tested after processes are killed, not after
  the machine loses power with writes in flight.
- An image published by the release workflow. Its image job needs the
  repository secrets `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN`, which the
  repository did not have. The image of `v0.7.0` was built from the tagged
  commit, scanned with the settings of CI (Trivy, HIGH and CRITICAL
  vulnerabilities that have a fix), started as three nodes and pushed, by
  hand, as for `v0.6.0`. It has no signed provenance; its digest is in the
  release notes.

## What v0.8.0 was released without

The runtime fixes do not close every product or validation gap:

- Multi-host fault/load validation, real machine power cuts, deployment-specific
  capacity, replica refills beyond the tested size, and a new v0.8.0 throughput
  measurement are not established. Older dated benchmarks are not measurements
  of this release.
- Lifecycle v1 excludes CDC, cross-region replication, transactions and online
  splitting. Cancellation only affects future work; it cannot recall a sent
  event. Delivery remains at least once. Mixed-version commitment and one-way
  format activation have the limits above.
- Relocation is opt-in and bounded. Unfinished due work, unresolved mutations,
  lagging dead-letter replication and other retention gates can still pin a
  log. Dead-letter journal/tombstone history is not compacted.
- Distribution has one Linux amd64 image and five non-cgo binary targets. The
  image has no signed workflow provenance; automated publishing requires the
  Docker Hub repository secrets. The thresholded image scan found no fixable
  HIGH/CRITICAL findings at scan time; it is not a claim of zero vulnerabilities.
- Lag-driven automatic snapshots remain unwired, `TriggerRebalance` remains a
  soft stub, and tenant quota configuration remains unavailable in the shipped
  server. See the [architecture limitations](../ARCHITECTURE.md#known-limitations).

Known limits of what is released are in the
[audit](PRODUCTION_AUDIT_2026-09-27.md): log entries are removed from the
start of a replicated log only, so without eligible relocation one pinned event
keeps later segments; a partition shared by several topics keeps a completion record per
finished event until the start of its log passes it. Up to `v0.6.0` a leader
sent its followers at most 100,000 of those per group, so after a failover a
group could be given events again that it had finished; since `v0.7.0` they
go in pages, all of them.

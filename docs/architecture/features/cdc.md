# CDC Architecture

## Purpose

Change data capture hands every accepted event to external systems (Kafka, a
webhook) without the publish path ever waiting for them.

## Key Files

- [internal/partition/changefeed.go](../../../internal/partition/changefeed.go) — the per-partition change feed: accepted watermark, feed goroutine, stored position.
- [internal/cdc/sink.go](../../../internal/cdc/sink.go) — `Manager.Deliver`, the `Sink` and `BatchSink` interfaces.
- [internal/cdc/kafka_sink.go](../../../internal/cdc/kafka_sink.go)
- [internal/cdc/webhook_sink.go](../../../internal/cdc/webhook_sink.go)
- [cmd/api/main.go](../../../cmd/api/main.go) — registers sinks from the environment and calls `PartitionManager.SetChangeFeed`.

## Main Flow

1. Startup registers sinks from the environment
   (`CRONOS_CDC_KAFKA_BROKERS`, `CRONOS_CDC_KAFKA_TOPIC`,
   `CRONOS_CDC_WEBHOOK_URL`). If there is a sink, or a region is configured for
   cross-region replication, the partition manager is given a change feed
   before any partition exists, so partitions created later have one too.
2. Each partition runs one feed goroutine. It reads the log behind the
   **accepted watermark** and hands the events over in log order, up to 500 at
   a time.
3. `cdc.Manager.Deliver` writes them to every sink, in order, and returns once
   each sink has taken them or failed. A sink that implements `BatchSink`
   (Kafka) gets one call per batch.
4. On success the feed's position moves forward. On failure it stays, and the
   same events are offered again after a pause that grows from 100 ms to 5 s.

## What "accepted" means

`Partition.AcceptedThrough` is the offset up to which every log entry belongs
to an accepted publish. An entry is **not** accepted while:

- its publish is still in flight (`BeginPublish` … `EndPublish`);
- it is held after a publish that failed past the append (no quorum, a failed
  sync), until a retry or catch-up shows it is replicated;
- on a replicated partition, it is not known to be on `min-insync-replicas`
  replicas.

The feed never passes the first such entry, so events leave in log order. The
last rule also keeps positions valid across a failover: what a quorum holds is
in the log of whichever replica leads next.

## Production Decisions

- **Pulled from the log, not pushed from the append path.** The append hook
  this replaces fired before a publish was accepted, on every replica, and only
  for the partitions that existed at startup (in a cluster: one).
- **Leader only.** In a cluster the feed runs on the partition's leader.
  Followers hold the same events and export nothing.
- **A position that survives.** How far the feed has got is written to
  `changefeed.json` in the partition directory at most once a second, and sent
  to followers with the consumer progress. After a restart or a failover the
  feed continues from there. Delivery is at least once: the last second or so
  can be handed over again.
- **Nothing is dropped and nothing queues in memory.** A slow or failing sink
  makes the feed lag; events stay in the log until they are handed over.
- **Sinks do not get the same event once per retry.** The manager remembers,
  per sink and partition, the last offset that sink took.
- **Switching the feed on does not replay history.** A partition without a
  stored position starts at the end of its log.

## Limits

- The feed of a partition is one stream for all sinks. While one sink refuses
  events, the other sinks wait with it for that partition.
- Retention does not wait for the feed. If a segment is removed before the
  feed reached it, the feed logs the gap and continues after it.
- A replica that takes over before it was told the feed's position starts at
  the end of its log.
- Cross-region replication reads the same feed but stays asynchronous and
  best-effort beyond it: its buffer is bounded and drops the oldest events
  when a remote region is unreachable for long.

## Debug Pointers

- "Change feed is waiting at offset N": a sink is refusing events; the log
  line carries the sink's error. "Change feed continues" follows on recovery.
- Feed wiring in startup: [cmd/api/main.go](../../../cmd/api/main.go)
- Watermark and position: [internal/partition/changefeed.go](../../../internal/partition/changefeed.go)
- Sink behavior: [internal/cdc](../../../internal/cdc)

## Related Diagrams

- [Publish flow](api.md#publish-flow)
- [Cross-region replication](replication.md#cross-region-replication)

# Go client guide

The SDK provides publishing, consumer groups, replay, cancellation and
rescheduling, metadata-based leader routing, pooled connections, and
reconnect handling.

Start a development node with the [README quick start](../README.md#quick-start).
Server binaries are also available on the
[releases page](https://github.com/jatin711-debug/cronos_db_golang/releases).
For source builds, see [Contributing](../CONTRIBUTING.md#setup-environment).

## Install and connect

Use Go 1.26.11 or later:

```bash
go get github.com/jatin711-debug/cronos_db_golang@v0.12.1
```

Import `github.com/jatin711-debug/cronos_db_golang/pkg/client`. The
[README publisher example](../README.md#go-client) is a complete program that
dials a local node and schedules a JSON event ten seconds in the future.

`client.DefaultConfig` accepts one or more bootstrap addresses. The client
uses partition metadata to find leaders. Updated servers advertise client
addresses through `--advertise-grpc-addr`; explicit `NodeIDToAddress` mappings
take precedence. Advertised addresses must be reachable from the client.

The examples use plaintext development connections. For production, configure
`Config.Security`: TLS verification, CA/server name, optional client
certificates, and bearer credentials. The subject must have permissions in
the server's authorization policy. See the
[production configuration](PRODUCTION_RELEASE.md#configuration-and-installation)
and the [SDK API reference](https://pkg.go.dev/github.com/jatin711-debug/cronos_db_golang/pkg/client).

## Consume with automatic acknowledgements

Run this program against the development node to consume the `orders` topic.
It accepts both single-event and batched deliveries:

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "os"
    "os/signal"

    client "github.com/jatin711-debug/cronos_db_golang/pkg/client"
    "github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
    defer stop()

    c, err := client.Dial(ctx, client.DefaultConfig("127.0.0.1:9000"))
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    cfg := client.DefaultConsumerConfig("orders", "order-processors")
    err = c.Subscribe(ctx, cfg, func(_ context.Context, d client.Delivery) error {
        events := d.Batch
        if d.Event != nil {
            events = append([]*types.Event{d.Event}, events...)
        }
        for _, event := range events {
            var payload map[string]any
            if err := json.Unmarshal(event.GetPayload(), &payload); err != nil {
                return err
            }
            log.Printf("message_id=%s payload=%v", event.GetMessageId(), payload)
        }
        return nil
    })
    if err != nil && ctx.Err() == nil {
        log.Fatal(err)
    }
}
```

The default `PartitionID=-1` opens a stream for every partition, covering the
topic regardless of producer partition keys. Handlers can run concurrently
across partitions.

Automatic acknowledgement is the default: a successful handler return queues
an acknowledgement; a handler error marks processing as failed. Acknowledgements
are batched asynchronously by default. Delivery is at least once, so handlers
should tolerate duplicates and partial processing of a batch.

For manual control, use `AckModeManual` and `Delivery.AckSuccess` or
`Delivery.AckFailure` to queue acknowledgements explicitly. With
`CommitStrategySync` these calls return the server's answer: `nil` once it has
recorded the acknowledgement, an error when it refused it, the stream ended
first or the context expired. With the default asynchronous strategy they
return once the acknowledgement is queued; a refusal then reaches only an
`ErrorHook` on `Config.Hooks` (operation `consumer.ack`, kind
`ErrorKindValidation`).

A delivery is acknowledged once; a second call returns the first call's result.
The server accepts a successful acknowledgement only for the offset after the
highest offset it delivered (`AckRequest.next_offset`), and refuses any other
value. `AckSuccess` and the automatic mode send it for you. If you call
`Delivery.Ack(ctx, true, nextOffset, nil)` yourself, pass the highest offset of
`Event` and `Batch`, plus one. A refused delivery stays in flight on the
server and is delivered again when it times out.

A subscription the server refuses, such as a consumer group name or
`SubscriptionID` longer than 255 bytes, or a new group on a partition that
already holds `--max-consumer-groups` groups, is retried like a lost connection:
the refusal reaches `OnReconnect`, and `Subscribe` does not return unless
`MaxReconnectAttempts` is set.

See the SDK reference for checkpoint stores, retries, and flow-control options.

## Run the end-to-end demo

With the development node running, run from a repository checkout:

```bash
go run ./examples/pubsub_demo
```

The demo subscribes, publishes a JSON event scheduled ten seconds ahead, and
prints its delivery metadata and payload. Use `-addr=host:port` to select a
different node. `make demo` runs the same example.

Source: [examples/pubsub_demo/main.go](../examples/pubsub_demo/main.go).

## Publish and handle refusals

```go
producer, err := c.NewProducer(client.DefaultProducerConfig())
if err != nil {
    log.Fatal(err)
}
defer producer.Close()

_, err = producer.Send(ctx, client.Message{
    Topic:      "orders",
    MessageID:  "order-1003",
    Value:      map[string]string{"order_id": "ord-1003"},
    ScheduleAt: time.Now().Add(time.Hour),
})
var ce *client.Error
if errors.As(err, &ce) {
    log.Printf("publish failed (%s): %v", ce.Kind, err)
}
```

`Send` returns a `*client.Error` whose `Kind` tells you what to do. It retries
unreachable nodes and leader changes by itself (`RetryPolicy`). The server also
refuses publishes that retrying at once does not cure:

| What happens | `Send` returns | What to do |
|--------------|----------------|------------|
| The topic has a JSON schema and the payload does not match it: a missing property the schema lists as `required`, or a wrong type (the message starts with `schema validation failed`). Only `type`, `properties`, `required` and `items` are enforced; since v0.11.0 `required` is | `ErrorKindValidation`, not retried | Fix the payload. Producers that relied on missing required properties being accepted now see this |
| The message ID was already accepted on this topic within `--dedup-ttl` (`duplicate message_id`) | `ErrorKindValidation`, not retried | The event is already stored; do not publish it again. If the message says an earlier publish with this ID is still in progress, retry later |
| The payload is over 4 MiB, the message ID over 128 bytes, or the topic name over 255 bytes | `ErrorKindValidation`, not retried | Shrink it. `MaxPayloadBytes` and `MaxSendMsgSize` default to 16 MiB, but the server's limit is lower |
| The node is at capacity (`partition N is at capacity`: its memory, ingest-rate or queue limits; or its disk limit), or the caller's publish rate or quota is used up (`tenant quota exceeded`: the `--tenant-limits-file` limit of `max_events_per_second` per JWT subject, enforced per node) | `ErrorKindOverloaded`, at once and not retried | The event was not stored. Send less, or retry later with backoff; a retry with the same message ID is safe |
| The write or replication failed after the event reached the log | `ErrorKindInternal` with the server's reason | Retry with the same message ID and payload: the server finishes the earlier publish instead of appending the event again |
| Retries ran out on a leadership/routing refusal | `ErrorKindLeaderChange` | Refresh routing and retry with the same message ID |
| No node answered and retries ran out | `ErrorKindUnavailable` | Retry with the same message ID |
| Authentication or permission failure | `ErrorKindAuth`; the gRPC status is in the wrapped error | Fix the credentials or the authorization policy |

These classifications require the updated SDK. Server response failures now carry
`PublishResponse.error_code`; when talking to older servers (including v0.11.0),
unclassified response failures retain the legacy `ErrorKindValidation` fallback.
An internal failure does not prove that an event was never stored.

`SendBatch` does not return these refusals as errors. It returns a
`BatchSendResult`: `PublishedCount`, `DuplicateCount` and `ErrorCount`, with the
first reason for a failed event in `Error`. Duplicates are an answer, not a
failure, so the SDK does not retry a batch that a node answered. `Results` has an
entry for each message of a group that a node answered, accepted or not, and an
entry's `Offset` is set only when the whole group was accepted (or, with
`ReturnReferences`, from the event's reference), so count with the totals.
`SendAsync` reports the same errors through its future and callback.

A message without an ID gets one derived from its topic, partition key, schedule
time and payload, so publishing the same message twice gives it the same ID and
the second is refused as a duplicate. Set your own IDs when events are distinct
by identity rather than by content.

## Cancel or reschedule an event

A node started with `--event-lifecycle` lets you cancel an event or move it to
a new time, as long as it is not yet due. See the
[lifecycle contract](RETENTION_AND_EVENT_LIFECYCLE.md) for the server setup
and its limits. With authentication enabled, the caller needs publish
permission on the event's topic.

The server finds an event by its reference: topic, partition, offset, term and
message ID, not by message ID alone. Keep the reference from the publish
result with your own record of the event:

```go
sent, err := producer.Send(ctx, client.Message{
    Topic:      "orders",
    MessageID:  "order-1002-reminder",
    Value:      map[string]string{"order_id": "ord-1002"},
    ScheduleAt: time.Now().Add(24 * time.Hour),
})
if err != nil {
    log.Fatal(err)
}
ref := sent.Reference() // store it, for example as JSON next to the order
```

Every change names the revision it replaces, so that two writers cannot
silently overwrite each other's changes, and a request ID, so that it can be
retried safely:

```go
state, err := c.GetEventState(ctx, ref)
if err != nil {
    log.Fatal(err)
}
state, err = c.RescheduleEvent(ctx, ref, client.EventChange{
    RequestID:        "order-1002-move-1",
    ExpectedRevision: state.Revision,
}, time.Now().Add(48*time.Hour))
if err != nil {
    log.Fatal(err)
}
_, err = c.CancelEvent(ctx, ref, client.EventChange{
    RequestID:        "order-1002-cancel",
    ExpectedRevision: state.Revision,
})
```

For an event never changed, `ref.InitialRevision()` is its revision and
`GetEventState` can be skipped. The calls go to the partition's leader and
retry by themselves while the outcome is unknown. When they give up, the
error's `Kind` says what to do:

| Kind | Meaning | What to do |
|------|---------|------------|
| `ErrorKindUnavailable` | The outcome is unknown: the leader may have changed, or the change has not reached enough replicas yet | Retry with the same `EventChange` and time. The server applies it at most once |
| `ErrorKindTimeout` | The deadline expired or the context was canceled; a mutation may already have applied | Use a fresh context and retry with the same `EventChange` and time |
| `ErrorKindConflict` | The event changed after `ExpectedRevision` | Read the state again and decide whether your change still applies |
| `ErrorKindPrecondition` | The event is canceled or already due, or the server runs without `--event-lifecycle` | Do not retry |
| `ErrorKindNotFound` | The reference does not match an event the server still keeps | Do not retry. The event was removed by retention, or the reference is wrong |
| `ErrorKindValidation` | A missing request ID, a past time or an incomplete reference | Fix the request |
| `ErrorKindOverloaded` | The node refused the change because it is at its disk limit | The change was not applied. Retry later with the same `EventChange` and time |
| `ErrorKindAuth` | Authentication or permission failure: the caller needs publish permission on the event's topic | Fix the credentials or the authorization policy |

Use a new request ID for every new change, and the same one only to retry it.
Keep the requested time too: recomputing `time.Now().Add(...)` changes the
request. A retry can resolve even after that time has passed, while the
server still retains the request's result. A later change can supersede that
result; read the current state if the retry reports a revision conflict.
`SendBatch` results carry complete references only when
`ProducerConfig.ReturnReferences` is set.

## SDK settings

Start from `client.DefaultConfig`, `client.DefaultProducerConfig` and
`client.DefaultConsumerConfig` and change only what you need. Many zero
values fall back to defaults, but booleans and values such as partition `0`
and offset `0` have their own meaning; use the constructors first. The
[SDK API reference](https://pkg.go.dev/github.com/jatin711-debug/cronos_db_golang/pkg/client)
documents every field.

### Connection (`client.Config`)

| Field | Default | What it is for |
|-------|---------|----------------|
| `BootstrapAddresses` | required | One or more nodes' gRPC addresses. The client learns the rest of the cluster from them |
| `NodeIDToAddress` | empty | Maps node IDs to addresses when nodes do not advertise one (`--advertise-grpc-addr`) |
| `PartitionCount` | `0` | Partition count to hash keys with while metadata is not yet loaded. `0` waits for the server's count |
| `ConnectionsPerNode` | `2` | gRPC connections kept per node. Raise it for very high request rates |
| `DialTimeout` | `5s` | How long connecting to a node may take |
| `RequestTimeout` | `10s` | Deadline of a call whose context has none |
| `ResolveDNS` | `true` (`DefaultConfig`) | Resolve bootstrap host names |
| `KeepaliveTime`, `KeepaliveTimeout` | `10s`, `5s` | How fast a node that stops answering is noticed: after about the sum. The server refuses pings more often than every 5 seconds |
| `MaxRecvMsgSize`, `MaxSendMsgSize` | 16 MiB | Largest message the client receives or sends. Must cover your largest event |
| `ReadBufferSize`, `WriteBufferSize` | 4 MiB | gRPC transport buffers |
| `Metadata.TTL`, `Metadata.RefreshInterval` | `15s`, `5s` | How long partition and leader information is trusted, and how often it is refreshed. The interval must not exceed the TTL |
| `Security` | plaintext | `Insecure: false` plus `CACertFile`/`ServerName` for TLS; `ClientCertFile` and `ClientKeyFile` for mutual TLS; `BearerToken` or `PerRPCCredentials` when the server runs with `--auth-enabled`. `InsecureSkipVerify` is for development only |
| `Hooks` | none | Callbacks for metrics: metadata refreshes (`OnMetadataRefresh`) and every request (`OnRequest`). A value that also implements `ErrorHook` receives errors with their `ErrorKind`, including acknowledgements the server refused |
| `CircuitBreaker` | 5 failures, 10s, 2 recovery successes | Shared by producers of this client, per node. Pauses publishes after consecutive failures; probes after the timeout and closes after `SuccessThreshold` successes. `FailureThreshold: 0` disables it. It does not gate lifecycle or consumer calls |

### Publishing (`client.ProducerConfig`)

| Field | Default | What it is for |
|-------|---------|----------------|
| `AutoMessageID` | `true` | Derive a message ID (`auto-` and a hash of topic, partition key, schedule time and payload) when a message has none, so identical messages get the same ID. Set your own IDs when a retry after a crash must not publish twice: the server does not store a message ID again on the same topic within `--dedup-ttl`. The same ID on two topics names two events |
| `DefaultAllowDuplicate` | `false` | Publish even if the message ID was already stored. For tests |
| `Codec` | JSON | Encodes `Message.Value` when `Payload` is empty |
| `MaxPayloadBytes` | `MaxSendMsgSize` | Refuse larger payloads before sending them. The server refuses payloads over 4 MiB whatever this is |
| `RetryPolicy` | 4 attempts, 50ms to 2s backoff | Retries after unreachable nodes and leader changes. A refusal by the server (see [Publish and handle refusals](#publish-and-handle-refusals)) is not retried |
| `MaxInFlight` | `1024` | Concurrent publishes; further calls wait |
| `AsyncQueueSize`, `AsyncWorkers` | `4096`, `2` | Queue and workers behind `SendAsync` |
| `BlockOnQueueFull` | `true` | `SendAsync` waits when the queue is full; `false` returns an error instead |
| `MaxQueuedBytes` | 256 MiB | Payload bytes the async queue may hold |
| `CircuitBreaker` | reserved | Currently unused; configure `client.Config.CircuitBreaker` instead |
| `Hedging` | disabled | Also send a slow publish to the next node after `Delay`, to cut tail latency at the cost of extra requests |
| `ReturnReferences` | `false` | Make `SendBatch` results carry complete event references, for cancel and reschedule |
| `Partitioner` | server hash | How a partition key picks a partition. Keep the default unless you also route by partition ID |

### Consuming (`client.ConsumerConfig`)

| Field | Default | What it is for |
|-------|---------|----------------|
| `Topic`, `ConsumerGroup` | required | What to consume, and the group whose progress (acknowledged offsets) the server tracks |
| `PartitionID` | `-1` | `-1` consumes every partition; a number, `0` included, consumes only that one |
| `StartOffset` | `-1` | Where redelivery of retained, unacknowledged events starts. `-1` continues from the group's progress; a higher offset skips events before it |
| `MaxBufferSize` | `1024` | How many deliveries may be unacknowledged at once (at most 50000). Higher means more throughput and more events redelivered after a crash |
| `WorkerConcurrency` | `1` | Handler goroutines per partition stream |
| `AckMode`, `AutoAck` | auto, `true` | `AckModeAuto` acknowledges when the handler returns; `AckModeManual` needs `Delivery.AckSuccess` or `AckFailure`. `AutoAck` is the older compatibility field; prefer an explicit `AckMode` |
| `CommitStrategy` | async | Async batches acknowledgements (`AckBatchSize` 64, every `AckFlushInterval` 50ms); sync sends each one at once and returns the server's answer from `Ack` |
| `AckQueueSize` | `4096` | Acknowledgements waiting to be sent |
| `CheckpointStore`, `ResumeCheckpoint` | none, `true` | Optional local store of progress, used as `StartOffset` for a single pinned partition. It holds the group's completion floor as the server reports it: every offset below it is complete. Events are delivered in schedule order, not offset order, so the offset of the last acknowledged delivery is not saved |
| `MaxPayloadBytes` | `MaxRecvMsgSize` | Larger deliveries skip the handler and, in auto mode, count as failed |
| `PreferredCodec` | JSON | Codec for `Delivery.Decode` |
| `NodeAddress` | empty | Try this node first |
| `ReconnectBackoff`, `MaxReconnectBackoff`, `ReconnectJitter` | `1s`, `15s`, `0.2` | Delay between reconnect attempts |
| `MaxReconnectAttempts` | `0` | Give up after this many failed reconnects in a row (`Subscribe` then returns `ErrorKindUnavailable`); `0` never gives up, and a subscription the server refuses is retried the same way |
| `HeartbeatInterval` and `On*` callbacks | `3s` | Notifications on connect, assignment, reconnect and disconnect |

## Replay and routing

- `Client.Replay`, `ReplayByOffsetRange`, and `ReplayByTimeRange` read retained
  events. `ReplayToLive` combines historical replay with live consumption; its
  handler receives replayed events as deliveries that cannot be acknowledged.
- `Client.DetectCapabilities` and `RequireCapabilities` check server support.
- `Client.RouteForPartition` exposes routing for diagnostics.

A replay reads one partition: `PartitionID` is required, and `0` is partition 0.
It returns only events of the requested topic, and needs subscribe permission on
the topic when authentication is enabled. It does not read or change any
consumer group's progress.

- `ReplayByOffsetRange` returns the events at offsets `[start, end]`. A
  `ReplayRequest` can also bound an offset replay with `Count` (the server uses
  1000 when it is not positive). When `EndOffset` is at or above a non-negative
  `StartOffset` the range wins and `Count` is ignored; a zero `EndOffset` counts
  as unset while `Count` is positive. A request with every offset and `Count` at
  zero asks for one event, at offset 0. Before this was fixed (v0.12.0), a request that set only `Count` asked for one event.
- `ReplayByTimeRange`, or `Replay` with `StartTime` or `EndTime`, selects events
  by their schedule time, not by when they were published. `Count` does not
  apply, and without an `EndTime` the server uses the current time.
- `ReplayRequest.Speed` of `0` or `1` sends events without a pause; any other
  value pauses 10 ms divided by `Speed` after each event, and the server refuses
  a negative value or one below `0.001`.
- `ReplayEvent.ReplayOffset` is the event's position in the replay, not its log
  offset; read `Event.Offset` for that.
- A handler can return `client.ErrStopReplay` to stop without an error.

Replay availability depends on retention and the supported read path; it is
not an archive of events already removed from the log. See the
[replay architecture](architecture/features/replay.md).

## Inspect the gRPC API

The wire contracts are in [events.proto](../proto/events.proto) and
[admin.proto](../proto/admin.proto); the
[API architecture](architecture/features/api.md) distinguishes public and
internal listeners.

For a development node, a Bash shell with `grpcurl` can publish an event:

```bash
grpcurl -plaintext \
  -d '{"event":{"messageId":"example-1","topic":"orders","scheduleTs":'"$(($(date -u +%s) * 1000))"',"payload":"eyJvcmRlcl9pZCI6Im9yZC0xMDAyIn0="}}' \
  localhost:9000 cronos_db.EventService.Publish
```

Use a fresh message ID for each new event. Use the SDK consumer above when
you need acknowledgements and reconnect handling.

# Go client guide

The SDK provides publishing, consumer groups, replay, cancellation and
rescheduling, metadata-based leader routing, pooled connections, and
reconnect handling.

Start a development node with the [README quick start](../README.md#quick-start).
Server binaries are also available on the
[releases page](https://github.com/jatin711-debug/cronos_db_golang/releases).
For source builds, see [Contributing](../CONTRIBUTING.md#setup-environment).

## Install and connect

Use Go 1.26.10 or later:

```bash
go get github.com/jatin711-debug/cronos_db_golang@v0.9.0
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
`Delivery.AckFailure` to queue acknowledgements explicitly.
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
| `Hooks` | none | Callbacks for metrics: metadata refreshes, every request, and errors with their `ErrorKind` |
| `CircuitBreaker` | 5 failures, 10s, 2 recovery successes | Shared by producers of this client, per node. Pauses publishes after consecutive failures; probes after the timeout and closes after `SuccessThreshold` successes. `FailureThreshold: 0` disables it. It does not gate lifecycle or consumer calls |

### Publishing (`client.ProducerConfig`)

| Field | Default | What it is for |
|-------|---------|----------------|
| `AutoMessageID` | `true` | Generate a message ID when a message has none. Set your own IDs when a retry after a crash must not publish twice: the server does not store a message ID again within `--dedup-ttl` |
| `DefaultAllowDuplicate` | `false` | Publish even if the message ID was already stored. For tests |
| `Codec` | JSON | Encodes `Message.Value` when `Payload` is empty |
| `MaxPayloadBytes` | `MaxSendMsgSize` | Refuse larger payloads before sending them |
| `RetryPolicy` | 4 attempts, 50ms to 2s backoff | Retries after transient failures and leader changes |
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
| `PartitionID` | `-1` | `-1` consumes every partition; a number consumes only that one |
| `StartOffset` | `-1` | Where redelivery of retained, unacknowledged events starts. `-1` continues from the group's progress; a higher offset skips events before it |
| `MaxBufferSize` | `1024` | How many deliveries may be unacknowledged at once (at most 50000). Higher means more throughput and more events redelivered after a crash |
| `WorkerConcurrency` | `1` | Handler goroutines per partition stream |
| `AckMode`, `AutoAck` | auto, `true` | `AckModeAuto` acknowledges when the handler returns; `AckModeManual` needs `Delivery.AckSuccess` or `AckFailure`. `AutoAck` is the older compatibility field; prefer an explicit `AckMode` |
| `CommitStrategy` | async | Async batches acknowledgements (`AckBatchSize` 64, every `AckFlushInterval` 50ms); sync waits for each one |
| `AckQueueSize` | `4096` | Acknowledgements waiting to be sent |
| `CheckpointStore`, `ResumeCheckpoint` | none, `true` | Optional local store of progress, used as `StartOffset` for a single pinned partition |
| `MaxPayloadBytes` | `MaxRecvMsgSize` | Larger deliveries skip the handler and, in auto mode, count as failed |
| `PreferredCodec` | JSON | Codec for `Delivery.Decode` |
| `NodeAddress` | empty | Try this node first |
| `ReconnectBackoff`, `MaxReconnectBackoff`, `ReconnectJitter` | `1s`, `15s`, `0.2` | Delay between reconnect attempts |
| `MaxReconnectAttempts` | `0` | Give up after this many failed reconnects in a row; `0` never gives up |
| `HeartbeatInterval` and `On*` callbacks | `3s` | Notifications on connect, assignment, reconnect and disconnect |

## Replay and routing

- `Client.Replay`, `ReplayByOffsetRange`, and `ReplayByTimeRange` read retained
  events. `ReplayToLive` combines historical replay with live consumption.
- `Client.DetectCapabilities` and `RequireCapabilities` check server support.
- `Client.RouteForPartition` exposes routing for diagnostics.

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

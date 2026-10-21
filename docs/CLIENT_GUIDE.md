# Go client guide

The SDK provides publishing, consumer groups, replay, metadata-based leader
routing, pooled connections, and reconnect handling.

Start a development node with the [README quick start](../README.md#quick-start).
Server binaries are also available on the
[releases page](https://github.com/jatin711-debug/cronos_db_golang/releases).
For source builds, see [Contributing](../CONTRIBUTING.md#setup-environment).

## Install and connect

Use Go 1.26.10 or later:

```bash
go get github.com/jatin711-debug/cronos_db_golang@v0.8.1
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

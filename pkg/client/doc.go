// Package client provides a production-grade Go SDK for CronosDB.
//
// The SDK is metadata-aware and optimized for durable scheduled-event workloads:
//   - bootstrap with multiple node addresses and pooled gRPC connections
//   - partition-leader routing via metadata API with TTL cache and refresh
//   - producer APIs for single, batch, and async publishing
//   - consumer runtime with stream reconnect, ack control, and checkpoint resume
//   - replay/seek APIs and replay-to-live transition
//   - GetEventState, CancelEvent and RescheduleEvent for servers that run
//     with --event-lifecycle, using SendResult.Reference
//   - capability detection for graceful compatibility handling
//
// # Core workflow
//
//  1. Dial the cluster with DefaultConfig + security settings.
//  2. Create a Producer with Client.NewProducer and publish Message values
//     (Payload bytes or Value+Codec).
//  3. Start a consumer with Client.Subscribe(ctx, ConsumerConfig, handler).
//  4. Ack automatically (default) or manually for custom commit control.
//  5. Use Replay/Seek APIs for backfill and replay-to-live scenarios.
//  6. To cancel or move an event before it is due, keep the EventReference
//     from SendResult.Reference and call CancelEvent or RescheduleEvent with
//     a request ID and the revision from GetEventState.
//
// # Routing and partitioning
//
// For deterministic routing, use Message.PartitionKey.
// The server hashes partition_key to select the partition. If no partition_key is
// provided, message_id is used. A consumer with ConsumerConfig.PartitionID -1
// (the default) opens one stream for every partition of the cluster; a
// non-negative PartitionID, including 0, consumes only that partition.
//
// # Security and operability
//
// Config supports plaintext (development) and TLS/mTLS (production), per-RPC
// credentials, request deadlines, keepalive tuning, and hook-based observability.
//
// # Errors and refusals
//
// Errors returned by the SDK are *Error values whose Kind says what to do (see
// ErrorKind). The server can refuse a publish for reasons that retrying at once
// does not cure, and the producer reports them as follows:
//   - A refusal the server reports in its answer, such as a schema validation
//     failure (a payload missing a property that the topic's JSON schema
//     requires, for example), a duplicate message ID, or an oversized
//     payload (the server's limit is 4 MiB), comes back from Send as
//     ErrorKindValidation and is not retried.
//   - A classified server write or replication failure returns ErrorKindInternal.
//     The event may already be in the log; retry with the same ID and payload.
//     Older servers without error_code retain the legacy validation classification.
//   - Exhausted leadership retries return ErrorKindLeaderChange; publish
//     authentication and authorization failures return ErrorKindAuth.
//   - A node that is at capacity, or whose rate limit or quota for the caller
//     is used up (gRPC ResourceExhausted), comes back from Send as
//     ErrorKindOverloaded, immediately and without retries. The event was not
//     stored; send less, or retry later.
//   - SendBatch does not return an error for these; it counts them in
//     BatchSendResult.ErrorCount and gives the first reason in
//     BatchSendResult.Error.
//
// A consumer's acknowledgement of a successful delivery must carry the offset
// after the highest offset delivered (AckRequest.next_offset); Delivery.AckSuccess
// and the automatic mode do so, and the server refuses any other value.
// With a CheckpointStore, only the completion floor the server reports (every
// offset below it is complete) is saved.
//
// # Important defaults
//
// DefaultConfig and DefaultProducerConfig provide safe throughput-oriented
// defaults (connection pooling, metadata refresh, retries, and auto message IDs).
// Prefer defaults first, then tune for workload-specific latency/throughput goals.
package client

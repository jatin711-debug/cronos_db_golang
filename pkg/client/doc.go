// Package client provides a production-grade Go SDK for CronosDB.
//
// The SDK is metadata-aware and optimized for durable scheduled-event workloads:
//   - bootstrap with multiple node addresses and pooled gRPC connections
//   - partition-leader routing via metadata API with TTL cache and refresh
//   - producer APIs for single, batch, and async publishing
//   - consumer runtime with stream reconnect, ack control, and checkpoint resume
//   - replay/seek APIs and replay-to-live transition
//   - capability detection for graceful compatibility handling
//
// # Core workflow
//
//	1. Dial the cluster with DefaultConfig + security settings.
//	2. Create a Producer and publish Message values (Payload bytes or Value+Codec).
//	3. Start a Consumer with Subscribe(topic, group, handler).
//	4. Ack automatically (default) or manually for custom commit control.
//	5. Use Replay/Seek APIs for backfill and replay-to-live scenarios.
//
// # Routing and partitioning
//
// For deterministic routing, use Message.PartitionKey.
// The server hashes partition_key to select the partition. If no partition_key is
// provided, message_id is used. Consumer partition assignment can be explicit
// (ConsumerConfig.PartitionID) or server-assigned.
//
// # Security and operability
//
// Config supports plaintext (development) and TLS/mTLS (production), per-RPC
// credentials, request deadlines, keepalive tuning, and hook-based observability.
//
// # Important defaults
//
// DefaultConfig and DefaultProducerConfig provide safe throughput-oriented
// defaults (connection pooling, metadata refresh, retries, and auto message IDs).
// Prefer defaults first, then tune for workload-specific latency/throughput goals.
package client

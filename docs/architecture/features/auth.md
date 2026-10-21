# Auth Architecture

## Purpose

The auth module handles authentication and authorization policy enforcement before business handlers execute.

## Key Files

- [internal/auth/auth.go](../../../internal/auth/auth.go)
- [internal/api/grpc_server.go](../../../internal/api/grpc_server.go)
- [cmd/api/main.go](../../../cmd/api/main.go)

## Main Flow

1. Startup builds auth config from flags and environment.
2. Unary and stream auth interceptors validate credentials and claims on every public RPC.
3. Requests without a valid token never reach a handler. The interceptor only authenticates.
4. Each handler checks the policy for its own RPC (see [authorization by RPC](#authorization-by-rpc)).

## Production Decisions

- Authentication is off by default (`--auth-enabled=false`). Production mode (no `--dev`) refuses to start without `--auth-enabled`, a JWT secret or public key, and `--auth-policy-file`. `--dev` only lifts these production requirements; it does not switch auth off. With `--dev --auth-enabled`, every request is authenticated and authorized as in production.
- Supports HMAC (`--auth-jwt-secret`) and Ed25519, RSA or ECDSA public-key (`--auth-jwt-public-key`) JWT verification, and an RBAC policy loaded from a JSON file.
- Fails closed: with `--auth-enabled`, the server exits at start if the policy file is missing or cannot be loaded, or if the public key cannot be loaded.
- Runs early in interceptor chain to minimize wasted work for rejected requests.
- `GetEventState`, `CancelEvent` and `RescheduleEvent` also require topic
  **publish** permission in the lifecycle handler. A state query is a leader
  operation, not a bypass of topic authorization. See [API behavior](api.md).

## Authorization by RPC

The policy file is a JSON object keyed by JWT subject, for example
`{"ops":{"admin":true},"app":{"topics":{"orders":{"publish":true,"subscribe":true}}}}`.
With `--auth-enabled`, the handlers require:

| RPCs | Requirement |
|------|-------------|
| Every `AdminService` RPC, and the dashboard's `/api/admin/*` | Global `"admin": true` (`CheckAdminPermission`) |
| `PartitionService.Compact`, `RunRetention`, `SplitPartition` | Global `"admin": true` |
| `PartitionService.GetPartition`, `ListPartitions`, `GetWALStatus`, `GetSchedulerStatus` | A valid token only |
| `ConsumerGroupService.CreateConsumerGroup`, `GetConsumerGroup`, `ListConsumerGroups`, `RebalanceConsumerGroup` | Global `"admin": true` |
| `EventService.Publish`, `PublishBatch`, `GetEventState`, `CancelEvent`, `RescheduleEvent` | Topic `publish` |
| `EventService.Subscribe`, `Ack`, `Replay` | Topic `subscribe` |

The global `admin` flag grants no topic permission. Topic names match
exactly. The per-topic `admin` flag is parsed but no RPC checks it. A subject
missing from the policy gets `PermissionDenied` wherever a policy check
applies. The internal listener (`ReplicationService`, `RaftService`, and the
experimental `CrossRegionService`) reads no JWT; `--replication-tls-enabled`
protects it with mutual TLS. See the
[cronos-admin permissions](../../ADMINISTRATION.md#permissions).

## Debug Pointers

- Request authorization path: [internal/auth/auth.go](../../../internal/auth/auth.go)
- Interceptor ordering: [internal/api/grpc_server.go](../../../internal/api/grpc_server.go)

## Related Diagrams

- [System overview](../README.md#system-overview)
- [Observability feedback loop](slo.md#observability-feedback-loop)

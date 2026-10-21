# Auth Architecture

## Purpose

The auth module handles authentication and authorization policy enforcement before business handlers execute.

## Key Files

- [internal/auth/auth.go](../../../internal/auth/auth.go)
- [internal/api/grpc_server.go](../../../internal/api/grpc_server.go)
- [cmd/api/main.go](../../../cmd/api/main.go)

## Main Flow

1. Startup builds auth config from flags and environment.
2. Unary and stream auth interceptors validate credentials and claims.
3. Policy engine decides whether request is allowed.
4. Request proceeds to handlers only when auth passes.

## Production Decisions

- Authentication is off by default (`--auth-enabled=false`). Production mode (no `--dev`) refuses to start without `--auth-enabled`, a JWT secret or public key, and `--auth-policy-file`. `--dev` only lifts these production requirements; it does not switch auth off. With `--dev --auth-enabled`, every request is authenticated and authorized as in production.
- Supports HMAC (`--auth-jwt-secret`) and Ed25519/RSA public-key (`--auth-jwt-public-key`) JWT verification, and an RBAC policy loaded from a JSON file.
- Fails closed: with `--auth-enabled`, the server exits at start if the policy file is missing or cannot be loaded, or if the public key cannot be loaded.
- Runs early in interceptor chain to minimize wasted work for rejected requests.
- `GetEventState`, `CancelEvent` and `RescheduleEvent` also require topic
  **publish** permission in the lifecycle handler. A state query is a leader
  operation, not a bypass of topic authorization. See [API behavior](api.md).

## Debug Pointers

- Request authorization path: [internal/auth/auth.go](../../../internal/auth/auth.go)
- Interceptor ordering: [internal/api/grpc_server.go](../../../internal/api/grpc_server.go)

## Related Diagrams

- [System overview](../README.md#system-overview)
- [Observability feedback loop](slo.md#observability-feedback-loop)

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

- Authentication is required in production; `--dev` bypasses auth and must not be used in production.
- Supports multiple JWT verification modes and external policy loading.
- Fails closed when explicitly configured with strict policy inputs.
- Runs early in interceptor chain to minimize wasted work for rejected requests.

## Debug Pointers

- Request authorization path: [internal/auth/auth.go](../../../internal/auth/auth.go)
- Interceptor ordering: [internal/api/grpc_server.go](../../../internal/api/grpc_server.go)

## Related Diagrams

- [system_overview.mmd](../../mermaid/system_overview.mmd)
- [observability_feedback_loop.mmd](../../mermaid/observability_feedback_loop.mmd)

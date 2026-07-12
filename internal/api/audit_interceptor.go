package api

import (
	"context"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/audit"
	"google.golang.org/grpc"
)

// AuditUnaryInterceptor logs all unary RPC calls to the audit logger.
//
// When enabled is false the interceptor is a pass-through. This is used in
// --dev mode (auth disabled), where every subject is "anonymous" and audit
// logging adds no security value while consuming hot-path throughput.
func AuditUnaryInterceptor(logger *audit.Logger, enabled bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !enabled || logger == nil {
			return handler(ctx, req)
		}
		start := time.Now()
		resp, err := handler(ctx, req)
		latency := time.Since(start)
		outcome := "success"
		if err != nil {
			outcome = "failure"
		}
		logger.LogGRPC(ctx, info.FullMethod, "", outcome, latency.String())
		return resp, err
	}
}

// AuditStreamInterceptor logs stream RPC calls to the audit logger. See
// AuditUnaryInterceptor for the enabled flag semantics.
func AuditStreamInterceptor(logger *audit.Logger, enabled bool) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !enabled || logger == nil {
			return handler(srv, ss)
		}
		start := time.Now()
		err := handler(srv, ss)
		latency := time.Since(start)
		outcome := "success"
		if err != nil {
			outcome = "failure"
		}
		logger.LogGRPC(ss.Context(), info.FullMethod, "", outcome, latency.String())
		return err
	}
}

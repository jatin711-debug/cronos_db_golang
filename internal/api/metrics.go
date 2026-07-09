package api

import (
	"context"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// MetricsInterceptor creates a gRPC unary interceptor for prometheus metrics.
func MetricsInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()

		resp, err := handler(ctx, req)

		duration := time.Since(start).Seconds()
		statusCode := "OK"
		if err != nil {
			if st, ok := status.FromError(err); ok {
				statusCode = st.Code().String()
			} else {
				statusCode = "UNKNOWN"
			}
		}

		method := metrics.SanitizeMetricLabel(info.FullMethod)
		metrics.IncGRPCRequest(method, statusCode)
		metrics.ObserveGRPCRequestDuration(method, duration)

		return resp, err
	}
}

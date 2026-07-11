package api

import (
	"net"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// SLOStreamInterceptor records stream request latency and error status for SLO tracking.
func SLOStreamInterceptor(recorder SLORecorder) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if recorder == nil {
			return handler(srv, ss)
		}
		start := time.Now()
		err := handler(srv, ss)
		recorder.Record(time.Since(start), err != nil)
		return err
	}
}

// VersionStreamInterceptor adds wire version compatibility to gRPC stream metadata.
func VersionStreamInterceptor(vg *VersionGate) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if vg == nil {
			return handler(srv, ss)
		}
		md, ok := metadata.FromIncomingContext(ss.Context())
		if ok {
			vals := md.Get("x-cronos-version")
			if len(vals) > 0 {
				var clientVer int32
				if scanVersion(vals[0], &clientVer) == nil {
					if !vg.IsCompatible(clientVer) {

						return status.Errorf(codes.FailedPrecondition,
							"client version %d incompatible with server (min=%d max=%d)",
							clientVer, vg.minClientVersion.Load(), vg.maxClientVersion.Load())
					}
				}
			}
		}
		return handler(srv, ss)
	}
}

// MetricsStreamInterceptor creates a gRPC stream server interceptor for prometheus metrics.
func MetricsStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()

		err := handler(srv, ss)

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

		return err
	}
}

// RateLimitStreamInterceptor creates a gRPC stream server interceptor for per-IP rate limiting.
func RateLimitStreamInterceptor(requestsPerSecond float64, burstCapacity float64) grpc.StreamServerInterceptor {
	rl := newRateLimiter(requestsPerSecond, burstCapacity)
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		p, ok := peer.FromContext(ss.Context())
		if !ok {
			return handler(srv, ss)
		}

		host, _, err := net.SplitHostPort(p.Addr.String())
		if err != nil {
			host = p.Addr.String()
		}

		if !rl.allow(host) {
			return status.Errorf(codes.ResourceExhausted, "rate limit exceeded for IP %s", host)
		}

		return handler(srv, ss)
	}
}

// scanVersion parses a decimal version string into the target int32.
// It mirrors the Sscanf logic used by the unary interceptor so both paths behave identically.
func scanVersion(s string, v *int32) error {
	var n int32
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			if i == 0 {
				return status.Errorf(codes.InvalidArgument, "invalid version string %q", s)
			}
			break
		}
		n = n*10 + int32(c-'0')
	}
	*v = n
	return nil
}

package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// PropagationHeader is a gRPC metadata key historically used for trace propagation.
// W3C TraceContext headers are preferred via the OTel text-map propagator.
const PropagationHeader = "x-trace-id"

// GRPCServerInterceptor returns a unary server interceptor that extracts
// incoming trace context, starts a server span for the RPC method, and records errors.
func GRPCServerInterceptor() grpc.UnaryServerInterceptor {
	propagator := otel.GetTextMapPropagator()

	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Check if tracing is initialized
		if Tracer == nil {
			return handler(ctx, req)
		}

		// Extract trace context from gRPC metadata
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			md = metadata.New(nil)
		}

		// Extract trace context using TextMapCarrier
		ctx = propagator.Extract(ctx, &grpcMetadataCarrier{md: md})

		// Start a new span
		spanName := info.FullMethod
		ctx, span := Tracer.Start(ctx, spanName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("rpc.system", "grpc"),
				attribute.String("rpc.service", info.FullMethod),
			),
		)
		defer span.End()

		// Add trace ID to response metadata if available
		traceID := span.SpanContext().TraceID().String()
		if traceID != "" {
			span.SetAttributes(attribute.String("trace.id", traceID))
		}

		// Call the handler
		resp, err := handler(ctx, req)

		// Record error if any
		if err != nil {
			span.RecordError(err)
		}

		return resp, err
	}
}

// GRPCStreamServerInterceptor returns a stream server interceptor that starts
// a server span and injects the traced context into the stream.
func GRPCStreamServerInterceptor() grpc.StreamServerInterceptor {
	propagator := otel.GetTextMapPropagator()

	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Check if tracing is initialized
		if Tracer == nil {
			return handler(srv, ss)
		}

		// Extract trace context from gRPC metadata
		md, ok := metadata.FromIncomingContext(ss.Context())
		if !ok {
			md = metadata.New(nil)
		}

		// Propagate trace context
		ctx := propagator.Extract(ss.Context(), &grpcMetadataCarrier{md: md})

		// Start a new span
		spanName := info.FullMethod
		ctx, span := Tracer.Start(ctx, spanName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("rpc.system", "grpc"),
				attribute.String("rpc.service", info.FullMethod),
				attribute.String("rpc.type", "server_stream"),
			),
		)
		defer span.End()

		// Wrap the stream with trace context
		wrapped := &grpcServerStreamWithContext{
			ServerStream: ss,
			ctx:          ctx,
		}

		return handler(srv, wrapped)
	}
}

// grpcServerStreamWithContext wraps a grpc.ServerStream with trace context
type grpcServerStreamWithContext struct {
	grpc.ServerStream
	ctx context.Context
}

func (x *grpcServerStreamWithContext) Context() context.Context {
	return x.ctx
}

// GRPCClientInterceptor returns a unary client interceptor that starts a
// client span and injects trace context into outgoing metadata.
func GRPCClientInterceptor() grpc.UnaryClientInterceptor {
	propagator := otel.GetTextMapPropagator()

	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// Check if tracing is initialized
		if Tracer == nil {
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		// Start a new span
		ctx, span := Tracer.Start(ctx, method,
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(
				attribute.String("rpc.system", "grpc"),
				attribute.String("rpc.service", method),
				attribute.String("peer.address", cc.Target()),
			),
		)
		defer span.End()

		// Inject trace context into metadata
		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			md = metadata.New(nil)
		}

		propagator.Inject(ctx, &grpcMetadataCarrier{md: md})
		ctx = metadata.NewOutgoingContext(ctx, md)

		// Call the invoker
		err := invoker(ctx, method, req, reply, cc, opts...)

		// Record error if any
		if err != nil {
			span.RecordError(err)
		}

		return err
	}
}

// GRPCClientStreamInterceptor returns a stream client interceptor that starts
// a client span and injects trace context into the stream request.
func GRPCClientStreamInterceptor() grpc.StreamClientInterceptor {
	propagator := otel.GetTextMapPropagator()

	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		// Check if tracing is initialized
		if Tracer == nil {
			return streamer(ctx, desc, cc, method, opts...)
		}

		// Start a new span
		ctx, span := Tracer.Start(ctx, method,
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(
				attribute.String("rpc.system", "grpc"),
				attribute.String("rpc.service", method),
				attribute.String("rpc.type", "client_stream"),
			),
		)
		defer span.End()

		// Inject trace context
		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			md = metadata.New(nil)
		}

		propagator.Inject(ctx, &grpcMetadataCarrier{md: md})
		ctx = metadata.NewOutgoingContext(ctx, md)

		// Call the streamer
		stream, err := streamer(ctx, desc, cc, method, opts...)

		// Wrap the stream with trace context
		if err == nil {
			stream = &grpcClientStreamWithContext{
				ClientStream: stream,
				ctx:          ctx,
			}
		}

		return stream, err
	}
}

// grpcClientStreamWithContext wraps a grpc.ClientStream with trace context
type grpcClientStreamWithContext struct {
	grpc.ClientStream
	ctx context.Context
}

func (x *grpcClientStreamWithContext) Context() context.Context {
	return x.ctx
}

// grpcMetadataCarrier implements propagation.TextMapCarrier for gRPC metadata
type grpcMetadataCarrier struct {
	md metadata.MD
}

// Get returns the value associated with the passed key
func (c *grpcMetadataCarrier) Get(key string) string {
	values := c.md.Get(key)
	if len(values) > 0 {
		return values[0]
	}
	return ""
}

// Set stores the key-value pair
func (c *grpcMetadataCarrier) Set(key string, value string) {
	c.md.Set(key, value)
}

// Keys returns the keys stored in this carrier
func (c *grpcMetadataCarrier) Keys() []string {
	keys := make([]string, 0, len(c.md))
	for k := range c.md {
		keys = append(keys, k)
	}
	return keys
}

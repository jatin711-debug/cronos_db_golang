package api

import (
	"fmt"
	"net"
	"time"

	"cronos_db/internal/tracing"
	"cronos_db/pkg/types"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

// GRPCServer represents the gRPC server
type GRPCServer struct {
	server   *grpc.Server
	listener net.Listener
	config   *Config
}

// Config represents gRPC server configuration
type Config struct {
	Address             string
	MaxRecvMsgSize      int
	MaxSendMsgSize      int
	KeepaliveMinTime    time.Duration
	KeepaliveTimeout    time.Duration
	MaxConnectionIdle   time.Duration
	MaxConnectionAge    time.Duration
	MaxConnectionAgeGrace time.Duration
}

// DefaultConfig returns default gRPC server configuration
func DefaultConfig() *Config {
	return &Config{
		Address:                   ":9000",
		MaxRecvMsgSize:           16 * 1024 * 1024, // 16MB - supports large batches (4000 events × 256B+)
		MaxSendMsgSize:           16 * 1024 * 1024, // 16MB
		KeepaliveMinTime:         10 * time.Second,
		KeepaliveTimeout:         20 * time.Second,
		MaxConnectionIdle:        120 * time.Second,
		MaxConnectionAge:         120 * time.Second,
		MaxConnectionAgeGrace:    5 * time.Second,
	}
}

// NewGRPCServer creates a new gRPC server
func NewGRPCServer(config *Config) *GRPCServer {
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(config.MaxRecvMsgSize),
		grpc.MaxSendMsgSize(config.MaxSendMsgSize),
		grpc.MaxConcurrentStreams(10000), // Prevent OOM from too many concurrent streams
		grpc.InitialWindowSize(16*1024*1024),     // 16MB stream flow control window
		grpc.InitialConnWindowSize(32*1024*1024), // 32MB connection flow control window
		grpc.WriteBufferSize(4*1024*1024),        // 4MB write buffer
		grpc.ReadBufferSize(4*1024*1024),         // 4MB read buffer
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    config.KeepaliveMinTime,
			Timeout: config.KeepaliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.ChainUnaryInterceptor(
			tracing.GRPCServerInterceptor(),
			MetricsInterceptor(),
			RateLimitInterceptor(1000000.0, 2000000.0), // 1M req/s, burst of 2M per IP for load testing
		),
	)

	return &GRPCServer{
		server: server,
		config: config,
	}
}

// RegisterServices registers all gRPC services
func (g *GRPCServer) RegisterServices(eventHandler *EventServiceHandler, consumerHandler *ConsumerGroupServiceHandler) {
	types.RegisterEventServiceServer(g.server, eventHandler)
	types.RegisterConsumerGroupServiceServer(g.server, consumerHandler)
}

// Start starts the gRPC server
func (g *GRPCServer) Start() error {
	lis, err := net.Listen("tcp", g.config.Address)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	g.listener = lis

	reflection.Register(g.server)

	go func() {
		if err := g.server.Serve(lis); err != nil {
			// Log error
		}
	}()

	return nil
}

// Stop stops the gRPC server
func (g *GRPCServer) Stop() {
	if g.server != nil {
		g.server.Stop()
	}
	if g.listener != nil {
		g.listener.Close()
	}
}

// GracefulStop performs graceful shutdown
func (g *GRPCServer) GracefulStop() {
	if g.server != nil {
		g.server.GracefulStop()
	}
}

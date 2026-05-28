package api

import (
	"fmt"
	"net"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/audit"
	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
	"github.com/jatin711-debug/cronos_db_golang/internal/tracing"
	"github.com/jatin711-debug/cronos_db_golang/internal/tx"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

// GRPCServer represents the gRPC server
type GRPCServer struct {
	server    *grpc.Server
	listener  net.Listener
	config    *Config
	txHandler *tx.Handler
}

// Config represents gRPC server configuration
type Config struct {
	Address               string
	MaxRecvMsgSize        int
	MaxSendMsgSize        int
	KeepaliveMinTime      time.Duration
	KeepaliveTimeout      time.Duration
	MaxConnectionIdle     time.Duration
	MaxConnectionAge      time.Duration
	MaxConnectionAgeGrace time.Duration
	TLS                   *TLSConfig
	Auth                  *auth.Config
	TopicRateLimiter      *TopicRateLimiter
	AuditLogger           *audit.Logger
	VersionGate           *VersionGate
}

// DefaultConfig returns default gRPC server configuration
func DefaultConfig() *Config {
	return &Config{
		Address:               ":9000",
		MaxRecvMsgSize:        16 * 1024 * 1024, // 16MB - supports large batches (4000 events × 256B+)
		MaxSendMsgSize:        16 * 1024 * 1024, // 16MB
		KeepaliveMinTime:      10 * time.Second,
		KeepaliveTimeout:      20 * time.Second,
		MaxConnectionIdle:     120 * time.Second,
		MaxConnectionAge:      120 * time.Second,
		MaxConnectionAgeGrace: 5 * time.Second,
	}
}

// NewGRPCServer creates a new gRPC server
func NewGRPCServer(config *Config) *GRPCServer {
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(config.MaxRecvMsgSize),
		grpc.MaxSendMsgSize(config.MaxSendMsgSize),
		grpc.MaxConcurrentStreams(10000),
		grpc.InitialWindowSize(16 * 1024 * 1024),
		grpc.InitialConnWindowSize(32 * 1024 * 1024),
		grpc.WriteBufferSize(4 * 1024 * 1024),
		grpc.ReadBufferSize(4 * 1024 * 1024),
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
			VersionInterceptor(config.VersionGate),
			auth.Interceptor(config.Auth),
			TopicRateLimitInterceptor(config.TopicRateLimiter),
			AuditUnaryInterceptor(config.AuditLogger),
			MetricsInterceptor(),
			RateLimitInterceptor(1000000.0, 2000000.0),
		),
		grpc.ChainStreamInterceptor(
			auth.StreamInterceptor(config.Auth),
			AuditStreamInterceptor(config.AuditLogger),
		),
	}

	if config.TLS != nil && config.TLS.Enabled {
		tlsCfg, err := BuildServerTLSConfig(config.TLS)
		if err != nil {
			// Log and continue without TLS rather than panic
			fmt.Printf("[GRPC] TLS config error: %v; starting without TLS\n", err)
		} else if tlsCfg != nil {
			opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
		}
	}

	server := grpc.NewServer(opts...)

	return &GRPCServer{
		server: server,
		config: config,
	}
}

// SetTransactionHandler sets the transaction service handler.
func (g *GRPCServer) SetTransactionHandler(h *tx.Handler) {
	g.txHandler = h
}

// RegisterServices registers all gRPC services
func (g *GRPCServer) RegisterServices(
	eventHandler *EventServiceHandler,
	consumerHandler *ConsumerGroupServiceHandler,
	partitionHandler *PartitionServiceHandler,
) {
	types.RegisterEventServiceServer(g.server, eventHandler)
	if consumerHandler != nil {
		types.RegisterConsumerGroupServiceServer(g.server, consumerHandler)
	}
	if partitionHandler != nil {
		types.RegisterPartitionServiceServer(g.server, partitionHandler)
	}
	if g.txHandler != nil {
		types.RegisterTransactionServiceServer(g.server, g.txHandler)
	}
}

// RegisterCrossRegionServer registers the cross-region replication service.
func (g *GRPCServer) RegisterCrossRegionServer(srv types.CrossRegionServiceServer) {
	types.RegisterCrossRegionServiceServer(g.server, srv)
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

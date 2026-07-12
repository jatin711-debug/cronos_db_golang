package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	serveErr  chan error
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
	SLORecorder           SLORecorder
}

// SLORecorder is the minimal interface for SLO latency tracking.
type SLORecorder interface {
	Record(latency time.Duration, err bool)
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

// NewGRPCServer creates a new gRPC server.
// Returns an error if TLS is requested but cannot be configured.
func NewGRPCServer(config *Config) (*GRPCServer, error) {
	// In --dev mode (auth disabled) skip heavy observability and IP-rate-limit
	// interceptors to maximize throughput. Tracing, version gating, auth and
	// topic-level rate limiting remain active because they are either cheap or
	// required for request routing.
	devMode := config.Auth == nil || !config.Auth.Enabled

	unary := []grpc.UnaryServerInterceptor{
		tracing.GRPCServerInterceptor(),
	}
	if !devMode {
		unary = append(unary,
			SLOUnaryInterceptor(config.SLORecorder),
		)
	}
	unary = append(unary,
		VersionInterceptor(config.VersionGate),
		auth.Interceptor(config.Auth),
		TopicRateLimitInterceptor(config.TopicRateLimiter),
		// Audit only when authentication is active; in --dev mode every
		// subject is "anonymous", so audit records carry no security value.
		AuditUnaryInterceptor(config.AuditLogger, config.Auth != nil && config.Auth.Enabled),
	)
	if !devMode {
		unary = append(unary,
			MetricsInterceptor(),
			RateLimitInterceptor(1000000.0, 2000000.0),
		)
	}

	stream := []grpc.StreamServerInterceptor{
		tracing.GRPCStreamServerInterceptor(),
	}
	if !devMode {
		stream = append(stream,
			SLOStreamInterceptor(config.SLORecorder),
		)
	}
	stream = append(stream,
		VersionStreamInterceptor(config.VersionGate),
		auth.StreamInterceptor(config.Auth),
		AuditStreamInterceptor(config.AuditLogger, config.Auth != nil && config.Auth.Enabled),
	)
	if !devMode {
		stream = append(stream,
			MetricsStreamInterceptor(),
			RateLimitStreamInterceptor(1000000.0, 2000000.0),
		)
	}

	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(config.MaxRecvMsgSize),
		grpc.MaxSendMsgSize(config.MaxSendMsgSize),
		grpc.MaxConcurrentStreams(10000),
		grpc.InitialWindowSize(16 * 1024 * 1024),
		grpc.InitialConnWindowSize(32 * 1024 * 1024),
		grpc.WriteBufferSize(4 * 1024 * 1024),
		grpc.ReadBufferSize(4 * 1024 * 1024),
		grpc.ConnectionTimeout(30 * time.Second),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    config.KeepaliveMinTime,
			Timeout: config.KeepaliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.ChainUnaryInterceptor(unary...),
		grpc.ChainStreamInterceptor(stream...),
	}

	if config.TLS != nil && config.TLS.Enabled {
		tlsCfg, err := BuildServerTLSConfig(config.TLS)
		if err != nil {
			return nil, fmt.Errorf("TLS requested but config failed: %w", err)
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}

	server := grpc.NewServer(opts...)

	return &GRPCServer{
		server: server,
		config: config,
	}, nil
}

// SLOUnaryInterceptor records request latency and error status for SLO tracking.
func SLOUnaryInterceptor(recorder SLORecorder) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		if recorder == nil {
			return handler(ctx, req)
		}
		start := time.Now()
		resp, err = handler(ctx, req)
		recorder.Record(time.Since(start), err != nil)
		return resp, err
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

// RegisterReplicationServer registers the internal replication service.
func (g *GRPCServer) RegisterReplicationServer(srv types.ReplicationServiceServer) {
	types.RegisterReplicationServiceServer(g.server, srv)
}

// RegisterRaftServer registers the internal raft metadata service.
func (g *GRPCServer) RegisterRaftServer(srv types.RaftServiceServer) {
	types.RegisterRaftServiceServer(g.server, srv)
}

// Start starts the gRPC server.
// The returned error is only from net.Listen; server-serve errors can be read
// from ServeError().
func (g *GRPCServer) Start() error {
	lis, err := net.Listen("tcp", g.config.Address)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	g.listener = lis
	g.serveErr = make(chan error, 1)

	reflection.Register(g.server)

	go func() {
		err := g.server.Serve(lis)
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			g.serveErr <- err
		}
		close(g.serveErr)
	}()

	return nil
}

// ServeError returns a channel that receives a non-nil error if the gRPC
// server exits unexpectedly. It is closed when the server stops.
func (g *GRPCServer) ServeError() <-chan error {
	return g.serveErr
}

// Address returns the address the server is listening on, or an empty string
// if the server has not been started.
func (g *GRPCServer) Address() string {
	if g.listener != nil {
		return g.listener.Addr().String()
	}
	return ""
}

// Stop stops the gRPC server immediately.
func (g *GRPCServer) Stop() {
	if g.server != nil {
		g.server.Stop()
	}
	if g.listener != nil {
		_ = g.listener.Close()
	}
}

// GracefulStop performs graceful shutdown.
func (g *GRPCServer) GracefulStop() {
	if g.server != nil {
		g.server.GracefulStop()
	}
}

// GracefulStopWithTimeout performs a graceful shutdown and falls back to a
// forced Stop if the supplied context expires before the server drains.
func (g *GRPCServer) GracefulStopWithTimeout(ctx context.Context) {
	if g.server == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		g.server.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		slog.Warn("gRPC graceful stop timed out, forcing stop")
		g.server.Stop()
		<-done
	}
}

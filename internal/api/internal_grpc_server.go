package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"runtime"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/replication"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// InternalGRPCServer is the dedicated cluster-internal gRPC listener. It serves
// ReplicationService and RaftService, keeping internal node traffic separate
// from the public client-facing gRPC port. It optionally enforces replication
// mTLS so that only cluster members can connect.
type InternalGRPCServer struct {
	server   *grpc.Server
	listener net.Listener
	config   *InternalConfig
	serveErr chan error
}

// InternalConfig configures the internal cluster gRPC listener.
type InternalConfig struct {
	Address string
	// TLS is the optional replication mTLS configuration. When enabled, the
	// server requires and verifies client certificates.
	TLS *replication.MTLSConfig
}

// DefaultInternalConfig returns a default internal listener configuration.
func DefaultInternalConfig() *InternalConfig {
	return &InternalConfig{
		Address: ":7947",
	}
}

// NewInternalGRPCServer creates a new internal cluster gRPC server.
func NewInternalGRPCServer(config *InternalConfig) (*InternalGRPCServer, error) {
	streamWorkers := runtime.NumCPU()
	if streamWorkers < 1 {
		streamWorkers = 1
	}

	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(64 * 1024 * 1024),
		grpc.MaxSendMsgSize(64 * 1024 * 1024),
		grpc.MaxConcurrentStreams(10000),
		grpc.NumStreamWorkers(uint32(streamWorkers)),
		grpc.InitialWindowSize(16 * 1024 * 1024),
		grpc.InitialConnWindowSize(32 * 1024 * 1024),
		grpc.WriteBufferSize(4 * 1024 * 1024),
		grpc.ReadBufferSize(4 * 1024 * 1024),
		grpc.ConnectionTimeout(30 * time.Second),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    10 * time.Second,
			Timeout: 20 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	}

	if config.TLS != nil && config.TLS.Enabled {
		tlsCfg, err := replication.BuildServerTLSConfig(config.TLS)
		if err != nil {
			return nil, fmt.Errorf("replication mTLS requested but config failed: %w", err)
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}

	server := grpc.NewServer(opts...)

	return &InternalGRPCServer{
		server: server,
		config: config,
	}, nil
}

// RegisterReplicationServer registers the internal replication service.
func (g *InternalGRPCServer) RegisterReplicationServer(srv types.ReplicationServiceServer) {
	types.RegisterReplicationServiceServer(g.server, srv)
}

// RegisterRaftServer registers the internal raft metadata service.
func (g *InternalGRPCServer) RegisterRaftServer(srv types.RaftServiceServer) {
	types.RegisterRaftServiceServer(g.server, srv)
}

// Start starts the internal gRPC server.
func (g *InternalGRPCServer) Start() error {
	lis, err := net.Listen("tcp", g.config.Address)
	if err != nil {
		return fmt.Errorf("internal listen: %w", err)
	}

	g.listener = lis
	g.serveErr = make(chan error, 1)

	go func() {
		err := g.server.Serve(lis)
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			g.serveErr <- err
		}
		close(g.serveErr)
	}()

	return nil
}

// ServeError returns a channel that receives a non-nil error if the server
// exits unexpectedly. It is closed when the server stops.
func (g *InternalGRPCServer) ServeError() <-chan error {
	return g.serveErr
}

// Address returns the address the server is listening on.
func (g *InternalGRPCServer) Address() string {
	if g.listener != nil {
		return g.listener.Addr().String()
	}
	return ""
}

// Stop stops the server immediately.
func (g *InternalGRPCServer) Stop() {
	if g.server != nil {
		g.server.Stop()
	}
	if g.listener != nil {
		_ = g.listener.Close()
	}
}

// GracefulStop performs a graceful shutdown.
func (g *InternalGRPCServer) GracefulStop() {
	if g.server != nil {
		g.server.GracefulStop()
	}
}

// GracefulStopWithTimeout performs a graceful shutdown and falls back to Stop
// if the context expires first.
func (g *InternalGRPCServer) GracefulStopWithTimeout(ctx context.Context) {
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
		slog.Warn("internal gRPC graceful stop timed out, forcing stop")
		g.server.Stop()
		<-done
	}
}

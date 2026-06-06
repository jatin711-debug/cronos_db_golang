package cluster

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// ClusterService handles cluster-related gRPC calls
type ClusterService struct {
	manager *Manager
	server  *grpc.Server
}

// NewClusterService creates a new cluster service
func NewClusterService(manager *Manager) *ClusterService {
	return &ClusterService{
		manager: manager,
	}
}

// Start starts the gRPC server for cluster communication
func (s *ClusterService) Start(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	s.server = grpc.NewServer()
	// Register services here when proto is generated

	go func() {
		if err := s.server.Serve(listener); err != nil {
			log.Printf("[CLUSTER-SERVICE] Server error: %v", err)
		}
	}()

	log.Printf("[CLUSTER-SERVICE] Started on %s", addr)
	return nil
}

// Stop stops the gRPC server
func (s *ClusterService) Stop() {
	if s.server != nil {
		s.server.GracefulStop()
	}
}

// ClusterClient is a client for cluster communication
type ClusterClient struct {
	mu        sync.RWMutex
	conns     map[string]*grpc.ClientConn
	timeout   time.Duration
	tlsConfig *tls.Config
}

// NewClusterClient creates a new cluster client.
// If tlsConfig is non-nil, all outbound connections will use TLS.
func NewClusterClient(timeout time.Duration, tlsConfig *tls.Config) *ClusterClient {
	return &ClusterClient{
		conns:     make(map[string]*grpc.ClientConn),
		timeout:   timeout,
		tlsConfig: tlsConfig,
	}
}

// GetConnection gets or creates a connection to a node
func (c *ClusterClient) GetConnection(addr string) (*grpc.ClientConn, error) {
	c.mu.RLock()
	conn, exists := c.conns[addr]
	c.mu.RUnlock()

	if exists && conn != nil {
		return conn, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check
	if conn, exists = c.conns[addr]; exists && conn != nil {
		return conn, nil
	}

	// Create new connection — non-blocking dial.
	// grpc.WithBlock() was removed because it blocks the caller for the entire
	// timeout duration if the target is unreachable. Without WithBlock(),
	// the connection is established lazily on the first RPC call, and
	// the context timeout on the RPC itself handles unreachable nodes.
	var creds credentials.TransportCredentials
	if c.tlsConfig != nil {
		creds = credentials.NewTLS(c.tlsConfig)
	} else {
		creds = insecure.NewCredentials()
	}
	conn, err := grpc.DialContext(context.Background(), addr,
		grpc.WithTransportCredentials(creds),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	c.conns[addr] = conn
	return conn, nil
}

// Close closes all connections
func (c *ClusterClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for addr, conn := range c.conns {
		if conn != nil {
			conn.Close()
		}
		delete(c.conns, addr)
	}
}

// RequestJoin sends a join request to the leader
func (c *ClusterClient) RequestJoin(_ context.Context, leaderAddr string, node *Node) error {
	conn, err := c.GetConnection(leaderAddr)
	if err != nil {
		return err
	}

	// In production, call the ClusterService.Join RPC
	_ = conn
	log.Printf("[CLUSTER-CLIENT] Would send join request to %s for node %s", leaderAddr, node.ID)

	return nil
}

// ForwardPublish forwards a publish request to the partition leader
func (c *ClusterClient) ForwardPublish(_ context.Context, leaderAddr string, data []byte) ([]byte, error) {
	conn, err := c.GetConnection(leaderAddr)
	if err != nil {
		return nil, err
	}

	// In production, call the EventService.Publish RPC on the leader
	_ = conn
	_ = data
	return nil, fmt.Errorf("not implemented")
}

// ReplicateWAL replicates WAL entries to a follower
func (c *ClusterClient) ReplicateWAL(_ context.Context, followerAddr string, entries []byte) error {
	conn, err := c.GetConnection(followerAddr)
	if err != nil {
		return err
	}

	// In production, call the ReplicationService.Append RPC
	_ = conn
	_ = entries
	return nil
}

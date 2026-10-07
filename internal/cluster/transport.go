package cluster

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/hashicorp/raft"
)

// The nodes of a cluster talk to each other on three ports: replication,
// membership and Raft. Replication is gRPC and gets its mutual TLS there.
// Membership and Raft are plain TCP protocols, and what they carry decides
// who belongs to the cluster and who leads what; a caller that can reach
// those ports unauthenticated can join as a node and vote. With a TLS
// configuration both ports speak mutual TLS only, with the certificates of
// the replication channel: the three are one trust domain.

// listenPeers opens a listener for connections from other nodes. With a TLS
// configuration a caller must present a certificate signed by the cluster's
// CA before anything it sends is read.
func listenPeers(addr string, serverTLS *tls.Config) (net.Listener, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	if serverTLS != nil {
		return tls.NewListener(listener, serverTLS), nil
	}
	return listener, nil
}

// dialPeer connects to another node, over mutual TLS when a configuration is
// given. The handshake counts towards the timeout, and the certificate the
// node presents must be valid for the host the address names.
func dialPeer(ctx context.Context, addr string, timeout time.Duration, clientTLS *tls.Config) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if clientTLS != nil {
		return (&tls.Dialer{Config: clientTLS}).DialContext(ctx, "tcp", addr)
	}
	return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
}

// tlsStreamLayer carries Raft's connections over mutual TLS. It is to
// raft.NetworkTransport what raft.TCPStreamLayer is for plain TCP.
type tlsStreamLayer struct {
	net.Listener
	advertise net.Addr
	clientTLS *tls.Config
}

// Dial implements raft.StreamLayer.
func (l *tlsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return dialPeer(context.Background(), string(address), timeout, l.clientTLS)
}

// Addr returns the address other nodes are told, as raft.TCPStreamLayer does.
func (l *tlsStreamLayer) Addr() net.Addr {
	return l.advertise
}

// newRaftTransport opens the Raft port: over mutual TLS when the cluster has
// a TLS configuration, as plain TCP otherwise.
func newRaftTransport(config *ClusterConfig) (*raft.NetworkTransport, error) {
	advertise, err := net.ResolveTCPAddr("tcp", config.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve raft addr: %w", err)
	}
	if config.ServerTLS == nil {
		return raft.NewTCPTransport(config.RaftAddr, advertise, raftMaxPool, raftTimeout, os.Stderr)
	}
	if advertise.IP == nil || advertise.IP.IsUnspecified() {
		return nil, fmt.Errorf("raft address %s is not one that other nodes can connect to", config.RaftAddr)
	}
	listener, err := listenPeers(config.RaftAddr, config.ServerTLS)
	if err != nil {
		return nil, err
	}
	stream := &tlsStreamLayer{Listener: listener, advertise: advertise, clientTLS: config.ClientTLS}
	return raft.NewNetworkTransport(stream, raftMaxPool, raftTimeout, os.Stderr), nil
}

const (
	// raftMaxPool is how many connections to one peer Raft keeps for reuse.
	raftMaxPool = 3
	// raftTimeout bounds a Raft connection attempt and each of its I/O steps.
	raftTimeout = 10 * time.Second
)

// checkTransportTLS refuses a configuration that secures one direction only:
// a node that listens with TLS and dials without it, or the other way round,
// cannot talk to its own kind.
func checkTransportTLS(config *ClusterConfig) error {
	if (config.ServerTLS == nil) != (config.ClientTLS == nil) {
		return fmt.Errorf("cluster TLS needs both a server and a client configuration")
	}
	return nil
}

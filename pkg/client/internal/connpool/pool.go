package connpool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"

	"google.golang.org/grpc"
)

// ErrNodeNotFound is returned when the requested address is not present in the pool.
var ErrNodeNotFound = errors.New("node not found in pool")

// Config controls pool dialing behavior.
type Config struct {
	ConnectionsPerNode int
	DialTimeout        time.Duration
	ResolveDNS         bool
	DialOptions        []grpc.DialOption
}

type nodeState struct {
	conns []*grpc.ClientConn
	next  atomic.Uint64
}

// Pool maintains per-node gRPC connection pools.
type Pool struct {
	cfg Config

	mu     sync.RWMutex
	nodes  map[string]*nodeState
	order  []string
	closed bool
}

// New creates a connection pool from bootstrap addresses.
func New(ctx context.Context, bootstrap []string, cfg Config) (*Pool, error) {
	if cfg.ConnectionsPerNode <= 0 {
		return nil, fmt.Errorf("connections_per_node must be > 0")
	}
	if cfg.DialTimeout <= 0 {
		return nil, fmt.Errorf("dial_timeout must be > 0")
	}
	if len(cfg.DialOptions) == 0 {
		return nil, fmt.Errorf("at least one dial option is required")
	}

	p := &Pool{
		cfg:   cfg,
		nodes: make(map[string]*nodeState),
		order: make([]string, 0),
	}

	expanded := p.expandBootstrap(ctx, bootstrap)
	var lastErr error
	for _, addr := range expanded {
		if err := p.AddNode(ctx, addr); err != nil {
			lastErr = err
		}
	}
	if len(p.order) == 0 {
		if lastErr == nil {
			lastErr = fmt.Errorf("no nodes available")
		}
		return nil, lastErr
	}

	return p, nil
}

func (p *Pool) expandBootstrap(ctx context.Context, bootstrap []string) []string {
	uniq := make(map[string]struct{}, len(bootstrap))
	result := make([]string, 0, len(bootstrap))

	for _, raw := range bootstrap {
		addr := strings.TrimSpace(raw)
		if addr == "" {
			continue
		}

		if !p.cfg.ResolveDNS {
			if _, exists := uniq[addr]; !exists {
				uniq[addr] = struct{}{}
				result = append(result, addr)
			}
			continue
		}

		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			if _, exists := uniq[addr]; !exists {
				uniq[addr] = struct{}{}
				result = append(result, addr)
			}
			continue
		}

		if ip := net.ParseIP(host); ip != nil {
			if _, exists := uniq[addr]; !exists {
				uniq[addr] = struct{}{}
				result = append(result, addr)
			}
			continue
		}

		ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil || len(ips) == 0 {
			if _, exists := uniq[addr]; !exists {
				uniq[addr] = struct{}{}
				result = append(result, addr)
			}
			continue
		}

		for _, ip := range ips {
			resolved := net.JoinHostPort(ip.IP.String(), port)
			if _, exists := uniq[resolved]; exists {
				continue
			}
			uniq[resolved] = struct{}{}
			result = append(result, resolved)
		}
	}

	return result
}

// AddNode adds a node and creates pooled gRPC connections.
func (p *Pool) AddNode(ctx context.Context, addr string) error {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return fmt.Errorf("node address is empty")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return fmt.Errorf("pool is closed")
	}
	if _, exists := p.nodes[addr]; exists {
		return nil
	}

	conns := make([]*grpc.ClientConn, 0, p.cfg.ConnectionsPerNode)
	for i := 0; i < p.cfg.ConnectionsPerNode; i++ {
		dialCtx, cancel := context.WithTimeout(ctx, p.cfg.DialTimeout)
		opts := append([]grpc.DialOption{}, p.cfg.DialOptions...)
		opts = append(opts, grpc.WithBlock())
		conn, err := grpc.DialContext(dialCtx, addr, opts...)
		cancel()
		if err != nil {
			continue
		}
		conns = append(conns, conn)
	}

	if len(conns) == 0 {
		return fmt.Errorf("failed to dial node %s", addr)
	}

	p.nodes[addr] = &nodeState{conns: conns}
	p.order = append(p.order, addr)
	return nil
}

// Addresses returns currently known node addresses.
func (p *Pool) Addresses() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	out := make([]string, len(p.order))
	copy(out, p.order)
	return out
}

func (p *Pool) getConn(addr string) (*grpc.ClientConn, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return nil, fmt.Errorf("pool is closed")
	}
	state, exists := p.nodes[addr]
	if !exists || len(state.conns) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNodeNotFound, addr)
	}
	idx := state.next.Add(1)
	return state.conns[idx%uint64(len(state.conns))], nil
}

// PartitionClient returns a partition service client for a specific node address.
func (p *Pool) PartitionClient(addr string) (types.PartitionServiceClient, error) {
	conn, err := p.getConn(addr)
	if err != nil {
		return nil, err
	}
	return types.NewPartitionServiceClient(conn), nil
}

// EventClient returns an event service client for a specific node address.
func (p *Pool) EventClient(addr string) (types.EventServiceClient, error) {
	conn, err := p.getConn(addr)
	if err != nil {
		return nil, err
	}
	return types.NewEventServiceClient(conn), nil
}

// Close closes all pooled connections.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	var firstErr error
	for _, state := range p.nodes {
		for _, conn := range state.conns {
			if conn == nil {
				continue
			}
			if err := conn.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	p.nodes = map[string]*nodeState{}
	p.order = nil

	return firstErr
}

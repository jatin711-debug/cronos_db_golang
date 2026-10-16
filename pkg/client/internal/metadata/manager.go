package metadata

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client/internal/connpool"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
)

// Config controls metadata cache TTL, refresh cadence, and address resolution.
type Config struct {
	// TTL is how long cached metadata is considered fresh.
	TTL time.Duration
	// RefreshInterval is the background refresh period (should be <= TTL).
	RefreshInterval time.Duration
	// RequestTimeout bounds each ListPartitions metadata RPC.
	RequestTimeout time.Duration
	// NodeIDToAddress maps cluster node IDs to gRPC addresses when metadata omits them.
	NodeIDToAddress map[string]string
	// StaticPartitionCount is a fallback partition count when metadata is empty (0 = none).
	StaticPartitionCount int
	// OnRefresh is an optional callback after each refresh attempt.
	OnRefresh func(duration time.Duration, err error)
}

// Manager caches partition/leader metadata and refreshes it periodically.
type Manager struct {
	refreshMu sync.Mutex
	bootstrap map[string]bool
	pool      *connpool.Pool // connection pool used to fetch metadata
	cfg       Config         // TTL, refresh, and mapping settings

	mu             sync.RWMutex                   // guards partitions, leaderAddrByID, lastUpdated
	partitions     map[int32]*types.PartitionInfo // partition ID → last known info
	leaderAddrByID map[string]string              // node ID → dial address
	lastUpdated    time.Time                      // wall time of last successful refresh
	stale          atomic.Bool                    // forces refresh on EnsureFresh when true

	stopOnce sync.Once      // ensures Close is idempotent
	stopCh   chan struct{}  // closed to stop the refresh loop
	wg       sync.WaitGroup // waits for background refresh goroutine
}

// NewManager creates a metadata manager.
func NewManager(pool *connpool.Pool, cfg Config) *Manager {
	if cfg.NodeIDToAddress == nil {
		cfg.NodeIDToAddress = map[string]string{}
	}
	bootstrap := make(map[string]bool)
	if pool != nil {
		for _, addr := range pool.Addresses() {
			bootstrap[addr] = true
		}
	}
	return &Manager{
		bootstrap:      bootstrap,
		pool:           pool,
		cfg:            cfg,
		partitions:     map[int32]*types.PartitionInfo{},
		leaderAddrByID: cloneStringMap(cfg.NodeIDToAddress),
		stopCh:         make(chan struct{}),
	}
}

// Start starts background metadata refresh loop.
func (m *Manager) Start(ctx context.Context) {
	m.wg.Add(1)
	utils.GoSafe("metadata-refresh", func() {
		defer m.wg.Done()
		ticker := time.NewTicker(m.cfg.RefreshInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-m.stopCh:
				return
			case <-ticker.C:
				refreshCtx, cancel := context.WithTimeout(context.Background(), m.cfg.RequestTimeout)
				_ = m.Refresh(refreshCtx)
				cancel()
			}
		}
	})
}

// Close stops background workers.
func (m *Manager) Close() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
		m.wg.Wait()
	})
}

// MarkStale forces next EnsureFresh call to refresh metadata.
func (m *Manager) MarkStale() {
	m.stale.Store(true)
}

// Refresh fetches metadata from cluster nodes.
func (m *Manager) Refresh(ctx context.Context) error {
	m.refreshMu.Lock()
	defer m.refreshMu.Unlock()
	start := time.Now()
	var refreshErr error
	defer func() {
		if m.cfg.OnRefresh != nil {
			m.cfg.OnRefresh(time.Since(start), refreshErr)
		}
	}()

	addresses := m.CandidateAddresses(-1)
	if len(addresses) == 0 {
		refreshErr = fmt.Errorf("no cluster addresses available")
		return refreshErr
	}

	var lastErr error
	for _, addr := range addresses {
		client, err := m.pool.PartitionClient(addr)
		if err != nil {
			if addErr := m.pool.AddNode(ctx, addr); addErr == nil {
				client, err = m.pool.PartitionClient(addr)
			}
		}
		if err != nil {
			lastErr = err
			continue
		}

		reqCtx, cancel := context.WithTimeout(ctx, m.cfg.RequestTimeout)
		resp, err := client.ListPartitions(reqCtx, &types.ListPartitionsRequest{})
		cancel()
		if err != nil {
			lastErr = err
			continue
		}

		partitions := make(map[int32]*types.PartitionInfo, len(resp.GetPartitions()))
		leaderAddrByID := cloneStringMap(m.cfg.NodeIDToAddress)
		// Keep discovered endpoints when refreshing through an older server
		// during a rolling upgrade. Explicit client mappings take precedence
		// (for example, when clients connect through a proxy).
		m.mu.RLock()
		if resp.GetDiscoveryVersion() == 0 {
			for id, address := range m.leaderAddrByID {
				if _, configured := leaderAddrByID[id]; !configured {
					leaderAddrByID[id] = address
				}
			}
		}
		m.mu.RUnlock()
		for id, address := range resp.GetNodeAddresses() {
			if _, configured := m.cfg.NodeIDToAddress[id]; !configured && id != "" && validPublicAddress(address) {
				leaderAddrByID[id] = address
			}
		}
		for _, info := range resp.GetPartitions() {
			cloned := clonePartitionInfo(info)
			partitions[cloned.GetPartitionId()] = cloned
			leaderID := strings.TrimSpace(cloned.GetLeaderId())
			if leaderID == "" {
				continue
			}
			if _, ok := leaderAddrByID[leaderID]; ok {
				continue
			}
			if strings.ContainsRune(leaderID, ':') {
				leaderAddrByID[leaderID] = leaderID
			}
		}

		m.mu.Lock()
		oldAddresses := m.leaderAddrByID
		m.partitions = partitions
		m.leaderAddrByID = leaderAddrByID
		m.lastUpdated = time.Now()
		m.mu.Unlock()
		if resp.GetDiscoveryVersion() > 0 {
			keep := make(map[string]bool, len(leaderAddrByID))
			for _, address := range leaderAddrByID {
				keep[address] = true
			}
			for _, address := range oldAddresses {
				if !keep[address] && !m.bootstrap[address] {
					m.pool.RemoveNode(address)
				}
			}
		}
		m.stale.Store(false)
		return nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no metadata source returned a response")
	}
	refreshErr = lastErr
	return refreshErr
}

// EnsureFresh refreshes metadata if stale or TTL-expired.
func (m *Manager) EnsureFresh(ctx context.Context) error {
	m.mu.RLock()
	expired := m.lastUpdated.IsZero() || time.Since(m.lastUpdated) > m.cfg.TTL
	m.mu.RUnlock()

	if !expired && !m.stale.Load() {
		return nil
	}
	return m.Refresh(ctx)
}

// Partitions returns a sorted snapshot of known partition metadata.
func (m *Manager) Partitions() []*types.PartitionInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]*types.PartitionInfo, 0, len(m.partitions))
	for _, info := range m.partitions {
		out = append(out, clonePartitionInfo(info))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetPartitionId() < out[j].GetPartitionId() })
	return out
}

// GetPartitionInfo returns partition metadata.
func (m *Manager) GetPartitionInfo(partitionID int32) (*types.PartitionInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	info, exists := m.partitions[partitionID]
	if !exists {
		return nil, false
	}
	return clonePartitionInfo(info), true
}

// PartitionCount returns metadata partition count (or static fallback).
func (m *Manager) PartitionCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.partitions) > 0 {
		return len(m.partitions)
	}
	return m.cfg.StaticPartitionCount
}

// ResolveLeaderAddress resolves leader_id -> node address if known.
func (m *Manager) ResolveLeaderAddress(partitionID int32) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	info, exists := m.partitions[partitionID]
	if !exists {
		return "", false
	}
	addr, ok := m.leaderAddrByID[info.GetLeaderId()]
	return addr, ok
}

// CandidateAddresses returns a leader-first candidate list for a partition.
func (m *Manager) CandidateAddresses(partitionID int32) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0)

	if addr, ok := m.ResolveLeaderAddress(partitionID); ok && addr != "" {
		out = append(out, addr)
		seen[addr] = struct{}{}
	}

	for _, addr := range m.pool.Addresses() {
		if _, exists := seen[addr]; exists {
			continue
		}
		seen[addr] = struct{}{}
		out = append(out, addr)
	}
	// Also retain metadata sources which have not served traffic yet. This
	// lets a client refresh after every original bootstrap node has left.
	m.mu.RLock()
	discovered := make([]string, 0, len(m.leaderAddrByID))
	for _, addr := range m.leaderAddrByID {
		discovered = append(discovered, addr)
	}
	m.mu.RUnlock()
	sort.Strings(discovered)
	for _, addr := range discovered {
		if _, exists := seen[addr]; !exists && addr != "" {
			seen[addr] = struct{}{}
			out = append(out, addr)
		}
	}

	return out
}

func validPublicAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return false
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return false
	}
	ip := net.ParseIP(host)
	return ip == nil || !ip.IsUnspecified()
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func clonePartitionInfo(info *types.PartitionInfo) *types.PartitionInfo {
	if info == nil {
		return &types.PartitionInfo{}
	}
	return &types.PartitionInfo{
		PartitionId:    info.GetPartitionId(),
		Topic:          info.GetTopic(),
		LeaderId:       info.GetLeaderId(),
		ReplicaIds:     append([]string(nil), info.GetReplicaIds()...),
		HighWatermark:  info.GetHighWatermark(),
		LastOffset:     info.GetLastOffset(),
		SegmentCount:   info.GetSegmentCount(),
		DiskUsageBytes: info.GetDiskUsageBytes(),
	}
}

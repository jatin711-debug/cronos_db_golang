package replication

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// RegionID identifies a geographic region for cross-region replication.
type RegionID string

const (
	crossRegionBatchSize     = 100
	crossRegionFlushInterval = 100 * time.Millisecond
	// crossRegionMaxBufferedEvents caps the per-region retry buffer so a
	// persistently-unreachable remote cannot grow memory without bound. When the
	// cap is exceeded the oldest events are dropped (best-effort async replication).
	crossRegionMaxBufferedEvents = 100000
)

// CrossRegionReplicator handles best-effort async replication of events to
// remote regions with per-region batching and bounded retry buffers.
type CrossRegionReplicator struct {
	mu          sync.RWMutex
	regions     map[RegionID]string // regionID -> endpoint
	localRegion RegionID
	quit        chan struct{}
	clientMgr   *regionClientManager
	tlsConfig   *tls.Config

	// Per-region batching
	batches map[RegionID]*regionBatch
	batchMu sync.Mutex
}

// regionBatch holds pending events for a single remote region.
type regionBatch struct {
	regionID RegionID
	endpoint string
	events   []*types.Event
	mu       sync.Mutex
}

// RegionConnection describes a remote region endpoint for registration.
type RegionConnection struct {
	// RegionID is the remote region's identifier.
	RegionID RegionID
	// Endpoint is the remote region's gRPC address for CrossRegionService.
	Endpoint string
	// Latency is an optional measured RTT hint (informational).
	Latency time.Duration
}

// NewCrossRegionReplicator creates a cross-region replicator.
// If tlsConfig is non-nil, all outbound cross-region connections will use TLS.
func NewCrossRegionReplicator(localRegion RegionID, tlsConfig ...*tls.Config) *CrossRegionReplicator {
	var tc *tls.Config
	if len(tlsConfig) > 0 {
		tc = tlsConfig[0]
	}
	return &CrossRegionReplicator{
		regions:     make(map[RegionID]string),
		localRegion: localRegion,
		quit:        make(chan struct{}),
		clientMgr:   newRegionClientManager(5*time.Second, tc),
		tlsConfig:   tc,
		batches:     make(map[RegionID]*regionBatch),
	}
}

// AddRegion registers a remote region by its gRPC endpoint.
func (crr *CrossRegionReplicator) AddRegion(conn *RegionConnection) {
	crr.mu.Lock()
	defer crr.mu.Unlock()
	crr.regions[conn.RegionID] = conn.Endpoint

	// Initialize batch for this region
	crr.batchMu.Lock()
	crr.batches[conn.RegionID] = &regionBatch{
		regionID: conn.RegionID,
		endpoint: conn.Endpoint,
		events:   make([]*types.Event, 0, crossRegionBatchSize),
	}
	crr.batchMu.Unlock()

	slog.Info("Cross-region replication region registered", "region", conn.RegionID, "endpoint", conn.Endpoint)

	// Start background flush goroutine for this region
	go crr.flushLoop(conn.RegionID)
}

// ReplicateAsync queues an event for replication to all remote regions.
func (crr *CrossRegionReplicator) ReplicateAsync(event *types.Event) {
	if event == nil {
		return
	}

	crr.mu.RLock()
	regions := make([]RegionID, 0, len(crr.regions))
	for rid := range crr.regions {
		regions = append(regions, rid)
	}
	crr.mu.RUnlock()

	for _, regionID := range regions {
		crr.batchMu.Lock()
		batch, ok := crr.batches[regionID]
		crr.batchMu.Unlock()
		if !ok {
			continue
		}

		batch.mu.Lock()
		batch.events = append(batch.events, event)
		shouldFlush := len(batch.events) >= crossRegionBatchSize
		batch.mu.Unlock()

		if shouldFlush {
			go crr.flushRegion(regionID)
		}
	}
}

// flushLoop runs a background timer that periodically flushes the batch.
func (crr *CrossRegionReplicator) flushLoop(regionID RegionID) {
	ticker := time.NewTicker(crossRegionFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			crr.flushRegion(regionID)
		case <-crr.quit:
			// Final flush before exit
			crr.flushRegion(regionID)
			return
		}
	}
}

// flushRegion sends the current batch to the remote region.
func (crr *CrossRegionReplicator) flushRegion(regionID RegionID) {
	crr.batchMu.Lock()
	batch, ok := crr.batches[regionID]
	crr.batchMu.Unlock()
	if !ok {
		return
	}

	batch.mu.Lock()
	if len(batch.events) == 0 {
		batch.mu.Unlock()
		return
	}
	events := make([]*types.Event, len(batch.events))
	copy(events, batch.events)
	batch.events = batch.events[:0]
	batch.mu.Unlock()

	crr.mu.RLock()
	endpoint := crr.regions[regionID]
	crr.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := crr.replicateToRegion(ctx, regionID, endpoint, events); err != nil {
		slog.Warn("Cross-region replication batch failed; re-queuing for retry", "region", regionID, "count", len(events), "error", err)
		// The buffer was cleared before the send, so on failure the events would be
		// lost permanently. Re-queue them (bounded) so the next flush retries them.
		// Events are prepended to preserve ordering relative to newly-appended ones.
		batch.mu.Lock()
		if len(batch.events)+len(events) <= crossRegionMaxBufferedEvents {
			batch.events = append(events, batch.events...)
		} else {
			// Buffer is saturated (remote persistently unreachable). Keep the most
			// recent events up to the cap and drop the oldest to bound memory.
			combined := append(events, batch.events...)
			if len(combined) > crossRegionMaxBufferedEvents {
				combined = combined[len(combined)-crossRegionMaxBufferedEvents:]
			}
			batch.events = combined
			slog.Warn("Cross-region retry buffer full; dropped oldest events", "region", regionID, "cap", crossRegionMaxBufferedEvents)
		}
		batch.mu.Unlock()
	} else {
		slog.Debug("Cross-region replication batch success", "region", regionID, "count", len(events))
	}
}

func (crr *CrossRegionReplicator) replicateToRegion(ctx context.Context, regionID RegionID, endpoint string, events []*types.Event) error {
	rc, err := crr.clientMgr.GetClient(ctx, regionID, endpoint)
	if err != nil {
		return fmt.Errorf("get region client for %s: %w", regionID, err)
	}

	var firstOffset int64
	var partitionID int32
	if len(events) > 0 {
		firstOffset = events[0].Offset
		partitionID = events[0].PartitionId
	}

	req := &types.RegionReplicateRequest{
		RegionId:    string(crr.localRegion),
		PartitionId: partitionID,
		FirstOffset: firstOffset,
		Events:      events,
	}

	resp, err := rc.Replicate(ctx, req)
	if err != nil {
		return fmt.Errorf("replicate to %s: %w", regionID, err)
	}
	if !resp.Success {
		return fmt.Errorf("replicate to %s: %s", regionID, resp.Error)
	}
	return nil
}

// Close shuts down the replicator and closes all client connections.
func (crr *CrossRegionReplicator) Close() error {
	close(crr.quit)
	return crr.clientMgr.CloseAll()
}

// regionClient is a gRPC client for a single remote region's CrossRegionService.
type regionClient struct {
	conn   *grpc.ClientConn
	client types.CrossRegionServiceClient
}

// newRegionClient dials the given endpoint and returns a CrossRegionService client.
func newRegionClient(ctx context.Context, endpoint string, dialTimeout time.Duration, tlsConfig *tls.Config) (*regionClient, error) {
	var creds credentials.TransportCredentials
	if tlsConfig != nil {
		creds = credentials.NewTLS(tlsConfig)
	} else {
		creds = insecure.NewCredentials()
	}
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(
		dialCtx,
		endpoint,
		grpc.WithTransportCredentials(creds),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("dial cross-region endpoint %s: %w", endpoint, err)
	}
	return &regionClient{
		conn:   conn,
		client: types.NewCrossRegionServiceClient(conn),
	}, nil
}

// Replicate sends a batch of events to the remote region.
func (rc *regionClient) Replicate(ctx context.Context, req *types.RegionReplicateRequest) (*types.RegionReplicateResponse, error) {
	return rc.client.ReplicateEvents(ctx, req)
}

// Close closes the gRPC connection.
func (rc *regionClient) Close() error {
	if rc.conn != nil {
		return rc.conn.Close()
	}
	return nil
}

// regionClientManager manages gRPC clients per region, lazily dialing on first use.
type regionClientManager struct {
	mu          sync.RWMutex
	clients     map[RegionID]*regionClient
	dialTimeout time.Duration
	tlsConfig   *tls.Config
}

func newRegionClientManager(dialTimeout time.Duration, tlsConfig *tls.Config) *regionClientManager {
	return &regionClientManager{
		clients:     make(map[RegionID]*regionClient),
		dialTimeout: dialTimeout,
		tlsConfig:   tlsConfig,
	}
}

// GetClient returns the client for a region, dialing lazily if needed.
func (m *regionClientManager) GetClient(ctx context.Context, regionID RegionID, endpoint string) (*regionClient, error) {
	m.mu.RLock()
	rc, ok := m.clients[regionID]
	m.mu.RUnlock()
	if ok {
		return rc, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// Re-check after acquiring write lock
	rc, ok = m.clients[regionID]
	if ok {
		return rc, nil
	}

	client, err := newRegionClient(ctx, endpoint, m.dialTimeout, m.tlsConfig)
	if err != nil {
		return nil, err
	}
	m.clients[regionID] = client
	return client, nil
}

// CloseAll closes all region clients.
func (m *regionClientManager) CloseAll() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, rc := range m.clients {
		_ = rc.Close()
	}
	m.clients = make(map[RegionID]*regionClient)
	return nil
}

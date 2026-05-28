package replication

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RegionID identifies a geographic region.
type RegionID string

const (
	crossRegionBatchSize     = 100
	crossRegionFlushInterval = 100 * time.Millisecond
)

// CrossRegionReplicator handles async replication between regions.
type CrossRegionReplicator struct {
	mu          sync.RWMutex
	regions     map[RegionID]string // regionID -> endpoint
	localRegion RegionID
	quit        chan struct{}
	clientMgr   *regionClientManager

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

// RegionConnection represents a connection to a remote region.
type RegionConnection struct {
	RegionID RegionID
	Endpoint string
	Latency  time.Duration
}

// NewCrossRegionReplicator creates a cross-region replicator.
func NewCrossRegionReplicator(localRegion RegionID) *CrossRegionReplicator {
	return &CrossRegionReplicator{
		regions:     make(map[RegionID]string),
		localRegion: localRegion,
		quit:        make(chan struct{}),
		clientMgr:   newRegionClientManager(5 * time.Second),
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
		slog.Warn("Cross-region replication batch failed", "region", regionID, "count", len(events), "error", err)
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
func newRegionClient(ctx context.Context, endpoint string, dialTimeout time.Duration) (*regionClient, error) {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(
		dialCtx,
		endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
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
}

func newRegionClientManager(dialTimeout time.Duration) *regionClientManager {
	return &regionClientManager{
		clients:     make(map[RegionID]*regionClient),
		dialTimeout: dialTimeout,
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

	client, err := newRegionClient(ctx, endpoint, m.dialTimeout)
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

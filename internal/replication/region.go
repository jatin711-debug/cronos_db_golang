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

// CrossRegionReplicator handles async replication between regions.
type CrossRegionReplicator struct {
	mu          sync.RWMutex
	regions     map[RegionID]string // regionID -> endpoint
	localRegion RegionID
	quit        chan struct{}
	clientMgr   *regionClientManager
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
	}
}

// AddRegion registers a remote region by its gRPC endpoint.
func (crr *CrossRegionReplicator) AddRegion(conn *RegionConnection) {
	crr.mu.Lock()
	defer crr.mu.Unlock()
	crr.regions[conn.RegionID] = conn.Endpoint
	slog.Info("Cross-region replication region registered", "region", conn.RegionID, "endpoint", conn.Endpoint)
}

// ReplicateAsync sends an event to all remote regions asynchronously.
func (crr *CrossRegionReplicator) ReplicateAsync(partitionID int32, offset int64, data []byte) {
	crr.mu.RLock()
	regions := make([]RegionID, 0, len(crr.regions))
	endpoints := make(map[RegionID]string)
	for rid, ep := range crr.regions {
		regions = append(regions, rid)
		endpoints[rid] = ep
	}
	crr.mu.RUnlock()

	for _, regionID := range regions {
		go func(rid RegionID) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := crr.replicateToRegion(ctx, rid, endpoints[rid], partitionID, offset, data); err != nil {
				slog.Warn("Cross-region replication failed", "region", rid, "error", err)
			}
		}(regionID)
	}
}

func (crr *CrossRegionReplicator) replicateToRegion(ctx context.Context, regionID RegionID, endpoint string, partitionID int32, offset int64, data []byte) error {
	rc, err := crr.clientMgr.GetClient(ctx, regionID, endpoint)
	if err != nil {
		return fmt.Errorf("get region client for %s: %w", regionID, err)
	}

	req := &types.RegionReplicateRequest{
		RegionId:    string(regionID),
		PartitionId: partitionID,
		FirstOffset: offset,
		Events: []*types.Event{{
			PartitionId: partitionID,
			Offset:      offset,
			Payload:     data,
		}},
	}

	resp, err := rc.Replicate(ctx, req)
	if err != nil {
		return fmt.Errorf("replicate to %s: %w", regionID, err)
	}
	if !resp.Success {
		return fmt.Errorf("replicate to %s: %s", regionID, resp.Error)
	}
	slog.Debug("Cross-region replication success", "region", regionID, "offset", resp.LastOffset)
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
	conn, err := grpc.NewClient(
		endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(dialTimeout),
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

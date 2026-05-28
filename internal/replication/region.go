package replication

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// RegionID identifies a geographic region.
type RegionID string

// CrossRegionReplicator handles async replication between regions.
type CrossRegionReplicator struct {
	mu        sync.RWMutex
	regions   map[RegionID]*RegionConnection
	localRegion RegionID
	quit      chan struct{}
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
		regions:     make(map[RegionID]*RegionConnection),
		localRegion: localRegion,
		quit:        make(chan struct{}),
	}
}

// AddRegion registers a remote region.
func (crr *CrossRegionReplicator) AddRegion(conn *RegionConnection) {
	crr.mu.Lock()
	defer crr.mu.Unlock()
	crr.regions[conn.RegionID] = conn
	slog.Info("Cross-region replication added", "region", conn.RegionID, "endpoint", conn.Endpoint)
}

// ReplicateAsync sends an event to all remote regions asynchronously.
func (crr *CrossRegionReplicator) ReplicateAsync(partitionID int32, offset int64, data []byte) {
	crr.mu.RLock()
	regions := make([]*RegionConnection, 0, len(crr.regions))
	for _, r := range crr.regions {
		regions = append(regions, r)
	}
	crr.mu.RUnlock()

	for _, region := range regions {
		go func(r *RegionConnection) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := crr.replicateToRegion(ctx, r, partitionID, offset, data); err != nil {
				slog.Warn("Cross-region replication failed", "region", r.RegionID, "error", err)
			}
		}(region)
	}
}

func (crr *CrossRegionReplicator) replicateToRegion(ctx context.Context, region *RegionConnection, partitionID int32, offset int64, data []byte) error {
	// Placeholder: real implementation would use a region-aware replication client
	_ = ctx
	_ = data
	slog.Debug("Replicating to region", "region", region.RegionID, "partition", partitionID, "offset", offset)
	return fmt.Errorf("cross-region replication to %s not yet wired", region.RegionID)
}

// Close shuts down the replicator.
func (crr *CrossRegionReplicator) Close() error {
	close(crr.quit)
	return nil
}

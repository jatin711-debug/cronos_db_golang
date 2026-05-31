package api

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	conflictResolutionCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cronos_crossregion_conflicts_total",
			Help: "Total number of cross-region conflict resolution events",
		},
		[]string{"resolution"},
	)
)

// CrossRegionServer implements the CrossRegionService gRPC server.
// It receives replicated events from remote regions and appends them to local partitions.
type CrossRegionServer struct {
	types.UnimplementedCrossRegionServiceServer
	pm *partition.PartitionManager
}

// NewCrossRegionServer creates a cross-region replication server.
func NewCrossRegionServer(pm *partition.PartitionManager) *CrossRegionServer {
	return &CrossRegionServer{pm: pm}
}

// ReplicateEvents receives events from a remote region and appends them locally.
// It applies Last-Write-Wins (LWW) conflict resolution: if an event with the same
// message_id already exists, the one with the higher created_ts wins.
func (s *CrossRegionServer) ReplicateEvents(ctx context.Context, req *types.RegionReplicateRequest) (*types.RegionReplicateResponse, error) {
	if req == nil || req.RegionId == "" {
		return nil, status.Error(codes.InvalidArgument, "region_id is required")
	}
	if req.PartitionId < 0 {
		return nil, status.Error(codes.InvalidArgument, "partition_id must be >= 0")
	}
	if len(req.Events) == 0 {
		return &types.RegionReplicateResponse{Success: true}, nil
	}

	p, err := s.pm.GetOrCreateInternalPartition(req.PartitionId, req.RegionId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get partition %d: %v", req.PartitionId, err)
	}
	if p.Wal == nil {
		return nil, status.Errorf(codes.Internal, "partition %d WAL not initialized", req.PartitionId)
	}

	// Apply LWW conflict resolution and deduplication
	accepted := make([]*types.Event, 0, len(req.Events))
	for _, event := range req.Events {
		if event.MessageId == "" {
			continue // Skip events without message_id
		}

		// Mark source region in metadata
		if event.Meta == nil {
			event.Meta = make(map[string]string)
		}
		event.Meta["source_region"] = req.RegionId

		// Check dedup store
		if p.DedupStore != nil {
			isDup, dupErr := p.DedupStore.IsDuplicate(event.MessageId, event.Offset)
			if dupErr != nil {
				slog.Warn("Cross-region dedup check failed", "message_id", event.MessageId, "error", dupErr)
				conflictResolutionCounter.WithLabelValues("no_conflict").Inc()
				// On dedup error, proceed with append (safer than dropping)
			} else if isDup {
				// Event exists — apply LWW: keep the one with higher created_ts
				storedTS, exists, tsErr := p.DedupStore.GetTimestamp(event.MessageId)
				if tsErr != nil {
					slog.Warn("Cross-region LWW: failed to get stored timestamp", "message_id", event.MessageId, "error", tsErr)
					conflictResolutionCounter.WithLabelValues("no_conflict").Inc()
					continue
				}
				if exists {
					var incomingTime time.Time
					if event.CreatedTs > 1e16 { // nanoseconds
						incomingTime = time.Unix(0, event.CreatedTs)
					} else { // milliseconds
						incomingTime = time.Unix(0, event.CreatedTs*1_000_000)
					}

					if incomingTime.After(storedTS) {
						// Incoming is newer! Keep it and replace
						slog.Info("Cross-region LWW: replacing duplicate with newer event", "message_id", event.MessageId, "incoming_ts", incomingTime, "stored_ts", storedTS)
						conflictResolutionCounter.WithLabelValues("lww_replaced").Inc()
						accepted = append(accepted, event)
					} else {
						// Incoming is older or equal — skip it
						slog.Debug("Cross-region LWW: skipping duplicate (stored is newer/equal)", "message_id", event.MessageId, "incoming_ts", incomingTime, "stored_ts", storedTS)
						conflictResolutionCounter.WithLabelValues("lww_kept").Inc()
					}
					continue
				} else {
					// Duplicate according to bloom filter but not in PebbleDB
					conflictResolutionCounter.WithLabelValues("no_conflict").Inc()
				}
			} else {
				// Not a duplicate
				conflictResolutionCounter.WithLabelValues("no_conflict").Inc()
			}
		} else {
			conflictResolutionCounter.WithLabelValues("no_conflict").Inc()
		}

		accepted = append(accepted, event)
	}

	if len(accepted) == 0 {
		return &types.RegionReplicateResponse{Success: true, LastOffset: req.FirstOffset}, nil
	}

	if err := p.Wal.AppendBatch(accepted); err != nil {
		slog.Warn("Cross-region replicate: WAL append failed",
			"region", req.RegionId, "partition", req.PartitionId, "error", err)
		return &types.RegionReplicateResponse{
			Success: false,
			Error:   fmt.Sprintf("append batch: %v", err),
		}, nil
	}

	// Update the DedupStore with the assigned local offsets and timestamps
	if p.DedupStore != nil {
		for _, event := range accepted {
			var incomingTS int64
			if event.CreatedTs > 1e16 {
				incomingTS = event.CreatedTs
			} else {
				incomingTS = event.CreatedTs * 1_000_000
			}
			if err := p.DedupStore.Put(event.MessageId, event.Offset, incomingTS); err != nil {
				slog.Warn("Failed to update dedup store after append", "message_id", event.MessageId, "error", err)
			}
		}
	}

	// Schedule events so they become deliverable
	if p.Scheduler != nil {
		if err := p.Scheduler.ScheduleBatch(accepted); err != nil {
			slog.Warn("Cross-region replicate: schedule failed",
				"region", req.RegionId, "partition", req.PartitionId, "error", err)
		}
	}

	var lastOffset int64
	if len(accepted) > 0 {
		lastOffset = accepted[len(accepted)-1].Offset
	}

	slog.Debug("Cross-region replicate: accepted",
		"region", req.RegionId, "partition", req.PartitionId,
		"accepted", len(accepted), "total", len(req.Events), "last_offset", lastOffset)

	return &types.RegionReplicateResponse{
		Success:    true,
		LastOffset: lastOffset,
	}, nil
}

// FetchEvents streams events from a local partition to a remote region for catch-up.
func (s *CrossRegionServer) FetchEvents(req *types.RegionFetchRequest, stream types.CrossRegionService_FetchEventsServer) error {
	if req == nil || req.RegionId == "" {
		return status.Error(codes.InvalidArgument, "region_id is required")
	}
	if req.PartitionId < 0 {
		return status.Error(codes.InvalidArgument, "partition_id must be >= 0")
	}

	p, err := s.pm.GetInternalPartition(req.PartitionId)
	if err != nil || p.Wal == nil {
		return status.Errorf(codes.NotFound, "partition %d not found", req.PartitionId)
	}

	// Stream events from WAL in chunks
	const batchSize = 100
	offset := req.StartOffset
	maxBytes := req.MaxBytes
	if maxBytes <= 0 {
		maxBytes = 10 << 20 // 10MB default
	}
	var sentBytes int64

	for sentBytes < maxBytes {
		events, err := p.Wal.ReadEvents(offset, offset+batchSize)
		if err != nil {
			return status.Errorf(codes.Internal, "read events: %v", err)
		}
		if len(events) == 0 {
			break
		}

		resp := &types.RegionFetchResponse{
			Events: events,
		}
		for _, ev := range events {
			sentBytes += int64(len(ev.Payload))
		}
		if sentBytes >= maxBytes {
			resp.HasMore = false
		} else {
			// Peek ahead
			nextEvents, _ := p.Wal.ReadEvents(offset+batchSize, offset+batchSize*2)
			resp.HasMore = len(nextEvents) > 0
		}

		if err := stream.Send(resp); err != nil {
			return status.Errorf(codes.Internal, "send: %v", err)
		}

		offset += int64(len(events))
	}

	return nil
}

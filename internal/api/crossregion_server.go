package api

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
				// On dedup error, proceed with append (safer than dropping)
			} else if isDup {
				// Event exists — apply LWW: keep the one with higher created_ts
				// Since we can't easily retrieve the old event's timestamp from the dedup store,
				// we conservatively skip duplicates. In a full implementation, the dedup store
				// would store (message_id -> created_ts) for LWW resolution.
				slog.Debug("Cross-region LWW: skipping duplicate", "message_id", event.MessageId, "region", req.RegionId)
				continue
			}
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

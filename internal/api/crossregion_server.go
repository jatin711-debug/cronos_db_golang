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

	if err := p.Wal.AppendBatch(req.Events); err != nil {
		slog.Warn("Cross-region replicate: WAL append failed",
			"region", req.RegionId, "partition", req.PartitionId, "error", err)
		return &types.RegionReplicateResponse{
			Success: false,
			Error:   fmt.Sprintf("append batch: %v", err),
		}, nil
	}

	// Schedule events so they become deliverable
	if p.Scheduler != nil {
		if err := p.Scheduler.ScheduleBatch(req.Events); err != nil {
			slog.Warn("Cross-region replicate: schedule failed",
				"region", req.RegionId, "partition", req.PartitionId, "error", err)
		}
	}

	var lastOffset int64
	if len(req.Events) > 0 {
		lastOffset = req.Events[len(req.Events)-1].Offset
	}

	slog.Debug("Cross-region replicate: accepted",
		"region", req.RegionId, "partition", req.PartitionId,
		"count", len(req.Events), "last_offset", lastOffset)

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

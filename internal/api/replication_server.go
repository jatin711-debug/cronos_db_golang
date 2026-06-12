package api

import (
	"context"
	"fmt"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ReplicationServiceHandler implements internal leader/follower replication RPCs.
type ReplicationServiceHandler struct {
	types.UnimplementedReplicationServiceServer
	partitionManager *partition.PartitionManager
}

// NewReplicationServiceHandler creates a replication service handler.
func NewReplicationServiceHandler(pm *partition.PartitionManager) *ReplicationServiceHandler {
	return &ReplicationServiceHandler{partitionManager: pm}
}

// Append appends a leader-provided batch into the local follower WAL.
func (h *ReplicationServiceHandler) Append(ctx context.Context, req *types.ReplicationAppendRequest) (*types.ReplicationAppendResponse, error) {
	_ = ctx

	if req == nil || req.GetPartitionId() < 0 {
		return nil, status.Error(codes.InvalidArgument, "valid partition_id is required")
	}

	if h.partitionManager == nil {
		return nil, status.Error(codes.Unavailable, "partition manager not initialized")
	}

	topic := fmt.Sprintf("partition-%d", req.GetPartitionId())
	if events := req.GetEvents(); len(events) > 0 && events[0] != nil && events[0].GetTopic() != "" {
		topic = events[0].GetTopic()
	}

	p, err := h.partitionManager.GetOrCreateInternalPartition(req.GetPartitionId(), topic)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get partition %d: %v", req.GetPartitionId(), err)
	}
	if p.Wal == nil {
		return nil, status.Errorf(codes.Internal, "partition %d WAL not initialized", req.GetPartitionId())
	}

	expectedNext := req.GetExpectedNextOffset()
	localNext := p.Wal.GetNextOffset()
	if expectedNext > 0 && expectedNext != localNext {
		return &types.ReplicationAppendResponse{
			Success:    false,
			Error:      fmt.Sprintf("offset mismatch: expected %d, local %d", expectedNext, localNext),
			LastOffset: p.Wal.GetLastOffset(),
			NextOffset: localNext,
		}, nil
	}

	if len(req.GetEvents()) > 0 {
		if err := p.Wal.AppendBatch(req.GetEvents()); err != nil {
			return &types.ReplicationAppendResponse{
				Success:    false,
				Error:      fmt.Sprintf("append batch: %v", err),
				LastOffset: p.Wal.GetLastOffset(),
				NextOffset: p.Wal.GetNextOffset(),
			}, nil
		}
	}

	return &types.ReplicationAppendResponse{
		Success:    true,
		LastOffset: p.Wal.GetLastOffset(),
		NextOffset: p.Wal.GetNextOffset(),
	}, nil
}

// Sync streams events from local WAL to followers for catch-up replication.
func (h *ReplicationServiceHandler) Sync(req *types.ReplicationSyncRequest, stream grpc.ServerStreamingServer[types.ReplicationSyncResponse]) error {
	if req == nil || req.GetPartitionId() < 0 {
		return status.Error(codes.InvalidArgument, "valid partition_id is required")
	}

	if h.partitionManager == nil {
		return status.Error(codes.Unavailable, "partition manager not initialized")
	}

	p, err := h.partitionManager.GetInternalPartition(req.GetPartitionId())
	if err != nil || p.Wal == nil {
		return status.Errorf(codes.NotFound, "partition %d not found", req.GetPartitionId())
	}

	const batchSize int64 = 100
	offset := req.GetStartOffset()
	maxBytes := req.GetMaxBytes()
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

		sendEvents := make([]*types.Event, 0, len(events))
		for _, event := range events {
			eventSize := int64(len(event.GetPayload()))
			if len(sendEvents) > 0 && sentBytes+eventSize > maxBytes {
				break
			}
			sendEvents = append(sendEvents, event)
			sentBytes += eventSize
			if sentBytes >= maxBytes {
				break
			}
		}

		if len(sendEvents) == 0 {
			break
		}

		nextOffset := offset + int64(len(sendEvents))
		hasMore := len(sendEvents) < len(events)
		if !hasMore {
			nextEvents, _ := p.Wal.ReadEvents(nextOffset, nextOffset+1)
			hasMore = len(nextEvents) > 0
		}

		if err := stream.Send(&types.ReplicationSyncResponse{
			Success: true,
			Events:  sendEvents,
			HasMore: hasMore,
		}); err != nil {
			return status.Errorf(codes.Internal, "send: %v", err)
		}

		offset = nextOffset
		if !hasMore {
			break
		}
	}

	return nil
}

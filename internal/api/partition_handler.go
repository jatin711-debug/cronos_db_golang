package api

import (
	"context"
	"fmt"
	"sort"

	"github.com/jatin711-debug/cronos_db_golang/internal/cluster"
	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PartitionServiceHandler implements metadata-focused partition APIs.
// It is intentionally read-only on the hot path used by clients for routing.
type PartitionServiceHandler struct {
	types.UnimplementedPartitionServiceServer

	partitionManager *partition.PartitionManager
	clusterManager   *cluster.Manager // nil in standalone mode
	localNodeID      string
	splitManager     *partition.SplitManager
}

// NewPartitionServiceHandler creates a new partition service handler.
func NewPartitionServiceHandler(
	pm *partition.PartitionManager,
	clusterMgr *cluster.Manager,
	localNodeID string,
) *PartitionServiceHandler {
	sm := partition.NewSplitManager(pm)
	h := &PartitionServiceHandler{
		partitionManager: pm,
		clusterManager:   clusterMgr,
		localNodeID:      localNodeID,
		splitManager:     sm,
	}

	if clusterMgr != nil {
		sm.OnSplitComplete = func(sourceID, newID int32, sourceEpoch, newEpoch int64) error {
			// Get source partition info from cluster manager
			sourceInfo, err := clusterMgr.GetPartitionInfo(sourceID)
			if err != nil {
				return fmt.Errorf("get source partition info from cluster: %w", err)
			}

			// 1. Propose source partition update with new epoch
			updatedSource := &cluster.PartitionInfo{
				ID:       sourceID,
				Topic:    sourceInfo.Topic,
				LeaderID: sourceInfo.LeaderID,
				Replicas: sourceInfo.Replicas,
				ISR:      sourceInfo.ISR,
				Epoch:    sourceEpoch,
				State:    sourceInfo.State,
			}
			if err := clusterMgr.UpdatePartition(updatedSource); err != nil {
				return fmt.Errorf("update source partition epoch in cluster: %w", err)
			}

			// 2. Propose new partition assignment with new epoch
			newInfo := &cluster.PartitionInfo{
				ID:       newID,
				Topic:    sourceInfo.Topic, // split partition shares the same topic
				LeaderID: localNodeID,
				Replicas: []string{localNodeID},
				ISR:      []string{localNodeID},
				Epoch:    newEpoch,
				State:    cluster.PartitionStateOnline,
			}
			if err := clusterMgr.AssignPartition(newInfo); err != nil {
				return fmt.Errorf("assign new partition in cluster: %w", err)
			}

			return nil
		}
	}

	return h
}

func (h *PartitionServiceHandler) buildPartitionInfo(partitionID int32, includeStorageStats bool) (*types.PartitionInfo, error) {
	info := &types.PartitionInfo{
		PartitionId: partitionID,
		LeaderId:    h.localNodeID,
		ReplicaIds:  []string{h.localNodeID},
	}

	if h.clusterManager != nil {
		clusterInfo, err := h.clusterManager.GetPartitionInfo(partitionID)
		if err != nil {
			return nil, err
		}
		info.Topic = clusterInfo.Topic
		info.LeaderId = clusterInfo.LeaderID
		info.ReplicaIds = append([]string(nil), clusterInfo.Replicas...)
	}

	localPartition, err := h.partitionManager.GetInternalPartition(partitionID)
	if err != nil {
		// In cluster mode, this node may not host partition state locally.
		if h.clusterManager != nil {
			return info, nil
		}
		return nil, err
	}

	if info.Topic == "" {
		info.Topic = localPartition.Topic
	}

	if !includeStorageStats || localPartition.Wal == nil {
		return info, nil
	}

	info.HighWatermark = localPartition.Wal.GetHighWatermark()
	info.LastOffset = localPartition.Wal.GetLastOffset()
	segments := localPartition.Wal.GetSegments()
	info.SegmentCount = int32(len(segments))

	var diskUsageBytes int64
	for _, seg := range segments {
		diskUsageBytes += seg.GetSize()
	}
	info.DiskUsageBytes = diskUsageBytes

	return info, nil
}

// GetPartition returns partition metadata including leader assignment.
func (h *PartitionServiceHandler) GetPartition(ctx context.Context, req *types.GetPartitionRequest) (*types.PartitionInfo, error) {
	_ = ctx

	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetPartitionId() < 0 {
		return nil, status.Error(codes.InvalidArgument, "partition_id must be >= 0")
	}

	info, err := h.buildPartitionInfo(req.GetPartitionId(), true)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "partition %d not found: %v", req.GetPartitionId(), err)
	}

	return info, nil
}

// ListPartitions returns partition metadata for topology discovery.
func (h *PartitionServiceHandler) ListPartitions(ctx context.Context, req *types.ListPartitionsRequest) (*types.ListPartitionsResponse, error) {
	_ = ctx

	topicFilter := ""
	if req != nil {
		topicFilter = req.GetTopic()
	}

	partitionIDs := make([]int32, 0)
	if h.clusterManager != nil {
		all := h.clusterManager.GetAllPartitionInfo()
		partitionIDs = make([]int32, 0, len(all))
		for partitionID, meta := range all {
			if topicFilter != "" && meta.Topic != topicFilter {
				continue
			}
			partitionIDs = append(partitionIDs, partitionID)
		}
	} else {
		localPartitions := h.partitionManager.ListPartitions()
		partitionIDs = make([]int32, 0, len(localPartitions))
		for _, p := range localPartitions {
			if topicFilter != "" && p.Topic != topicFilter {
				continue
			}
			partitionIDs = append(partitionIDs, p.ID)
		}
	}

	sort.Slice(partitionIDs, func(i, j int) bool { return partitionIDs[i] < partitionIDs[j] })

	result := make([]*types.PartitionInfo, 0, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		info, err := h.buildPartitionInfo(partitionID, false)
		if err != nil {
			continue
		}
		result = append(result, info)
	}

	return &types.ListPartitionsResponse{
		Partitions: result,
	}, nil
}

// GetWALStatus returns local WAL status for a partition.
func (h *PartitionServiceHandler) GetWALStatus(ctx context.Context, req *types.GetWALStatusRequest) (*types.WALStatus, error) {
	_ = ctx

	if req == nil || req.GetPartitionId() < 0 {
		return nil, status.Error(codes.InvalidArgument, "valid partition_id is required")
	}

	p, err := h.partitionManager.GetInternalPartition(req.GetPartitionId())
	if err != nil || p.Wal == nil {
		return nil, status.Errorf(codes.NotFound, "local WAL for partition %d not found", req.GetPartitionId())
	}

	segments := p.Wal.GetSegments()
	segmentFiles := make([]string, 0, len(segments))
	var totalSize int64
	var firstOffset int64
	var lastOffset int64 = -1
	var oldestTS int64
	var newestTS int64
	for idx, seg := range segments {
		segmentFiles = append(segmentFiles, seg.GetFilename())
		totalSize += seg.GetSize()
		if idx == 0 {
			firstOffset = seg.GetFirstOffset()
			oldestTS = seg.GetFirstTS()
		}
		lastOffset = seg.GetLastOffset()
		newestTS = seg.GetLastTS()
	}

	return &types.WALStatus{
		PartitionId:         req.GetPartitionId(),
		SegmentFiles:        segmentFiles,
		FirstOffset:         firstOffset,
		LastOffset:          lastOffset,
		IndexEntries:        0,
		OldestSegmentTs:     oldestTS,
		NewestSegmentTs:     newestTS,
		TotalSizeBytes:      totalSize,
		AvailableSpaceBytes: 0,
	}, nil
}

// GetSchedulerStatus returns local scheduler status for a partition.
func (h *PartitionServiceHandler) GetSchedulerStatus(ctx context.Context, req *types.GetSchedulerStatusRequest) (*types.SchedulerStatus, error) {
	_ = ctx

	if req == nil || req.GetPartitionId() < 0 {
		return nil, status.Error(codes.InvalidArgument, "valid partition_id is required")
	}

	p, err := h.partitionManager.GetInternalPartition(req.GetPartitionId())
	if err != nil || p.Scheduler == nil {
		return nil, status.Errorf(codes.NotFound, "local scheduler for partition %d not found", req.GetPartitionId())
	}

	stats := p.Scheduler.GetStats()
	return &types.SchedulerStatus{
		PartitionId:  req.GetPartitionId(),
		ActiveTimers: stats.ActiveTimers,
		ReadyEvents:  stats.ReadyEvents,
		AvgDelayMs:   0,
		MaxDelayMs:   0,
		TickMs:       int32(stats.TickMs),
		WheelSize:    int32(stats.WheelSize),
	}, nil
}

// Compact is not exposed through this handler yet.
func (h *PartitionServiceHandler) Compact(ctx context.Context, req *types.CompactRequest) (*types.CompactResponse, error) {
	_ = ctx
	_ = req
	return nil, status.Error(codes.Unimplemented, "compact is not implemented in partition service")
}

// RunRetention is not exposed through this handler yet.
func (h *PartitionServiceHandler) RunRetention(ctx context.Context, req *types.RetentionRequest) (*types.RetentionResponse, error) {
	_ = ctx
	_ = req
	return nil, status.Error(codes.Unimplemented, "run retention is not implemented in partition service")
}

// SplitPartition splits a partition into two at a given offset.
func (h *PartitionServiceHandler) SplitPartition(ctx context.Context, req *types.SplitPartitionRequest) (*types.SplitPartitionResponse, error) {
	_ = ctx

	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetSourcePartitionId() < 0 {
		return nil, status.Error(codes.InvalidArgument, "source_partition_id must be >= 0")
	}
	if req.GetNewPartitionId() < 0 {
		return nil, status.Error(codes.InvalidArgument, "new_partition_id must be >= 0")
	}
	if req.GetSplitOffset() < 0 {
		return nil, status.Error(codes.InvalidArgument, "split_offset must be >= 0")
	}

	if h.splitManager == nil {
		return nil, status.Error(codes.Internal, "split manager not initialized")
	}

	err := h.splitManager.SplitPartition(req.GetSourcePartitionId(), req.GetNewPartitionId(), req.GetSplitOffset(), "")
	if err != nil {
		return &types.SplitPartitionResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	// Get updated high watermark for source partition
	p, err := h.partitionManager.GetInternalPartition(req.GetSourcePartitionId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get source partition: %v", err)
	}
	sourceHW := int64(0)
	if p.Wal != nil {
		sourceHW = p.Wal.GetHighWatermark()
	}

	return &types.SplitPartitionResponse{
		Success:            true,
		SourceHighWatermark: sourceHW,
		NewFirstOffset:     req.GetSplitOffset(),
	}, nil
}

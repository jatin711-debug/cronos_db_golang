package api

import (
	"context"

	"github.com/jatin711-debug/cronos_db_golang/internal/lifecycle"
	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (h *EventServiceHandler) lifecyclePartition(ctx context.Context, ref *types.EventReference) (*partition.Partition, error) {
	if !lifecycle.ValidReference(ref) {
		return nil, status.Error(codes.InvalidArgument, "valid event reference is required")
	}
	if err := h.checkTopicAuth(ctx, ref.Topic, "publish"); err != nil {
		return nil, err
	}
	if h.partitionManager == nil {
		return nil, status.Error(codes.Unavailable, "partition manager is not initialized")
	}
	if err := h.ensureClusterPartitionWritable(ref.PartitionId); err != nil {
		return nil, err
	}
	p, err := h.partitionManager.GetInternalPartition(ref.PartitionId)
	if err != nil {
		return nil, status.Error(codes.NotFound, "partition not found")
	}
	return p, nil
}

func (h *EventServiceHandler) GetEventState(ctx context.Context, req *types.EventStateRequest) (*types.EventStateResponse, error) {
	p, err := h.lifecyclePartition(ctx, req.GetReference())
	if err != nil {
		return nil, err
	}
	return p.EventState(req.Reference)
}

func (h *EventServiceHandler) CancelEvent(ctx context.Context, req *types.EventMutationRequest) (*types.EventStateResponse, error) {
	return h.mutateEvent(ctx, req, lifecycle.Cancel)
}

func (h *EventServiceHandler) RescheduleEvent(ctx context.Context, req *types.EventMutationRequest) (*types.EventStateResponse, error) {
	return h.mutateEvent(ctx, req, lifecycle.Reschedule)
}

func (h *EventServiceHandler) mutateEvent(ctx context.Context, req *types.EventMutationRequest, kind string) (*types.EventStateResponse, error) {
	p, err := h.lifecyclePartition(ctx, req.GetReference())
	if err != nil {
		return nil, err
	}
	if err = h.partitionManager.DiskAdmission(); err != nil {
		return nil, status.Error(codes.ResourceExhausted, err.Error())
	}
	if err = h.ensurePublishAllowed(p.ID); err != nil {
		return nil, err
	}
	return p.MutateEvent(ctx, req, kind)
}

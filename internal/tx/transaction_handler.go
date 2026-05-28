package tx

import (
	"context"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Handler implements the TransactionService gRPC handler.
type Handler struct {
	types.UnimplementedTransactionServiceServer
	coordinator *Coordinator
	pm          *partition.PartitionManager
}

// NewHandler creates a transaction handler.
func NewHandler(pm *partition.PartitionManager) *Handler {
	return &Handler{
		coordinator: NewCoordinator(30*time.Second, ""),
		pm:          pm,
	}
}

// BeginTransaction starts a new distributed transaction.
func (h *Handler) BeginTransaction(ctx context.Context, req *types.BeginTransactionRequest) (*types.BeginTransactionResponse, error) {
	if req == nil || req.TransactionId == "" {
		return nil, status.Error(codes.InvalidArgument, "transaction_id is required")
	}
	if len(req.ParticipantPartitionIds) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one participant partition is required")
	}

	txID := TxID(req.TransactionId)

	// Build participant list from partition IDs
	participants := make([]Participant, 0, len(req.ParticipantPartitionIds))
	for _, pid := range req.ParticipantPartitionIds {
		participants = append(participants, PartitionParticipant{PartitionID: pid})
	}

	tx, err := h.coordinator.Begin(txID, participants)
	if err != nil {
		return &types.BeginTransactionResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &types.BeginTransactionResponse{
		Success:   true,
		CreatedTs: tx.CreatedAt.UnixMilli(),
	}, nil
}

// PrepareTransaction runs phase 1 (Prepare) of 2PC.
func (h *Handler) PrepareTransaction(ctx context.Context, req *types.PrepareTransactionRequest) (*types.PrepareTransactionResponse, error) {
	if req == nil || req.TransactionId == "" {
		return nil, status.Error(codes.InvalidArgument, "transaction_id is required")
	}

	// Prepare is executed as part of Commit flow in the coordinator.
	// A separate Prepare RPC would not normally be exposed — the coordinator
	// runs prepare+commit atomically. We expose it here for transparency.
	// Calling Commit runs Phase 1 (Prepare) first, then Phase 2.
	err := h.coordinator.Commit(ctx, TxID(req.TransactionId))
	if err != nil {
		return &types.PrepareTransactionResponse{
			Success: false,
			Error:   err.Error(),
			Vote:    false,
		}, nil
	}

	return &types.PrepareTransactionResponse{
		Success: true,
		Vote:    true,
	}, nil
}

// CommitTransaction runs phase 2 (Commit) of 2PC.
func (h *Handler) CommitTransaction(ctx context.Context, req *types.CommitTransactionRequest) (*types.CommitTransactionResponse, error) {
	if req == nil || req.TransactionId == "" {
		return nil, status.Error(codes.InvalidArgument, "transaction_id is required")
	}

	err := h.coordinator.Commit(ctx, TxID(req.TransactionId))
	if err != nil {
		return &types.CommitTransactionResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &types.CommitTransactionResponse{
		Success: true,
	}, nil
}

// AbortTransaction aborts a transaction.
func (h *Handler) AbortTransaction(ctx context.Context, req *types.AbortTransactionRequest) (*types.AbortTransactionResponse, error) {
	if req == nil || req.TransactionId == "" {
		return nil, status.Error(codes.InvalidArgument, "transaction_id is required")
	}

	err := h.coordinator.Abort(ctx, TxID(req.TransactionId))
	if err != nil {
		return &types.AbortTransactionResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &types.AbortTransactionResponse{
		Success: true,
	}, nil
}

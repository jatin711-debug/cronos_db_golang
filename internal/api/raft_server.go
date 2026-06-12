package api

import (
	"context"

	"github.com/jatin711-debug/cronos_db_golang/internal/cluster"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RaftServiceHandler implements internal Raft cluster management RPCs.
type RaftServiceHandler struct {
	types.UnimplementedRaftServiceServer
	clusterManager *cluster.Manager
	localNodeID    string
}

// NewRaftServiceHandler creates a raft service handler.
func NewRaftServiceHandler(clusterManager *cluster.Manager, localNodeID string) *RaftServiceHandler {
	return &RaftServiceHandler{clusterManager: clusterManager, localNodeID: localNodeID}
}

// Join adds a node to the Raft cluster. Only leader can process this request.
func (h *RaftServiceHandler) Join(ctx context.Context, req *types.RaftJoinRequest) (*types.RaftJoinResponse, error) {
	_ = ctx

	if req == nil || req.GetNodeId() == "" || req.GetAddress() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and address are required")
	}

	if h.clusterManager == nil || h.clusterManager.GetRaft() == nil {
		return nil, status.Error(codes.Unavailable, "raft is not enabled")
	}

	raftNode := h.clusterManager.GetRaft()
	if !raftNode.IsLeader() {
		return &types.RaftJoinResponse{
			Success: false,
			Error:   "not raft leader",
			Term:    raftNode.GetCurrentTerm(),
		}, nil
	}

	if err := raftNode.Join(req.GetNodeId(), req.GetAddress()); err != nil {
		return &types.RaftJoinResponse{
			Success: false,
			Error:   err.Error(),
			Term:    raftNode.GetCurrentTerm(),
		}, nil
	}

	return &types.RaftJoinResponse{Success: true, Term: raftNode.GetCurrentTerm()}, nil
}

// Leave removes a node from the Raft cluster. Only leader can process this request.
func (h *RaftServiceHandler) Leave(ctx context.Context, req *types.RaftLeaveRequest) (*types.RaftLeaveResponse, error) {
	_ = ctx

	if req == nil || req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	if h.clusterManager == nil || h.clusterManager.GetRaft() == nil {
		return nil, status.Error(codes.Unavailable, "raft is not enabled")
	}

	raftNode := h.clusterManager.GetRaft()
	if !raftNode.IsLeader() {
		return &types.RaftLeaveResponse{Success: false, Error: "not raft leader"}, nil
	}

	if err := raftNode.Leave(req.GetNodeId()); err != nil {
		return &types.RaftLeaveResponse{Success: false, Error: err.Error()}, nil
	}

	return &types.RaftLeaveResponse{Success: true}, nil
}

// Status returns Raft status for this node.
func (h *RaftServiceHandler) Status(ctx context.Context, req *types.RaftStatusRequest) (*types.RaftStatusResponse, error) {
	_ = ctx
	_ = req

	if h.clusterManager == nil || h.clusterManager.GetRaft() == nil {
		return nil, status.Error(codes.Unavailable, "raft is not enabled")
	}

	raftNode := h.clusterManager.GetRaft()
	peers, err := raftNode.GetPeers()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read raft peers: %v", err)
	}

	return &types.RaftStatusResponse{
		NodeId:      h.localNodeID,
		IsLeader:    raftNode.IsLeader(),
		Term:        raftNode.GetCurrentTerm(),
		Peers:       peers,
		CommitIndex: raftNode.GetCommitIndex(),
		LastApplied: raftNode.GetLastApplied(),
	}, nil
}

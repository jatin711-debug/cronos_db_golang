package api

import (
	"context"
	"crypto/tls"
	"errors"

	"github.com/jatin711-debug/cronos_db_golang/internal/cluster"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// forwardedRemoval marks a request to remove a node that one node passed on
// to another, so that it is not passed on a second time.
const forwardedRemoval = "cronos-forwarded-removal"

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

// Leave removes a node that is gone for good from the cluster: from Raft,
// where it counts towards a majority, and from every node's list of members
// (see cluster.Manager.RemoveNode). Any node can be asked: one that does not
// coordinate the cluster passes the request on to the one that does.
func (h *RaftServiceHandler) Leave(ctx context.Context, req *types.RaftLeaveRequest) (*types.RaftLeaveResponse, error) {
	if req == nil || req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	if h.clusterManager == nil || h.clusterManager.GetRaft() == nil {
		return nil, status.Error(codes.Unavailable, "raft is not enabled")
	}

	forwarded := false
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		forwarded = len(md.Get(forwardedRemoval)) > 0
	}
	err := h.clusterManager.RemoveNodeAnywhere(ctx, req.GetNodeId(), forwarded, askToRemoveNode(req.GetNodeId()))
	if err != nil {
		return &types.RaftLeaveResponse{Success: false, Error: err.Error()}, nil
	}
	return &types.RaftLeaveResponse{Success: true}, nil
}

// askToRemoveNode returns how to ask the node at an address to remove a
// node, over the port the nodes use with each other.
func askToRemoveNode(nodeID string) func(ctx context.Context, addr string, clientTLS *tls.Config) error {
	return func(ctx context.Context, addr string, clientTLS *tls.Config) error {
		creds := insecure.NewCredentials()
		if clientTLS != nil {
			creds = credentials.NewTLS(clientTLS)
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
		if err != nil {
			return err
		}
		defer conn.Close()
		ctx = metadata.AppendToOutgoingContext(ctx, forwardedRemoval, "1")
		resp, err := types.NewRaftServiceClient(conn).Leave(ctx, &types.RaftLeaveRequest{NodeId: nodeID})
		if err != nil {
			return err
		}
		if !resp.GetSuccess() {
			return errors.New(resp.GetError())
		}
		return nil
	}
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

package api

import (
	"context"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRaftServiceHandler_StatusUnavailableWithoutRaft(t *testing.T) {
	h := NewRaftServiceHandler(nil, "node-1")

	_, err := h.Status(context.Background(), &types.RaftStatusRequest{})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", err)
	}
}

func TestRaftServiceHandler_JoinValidation(t *testing.T) {
	h := NewRaftServiceHandler(nil, "node-1")

	_, err := h.Join(context.Background(), &types.RaftJoinRequest{NodeId: "", Address: ""})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestRaftServiceHandler_LeaveValidation(t *testing.T) {
	h := NewRaftServiceHandler(nil, "node-1")

	_, err := h.Leave(context.Background(), &types.RaftLeaveRequest{NodeId: ""})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

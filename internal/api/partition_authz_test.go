package api

import (
	"context"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestPartitionHandler_AdminAuthz verifies that destructive partition RPCs
// enforce global admin permission once auth is enabled: a non-admin subject is
// denied, an admin is allowed past the auth gate, and with auth disabled the
// check is skipped.
func TestPartitionHandler_AdminAuthz(t *testing.T) {
	policy := &auth.Policy{
		Subjects: map[string]*auth.Subject{
			"admin-user": {Admin: true},
			"plain-user": {Admin: false},
		},
	}

	h := &PartitionServiceHandler{}
	h.SetAuthPolicy(policy)

	// Non-admin → PermissionDenied.
	ctx := auth.WithClaims(context.Background(), auth.ClaimsWithSubject("plain-user"))
	_, err := h.Compact(ctx, &types.CompactRequest{PartitionId: 0})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("non-admin Compact: expected PermissionDenied, got %v", err)
	}
	_, err = h.RunRetention(ctx, &types.RetentionRequest{PartitionId: 0})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("non-admin RunRetention: expected PermissionDenied, got %v", err)
	}

	// Admin subject passes the auth gate. Use partition_id -1 so the call stops at
	// input validation (InvalidArgument) immediately after the auth check, without
	// dereferencing the (nil) partition manager.
	adminCtx := auth.WithClaims(context.Background(), auth.ClaimsWithSubject("admin-user"))
	_, err = h.Compact(adminCtx, &types.CompactRequest{PartitionId: -1})
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Fatalf("admin Compact should pass auth and hit InvalidArgument, got %v", err)
	}

	// Auth disabled → no check.
	hNoAuth := &PartitionServiceHandler{}
	_, err = hNoAuth.Compact(context.Background(), &types.CompactRequest{PartitionId: -1})
	if status.Code(err) == codes.PermissionDenied {
		t.Fatalf("auth-disabled Compact should not be PermissionDenied, got %v", err)
	}
}

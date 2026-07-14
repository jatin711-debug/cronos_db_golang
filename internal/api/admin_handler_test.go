package api

import (
	"context"
	"net"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// dialAdminServer wires a real grpc.Server bound to a random local port,
// registers the given AdminService handler, and returns a connected
// AdminServiceClient plus a teardown function.
func dialAdminServer(t *testing.T, h *AdminServiceHandler) (types.AdminServiceClient, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	types.RegisterAdminServiceServer(srv, h)

	go func() {
		_ = srv.Serve(lis)
	}()

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}

	teardown := func() {
		conn.Close()
		srv.Stop()
	}

	return types.NewAdminServiceClient(conn), teardown
}

// TestAdminServiceHandler_StandaloneMode verifies that when no cluster
// manager is wired, the handler returns a minimal topology with
// IsClusterMode=false and the local node present.
func TestAdminServiceHandler_StandaloneMode(t *testing.T) {
	// pm is nil-safe because GetClusterTopology does not touch it in
	// standalone mode. authCfg is nil (dev mode) so the auth check is
	// permissive.
	h := NewAdminServiceHandler(nil, nil, nil, "test-node")
	client, teardown := dialAdminServer(t, h)
	defer teardown()

	resp, err := client.GetClusterTopology(context.Background(), &types.GetClusterTopologyRequest{})
	if err != nil {
		t.Fatalf("GetClusterTopology standalone: %v", err)
	}
	if resp.IsClusterMode {
		t.Errorf("expected IsClusterMode=false, got true")
	}
	if resp.LocalNodeId != "test-node" {
		t.Errorf("expected LocalNodeId=test-node, got %q", resp.LocalNodeId)
	}
	if resp.TotalNodes != 1 || resp.AliveNodes != 1 {
		t.Errorf("expected single-node counts (1,1), got (%d,%d)", resp.TotalNodes, resp.AliveNodes)
	}
	if len(resp.Nodes) != 1 {
		t.Fatalf("expected 1 node in standalone mode, got %d", len(resp.Nodes))
	}
	if !resp.Nodes[0].IsLocal || !resp.Nodes[0].IsAlive || resp.Nodes[0].State != "alive" {
		t.Errorf("expected standalone local node to be marked local+alive+state=alive, got %+v", resp.Nodes[0])
	}
	if resp.CapturedAtUnixMs <= 0 {
		t.Errorf("expected positive CapturedAtUnixMs, got %d", resp.CapturedAtUnixMs)
	}
}

// TestAdminServiceHandler_AdminAuth_Allowed verifies that an authenticated
// subject with Subject.Admin=true passes the auth check.
func TestAdminServiceHandler_AdminAuth_Allowed(t *testing.T) {
	policy := &auth.Policy{
		Subjects: map[string]*auth.Subject{
			"ops": {Admin: true},
		},
	}
	authCfg := &auth.Config{Enabled: true, Policy: policy}
	h := NewAdminServiceHandler(nil, nil, authCfg, "test-node")

	if err := h.checkAdmin(auth.WithClaims(context.Background(), auth.ClaimsWithSubject("ops"))); err != nil {
		t.Fatalf("expected admin subject to be allowed, got: %v", err)
	}
}

// TestAdminServiceHandler_AdminAuth_Denied verifies that a subject without
// Subject.Admin=true is rejected.
func TestAdminServiceHandler_AdminAuth_Denied(t *testing.T) {
	policy := &auth.Policy{
		Subjects: map[string]*auth.Subject{
			"user": {Admin: false},
		},
	}
	authCfg := &auth.Config{Enabled: true, Policy: policy}
	h := NewAdminServiceHandler(nil, nil, authCfg, "test-node")

	err := h.checkAdmin(auth.WithClaims(context.Background(), auth.ClaimsWithSubject("user")))
	if err == nil {
		t.Fatal("expected PermissionDenied, got nil")
	}
	if got := status.Code(err); got.String() != "PermissionDenied" {
		t.Fatalf("expected PermissionDenied, got %v", got)
	}
}

// TestAdminServiceHandler_DevMode_AllowsAdmin verifies that when authCfg
// is nil (--dev mode), the handler does not enforce auth. This mirrors
// the rest of the auth interceptor behavior in dev mode.
func TestAdminServiceHandler_DevMode_AllowsAdmin(t *testing.T) {
	h := NewAdminServiceHandler(nil, nil, nil, "test-node")
	if err := h.checkAdmin(context.Background()); err != nil {
		t.Fatalf("expected dev mode (nil authCfg) to permit, got: %v", err)
	}
}

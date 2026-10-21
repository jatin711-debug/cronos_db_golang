package api

import (
	"context"
	"net"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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
	h := NewAdminServiceHandler(nil, nil, nil, "test-node", t.TempDir(), nil, nil)
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
	h := NewAdminServiceHandler(nil, nil, authCfg, "test-node", t.TempDir(), nil, nil)

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
	h := NewAdminServiceHandler(nil, nil, authCfg, "test-node", t.TempDir(), nil, nil)

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
	h := NewAdminServiceHandler(nil, nil, nil, "test-node", t.TempDir(), nil, nil)
	if err := h.checkAdmin(context.Background()); err != nil {
		t.Fatalf("expected dev mode (nil authCfg) to permit, got: %v", err)
	}
}

// TestAdminServiceHandler_StubRPCs_ReturnDeferred verifies that the
// schema and tenant RPCs return structured "deferred" responses rather
// than fake data or Unimplemented. The CLI depends on this contract
// to surface the gap clearly to operators.
func TestAdminServiceHandler_StubRPCs_ReturnDeferred(t *testing.T) {
	h := NewAdminServiceHandler(nil, nil, nil, "test-node", t.TempDir(), nil, nil)
	client, teardown := dialAdminServer(t, h)
	defer teardown()

	ctx := context.Background()

	// ListSchemas: empty success.
	schemas, err := client.ListSchemas(ctx, &types.ListSchemasRequest{})
	if err != nil {
		t.Fatalf("ListSchemas: %v", err)
	}
	if len(schemas.Schemas) != 0 {
		t.Errorf("expected empty Schemas slice, got %d entries", len(schemas.Schemas))
	}

	// GetSchema: returns Unimplemented (the only explicit Unimplemented).
	_, err = client.GetSchema(ctx, &types.GetSchemaRequest{Topic: "any"})
	if err == nil {
		t.Fatal("expected GetSchema to return Unimplemented error")
	}
	if status.Code(err).String() != "Unimplemented" {
		t.Fatalf("expected Unimplemented, got %v", status.Code(err))
	}

	// GetTenantUsage: structured response with LimitsConfigured=false.
	tenant, err := client.GetTenantUsage(ctx, &types.TenantUsage{TenantId: "acme"})
	if err != nil {
		t.Fatalf("GetTenantUsage: %v", err)
	}
	if tenant.LimitsConfigured {
		t.Error("expected LimitsConfigured=false in deferred response")
	}
	if tenant.TenantId != "acme" {
		t.Errorf("expected TenantId=acme, got %q", tenant.TenantId)
	}

	// TriggerRebalance: success with descriptive error explaining gap.
	rb, err := client.TriggerRebalance(ctx, &types.RebalanceRequest{})
	if err != nil {
		t.Fatalf("TriggerRebalance: %v", err)
	}
	if !rb.Success {
		t.Error("expected Success=true with descriptive error")
	}
	if rb.Error == "" {
		t.Error("expected non-empty Error describing the rebalance gap")
	}
}

// TestAdminServiceHandler_RunRetention_PermissionDenied verifies that
// mutating RPCs reject non-admin subjects.
func TestAdminServiceHandler_RunRetention_PermissionDenied(t *testing.T) {
	policy := &auth.Policy{
		Subjects: map[string]*auth.Subject{
			"user": {Admin: false},
		},
	}
	authCfg := &auth.Config{Enabled: true, Policy: policy}
	h := NewAdminServiceHandler(nil, nil, authCfg, "test-node", t.TempDir(), nil, nil)
	client, teardown := dialAdminServer(t, h)
	defer teardown()

	_, err := client.RunRetention(context.Background(), &types.RunRetentionRequest{})
	if err == nil {
		t.Fatal("expected authorization error")
	}
	// Without an auth interceptor in front of the in-process server, no
	// claims are put in ctx and CheckAdminPermission returns
	// Unauthenticated. With a real production interceptor, it would be
	// PermissionDenied. Either way the call is denied.
	code := status.Code(err).String()
	if code != "PermissionDenied" && code != "Unauthenticated" {
		t.Fatalf("expected PermissionDenied or Unauthenticated, got %v", code)
	}
}

// TestAdminServiceHandler_RunCompaction_NeedsPartitions verifies that
// RunCompaction on a node without partitions reports a structured failure.
func TestAdminServiceHandler_RunCompaction_NeedsPartitions(t *testing.T) {
	h := NewAdminServiceHandler(nil, nil, nil, "test-node", t.TempDir(), nil, nil)
	client, teardown := dialAdminServer(t, h)
	defer teardown()

	resp, err := client.RunCompaction(context.Background(), &types.RunCompactionRequest{})
	if err != nil {
		t.Fatalf("RunCompaction: %v", err)
	}
	if resp.Success || resp.Error == "" {
		t.Fatalf("expected a structured failure without partitions, got %+v", resp)
	}
}

// Partition 0 is a partition like any other: the default node has only that
// one, and the per-partition admin RPCs must be able to name it.
func TestAdminServiceHandler_PartitionZeroCanBeNamed(t *testing.T) {
	cfg := &types.Config{DataDir: t.TempDir(), PartitionCount: 1, FsyncMode: "periodic", FlushIntervalMS: 10, TickMS: 10, WheelSize: 64, DedupTTLHours: 1, BloomCapacity: 1000}
	pm := partition.NewPartitionManager("test-node", cfg)
	defer pm.Close()
	if err := pm.CreatePartition(0, "orders"); err != nil {
		t.Fatal(err)
	}
	h := NewAdminServiceHandler(pm, nil, nil, "test-node", cfg.DataDir, nil, nil)
	client, teardown := dialAdminServer(t, h)
	defer teardown()
	ctx := context.Background()

	health, err := client.GetPartitionHealth(ctx, &types.PartitionHealth{PartitionId: 0})
	if err != nil || health.GetPartitionId() != 0 {
		t.Fatalf("partition health of partition 0: %+v, %v", health, err)
	}
	if _, err = client.GetConsumerGroupLag(ctx, &types.AdminConsumerGroupLag{GroupId: "g", PartitionId: 0}); err != nil {
		t.Fatalf("consumer group lag on partition 0: %v", err)
	}
	if _, err = client.GetPartitionHealth(ctx, &types.PartitionHealth{PartitionId: -1}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("negative partition: %v", err)
	}
	// 0 names every partition of the node for compaction, as for retention.
	resp, err := client.RunCompaction(ctx, &types.RunCompactionRequest{PartitionId: 0})
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("compaction of every partition: %+v, %v", resp, err)
	}
}

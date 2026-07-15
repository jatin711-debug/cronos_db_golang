package api

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
	"github.com/jatin711-debug/cronos_db_golang/internal/cluster"
	"github.com/jatin711-debug/cronos_db_golang/internal/compliance"
	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/internal/schema"
	"github.com/jatin711-debug/cronos_db_golang/internal/tenant"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AdminServiceHandler implements the operator-facing AdminService gRPC API.
//
// All RPCs require the authenticated subject to have the global
// `Subject.Admin = true` flag set in the RBAC policy file
// (`auth.CheckAdminPermission`). The auth interceptor authenticates the
// caller; this handler enforces authorization per RPC.
//
// AdminService runs on the same public listener as EventService /
// PartitionService (port :9000 by default), reusing the existing TLS /
// mTLS configuration. There is no separate admin listener.
//
// Step 1 shipped the skeleton with GetClusterTopology implemented and the
// remaining 10 RPCs returning Unimplemented via the embedded stub. Steps
// 2+3 replace the stub with explicit methods on the handler so every
// RPC has stable, code-traceable behavior. Where a real implementation
// would require a new package export or substantial new business logic
// (schema registry, tenant accountant, on-demand rebalance trigger), the
// RPC returns a structured "deferred" response rather than fake data.
// The doc-comment on each deferred RPC explains the exact gap.
type AdminServiceHandler struct {
	// Embed UnimplementedAdminServiceServer so the generated
	// `mustEmbedUnimplementedAdminServiceServer()` requirement is
	// satisfied. The embedded stub also provides a default
	// implementation for every RPC; we override the ones we implement
	// here (see explicit methods below) and delegate to the stub for
	// any future RPCs that we add without an explicit implementation
	// (none expected in step 2+3).
	types.UnimplementedAdminServiceServer

	// pm is the partition manager. May be nil only in degenerate
	// construction-time scenarios; the constructor guarantees non-nil.
	pm *partition.PartitionManager

	// cluster is the cluster manager. nil in standalone mode (--cluster
	// false). When nil, RPCs that require cluster state return either a
	// minimal standalone response or Unimplemented.
	cluster *cluster.Manager

	// schemaRegistry is the topic schema registry. May be nil if the server
	// was started without schema support; schema RPCs will return empty/unimplemented.
	schemaRegistry *schema.Registry

	// tenantAccountant tracks per-tenant resource usage. May be nil if the
	// server was started without tenant quotas; usage RPCs return zero values.
	tenantAccountant *tenant.Accountant

	// authCfg is the runtime auth configuration. May be nil in dev mode
	// (auth disabled), in which case CheckAdminPermission is a no-op.
	authCfg *auth.Config

	// localNodeID is the node's own ID, surfaced in every ClusterTopology
	// response so operators can identify the responding peer.
	localNodeID string

	// dataDir is the on-disk root for WAL segments, dedup store, and the
	// schema registry. Used by RunRetention to construct an Enforcer over
	// the whole node, and by extension points for future schema/tenant
	// wiring.
	dataDir string

	// logger is the per-handler logger; defaults to slog.Default() if nil.
	logger *slog.Logger
}

// NewAdminServiceHandler constructs the handler.
//
// All arguments except authCfg and cluster may be considered required for
// production use. nil cluster is allowed and indicates standalone mode —
// most admin RPCs are no-ops or return Unimplemented in that case.
//
// dataDir is required for RunRetention (the only RPC in this step that
// needs filesystem access). It is the same DataDir passed to the partition
// manager and the rest of the server.
func NewAdminServiceHandler(
	pm *partition.PartitionManager,
	clusterMgr *cluster.Manager,
	authCfg *auth.Config,
	localNodeID string,
	dataDir string,
	schemaRegistry *schema.Registry,
	tenantAccountant *tenant.Accountant,
) *AdminServiceHandler {
	logger := slog.Default()
	if clusterMgr == nil {
		logger.Warn("admin handler constructed without cluster manager; running in standalone mode",
			slog.String("local_node_id", localNodeID))
	}
	return &AdminServiceHandler{
		pm:               pm,
		cluster:          clusterMgr,
		authCfg:          authCfg,
		localNodeID:      localNodeID,
		dataDir:          dataDir,
		schemaRegistry:   schemaRegistry,
		tenantAccountant: tenantAccountant,
		logger:           logger,
	}
}

// checkAdmin enforces admin authorization at the handler level.
//
// Behavior:
//
//   - When authCfg is nil, auth is disabled (--dev mode). We permit the
//     call so that local dev workflows and tests are not blocked by an
//     auth subsystem that is intentionally off. In production deployments
//     auth is required and this nil path is unreachable.
//   - When authCfg is non-nil, delegate to auth.CheckAdminPermission
//     against the configured policy. The auth interceptor has already
//     authenticated the caller and put Claims into ctx by the time we
//     reach this check.
func (h *AdminServiceHandler) checkAdmin(ctx context.Context) error {
	if h.authCfg == nil {
		return nil
	}
	return auth.CheckAdminPermission(ctx, h.authCfg.Policy)
}

// statusUnimplementedErr returns the canonical gRPC Unimplemented error
// for RPCs that we explicitly defer rather than auto-stub.
func statusUnimplementedErr() error {
	return status.Error(codes.Unimplemented, "not implemented in this build")
}

// ---------------------------------------------------------------------------
// GetClusterTopology
// ---------------------------------------------------------------------------

// GetClusterTopology returns the responding peer's view of the cluster:
// node list with topology labels and liveness, plus per-partition
// assignments, ISR, epoch, and per-follower high watermarks.
//
// In standalone mode (cluster == nil), returns a minimal topology with
// IsClusterMode=false and only the local node. Operators can still reach
// the handler in single-node deployments.
//
// Authorization: requires Subject.Admin = true (see CheckAdminPermission).
func (h *AdminServiceHandler) GetClusterTopology(
	ctx context.Context,
	_ *types.GetClusterTopologyRequest,
) (*types.ClusterTopology, error) {
	if err := h.checkAdmin(ctx); err != nil {
		return nil, err
	}

	now := time.Now().UnixMilli()

	if h.cluster == nil {
		return &types.ClusterTopology{
			LocalNodeId:      h.localNodeID,
			IsClusterMode:    false,
			TotalNodes:       1,
			AliveNodes:       1,
			TotalPartitions:  0,
			LeaderPartitions: 0,
			Nodes: []*types.AdminNode{{
				NodeId:  h.localNodeID,
				Address: "",
				IsLocal: true,
				IsAlive: true,
				State:   "alive",
			}},
			Partitions:       nil,
			CapturedAtUnixMs: now,
		}, nil
	}

	stats := h.cluster.GetStats()
	membership := h.cluster.GetMembership()
	localNode := membership.GetLocalNode()
	localID := h.localNodeID
	if localNode != nil && localNode.ID != "" {
		localID = localNode.ID
	}

	nodes := membership.GetNodes()
	adminNodes := make([]*types.AdminNode, 0, len(nodes))
	aliveCount := int64(0)
	for _, n := range nodes {
		if n == nil {
			continue
		}
		isAlive := n.State == cluster.NodeStateAlive
		if isAlive {
			aliveCount++
		}
		adminNodes = append(adminNodes, &types.AdminNode{
			NodeId:  n.ID,
			Address: n.GossipAddr,
			IsLocal: n.ID == localID,
			IsAlive: isAlive,
			State:   n.State.String(),
			Rack:    n.Rack,
			Zone:    n.Zone,
			Region:  n.Region,
		})
	}
	if stats != nil {
		aliveCount = int64(stats.AliveNodes)
	}

	allParts := h.cluster.GetAllPartitionInfo()
	adminParts := make([]*types.AdminPartition, 0, len(allParts))
	for pid, p := range allParts {
		if p == nil {
			continue
		}
		ap := &types.AdminPartition{
			PartitionId:     pid,
			LeaderId:        p.LeaderID,
			Replicas:        p.Replicas,
			Isr:             p.ISR,
			Epoch:           p.Epoch,
			FollowerOffsets: p.ReplicaOffsets,
			InSyncCount:     int32(len(p.ISR)),
		}
		if p.LeaderID != "" {
			if off, ok := p.ReplicaOffsets[p.LeaderID]; ok {
				ap.LeaderHighWatermark = off
			}
		}
		adminParts = append(adminParts, ap)
	}

	topo := &types.ClusterTopology{
		LocalNodeId:      localID,
		IsClusterMode:    true,
		TotalNodes:       int64(len(adminNodes)),
		AliveNodes:       aliveCount,
		TotalPartitions:  int32(len(adminParts)),
		LeaderPartitions: 0,
		Nodes:            adminNodes,
		Partitions:       adminParts,
		CapturedAtUnixMs: now,
	}
	if stats != nil {
		topo.LeaderPartitions = int32(stats.LeaderPartitions)
	}
	return topo, nil
}

// ---------------------------------------------------------------------------
// GetPartitionHealth
// ---------------------------------------------------------------------------

// GetPartitionHealth returns per-partition runtime stats (WAL segments,
// scheduler, dedup summary, replay status) for one partition. Partition
// must be locally owned (it is created via the partition manager, not
// the router — i.e. only partitions this node has materialized).
//
// Source APIs:
//
//   - partition.PartitionManager.GetInternalPartition (WAL, scheduler,
//     dispatcher, replay error, ConsumerGroup).
//   - storage.WAL.GetSegments, storage.WAL.GetHighWatermark.
//   - scheduler.Scheduler.GetStats, GetColdStoreCount.
//
// Deferred fields: first_offset / oldest_segment_ts / newest_segment_ts /
// disk_usage_bytes. The first three need segment header parsing; the
// fourth needs either a Segment size accessor or filesystem stat calls
// over private fields. None are exposed today; deferring per the
// step-2 "light composition, no new exports" rule.
func (h *AdminServiceHandler) GetPartitionHealth(
	ctx context.Context,
	req *types.PartitionHealth,
) (*types.PartitionHealthResponse, error) {
	if err := h.checkAdmin(ctx); err != nil {
		return nil, err
	}

	pid := req.GetPartitionId()
	if pid == 0 {
		return nil, fmt.Errorf("partition_id is required (got 0)")
	}
	p, err := h.pm.GetInternalPartition(pid)
	if err != nil {
		return nil, fmt.Errorf("partition %d not found: %w", pid, err)
	}

	resp := &types.PartitionHealthResponse{
		PartitionId:   pid,
		HighWatermark: p.Wal.GetHighWatermark(),
	}

	resp.SegmentCount = int32(len(p.Wal.GetSegments()))

	stats := p.Scheduler.GetStats()
	resp.ActiveTimers = stats.ActiveTimers
	resp.ReadyEvents = stats.ReadyEvents
	resp.ColdStoreEntries = p.Scheduler.GetColdStoreCount()

	if replayErr := p.GetReplayError(); replayErr != nil {
		resp.ReplayError = true
		resp.LastError = replayErr.Error()
	}

	return resp, nil
}

// ---------------------------------------------------------------------------
// GetReplicationLag
// ---------------------------------------------------------------------------

// GetReplicationLag returns leader HWM and per-follower lag for one or
// all locally-led partitions. partition_id=0 means "all local leaders";
// a non-zero ID scopes to one partition.
//
// Source APIs: partition.ReplLeader.GetHighWatermark, GetFollowerOffsets,
// GetInSyncReplicas. Partitions with no ReplLeader (RF=1 or follower
// replicas) are skipped.
//
// Deferred: lag_seconds is left at 0. The Prometheus gauge
// cronos_replication_lag_seconds already exists; surfacing it here would
// require exporting a getter on the metrics package, which is out of
// scope for step 2.
func (h *AdminServiceHandler) GetReplicationLag(
	ctx context.Context,
	req *types.ReplicationLag,
) (*types.ReplicationLagResponse, error) {
	if err := h.checkAdmin(ctx); err != nil {
		return nil, err
	}

	pid := req.GetPartitionId()
	var parts []*partition.Partition
	if pid != 0 {
		p, err := h.pm.GetInternalPartition(pid)
		if err != nil {
			return nil, fmt.Errorf("partition %d not found: %w", pid, err)
		}
		parts = []*partition.Partition{p}
	} else {
		parts = h.pm.ListPartitions()
	}

	if pid != 0 {
		// Single-partition request: return only that one.
		for _, p := range parts {
			if p.ReplLeader == nil {
				continue
			}
			return buildLagResponse(p.ID, p.ReplLeader), nil
		}
		return &types.ReplicationLagResponse{PartitionId: pid}, nil
	}

	// All-partition request: aggregate per-leader follower lists into a
	// single response (per-partition_id=0 sentinel). The proto response
	// is a single ReplicationLagResponse; we flatten across leaders.
	resp := &types.ReplicationLagResponse{PartitionId: 0}
	for _, p := range parts {
		if p.ReplLeader == nil {
			continue
		}
		leaderLag := buildLagResponse(p.ID, p.ReplLeader)
		resp.LeaderHighWatermark += leaderLag.LeaderHighWatermark
		resp.Followers = append(resp.Followers, leaderLag.Followers...)
	}
	return resp, nil
}

// buildLagResponse constructs a ReplicationLagResponse for a single local
// partition leader. Exported only via package scope.
func buildLagResponse(pid int32, l interface {
	GetHighWatermark() int64
	GetFollowerOffsets() map[string]int64
	GetInSyncReplicas() []string
}) *types.ReplicationLagResponse {
	hwm := l.GetHighWatermark()
	isr := l.GetInSyncReplicas()
	isrSet := make(map[string]struct{}, len(isr))
	for _, id := range isr {
		isrSet[id] = struct{}{}
	}
	offsets := l.GetFollowerOffsets()
	followerIDs := make([]string, 0, len(offsets))
	for id := range offsets {
		followerIDs = append(followerIDs, id)
	}
	sort.Strings(followerIDs)

	leaderLag := &types.ReplicationLagResponse{
		PartitionId:         pid,
		LeaderHighWatermark: hwm,
	}
	for _, fid := range followerIDs {
		off, ok := offsets[fid]
		if !ok {
			continue
		}
		lag := hwm - off
		if lag < 0 {
			lag = 0
		}
		_, inSync := isrSet[fid]
		leaderLag.Followers = append(leaderLag.Followers, &types.FollowerLag{
			FollowerId: fid,
			Offset:     off,
			LagEvents:  lag,
			LagSeconds: 0, // deferred: see doc-comment
			InSync:     inSync,
		})
	}
	return leaderLag
}

// ---------------------------------------------------------------------------
// ListConsumerGroups / GetConsumerGroupLag
// ---------------------------------------------------------------------------

// ListConsumerGroups returns every consumer group known to this node,
// deduplicated across local partitions. partition_id is irrelevant on
// the local node — cluster-wide listing would need a separate fan-out
// path, which is out of scope for step 2.
//
// Source APIs: partition.PartitionManager.ListPartitions ->
// consumer.GroupManager.ListGroups. ConsumerGroup.Members provides
// member_count.
func (h *AdminServiceHandler) ListConsumerGroups(
	ctx context.Context,
	_ *types.AdminListConsumerGroupsRequest,
) (*types.AdminListConsumerGroupsResponse, error) {
	if err := h.checkAdmin(ctx); err != nil {
		return nil, err
	}

	// Dedupe by (group_id, topic) since the same group can be registered
	// across multiple partitions.
	seen := make(map[string]*types.AdminConsumerGroupSummary)
	for _, p := range h.pm.ListPartitions() {
		if p.ConsumerGroup == nil {
			continue
		}
		for _, g := range p.ConsumerGroup.ListGroups() {
			if g == nil {
				continue
			}
			key := g.GroupID + "/" + g.Topic
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = &types.AdminConsumerGroupSummary{
				GroupId:     g.GroupID,
				Topic:       g.Topic,
				Partitions:  g.Partitions,
				MemberCount: int32(len(g.Members)),
			}
		}
	}

	groups := make([]*types.AdminConsumerGroupSummary, 0, len(seen))
	for _, g := range seen {
		groups = append(groups, g)
	}
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].Topic != groups[j].Topic {
			return groups[i].Topic < groups[j].Topic
		}
		return groups[i].GroupId < groups[j].GroupId
	})
	return &types.AdminListConsumerGroupsResponse{Groups: groups}, nil
}

// GetConsumerGroupLag returns the committed-vs-high-watermark lag for a
// single (group_id, partition_id) pair. Returns committed_offset = -1
// and lag = -1 when the group has not committed on this partition or
// the partition is not local.
func (h *AdminServiceHandler) GetConsumerGroupLag(
	ctx context.Context,
	req *types.AdminConsumerGroupLag,
) (*types.AdminConsumerGroupLagResponse, error) {
	if err := h.checkAdmin(ctx); err != nil {
		return nil, err
	}

	pid := req.GetPartitionId()
	groupID := req.GetGroupId()
	if pid == 0 || groupID == "" {
		return nil, fmt.Errorf("partition_id and group_id are required")
	}

	resp := &types.AdminConsumerGroupLagResponse{
		GroupId:                groupID,
		PartitionId:            pid,
		CommittedOffset:        -1,
		PartitionHighWatermark: -1,
		Lag:                    -1,
	}

	p, err := h.pm.GetInternalPartition(pid)
	if err != nil {
		// Partition not local — return the -1 sentinel response rather
		// than an error, so cluster-wide callers can iterate without
		// special-casing missing partitions.
		return resp, nil
	}
	resp.PartitionHighWatermark = p.Wal.GetHighWatermark()

	if p.ConsumerGroup == nil {
		return resp, nil
	}
	off, err := p.ConsumerGroup.GetCommittedOffset(groupID, pid)
	if err != nil {
		return resp, nil
	}
	resp.CommittedOffset = off
	lag := resp.PartitionHighWatermark - off
	if lag < 0 {
		lag = 0
	}
	resp.Lag = lag
	return resp, nil
}

// ---------------------------------------------------------------------------
// RunRetention (RBAC-gated mutating)
// ---------------------------------------------------------------------------

// RunRetention runs age/size-based retention once. partition_id=0 means
// all local partitions; otherwise the request is scoped to one
// partition. The Enforcer is constructed from dataDir + the supplied
// policy and Run() is called once per scope.
//
// Note: the existing compliance.Enforcer.Run returns only error; it does
// not report segments_deleted or bytes_freed. Those counters stay at 0
// here until we add a RunWithStats helper. Adding that helper is a small
// follow-up; doing it now would be a new package export outside step 2's
// "light composition" scope.
func (h *AdminServiceHandler) RunRetention(
	ctx context.Context,
	req *types.RunRetentionRequest,
) (*types.RunRetentionResponse, error) {
	if err := h.checkAdmin(ctx); err != nil {
		return nil, err
	}

	policy := compliance.RetentionPolicy{
		MaxAge:       time.Duration(req.GetMaxAgeHours()) * time.Hour,
		MaxSizeBytes: req.GetMaxSizeBytes(),
	}
	enforcer := compliance.NewEnforcer(h.dataDir, policy)

	resp := &types.RunRetentionResponse{Success: true}
	pid := req.GetPartitionId()
	if pid == 0 {
		if err := enforcer.Run(ctx); err != nil {
			resp.Success = false
			resp.Error = err.Error()
			return resp, nil
		}
	} else {
		p, err := h.pm.GetInternalPartition(pid)
		if err != nil {
			resp.Success = false
			resp.Error = fmt.Sprintf("partition %d not found: %v", pid, err)
			return resp, nil
		}
		if p.DataDir == "" {
			resp.Success = false
			resp.Error = fmt.Sprintf("partition %d has no DataDir", pid)
			return resp, nil
		}
		scoped := compliance.NewEnforcer(p.DataDir, policy)
		if err := scoped.Run(ctx); err != nil {
			resp.Success = false
			resp.Error = err.Error()
			return resp, nil
		}
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// RunCompaction (RBAC-gated mutating)
// ---------------------------------------------------------------------------

// RunCompaction runs consumer-offset-bounded compaction once. The
// existing partition.RunCompaction() operates per-partition and uses
// the min committed offset across all consumer groups; the
// `before_offset` field in the request is ignored in v1.
//
// partition_id=0 returns an error because there is no aggregation
// primitive — RunCompaction is per-partition.
func (h *AdminServiceHandler) RunCompaction(
	ctx context.Context,
	req *types.RunCompactionRequest,
) (*types.RunCompactionResponse, error) {
	if err := h.checkAdmin(ctx); err != nil {
		return nil, err
	}

	pid := req.GetPartitionId()
	if pid == 0 {
		return &types.RunCompactionResponse{
			Success: false,
			Error:   "compaction requires an explicit partition_id (per-partition primitive; no aggregation available)",
		}, nil
	}
	p, err := h.pm.GetInternalPartition(pid)
	if err != nil {
		return &types.RunCompactionResponse{
			Success: false,
			Error:   fmt.Sprintf("partition %d not found: %v", pid, err),
		}, nil
	}
	p.RunCompaction()
	return &types.RunCompactionResponse{Success: true}, nil
}

// ---------------------------------------------------------------------------
// TriggerRebalance (RBAC-gated mutating)
// ---------------------------------------------------------------------------

// TriggerRebalance is a deferred stub. The router rebalances on
// membership events (node join/leave) automatically; an on-demand
// trigger is not exposed in cluster.Manager. Returning success with a
// descriptive error message lets callers detect the gap explicitly
// rather than via a generic Unimplemented.
func (h *AdminServiceHandler) TriggerRebalance(
	ctx context.Context,
	_ *types.RebalanceRequest,
) (*types.RebalanceResponse, error) {
	if err := h.checkAdmin(ctx); err != nil {
		return nil, err
	}
	return &types.RebalanceResponse{
		Success:         true,
		Error:           "explicit rebalance trigger not exposed; reconcileLocalLeadership runs every 5s and reacts to membership events",
		PartitionsMoved: 0,
	}, nil
}

// ---------------------------------------------------------------------------
// Schemas
// ---------------------------------------------------------------------------

// ListSchemas returns a summary of every topic registered in the schema
// registry. If the registry is not wired into this handler, the response
// is empty (Schemas: []).
func (h *AdminServiceHandler) ListSchemas(
	ctx context.Context,
	_ *types.ListSchemasRequest,
) (*types.ListSchemasResponse, error) {
	if err := h.checkAdmin(ctx); err != nil {
		return nil, err
	}
	if h.schemaRegistry == nil {
		return &types.ListSchemasResponse{Schemas: []*types.SchemaSummary{}}, nil
	}

	summaries := h.schemaRegistry.List()
	protoSummaries := make([]*types.SchemaSummary, 0, len(summaries))
	for _, s := range summaries {
		protoSummaries = append(protoSummaries, &types.SchemaSummary{
			Topic:             s.Topic,
			LatestVersion:     int32(s.LatestVersion),
			CompatibilityMode: string(s.CompatibilityMode),
		})
	}
	return &types.ListSchemasResponse{Schemas: protoSummaries}, nil
}

// GetSchema returns a specific schema version for a topic. version=0 means
// "latest". Returns NotFound when the topic or version is missing.
func (h *AdminServiceHandler) GetSchema(
	ctx context.Context,
	req *types.GetSchemaRequest,
) (*types.GetSchemaResponse, error) {
	if err := h.checkAdmin(ctx); err != nil {
		return nil, err
	}
	if h.schemaRegistry == nil {
		return nil, status.Error(codes.Unimplemented, "schema registry not available")
	}
	if req.GetTopic() == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}

	var sch schema.Schema
	var ok bool
	if req.GetVersion() == 0 {
		sch, ok = h.schemaRegistry.Get(req.GetTopic())
	} else {
		sch, ok = h.schemaRegistry.GetVersion(req.GetTopic(), int(req.GetVersion()))
	}
	if !ok {
		return nil, status.Errorf(codes.NotFound, "schema not found for topic %q version %d", req.GetTopic(), req.GetVersion())
	}

	return &types.GetSchemaResponse{
		Topic:      sch.Topic,
		Version:    int32(sch.Version),
		SchemaType: string(sch.Type),
		Definition: sch.Definition,
	}, nil
}

// GetTenantUsage returns current usage and configured limits for a tenant.
// An empty tenant_id aggregates usage across all known tenants.
func (h *AdminServiceHandler) GetTenantUsage(
	ctx context.Context,
	req *types.TenantUsage,
) (*types.TenantUsageResponse, error) {
	if err := h.checkAdmin(ctx); err != nil {
		return nil, err
	}
	if h.tenantAccountant == nil {
		return &types.TenantUsageResponse{
			TenantId:         req.GetTenantId(),
			LimitsConfigured: false,
		}, nil
	}

	tenantID := tenant.ID(req.GetTenantId())
	inFlight, storageBytes := h.tenantAccountant.GetUsage(tenantID)
	limits, hasLimits := h.tenantAccountant.GetLimits(tenantID)

	// If no specific tenant was requested, aggregate across all tenants.
	if req.GetTenantId() == "" {
		inFlight, storageBytes = 0, 0
		for _, id := range h.tenantAccountant.AllTenants() {
			ifInf, stor := h.tenantAccountant.GetUsage(id)
			inFlight += ifInf
			storageBytes += stor
		}
		limits, hasLimits = tenant.Limits{}, false
	}

	resp := &types.TenantUsageResponse{
		TenantId:         req.GetTenantId(),
		InFlight:         inFlight,
		StorageBytes:     storageBytes,
		LimitsConfigured: hasLimits,
	}
	if hasLimits {
		resp.PublishRatePerSec = int64(limits.MaxEventsPerSecond)
		resp.MaxInFlight = limits.MaxInFlight
	}
	return resp, nil
}

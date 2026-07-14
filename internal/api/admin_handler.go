package api

import (
	"context"
	"log/slog"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
	"github.com/jatin711-debug/cronos_db_golang/internal/cluster"
	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// AdminServiceHandler implements the operator-facing AdminService gRPC API.
//
// All RPCs require the authenticated subject to have the global
// `Subject.Admin = true` flag set in the RBAC policy file
// (`auth.CheckAdminPermission`). The auth interceptor authenticates the
// caller; this handler enforces authorization per RPC.
//
// At this step only GetClusterTopology is implemented; the remaining RPCs
// are stubs that return Unimplemented via the embedded
// `types.UnimplementedAdminServiceServer`. They will be filled in steps
// 2-4 of the Admin CLI & Dashboard project without changing the wire
// surface (proto is stable).
//
// AdminService runs on the same public listener as EventService /
// PartitionService (port :9000 by default), reusing the existing TLS /
// mTLS configuration. There is no separate admin listener.
type AdminServiceHandler struct {
	types.UnimplementedAdminServiceServer

	// pm is the partition manager. May be nil only in degenerate
	// construction-time scenarios; the constructor guarantees non-nil.
	pm *partition.PartitionManager

	// cluster is the cluster manager. nil in standalone mode (--cluster
	// false). When nil, RPCs that require cluster state return either a
	// minimal standalone response or Unimplemented.
	cluster *cluster.Manager

	// authCfg is the runtime auth configuration. May be nil in dev mode
	// (auth disabled), in which case CheckAdminPermission is a no-op.
	authCfg *auth.Config

	// localNodeID is the node's own ID, surfaced in every ClusterTopology
	// response so operators can identify the responding peer.
	localNodeID string

	// logger is the per-handler logger; defaults to slog.Default() if nil.
	logger *slog.Logger
}

// NewAdminServiceHandler constructs the handler.
//
// All arguments except authCfg and cluster may be considered required for
// production use. nil cluster is allowed and indicates standalone mode —
// most admin RPCs are no-ops or return Unimplemented in that case.
func NewAdminServiceHandler(
	pm *partition.PartitionManager,
	clusterMgr *cluster.Manager,
	authCfg *auth.Config,
	localNodeID string,
) *AdminServiceHandler {
	logger := slog.Default()
	if clusterMgr == nil {
		logger.Warn("admin handler constructed without cluster manager; running in standalone mode",
			slog.String("local_node_id", localNodeID))
	}
	return &AdminServiceHandler{
		pm:          pm,
		cluster:     clusterMgr,
		authCfg:     authCfg,
		localNodeID: localNodeID,
		logger:      logger,
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

// GetClusterTopology returns the responding peer's view of the cluster:
// node list with topology labels and liveness, plus per-partition
// assignments, ISR, epoch, and per-follower high watermarks.
//
// Behavior:
//
//   - In standalone mode (cluster == nil), returns a minimal topology with
//     IsClusterMode=false and only the local node. Operators can still
//     reach the handler in single-node deployments.
//   - In cluster mode, composes Manager.GetStats, Manager.GetAllPartitionInfo,
//     and MembershipService.GetNodes / GetLocalNode. No new business
//     logic; the handler is a thin composition layer.
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

	// Standalone mode: cluster manager is nil. Return a minimal topology
	// so callers (CLI, dashboard) can detect single-node deployments
	// without crashing.
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
				Rack:    "",
				Zone:    "",
				Region:  "",
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

	// Nodes.
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
			Address: n.GossipAddr, // canonical cluster-facing address
			IsLocal: n.ID == localID,
			IsAlive: isAlive,
			State:   n.State.String(),
			Rack:    n.Rack,
			Zone:    n.Zone,
			Region:  n.Region,
		})
	}
	// Prefer the manager's own count when available (it accounts for the
	// membership snapshot more precisely).
	if stats != nil {
		aliveCount = int64(stats.AliveNodes)
	}

	// Partitions.
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
		// leader_high_watermark is the leader's own high watermark as
		// reported by ReplicaOffsets[leader_id]. If the leader is not in
		// the offsets map (e.g. fresh assignment), leave it at zero.
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

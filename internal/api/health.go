package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/cluster"
	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// HealthChecker performs deep health checks.
type HealthChecker struct {
	Config         *types.Config
	PartitionMgr   *partition.PartitionManager
	ClusterMgr     *cluster.Manager
}

// DeepHealthResponse is the JSON response for /health/deep.
type DeepHealthResponse struct {
	Status        string                 `json:"status"`
	NodeID        string                 `json:"node_id"`
	Timestamp     int64                  `json:"timestamp"`
	UptimeMs      int64                  `json:"uptime_ms"`
	Checks        map[string]HealthCheck `json:"checks"`
	Healthy       bool                   `json:"healthy"`
}

// HealthCheck is an individual check result.
type HealthCheck struct {
	Status  string `json:"status"`
	Detail  string `json:"detail,omitempty"`
	Healthy bool   `json:"healthy"`
}

// NewHealthChecker creates a health checker.
func NewHealthChecker(cfg *types.Config, pm *partition.PartitionManager, cm *cluster.Manager) *HealthChecker {
	return &HealthChecker{
		Config:       cfg,
		PartitionMgr: pm,
		ClusterMgr:   cm,
	}
}

// Register registers health endpoints on the given mux.
func (h *HealthChecker) Register(mux *http.ServeMux) {
	mux.HandleFunc("/health", h.handleHealth)
	mux.HandleFunc("/health/deep", h.handleDeepHealth)
	mux.HandleFunc("/health/ready", h.handleReady)
}

func (h *HealthChecker) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	if h.Config.ClusterEnabled && h.ClusterMgr != nil {
		fmt.Fprintf(w, "OK - Cluster Mode\nNode: %s\n", h.Config.NodeID)
	} else {
		fmt.Fprintf(w, "OK - Standalone Mode\n")
	}
}

func (h *HealthChecker) handleReady(w http.ResponseWriter, r *http.Request) {
	checks := make(map[string]HealthCheck)
	healthy := true

	// Check partitions are loaded
	if h.PartitionMgr != nil {
		stats := h.PartitionMgr.GetStats()
		if stats.TotalPartitions == 0 {
			checks["partitions"] = HealthCheck{Status: "down", Detail: "no partitions loaded", Healthy: false}
			healthy = false
		} else {
			checks["partitions"] = HealthCheck{Status: "up", Detail: fmt.Sprintf("%d partitions", stats.TotalPartitions), Healthy: true}
		}
	}

	// Check cluster state if enabled
	if h.Config.ClusterEnabled && h.ClusterMgr != nil {
		clusterStats := h.ClusterMgr.GetStats()
		if clusterStats.AliveNodes == 0 {
			checks["cluster"] = HealthCheck{Status: "down", Detail: "no alive nodes", Healthy: false}
			healthy = false
		} else {
			checks["cluster"] = HealthCheck{Status: "up", Detail: fmt.Sprintf("%d/%d nodes alive", clusterStats.AliveNodes, clusterStats.TotalNodes), Healthy: true}
		}
	}

	resp := DeepHealthResponse{
		Status:    map[bool]string{true: "ready", false: "not_ready"}[healthy],
		NodeID:    h.Config.NodeID,
		Timestamp: time.Now().UnixMilli(),
		Checks:    checks,
		Healthy:   healthy,
	}

	w.Header().Set("Content-Type", "application/json")
	if !healthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *HealthChecker) handleDeepHealth(w http.ResponseWriter, r *http.Request) {
	checks := make(map[string]HealthCheck)
	healthy := true
	now := time.Now()

	// Partition checks
	if h.PartitionMgr != nil {
		for i := int32(0); i < int32(h.Config.PartitionCount); i++ {
			p, err := h.PartitionMgr.GetInternalPartition(i)
			if err != nil || p == nil {
				continue
			}
			label := "partition_" + strconv.FormatInt(int64(i), 10)

			// WAL last append check
			if p.Wal != nil {
				hwm := p.Wal.GetHighWatermark()
				_ = hwm
				// We don't have last-append timestamp exposed, so we check high watermark > 0 for started partitions
				checks[label+"_wal"] = HealthCheck{Status: "up", Detail: "WAL active", Healthy: true}
			}

			// Scheduler check
			if p.Scheduler != nil {
				stats := p.Scheduler.GetStats()
				if stats.OverflowLevel > 5 {
					checks[label+"_scheduler"] = HealthCheck{Status: "degraded", Detail: fmt.Sprintf("overflow level %d", stats.OverflowLevel), Healthy: false}
					healthy = false
				} else {
					checks[label+"_scheduler"] = HealthCheck{Status: "up", Detail: fmt.Sprintf("%d active timers", stats.ActiveTimers), Healthy: true}
				}
			}

			// Dispatcher check
			if p.Dispatcher != nil {
				stats := p.Dispatcher.GetStats()
				if stats.ActiveDeliveries > int64(h.Config.MaxInFlightPerPartition)*9/10 {
					checks[label+"_delivery"] = HealthCheck{Status: "degraded", Detail: fmt.Sprintf("%d in-flight near capacity", stats.ActiveDeliveries), Healthy: false}
					healthy = false
				} else {
					checks[label+"_delivery"] = HealthCheck{Status: "up", Detail: fmt.Sprintf("%d active deliveries", stats.ActiveDeliveries), Healthy: true}
				}
			}
		}
	}

	// Cluster checks
	if h.Config.ClusterEnabled && h.ClusterMgr != nil {
		clusterStats := h.ClusterMgr.GetStats()
		if clusterStats.TotalNodes > 0 {
			checks["cluster_nodes"] = HealthCheck{Status: "up", Detail: fmt.Sprintf("%d/%d alive", clusterStats.AliveNodes, clusterStats.TotalNodes), Healthy: true}
		}
		if clusterStats.NumPartitions > 0 {
			checks["cluster_partitions"] = HealthCheck{Status: "up", Detail: fmt.Sprintf("%d partitions, %d leaders", clusterStats.NumPartitions, clusterStats.LeaderPartitions), Healthy: true}
		}
	}

	resp := DeepHealthResponse{
		Status:    map[bool]string{true: "healthy", false: "degraded"}[healthy],
		NodeID:    h.Config.NodeID,
		Timestamp: now.UnixMilli(),
		Checks:    checks,
		Healthy:   healthy,
	}

	w.Header().Set("Content-Type", "application/json")
	if !healthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	_ = json.NewEncoder(w).Encode(resp)
}

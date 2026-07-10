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
	Config       *types.Config
	PartitionMgr *partition.PartitionManager
	ClusterMgr   *cluster.Manager
	startTime    time.Time
}

// DeepHealthResponse is the JSON response for /health/deep.
type DeepHealthResponse struct {
	Status    string                 `json:"status"`
	NodeID    string                 `json:"node_id"`
	Timestamp int64                  `json:"timestamp"`
	UptimeMs  int64                  `json:"uptime_ms"`
	Checks    map[string]HealthCheck `json:"checks"`
	Healthy   bool                   `json:"healthy"`
}

// HealthCheck is an individual check result.
type HealthCheck struct {
	Status  string `json:"status"`
	Detail  string `json:"detail,omitempty"`
	Healthy bool   `json:"healthy"`
}

// MetricsResponse is a simple Prometheus-compatible metrics output.
type MetricsResponse struct {
	Timestamp int64                  `json:"timestamp"`
	NodeID    string                 `json:"node_id"`
	Metrics   map[string]interface{} `json:"metrics"`
}

// NewHealthChecker creates a health checker.
func NewHealthChecker(cfg *types.Config, pm *partition.PartitionManager, cm *cluster.Manager) *HealthChecker {
	return &HealthChecker{
		Config:       cfg,
		PartitionMgr: pm,
		ClusterMgr:   cm,
		startTime:    time.Now(),
	}
}

// Register registers health endpoints on the given mux.
func (h *HealthChecker) Register(mux *http.ServeMux) {
	mux.HandleFunc("/health", h.handleHealth)
	mux.HandleFunc("/health/deep", h.handleDeepHealth)
	mux.HandleFunc("/health/ready", h.handleReady)
	// Note: /metrics is registered in main.go with promhttp.Handler()
	// This avoids double-registration panic
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
	} else {
		checks["partitions"] = HealthCheck{Status: "down", Detail: "partition manager not initialized", Healthy: false}
		healthy = false
	}

	// Check WAL readability - can we read from the WAL without mutating it?
	if h.PartitionMgr != nil {
		walReadable, walDetail := h.checkWALReadable()
		if !walReadable {
			checks["wal_readable"] = HealthCheck{Status: "down", Detail: walDetail, Healthy: false}
			healthy = false
		} else {
			checks["wal_readable"] = HealthCheck{Status: "up", Detail: walDetail, Healthy: true}
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
		UptimeMs:  time.Since(h.startTime).Milliseconds(),
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

			// WAL check - test readability, not mutability
			if p.Wal != nil {
				hwm := p.Wal.GetHighWatermark()
				flushErrors := p.Wal.GetFlushErrors()
				replayErr := p.GetReplayError()
				detail := fmt.Sprintf("HWM=%d, flush_errors=%d", hwm, flushErrors)
				walHealthy := true
				if replayErr != nil {
					detail += fmt.Sprintf(", replay_error=%v", replayErr)
					walHealthy = false
				}
				if flushErrors > 0 {
					walHealthy = false
				}
				if hwm >= 0 {
					if _, err := p.Wal.ReadEvent(hwm); err != nil {
						detail = fmt.Sprintf("WAL read failed at HWM %d: %v", hwm, err)
						walHealthy = false
					}
				}
				status := "up"
				if !walHealthy {
					status = "degraded"
					healthy = false
				}
				checks[label+"_wal"] = HealthCheck{Status: status, Detail: detail, Healthy: walHealthy}
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

			// Dedup store check
			if p.DedupStore != nil {
				if stats, err := p.DedupStore.GetStats(); err == nil && stats != nil {
					checks[label+"_dedup"] = HealthCheck{Status: "up", Detail: fmt.Sprintf("bloom %d bytes, FPR %.4f", stats.BloomMemoryBytes, float64(stats.BloomFalsePositives)/float64(stats.BloomHits+stats.BloomFalsePositives+1)), Healthy: true}
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
		UptimeMs:  time.Since(h.startTime).Milliseconds(),
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

// handleMetrics returns a simple JSON metrics endpoint.
// For Prometheus compatibility, this outputs basic metrics in a simple format.
func (h *HealthChecker) handleMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := make(map[string]interface{})

	// Basic node metrics
	metrics["cronos_node_uptime_ms"] = time.Since(h.startTime).Milliseconds()
	metrics["cronos_node_partitions_total"] = h.Config.PartitionCount

	// Partition metrics
	if h.PartitionMgr != nil {
		stats := h.PartitionMgr.GetStats()
		metrics["cronos_partitions_active"] = stats.TotalPartitions
		metrics["cronos_partitions_leader"] = stats.LeaderPartitions

		for i := int32(0); i < int32(h.Config.PartitionCount); i++ {
			p, err := h.PartitionMgr.GetInternalPartition(i)
			if err != nil || p == nil {
				continue
			}
			label := strconv.FormatInt(int64(i), 10)

			if p.Wal != nil {
				metrics["cronos_wal_high_watermark{partition=\""+label+"\"}"] = p.Wal.GetHighWatermark()
				metrics["cronos_wal_segments{partition=\""+label+"\"}"] = len(p.Wal.GetSegments())
			}

			if p.Scheduler != nil {
				schedStats := p.Scheduler.GetStats()
				metrics["cronos_scheduler_active_timers{partition=\""+label+"\"}"] = schedStats.ActiveTimers
				metrics["cronos_scheduler_overflow_level{partition=\""+label+"\"}"] = schedStats.OverflowLevel
			}

			if p.Dispatcher != nil {
				dispStats := p.Dispatcher.GetStats()
				metrics["cronos_dispatcher_active_deliveries{partition=\""+label+"\"}"] = dispStats.ActiveDeliveries
				metrics["cronos_dispatcher_credits_in_use{partition=\""+label+"\"}"] = dispStats.CreditsInUse
			}
		}
	}

	// Cluster metrics
	if h.Config.ClusterEnabled && h.ClusterMgr != nil {
		clusterStats := h.ClusterMgr.GetStats()
		metrics["cronos_cluster_nodes_total"] = clusterStats.TotalNodes
		metrics["cronos_cluster_nodes_alive"] = clusterStats.AliveNodes
		metrics["cronos_cluster_partitions"] = clusterStats.NumPartitions
		metrics["cronos_cluster_leaders"] = clusterStats.LeaderPartitions
	}

	resp := MetricsResponse{
		Timestamp: time.Now().UnixMilli(),
		NodeID:    h.Config.NodeID,
		Metrics:   metrics,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// checkWALReadable tests if we can read from the WAL without mutating it.
func (h *HealthChecker) checkWALReadable() (bool, string) {
	if h.PartitionMgr == nil {
		return false, "partition manager not initialized"
	}

	// Try partition 0 first
	p, err := h.PartitionMgr.GetInternalPartition(0)
	if err != nil || p == nil || p.Wal == nil {
		// Try any available partition
		for i := int32(0); i < int32(h.Config.PartitionCount); i++ {
			p, err = h.PartitionMgr.GetInternalPartition(i)
			if err == nil && p != nil && p.Wal != nil {
				break
			}
		}
	}

	if p == nil || p.Wal == nil {
		return false, "no WAL available"
	}

	hwm := p.Wal.GetHighWatermark()
	if hwm < 0 {
		return true, "empty WAL is readable"
	}

	if _, err := p.Wal.ReadEvent(hwm); err != nil {
		return false, fmt.Sprintf("WAL read at HWM %d failed: %v", hwm, err)
	}

	return true, fmt.Sprintf("WAL readable, HWM=%d", hwm)
}

package api

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

var (
	grpcRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cronos_api_grpc_requests_total",
			Help: "Total number of gRPC requests",
		},
		[]string{"method", "status"},
	)
	grpcRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "cronos_api_grpc_request_duration_seconds",
			Help:    "Duration of gRPC requests in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)

	// Critical path latency histograms (production observability)
	walAppendLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "cronos_wal_append_latency_seconds",
			Help:    "Latency of WAL append operations",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1},
		},
		[]string{"partition"},
	)
	dedupCheckLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "cronos_dedup_check_latency_seconds",
			Help:    "Latency of deduplication check operations",
			Buckets: []float64{0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01},
		},
		[]string{"partition", "path"}, // path = "bloom_fast" or "pebble_slow"
	)
	dispatchLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "cronos_dispatch_latency_seconds",
			Help:    "Latency of event dispatch operations",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.025},
		},
		[]string{"partition"},
	)
	segmentRotationLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "cronos_segment_rotation_latency_seconds",
			Help:    "Latency of WAL segment rotation",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
		},
		[]string{"partition"},
	)
)

// MetricsInterceptor creates a gRPC unary interceptor for prometheus metrics
func MetricsInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()

		resp, err := handler(ctx, req)

		duration := time.Since(start).Seconds()
		statusCode := "OK"
		if err != nil {
			if st, ok := status.FromError(err); ok {
				statusCode = st.Code().String()
			} else {
				statusCode = "UNKNOWN"
			}
		}

		grpcRequestsTotal.WithLabelValues(info.FullMethod, statusCode).Inc()
		grpcRequestDuration.WithLabelValues(info.FullMethod).Observe(duration)

		return resp, err
	}
}

// =============================================================================
// Production-Grade Metrics (Gauges)
// =============================================================================

var (
	// Timing wheel metrics
	timingWheelActiveTimers = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cronos_timing_wheel_active_timers",
			Help: "Number of active timers in the timing wheel per partition",
		},
		[]string{"partition"},
	)
	timingWheelOverflowLevel = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cronos_timing_wheel_overflow_level",
			Help: "Current overflow level (wheel depth) in the hierarchical timing wheel per partition",
		},
		[]string{"partition"},
	)

	// Replication metrics
	replicationLagSeconds = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cronos_replication_lag_seconds",
			Help: "Replication lag in seconds between leader and followers per partition",
		},
		[]string{"partition", "follower"},
	)
	partitionLeaderLeaseStatus = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cronos_partition_leader_lease_status",
			Help: "Leader lease status (1=valid, 0=expired) per partition",
		},
		[]string{"partition"},
	)

	// Consumer group metrics
	consumerGroupLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cronos_consumer_group_lag",
			Help: "Consumer group lag (high_watermark - committed_offset) per group and partition",
		},
		[]string{"group", "partition"},
	)
	consumerGroupMembers = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cronos_consumer_group_members",
			Help: "Number of active members in consumer group",
		},
		[]string{"group"},
	)

	// WAL metrics
	walSegmentCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cronos_wal_segment_count",
			Help: "Number of WAL segments per partition",
		},
		[]string{"partition"},
	)
	walSegmentSizeBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cronos_wal_segment_size_bytes",
			Help: "Size of WAL segments in bytes per partition",
		},
		[]string{"partition"},
	)
	walHighWatermark = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cronos_wal_high_watermark",
			Help: "Current high watermark (last committed offset) per partition",
		},
		[]string{"partition"},
	)

	// Dedup metrics
	dedupBloomMemoryBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cronos_dedup_bloom_memory_bytes",
			Help: "Memory usage of bloom filter in bytes per partition",
		},
		[]string{"partition"},
	)
	dedupBloomFalsePositiveRate = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cronos_dedup_bloom_false_positive_rate",
			Help: "Current false positive rate of bloom filter per partition",
		},
		[]string{"partition"},
	)
	dedupPebbleHits = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cronos_dedup_pebble_hits_total",
			Help: "Total number of actual hits in PebbleDB dedup store per partition",
		},
		[]string{"partition"},
	)

	// Cluster metrics
	clusterNodesTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "cronos_cluster_nodes_total",
			Help: "Total number of nodes in the cluster",
		},
	)
	clusterNodesAlive = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "cronos_cluster_nodes_alive",
			Help: "Number of alive nodes in the cluster",
		},
	)
	clusterPartitionsTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "cronos_cluster_partitions_total",
			Help: "Total number of partitions in the cluster",
		},
	)
	clusterPartitionsLeader = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "cronos_cluster_partitions_leader",
			Help: "Number of partitions where this node is leader",
		},
	)
)

// SetTimingWheelMetrics sets timing wheel metrics for a partition
func SetTimingWheelMetrics(partitionID string, activeTimers int64, overflowLevel int64) {
	timingWheelActiveTimers.WithLabelValues(partitionID).Set(float64(activeTimers))
	timingWheelOverflowLevel.WithLabelValues(partitionID).Set(float64(overflowLevel))
}

// SetReplicationMetrics sets replication metrics for a partition
func SetReplicationMetrics(partitionID, followerID string, lagSeconds float64, leaseValid bool) {
	replicationLagSeconds.WithLabelValues(partitionID, followerID).Set(lagSeconds)
	leaseStatus := 0.0
	if leaseValid {
		leaseStatus = 1.0
	}
	partitionLeaderLeaseStatus.WithLabelValues(partitionID).Set(leaseStatus)
}

// SetConsumerGroupMetrics sets consumer group metrics
func SetConsumerGroupMetrics(groupID string, lag int64, memberCount int) {
	consumerGroupLag.WithLabelValues(groupID, "").Set(float64(lag))
	consumerGroupMembers.WithLabelValues(groupID).Set(float64(memberCount))
}

// SetWALMetrics sets WAL metrics for a partition
func SetWALMetrics(partitionID string, segmentCount int, segmentSizeBytes int64, highWatermark int64) {
	walSegmentCount.WithLabelValues(partitionID).Set(float64(segmentCount))
	walSegmentSizeBytes.WithLabelValues(partitionID).Set(float64(segmentSizeBytes))
	walHighWatermark.WithLabelValues(partitionID).Set(float64(highWatermark))
}

// SetDedupMetrics sets dedup metrics for a partition
func SetDedupMetrics(partitionID string, bloomMemoryBytes uint64, falsePositiveRate float64, pebbleHits uint64) {
	dedupBloomMemoryBytes.WithLabelValues(partitionID).Set(float64(bloomMemoryBytes))
	dedupBloomFalsePositiveRate.WithLabelValues(partitionID).Set(falsePositiveRate)
	dedupPebbleHits.WithLabelValues(partitionID).Set(float64(pebbleHits))
}

// SetClusterMetrics sets cluster-level metrics
func SetClusterMetrics(totalNodes, aliveNodes, totalPartitions, leaderPartitions int64) {
	clusterNodesTotal.Set(float64(totalNodes))
	clusterNodesAlive.Set(float64(aliveNodes))
	clusterPartitionsTotal.Set(float64(totalPartitions))
	clusterPartitionsLeader.Set(float64(leaderPartitions))
}

// ObserveWALAppend records WAL append latency
func ObserveWALAppend(partitionID string, d time.Duration) {
	walAppendLatency.WithLabelValues(partitionID).Observe(d.Seconds())
}

// ObserveDedupCheck records dedup check latency
func ObserveDedupCheck(partitionID string, path string, d time.Duration) {
	dedupCheckLatency.WithLabelValues(partitionID, path).Observe(d.Seconds())
}

// ObserveDispatch records dispatch latency
func ObserveDispatch(partitionID string, d time.Duration) {
	dispatchLatency.WithLabelValues(partitionID).Observe(d.Seconds())
}

// ObserveSegmentRotation records segment rotation latency
func ObserveSegmentRotation(partitionID string, d time.Duration) {
	segmentRotationLatency.WithLabelValues(partitionID).Observe(d.Seconds())
}
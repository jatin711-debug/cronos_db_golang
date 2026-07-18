// Package metrics registers Prometheus metrics and helpers used across CronosDB
// subsystems (API, WAL, dedup, delivery, cluster, and admission control).
//
// Hot-path helpers such as IncGRPCRequest cache labeled counters to avoid
// repeated WithLabelValues allocations under load.
package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type counterKey struct {
	method string
	status string
}

var (
	counterCache  sync.Map // key: counterKey, value: prometheus.Counter
	observerCache sync.Map // key: string (method), value: prometheus.Observer
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
		[]string{"partition", "path"},
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

var (
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

	// NOTE: the canonical, populated replication-lag metric is
	// `cronos_replication_lag` (in events), registered and updated by the
	// replication leader (internal/replication/leader.go). A former
	// `cronos_replication_lag_seconds` gauge + SetReplicationMetrics() existed here
	// with zero callers (dead) and were removed to avoid advertising a metric that
	// was never emitted.

	consumerGroupLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cronos_consumer_group_lag",
			Help: "Consumer group lag (high_watermark - committed_offset) per group and partition",
		},
		[]string{"group", "partition"},
	)

	dispatcherActiveDeliveries = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cronos_dispatcher_active_deliveries",
			Help: "Number of active in-flight deliveries in dispatcher",
		},
		[]string{"partition"},
	)
	dispatcherCreditsInUse = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cronos_dispatcher_credits_in_use",
			Help: "Credits currently consumed by subscriptions",
		},
		[]string{"partition"},
	)
	workerQueueDepth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cronos_delivery_worker_queue_depth",
			Help: "Number of events waiting in delivery worker queue",
		},
		[]string{"partition"},
	)
	dispatcherBackpressureSkips = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cronos_dispatcher_backpressure_skips_total",
			Help: "Deliveries skipped due to backpressure (no subscriber credits or in-flight cap), by reason",
		},
		[]string{"partition", "reason"},
	)
	consumerGroupMembers = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cronos_consumer_group_members",
			Help: "Number of active members in consumer group",
		},
		[]string{"group"},
	)

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

	clockSkewMs = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "cronos_clock_skew_ms",
			Help: "Absolute clock skew from leader in milliseconds",
		},
	)

	admissionRejectedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "cronos_admission_rejected_total",
			Help: "Total number of publishes rejected by admission control",
		},
	)
)

// IncGRPCRequest increments the gRPC request counter for the given method and status.
func IncGRPCRequest(method, status string) {
	key := counterKey{method: method, status: status}
	var counter prometheus.Counter
	if val, ok := counterCache.Load(key); ok {
		counter = val.(prometheus.Counter)
	} else {
		counter = grpcRequestsTotal.WithLabelValues(method, status)
		if actual, loaded := counterCache.LoadOrStore(key, counter); loaded {
			counter = actual.(prometheus.Counter)
		}
	}
	counter.Inc()
}

// ObserveGRPCRequestDuration records the duration of a gRPC request.
func ObserveGRPCRequestDuration(method string, duration float64) {
	var observer prometheus.Observer
	if val, ok := observerCache.Load(method); ok {
		observer = val.(prometheus.Observer)
	} else {
		observer = grpcRequestDuration.WithLabelValues(method)
		if actual, loaded := observerCache.LoadOrStore(method, observer); loaded {
			observer = actual.(prometheus.Observer)
		}
	}
	observer.Observe(duration)
}

// SanitizeMetricLabel prevents high-cardinality strings from becoming metric labels.
func SanitizeMetricLabel(method string) string {
	if len(method) > 256 {
		method = method[:256]
	}
	return method
}

// SetTimingWheelMetrics sets timing wheel metrics for a partition.
func SetTimingWheelMetrics(partitionID string, activeTimers int64, overflowLevel int64) {
	timingWheelActiveTimers.WithLabelValues(partitionID).Set(float64(activeTimers))
	timingWheelOverflowLevel.WithLabelValues(partitionID).Set(float64(overflowLevel))
}

// SetConsumerGroupMetrics sets consumer group metrics.
func SetConsumerGroupMetrics(groupID, partitionID string, lag int64, memberCount int) {
	consumerGroupLag.WithLabelValues(groupID, partitionID).Set(float64(lag))
	consumerGroupMembers.WithLabelValues(groupID).Set(float64(memberCount))
}

// SetWALMetrics sets WAL metrics for a partition.
func SetWALMetrics(partitionID string, segmentCount int, segmentSizeBytes int64, highWatermark int64) {
	walSegmentCount.WithLabelValues(partitionID).Set(float64(segmentCount))
	walSegmentSizeBytes.WithLabelValues(partitionID).Set(float64(segmentSizeBytes))
	walHighWatermark.WithLabelValues(partitionID).Set(float64(highWatermark))
}

// SetDedupMetrics sets dedup metrics for a partition.
func SetDedupMetrics(partitionID string, bloomMemoryBytes uint64, falsePositiveRate float64, pebbleHits uint64) {
	dedupBloomMemoryBytes.WithLabelValues(partitionID).Set(float64(bloomMemoryBytes))
	dedupBloomFalsePositiveRate.WithLabelValues(partitionID).Set(falsePositiveRate)
	dedupPebbleHits.WithLabelValues(partitionID).Set(float64(pebbleHits))
}

// SetClusterMetrics sets cluster-level metrics.
func SetClusterMetrics(totalNodes, aliveNodes, totalPartitions, leaderPartitions int64) {
	clusterNodesTotal.Set(float64(totalNodes))
	clusterNodesAlive.Set(float64(aliveNodes))
	clusterPartitionsTotal.Set(float64(totalPartitions))
	clusterPartitionsLeader.Set(float64(leaderPartitions))
}

// SetDeliveryMetrics sets delivery subsystem gauges for a partition.
func SetDeliveryMetrics(partitionID string, activeDeliveries int64, creditsInUse int64, queueDepth int64) {
	dispatcherActiveDeliveries.WithLabelValues(partitionID).Set(float64(activeDeliveries))
	dispatcherCreditsInUse.WithLabelValues(partitionID).Set(float64(creditsInUse))
	workerQueueDepth.WithLabelValues(partitionID).Set(float64(queueDepth))
}

// IncDispatcherBackpressureSkip records a delivery skipped due to backpressure.
// reason is a low-cardinality label such as "no_credits" or "in_flight_cap".
func IncDispatcherBackpressureSkip(partitionID, reason string, n int) {
	if n <= 0 {
		n = 1
	}
	dispatcherBackpressureSkips.WithLabelValues(partitionID, reason).Add(float64(n))
}

// ObserveWALAppend records WAL append latency.
func ObserveWALAppend(partitionID string, d time.Duration) {
	walAppendLatency.WithLabelValues(partitionID).Observe(d.Seconds())
}

// ObserveDedupCheck records dedup check latency.
func ObserveDedupCheck(partitionID string, path string, d time.Duration) {
	dedupCheckLatency.WithLabelValues(partitionID, path).Observe(d.Seconds())
}

// ObserveDispatch records dispatch latency.
func ObserveDispatch(partitionID string, d time.Duration) {
	dispatchLatency.WithLabelValues(partitionID).Observe(d.Seconds())
}

// ObserveSegmentRotation records segment rotation latency.
func ObserveSegmentRotation(partitionID string, d time.Duration) {
	segmentRotationLatency.WithLabelValues(partitionID).Observe(d.Seconds())
}

// SetClockSkew records clock skew from leader.
func SetClockSkew(skewMs int64) {
	clockSkewMs.Set(float64(skewMs))
}

// IncAdmissionRejected increments the admission rejected counter.
func IncAdmissionRejected() {
	admissionRejectedTotal.Inc()
}

package metrics

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/sysmem"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Accounting: what became of the events a node took in.
//
// The counters follow an event from the publish that was acknowledged to the
// delivery that was acknowledged, so that a rate that does not add up shows
// where events are waiting or going wrong: accepted but not delivered, delivered
// but not acknowledged, delivered again and again, or dead-lettered. They count
// on the node that did the work, which for all of them is the partition's
// leader, and they start at zero when a node starts; compare rates, not totals.

// perPartition is a counter with a partition label. Looking a label up takes a
// lock and an allocation, which is too much for every delivery, so the counter
// of each partition is kept.
type perPartition struct {
	vec     *prometheus.CounterVec
	curried prometheus.Labels
	cache   sync.Map // int32 -> prometheus.Counter
}

func (p *perPartition) add(partitionID int32, n int) {
	if n <= 0 {
		return
	}
	counter, ok := p.cache.Load(partitionID)
	if !ok {
		labels := prometheus.Labels{"partition": strconv.FormatInt(int64(partitionID), 10)}
		for name, value := range p.curried {
			labels[name] = value
		}
		counter, _ = p.cache.LoadOrStore(partitionID, p.vec.With(labels))
	}
	counter.(prometheus.Counter).Add(float64(n))
}

var (
	eventsAccepted = &perPartition{vec: promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cronos_events_accepted_total",
		Help: "Events whose publish was acknowledged to the producer",
	}, []string{"partition"})}

	eventsDuplicate = &perPartition{vec: promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cronos_events_duplicate_total",
		Help: "Publishes answered as duplicates of an event already in the log",
	}, []string{"partition"})}

	eventsDeliveredVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cronos_events_delivered_total",
		Help: "Events sent to a consumer; attempt is first for a new delivery and retry for a delivery sent again after a timeout or a failed acknowledgement",
	}, []string{"partition", "attempt"})
	eventsDeliveredFirst = &perPartition{vec: eventsDeliveredVec, curried: prometheus.Labels{"attempt": "first"}}
	eventsDeliveredRetry = &perPartition{vec: eventsDeliveredVec, curried: prometheus.Labels{"attempt": "retry"}}

	eventsAckedVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cronos_events_acknowledged_total",
		Help: "Delivered events a consumer answered for; result is success or failure",
	}, []string{"partition", "result"})
	eventsAckedSuccess = &perPartition{vec: eventsAckedVec, curried: prometheus.Labels{"result": "success"}}
	eventsAckedFailure = &perPartition{vec: eventsAckedVec, curried: prometheus.Labels{"result": "failure"}}

	eventsTimedOut = &perPartition{vec: promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cronos_events_delivery_timeouts_total",
		Help: "Delivered events whose acknowledgement did not arrive within the ack timeout",
	}, []string{"partition"})}

	eventsDeadLettered = &perPartition{vec: promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cronos_events_dead_lettered_total",
		Help: "Events moved to the dead-letter queue after their deliveries ran out of retries",
	}, []string{"partition"})}

	walSegmentsRemoved = &perPartition{vec: promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cronos_wal_segments_removed_total",
		Help: "Log segments removed because every event in them was finished",
	}, []string{"partition"})}

	deliveryLateness = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cronos_delivery_lateness_seconds",
		Help:    "How long after its scheduled time an event was first sent to a consumer",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 1800},
	}, []string{"partition"})
	deliveryLatenessByPartition sync.Map // int32 -> prometheus.Observer

	walLogStartOffset = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cronos_wal_log_start_offset",
		Help: "Offset of the first event the partition's log still holds",
	}, []string{"partition"})

	changeFeedOffset = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cronos_change_feed_offset",
		Help: "Offset of the last event the partition's change feed has exported; compare with cronos_wal_high_watermark for its lag",
	}, []string{"partition"})

	// Memory is measured when it is asked for: it moves faster than the
	// other gauges are refreshed.
	_ = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cronos_memory_held_bytes",
		Help: "Memory the process holds and cannot hand back: anonymous resident memory, less what the Go runtime keeps for reuse",
	}, func() float64 { return float64(sysmem.Used()) })

	_ = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cronos_memory_limit_bytes",
		Help: "Memory the process may use: the container's limit, the configured one, or the machine's memory. Publishes are refused at --max-memory-percent of it",
	}, func() float64 { return float64(memoryLimit.Load()) })

	memoryLimit atomic.Uint64
)

// AddEventsAccepted counts events whose publish was acknowledged.
func AddEventsAccepted(partitionID int32, n int) { eventsAccepted.add(partitionID, n) }

// AddEventsDuplicate counts publishes answered as duplicates.
func AddEventsDuplicate(partitionID int32, n int) { eventsDuplicate.add(partitionID, n) }

// AddEventsDelivered counts events sent to a consumer, for the first time or
// again.
func AddEventsDelivered(partitionID int32, n int, retry bool) {
	if retry {
		eventsDeliveredRetry.add(partitionID, n)
		return
	}
	eventsDeliveredFirst.add(partitionID, n)
}

// AddEventsAcknowledged counts delivered events a consumer answered for.
func AddEventsAcknowledged(partitionID int32, n int, success bool) {
	if success {
		eventsAckedSuccess.add(partitionID, n)
		return
	}
	eventsAckedFailure.add(partitionID, n)
}

// AddEventsTimedOut counts delivered events whose acknowledgement did not
// arrive in time.
func AddEventsTimedOut(partitionID int32, n int) { eventsTimedOut.add(partitionID, n) }

// AddEventsDeadLettered counts events moved to the dead-letter queue.
func AddEventsDeadLettered(partitionID int32, n int) { eventsDeadLettered.add(partitionID, n) }

// AddSegmentsRemoved counts log segments removed by pruning.
func AddSegmentsRemoved(partitionID int32, n int) { walSegmentsRemoved.add(partitionID, n) }

// ObserveDeliveryLateness records how long after its scheduled time, in Unix
// milliseconds, an event was first sent to a consumer.
func ObserveDeliveryLateness(partitionID int32, scheduleTS int64, sent time.Time) {
	observer, ok := deliveryLatenessByPartition.Load(partitionID)
	if !ok {
		observer, _ = deliveryLatenessByPartition.LoadOrStore(partitionID,
			deliveryLateness.WithLabelValues(strconv.FormatInt(int64(partitionID), 10)))
	}
	late := float64(sent.UnixMilli()-scheduleTS) / 1000
	observer.(prometheus.Observer).Observe(max(late, 0))
}

// SetMemoryLimit sets how much memory the process may use.
func SetMemoryLimit(limit uint64) { memoryLimit.Store(limit) }

// SetLogStart sets where a partition's log starts.
func SetLogStart(partitionID string, logStart int64) {
	walLogStartOffset.WithLabelValues(partitionID).Set(float64(logStart))
}

// SetChangeFeedOffset sets how far a partition's change feed has got.
func SetChangeFeedOffset(partitionID string, offset int64) {
	changeFeedOffset.WithLabelValues(partitionID).Set(float64(offset))
}

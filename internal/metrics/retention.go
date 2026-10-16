package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"strconv"
)

var retentionPinOffset = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "cronos_retention_blocking_offset", Help: "Oldest offset blocking the last pruning pass, or -1 when no blocker was observed",
}, []string{"partition", "reason"})
var retentionPinAge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "cronos_retention_blocking_age_seconds", Help: "Age of the oldest event blocking the last pruning pass",
}, []string{"partition", "reason"})

func SetRetentionBlocker(partition int32, offset int64, reason string, ageSeconds float64) {
	id := strconv.FormatInt(int64(partition), 10)
	for _, known := range []string{"unaccepted", "change_feed", "future_schedule", "consumer_completion", "dead_letter_replication"} {
		value, age := float64(-1), float64(0)
		if known == reason {
			value, age = float64(offset), max(ageSeconds, 0)
		}
		retentionPinOffset.WithLabelValues(id, known).Set(value)
		retentionPinAge.WithLabelValues(id, known).Set(age)
	}
}

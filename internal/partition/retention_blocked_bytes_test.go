package partition

import (
	"context"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
)

// gauge returns the value of a gauge with the given labels, or -1.
func gauge(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
	metrics:
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if want, ok := labels[label.GetName()]; ok && want != label.GetValue() {
					continue metrics
				}
			}
			return metric.GetGauge().GetValue()
		}
	}
	return -1
}

// When an event that is not due yet stops retention, the pass reports how
// many bytes of closed segments it had to keep from that event's segment on,
// so that an alert can say what the event costs before the disk fills.
func TestRetentionReportsBytesHeldByAFutureEvent(t *testing.T) {
	pm := maintenanceManager(t, false)
	p, _ := pm.GetInternalPartition(0)
	if err := p.ConsumerGroup.CreateGroup("g", "a", []int32{0}); err != nil {
		t.Fatal(err)
	}
	finished := appendMaintenanceEvent(t, p, "finished", "a", time.Now().Add(-time.Hour), 2048)
	due := time.Now().Add(3 * time.Second)
	future := appendMaintenanceEvent(t, p, "future", "a", due, 2048)
	var later []*types.Event
	for _, id := range []string{"later-1", "later-2", "later-3"} {
		later = append(later, appendMaintenanceEvent(t, p, id, "a", time.Now().Add(-time.Hour), 2048))
	}
	if err := p.ConsumerGroup.CommitDelivery("g", 0, append([]*types.Event{finished}, later...)); err != nil {
		t.Fatal(err)
	}

	if _, err := p.PruneWAL(context.Background(), storage.PruneOptions{AllCompleted: true}); err != nil {
		t.Fatal(err)
	}
	segments := p.Wal.GetSegments()
	var want int64
	for _, seg := range segments[:len(segments)-1] {
		if seg.GetLastOffset() >= future.Offset {
			want += seg.GetSize()
		}
	}
	if want == 0 {
		t.Fatal("the test needs closed segments after the future event")
	}
	labels := map[string]string{"partition": "0", "reason": "future_schedule"}
	if got := gauge(t, "cronos_retention_blocked_bytes", labels); got != float64(want) {
		t.Fatalf("cronos_retention_blocked_bytes = %v, want %d", got, want)
	}
	if got := gauge(t, "cronos_retention_blocking_offset", labels); got != float64(future.Offset) {
		t.Fatalf("cronos_retention_blocking_offset = %v, want %d", got, future.Offset)
	}

	// Once the event is due and finished, nothing is held for it.
	time.Sleep(time.Until(due) + 50*time.Millisecond)
	if err := p.ConsumerGroup.CommitDelivery("g", 0, []*types.Event{future}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.PruneWAL(context.Background(), storage.PruneOptions{AllCompleted: true}); err != nil {
		t.Fatal(err)
	}
	if got := gauge(t, "cronos_retention_blocked_bytes", labels); got > 0 {
		t.Fatalf("cronos_retention_blocked_bytes = %v after the event finished", got)
	}
}

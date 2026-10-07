package api

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// metricTotal returns the value of a counter, or the sample count of a
// histogram, with the given name and labels.
func metricTotal(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	total := 0.0
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if !hasLabels(metric, labels) {
				continue
			}
			switch {
			case metric.GetCounter() != nil:
				total += metric.GetCounter().GetValue()
			case metric.GetHistogram() != nil:
				total += float64(metric.GetHistogram().GetSampleCount())
			}
		}
	}
	return total
}

func hasLabels(metric *dto.Metric, want map[string]string) bool {
	for name, value := range want {
		found := false
		for _, label := range metric.GetLabel() {
			if label.GetName() == name && label.GetValue() == value {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// The accounting counters follow an event from the acknowledged publish to
// the acknowledged delivery, so that rates that do not add up show where
// events are waiting.
func TestAccountingMetricsFollowAnEvent(t *testing.T) {
	const total = 40
	partition := map[string]string{"partition": "0"}
	read := func() map[string]float64 {
		return map[string]float64{
			"accepted":  metricTotal(t, "cronos_events_accepted_total", partition),
			"duplicate": metricTotal(t, "cronos_events_duplicate_total", partition),
			"delivered": metricTotal(t, "cronos_events_delivered_total", map[string]string{"partition": "0", "attempt": "first"}),
			"acked":     metricTotal(t, "cronos_events_acknowledged_total", map[string]string{"partition": "0", "result": "success"}),
			"lateness":  metricTotal(t, "cronos_delivery_lateness_seconds", partition),
		}
	}
	before := read()
	grew := func(name string) float64 { return read()[name] - before[name] }

	_, addr := startPartitionedServer(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, producer := dialOrders(t, ctx, addr)

	publishOrders(t, ctx, producer, "counted", total)
	if got := grew("accepted"); got != total {
		t.Fatalf("%v events counted as accepted, want %d", got, total)
	}
	if got := grew("delivered"); got != 0 {
		t.Fatalf("%v events counted as delivered before anyone subscribed", got)
	}

	received := countDeliveries(ctx, c, "accounting-group", 100)
	waitForCount(t, received, total, 20*time.Second, "deliveries")
	deadline := time.Now().Add(10 * time.Second)
	for grew("acked") < total && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if delivered, acked, lateness := grew("delivered"), grew("acked"), grew("lateness"); delivered < total || acked != total || lateness != delivered {
		t.Fatalf("after %d events were consumed: %v counted as delivered, %v as acknowledged, %v lateness samples; want at least %d, %d, and as many samples as deliveries",
			total, delivered, acked, lateness, total, total)
	}
	if got := grew("accepted"); got != total {
		t.Fatalf("%v events counted as accepted after delivery, want still %d", got, total)
	}
}

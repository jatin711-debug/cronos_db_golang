package partition

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/sysmem"
)

// monitorOf builds a monitor over a number the test controls, with a limit
// of 1000 bytes and refusal at 80%.
func monitorOf(held *atomic.Uint64) *MemoryMonitor {
	return &MemoryMonitor{maxPercent: 80, limit: 1000, checkInterval: 0, measure: func() sysmem.Sample { return sysmem.Sample{Held: held.Load()} }}
}

// Publishes are refused while the process holds its share of its own limit,
// and accepted again when it holds less. The limit is this process's: it does
// not matter how much memory the machine has or what else runs on it.
func TestMemoryMonitor_RefusesAtItsShareOfTheLimit(t *testing.T) {
	var held atomic.Uint64
	m := monitorOf(&held)
	bp := &BackpressureManager{memoryMonitor: m, rateLimiters: map[int32]*TokenBucket{}}

	held.Store(799)
	if !bp.CanAcceptN(0, 10) {
		t.Fatal("publishes refused below 80% of the limit")
	}
	held.Store(800)
	if bp.CanAcceptN(0, 10) {
		t.Fatal("publishes accepted at 80% of the limit")
	}
	if used, limit := bp.MemoryUsage(); used != 800 || limit != 1000 {
		t.Fatalf("usage reported as %d of %d, want 800 of 1000", used, limit)
	}
	held.Store(500)
	if !bp.CanAcceptN(0, 10) {
		t.Fatal("publishes still refused after memory was given up")
	}
}

// A measurement above the threshold is taken again after a collection: what
// the collector had not got to yet is no reason to refuse work.
func TestMemoryMonitor_CollectsBeforeItRefuses(t *testing.T) {
	asked := 0
	m := &MemoryMonitor{maxPercent: 80, limit: 1000, measure: func() sysmem.Sample {
		asked++
		if asked == 1 {
			return sysmem.Sample{Held: 950} // with garbage
		}
		return sysmem.Sample{Held: 300} // after the collection
	}}
	if m.IsOverLimit() {
		t.Fatal("publishes refused for memory that a collection frees")
	}
	if asked != 2 {
		t.Fatalf("memory was measured %d times, want once before and once after a collection", asked)
	}

	// Not again for a while: a node at its limit must not collect on every
	// measurement.
	m.measure = func() sysmem.Sample { asked++; return sysmem.Sample{Held: 950} }
	if !m.IsOverLimit() {
		t.Fatal("publishes accepted above the threshold")
	}
	if asked != 3 {
		t.Fatalf("memory was measured %d times; a second collection ran within %s of the first", asked, collectInterval)
	}
}

// A measurement is used for the check interval and not taken on every publish.
func TestMemoryMonitor_KeepsAMeasurementForTheInterval(t *testing.T) {
	var held atomic.Uint64
	m := monitorOf(&held)
	m.checkInterval = time.Hour
	held.Store(100)
	if m.IsOverLimit() {
		t.Fatal("refused at 10% of the limit")
	}
	held.Store(999)
	if m.IsOverLimit() {
		t.Fatal("a new measurement was taken within the check interval")
	}
}

// The Go runtime is told to collect before the process reaches the threshold,
// and what other libraries hold is taken off what the runtime may use.
// Otherwise garbage alone takes a node over the threshold.
func TestMemoryMonitor_HasTheRuntimeCollectBelowTheThreshold(t *testing.T) {
	const gib = int64(1) << 30
	sample := sysmem.Sample{Held: 1 << 30}
	var told []int64
	m := &MemoryMonitor{maxPercent: 80, limit: uint64(10 * gib),
		measure:         func() sysmem.Sample { return sample },
		setRuntimeLimit: func(bytes int64) { told = append(told, bytes) },
	}
	m.IsOverLimit()
	threshold := 8 * gib
	if len(told) != 1 || told[0] >= threshold || told[0] < threshold*8/10 {
		t.Fatalf("the runtime was told %v; want one limit a little below the threshold of %d", told, threshold)
	}

	// Three more gibibytes are held outside the runtime: it gets that much less.
	sample.Outside = uint64(3 * gib)
	m.IsOverLimit()
	if len(told) != 2 || told[1] != told[0]-3*gib {
		t.Fatalf("with 3 GiB held outside the runtime it was told %v; want %d less than before", told, 3*gib)
	}

	// A measurement that changes little is no reason to tell it again.
	sample.Outside += 1 << 20
	m.IsOverLimit()
	if len(told) != 2 {
		t.Fatalf("the runtime was told again for a change of 1 MiB: %v", told)
	}
}

func TestMemoryMonitor_OffWhenAskedOrWithoutALimit(t *testing.T) {
	// With this set the monitor leaves the runtime of this test process alone.
	t.Setenv("GOMEMLIMIT", "off")
	if NewMemoryMonitor(0, 1000, 1<<30) != nil {
		t.Fatal("a monitor was created with the share set to 0")
	}
	var none *MemoryMonitor
	if none.IsOverLimit() {
		t.Fatal("a monitor that is off refused publishes")
	}
	if m := NewMemoryMonitor(80, 1000, 1<<30); m == nil || m.limit != 1<<30 {
		t.Fatalf("a configured limit of 1 GiB gave %+v", m)
	}
}

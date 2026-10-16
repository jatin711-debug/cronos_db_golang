package partition

import (
	"errors"
	"testing"
)

// The guard refuses publishes from its limit on, and takes them again only
// two points below it, so that usage at the limit does not switch it back and
// forth. A failed reading changes nothing.
func TestDiskGuard_RefusesAtTheLimitWithHysteresis(t *testing.T) {
	g := NewDiskGuard(t.TempDir(), 0.90)
	var used float64
	var readErr error
	g.usage = func() (float64, error) { return used, readErr }

	steps := []struct {
		used    float64
		readErr error
		refuses bool
	}{
		{0.50, nil, false},
		{0.899, nil, false},
		{0.90, nil, true},
		{0.89, nil, true}, // within the gap
		{0.881, nil, true},
		{0.10, errors.New("statfs failed"), true}, // a failed reading changes nothing
		{0.879, nil, false},
		{0.89, nil, false},
		{0.95, nil, true},
	}
	for i, step := range steps {
		used, readErr = step.used, step.readErr
		g.sample()
		if refuses := g.Check() != nil; refuses != step.refuses {
			t.Fatalf("step %d (used %.3f, error %v): refuses=%v, want %v", i, step.used, step.readErr, refuses, step.refuses)
		}
	}

	var none *DiskGuard
	if err := none.Check(); err != nil {
		t.Fatalf("no guard refused: %v", err)
	}
}

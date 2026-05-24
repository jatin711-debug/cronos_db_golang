package retry

import (
	"math"
	"math/rand"
	"time"
)

// Policy controls client-side retry backoff.
type Policy struct {
	MaxAttempts int
	MinBackoff  time.Duration
	MaxBackoff  time.Duration
	Jitter      float64
}

// DefaultPolicy returns conservative retry defaults.
func DefaultPolicy() Policy {
	return Policy{
		MaxAttempts: 4,
		MinBackoff:  50 * time.Millisecond,
		MaxBackoff:  2 * time.Second,
		Jitter:      0.2,
	}
}

// Backoff returns a duration for the given attempt number (0-based).
func (p Policy) Backoff(attempt int) time.Duration {
	if attempt <= 0 {
		return p.MinBackoff
	}

	base := float64(p.MinBackoff) * math.Pow(2, float64(attempt))
	if base > float64(p.MaxBackoff) {
		base = float64(p.MaxBackoff)
	}

	if p.Jitter > 0 {
		// Randomize by +/- jitter%.
		delta := base * p.Jitter
		base = base - delta + rand.Float64()*(2*delta)
	}

	if base < float64(time.Millisecond) {
		base = float64(time.Millisecond)
	}
	return time.Duration(base)
}

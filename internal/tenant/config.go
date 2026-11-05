package tenant

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
)

// LoadRateLimits loads operator-managed per-node rate limits at startup.
// Durable storage and unfinished-work quotas are deliberately not exposed:
// their usage counters are not reconstructed from the logs after restart.
func (a *Accountant) LoadRateLimits(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read tenant limits: %w", err)
	}
	var limits map[string]struct {
		Rate float64 `json:"max_events_per_second"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&limits); err != nil {
		return fmt.Errorf("decode tenant limits: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("tenant limits must contain one JSON object")
	}
	if len(limits) == 0 {
		return fmt.Errorf("tenant limits must contain at least one subject")
	}
	for subject, limit := range limits {
		if strings.TrimSpace(subject) == "" || len(subject) > 255 || math.IsNaN(limit.Rate) || math.IsInf(limit.Rate, 0) || limit.Rate < 1 {
			return fmt.Errorf("tenant limits require a subject of 1–255 bytes and max_events_per_second >= 1")
		}
	}
	// Apply only after the complete configuration has passed validation.
	for subject, limit := range limits {
		a.SetLimits(ID(subject), Limits{MaxEventsPerSecond: limit.Rate})
	}
	return nil
}

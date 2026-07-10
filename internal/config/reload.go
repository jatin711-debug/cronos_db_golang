package config

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
)

// ReloadableConfig wraps a Config with atomic value access for hot reload.
// Only specific fields can be updated without restart; critical fields
// (data-dir, partition-count, cluster seeds) require a restart.
type ReloadableConfig struct {
	mu     sync.RWMutex
	config *types.Config

	// Callbacks invoked when specific fields change
	onChange []func(old, new *types.Config)
}

// NewReloadableConfig creates a reloadable configuration wrapper.
func NewReloadableConfig(cfg *types.Config) *ReloadableConfig {
	return &ReloadableConfig{
		config: cfg,
	}
}

// Get returns a copy of the current configuration (safe for reads).
func (rc *ReloadableConfig) Get() types.Config {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return *rc.config
}

// RegisterCallback adds a callback invoked on every successful reload.
func (rc *ReloadableConfig) RegisterCallback(fn func(old, new *types.Config)) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.onChange = append(rc.onChange, fn)
}

// Reload reads environment variables and updates reloadable fields.
// Returns true if any field changed.
func (rc *ReloadableConfig) Reload() bool {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	old := *rc.config
	changed := false

	// Reloadable fields (safe to change at runtime)
	if v := os.Getenv("CRONOS_MAX_CREDITS"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed != rc.config.MaxDeliveryCredits {
			rc.config.MaxDeliveryCredits = parsed
			changed = true
		}
	}
	if v := os.Getenv("CRONOS_ACK_TIMEOUT"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			newTimeout := time.Duration(parsed) * time.Second
			if newTimeout != rc.config.DefaultAckTimeout {
				rc.config.DefaultAckTimeout = newTimeout
				changed = true
			}
		}
	}
	if v := os.Getenv("CRONOS_MAX_RETRIES"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed != rc.config.MaxRetries {
			rc.config.MaxRetries = parsed
			changed = true
		}
	}
	if v := os.Getenv("CRONOS_LOAD_SHEDDING_THRESHOLD"); v != "" {
		if parsed, err := strconv.ParseFloat(v, 64); err == nil && parsed != rc.config.LoadSheddingThreshold {
			rc.config.LoadSheddingThreshold = parsed
			changed = true
		}
	}
	if v := os.Getenv("CRONOS_CB_FAILURE_THRESHOLD"); v != "" {
		if parsed, err := strconv.ParseFloat(v, 64); err == nil && parsed != rc.config.CircuitBreakerFailureThreshold {
			rc.config.CircuitBreakerFailureThreshold = parsed
			changed = true
		}
	}
	if v := os.Getenv("CRONOS_TRACING_SAMPLE_RATIO"); v != "" {
		if parsed, err := strconv.ParseFloat(v, 64); err == nil && parsed != rc.config.TracingSampleRatio {
			rc.config.TracingSampleRatio = parsed
			changed = true
		}
	}
	if v := os.Getenv("CRONOS_STATS_PRINT_INTERVAL_MS"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			newInterval := time.Duration(parsed) * time.Millisecond
			if newInterval != rc.config.StatsPrintInterval {
				rc.config.StatsPrintInterval = newInterval
				changed = true
			}
		}
	}

	if changed {
		slog.Info("Config reloaded from environment", "changed_fields", diffConfig(&old, rc.config))
		for _, cb := range rc.onChange {
			cb(&old, rc.config)
		}
	}
	return changed
}

// StartSIGHUPListener starts a goroutine that reloads config on SIGHUP.
func (rc *ReloadableConfig) StartSIGHUPListener(ctx context.Context) {
	utils.GoSafe("config-sighup", func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGHUP)
		defer signal.Stop(sigCh)

		for {
			select {
			case <-sigCh:
				rc.Reload()
			case <-ctx.Done():
				return
			}
		}
	})
}

func diffConfig(old, new *types.Config) []string {
	var diffs []string
	if old.MaxDeliveryCredits != new.MaxDeliveryCredits {
		diffs = append(diffs, fmt.Sprintf("max_credits: %d -> %d", old.MaxDeliveryCredits, new.MaxDeliveryCredits))
	}
	if old.DefaultAckTimeout != new.DefaultAckTimeout {
		diffs = append(diffs, fmt.Sprintf("ack_timeout: %v -> %v", old.DefaultAckTimeout, new.DefaultAckTimeout))
	}
	if old.MaxRetries != new.MaxRetries {
		diffs = append(diffs, fmt.Sprintf("max_retries: %d -> %d", old.MaxRetries, new.MaxRetries))
	}
	if old.LoadSheddingThreshold != new.LoadSheddingThreshold {
		diffs = append(diffs, fmt.Sprintf("load_shedding: %.2f -> %.2f", old.LoadSheddingThreshold, new.LoadSheddingThreshold))
	}
	if old.CircuitBreakerFailureThreshold != new.CircuitBreakerFailureThreshold {
		diffs = append(diffs, fmt.Sprintf("cb_threshold: %.2f -> %.2f", old.CircuitBreakerFailureThreshold, new.CircuitBreakerFailureThreshold))
	}
	if old.TracingSampleRatio != new.TracingSampleRatio {
		diffs = append(diffs, fmt.Sprintf("trace_ratio: %.4f -> %.4f", old.TracingSampleRatio, new.TracingSampleRatio))
	}
	return diffs
}

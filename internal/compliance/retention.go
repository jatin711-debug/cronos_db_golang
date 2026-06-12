package compliance

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// RetentionPolicy defines data lifecycle rules.
type RetentionPolicy struct {
	MaxAge       time.Duration // Delete events older than this
	MaxSizeBytes int64         // Delete oldest segments when total size exceeds this
}

// Enforcer applies retention policies to partition data.
type Enforcer struct {
	dataDir  string
	policy   RetentionPolicy
}

// NewEnforcer creates a retention enforcer.
func NewEnforcer(dataDir string, policy RetentionPolicy) *Enforcer {
	return &Enforcer{
		dataDir: dataDir,
		policy:  policy,
	}
}

// Run executes retention policy once.
func (e *Enforcer) Run(ctx context.Context) error {
	if e.policy.MaxAge > 0 {
		if err := e.enforceAge(ctx); err != nil {
			return fmt.Errorf("age enforcement: %w", err)
		}
	}
	if e.policy.MaxSizeBytes > 0 {
		if err := e.enforceSize(ctx); err != nil {
			return fmt.Errorf("size enforcement: %w", err)
		}
	}
	return nil
}

func isProtectedDir(name string) bool {
	return name == "raft" || name == "pebble" || name == "dedup" || name == "offsets" || name == "scheduler" || name == "cold_store" || name == "index" || name == "backups"
}

func isWALSegment(path string) bool {
	if filepath.Ext(path) != ".log" {
		return false
	}
	return filepath.Base(filepath.Dir(path)) == "segments"
}

func (e *Enforcer) enforceAge(ctx context.Context) error {
	cutoff := time.Now().Add(-e.policy.MaxAge)
	return filepath.Walk(e.dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			if isProtectedDir(info.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !isWALSegment(path) {
			return nil
		}
		if info.ModTime().Before(cutoff) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err := os.Remove(path); err != nil {
				slog.Warn("Retention age delete failed", "path", path, "error", err)
			} else {
				slog.Info("Retention: deleted old file", "path", path, "age", time.Since(info.ModTime()))
			}
		}
		return nil
	})
}

func (e *Enforcer) enforceSize(ctx context.Context) error {
	var total int64
	var files []struct {
		path string
		info os.FileInfo
	}

	_ = filepath.Walk(e.dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			if isProtectedDir(info.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !isWALSegment(path) {
			return nil
		}
		total += info.Size()
		files = append(files, struct {
			path string
			info os.FileInfo
		}{path, info})
		return nil
	})

	if total <= e.policy.MaxSizeBytes {
		return nil
	}

	// Sort by modification time (oldest first) — simple deletion
	// In production, use a min-heap
	for i := 0; i < len(files)-1; i++ {
		for j := i + 1; j < len(files); j++ {
			if files[j].info.ModTime().Before(files[i].info.ModTime()) {
				files[i], files[j] = files[j], files[i]
			}
		}
	}

	for _, f := range files {
		if total <= e.policy.MaxSizeBytes {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := os.Remove(f.path); err != nil {
			slog.Warn("Retention size delete failed", "path", f.path, "error", err)
		} else {
			total -= f.info.Size()
			slog.Info("Retention: deleted file for size limit", "path", f.path)
		}
	}
	return nil
}

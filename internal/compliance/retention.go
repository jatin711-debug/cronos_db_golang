// Package compliance enforces data lifecycle policies such as WAL segment
// retention by age and total size.
//
// Enforcer walks the data directory, never deletes the active (highest-offset)
// segment per partition, and removes matching sparse index files alongside segments.
package compliance

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// RetentionPolicy defines data lifecycle rules for WAL segments.
type RetentionPolicy struct {
	// MaxAge deletes non-active segments older than this duration; 0 disables age retention.
	MaxAge time.Duration
	// MaxSizeBytes deletes oldest non-active segments until total size fits; 0 disables.
	MaxSizeBytes int64
}

// segmentInfo holds parsed metadata for a WAL segment file.
type segmentInfo struct {
	path        string
	firstOffset int64
	createdTS   int64
	size        int64
}

// Enforcer applies RetentionPolicy to partition WAL data under dataDir.
type Enforcer struct {
	dataDir string
	policy  RetentionPolicy
	remove  func(context.Context, string) (bool, error)
}

// NewEnforcer is for offline directories only. Live WALs must use
// NewManagedEnforcer to synchronize deletion with readers and completion state.
func NewEnforcer(dataDir string, policy RetentionPolicy) *Enforcer {
	return &Enforcer{
		dataDir: dataDir,
		policy:  policy,
	}
}

// NewManagedEnforcer delegates deletion to the owning live WAL. The callback
// must verify completion and return false when a segment must be retained.
func NewManagedEnforcer(dataDir string, policy RetentionPolicy, remove func(context.Context, string) (bool, error)) *Enforcer {
	if remove == nil {
		remove = func(context.Context, string) (bool, error) {
			return false, fmt.Errorf("retention requires a WAL owner")
		}
	}
	return &Enforcer{dataDir: dataDir, policy: policy, remove: remove}
}

type RetentionStats struct {
	SegmentsDeleted int64
	BytesFreed      int64
}

func (e *Enforcer) Run(ctx context.Context) error {
	_, err := e.RunWithStats(ctx)
	return err
}

// RunWithStats counts active and protected segments toward the size budget,
// and subtracts each successfully deleted segment exactly once across policies.
func (e *Enforcer) RunWithStats(ctx context.Context) (RetentionStats, error) {
	var stats RetentionStats
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	if e.policy.MaxAge <= 0 && e.policy.MaxSizeBytes <= 0 {
		return stats, nil
	}
	segments, err := e.collectSegments()
	if err != nil {
		return stats, fmt.Errorf("collect segments: %w", err)
	}
	active := make(map[string]int64)
	var total int64
	for _, s := range segments {
		dir := filepath.Dir(s.path)
		if offset, ok := active[dir]; !ok || s.firstOffset > offset {
			active[dir] = s.firstOffset
		}
		total += s.size
	}
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].createdTS != segments[j].createdTS {
			return segments[i].createdTS < segments[j].createdTS
		}
		return segments[i].path < segments[j].path
	})
	cutoff := time.Now().Add(-e.policy.MaxAge).UnixMilli()
	for _, s := range segments {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if s.firstOffset == active[filepath.Dir(s.path)] {
			continue
		}
		aged := e.policy.MaxAge > 0 && s.createdTS < cutoff
		oversized := e.policy.MaxSizeBytes > 0 && total > e.policy.MaxSizeBytes
		if !aged && !oversized {
			continue
		}
		removed := false
		if e.remove != nil {
			removed, err = e.remove(ctx, s.path)
		} else {
			err = e.removeSegment(s)
			removed = err == nil
		}
		if err != nil {
			return stats, fmt.Errorf("retain %s: %w", s.path, err)
		}
		if removed {
			total -= s.size
			stats.SegmentsDeleted++
			stats.BytesFreed += s.size
		}
	}
	return stats, nil
}

// collectSegments walks the data directory and returns all valid WAL segment files.
func (e *Enforcer) collectSegments() ([]segmentInfo, error) {
	var segments []segmentInfo

	err := filepath.Walk(e.dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
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

		firstOffset, createdTS, ok := readSegmentHeader(path)
		if !ok {
			// Fallback: derive metadata from filename and file mtime so retention
			// still works on segments with missing/corrupt headers.
			firstOffset, createdTS, ok = fallbackSegmentMeta(path, info)
			if !ok {
				slog.Warn("Retention: skipping segment with invalid header", "path", path)
				return nil
			}
		}

		segments = append(segments, segmentInfo{
			path:        path,
			firstOffset: firstOffset,
			createdTS:   createdTS,
			size:        info.Size(),
		})
		return nil
	})

	return segments, err
}

// removeSegment deletes a segment file and its matching sparse index file.
func (e *Enforcer) removeSegment(s segmentInfo) error {
	if err := os.Remove(s.path); err != nil && !os.IsNotExist(err) {
		return err
	}
	indexPath := filepath.Join(filepath.Dir(s.path), "..", "index", strings.TrimSuffix(filepath.Base(s.path), ".log")+".index")
	indexPath = filepath.Clean(indexPath)
	if err := os.Remove(indexPath); err != nil && !os.IsNotExist(err) {
		slog.Warn("Retention: failed to delete index file", "path", indexPath, "error", err)
	}
	return nil
}

func fallbackSegmentMeta(path string, info os.FileInfo) (int64, int64, bool) {
	base := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	firstOffset, err := strconv.ParseInt(base, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return firstOffset, info.ModTime().UnixMilli(), true
}

func isProtectedDir(name string) bool {
	return name == "raft" || name == "pebble" || name == "dedup" || name == "offsets" || name == "scheduler" || name == "cold_store" || name == "index" || name == "backups" || name == "consumer_offsets" || name == "consumer_groups" || name == "snapshot-staging" || name == "segments.old"
}

func isWALSegment(path string) bool {
	if filepath.Ext(path) != ".log" {
		return false
	}
	return filepath.Base(filepath.Dir(path)) == "segments"
}

// readSegmentHeader parses the 64-byte segment header and returns the first offset,
// created timestamp, and whether the header is valid.
func readSegmentHeader(path string) (int64, int64, bool) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, false
	}
	defer f.Close()

	header := make([]byte, 64)
	if _, err := f.Read(header); err != nil {
		return 0, 0, false
	}

	if string(header[0:6]) != "CRNOS1" {
		return 0, 0, false
	}
	if header[7] != 1 {
		return 0, 0, false
	}
	stored := binary.BigEndian.Uint32(header[60:64])
	computed := crc32.ChecksumIEEE(header[0:60])
	if stored != computed {
		return 0, 0, false
	}

	firstOffset := int64(binary.BigEndian.Uint64(header[8:16]))
	createdTS := int64(binary.BigEndian.Uint64(header[24:32]))
	return firstOffset, createdTS, true
}

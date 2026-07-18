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
}

// NewEnforcer creates a retention enforcer for the given data directory and policy.
func NewEnforcer(dataDir string, policy RetentionPolicy) *Enforcer {
	return &Enforcer{
		dataDir: dataDir,
		policy:  policy,
	}
}

// Run executes retention policy once: collect segments, preserve the active
// segment per partition, then delete by MaxAge and/or MaxSizeBytes.
// It honors ctx cancellation between phases.
func (e *Enforcer) Run(ctx context.Context) error {
	segments, err := e.collectSegments()
	if err != nil {
		return fmt.Errorf("collect segments: %w", err)
	}

	if len(segments) == 0 {
		return nil
	}

	// Group by partition data directory (parent of "segments").
	byPartition := make(map[string][]segmentInfo)
	for _, s := range segments {
		partDir := filepath.Dir(filepath.Dir(s.path))
		byPartition[partDir] = append(byPartition[partDir], s)
	}

	var remaining []segmentInfo
	for _, partSegments := range byPartition {
		// Mark the segment with the highest first offset as active per partition.
		activeIdx := 0
		for i := 1; i < len(partSegments); i++ {
			if partSegments[i].firstOffset > partSegments[activeIdx].firstOffset {
				activeIdx = i
			}
		}
		for i, s := range partSegments {
			if i == activeIdx {
				slog.Debug("Retention: preserving active segment", "path", s.path)
				continue
			}
			remaining = append(remaining, s)
		}
	}

	if e.policy.MaxAge > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		cutoff := time.Now().Add(-e.policy.MaxAge)
		for _, s := range remaining {
			created := time.UnixMilli(s.createdTS)
			if created.Before(cutoff) {
				if err := e.removeSegment(s); err != nil {
					slog.Warn("Retention age delete failed", "path", s.path, "error", err)
				} else {
					slog.Info("Retention: deleted aged segment", "path", s.path, "age", time.Since(created))
				}
			}
		}
	}

	if e.policy.MaxSizeBytes > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := e.enforceSize(remaining); err != nil {
			return fmt.Errorf("size enforcement: %w", err)
		}
	}

	return nil
}

// collectSegments walks the data directory and returns all valid WAL segment files.
func (e *Enforcer) collectSegments() ([]segmentInfo, error) {
	var segments []segmentInfo

	err := filepath.Walk(e.dataDir, func(path string, info os.FileInfo, err error) error {
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

// enforceSize deletes the oldest non-active segments until total size is within the limit.
func (e *Enforcer) enforceSize(segments []segmentInfo) error {
	var total int64
	for _, s := range segments {
		total += s.size
	}
	if total <= e.policy.MaxSizeBytes {
		return nil
	}

	// Oldest first by creation timestamp; tie-break by first offset for determinism.
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].createdTS != segments[j].createdTS {
			return segments[i].createdTS < segments[j].createdTS
		}
		return segments[i].firstOffset < segments[j].firstOffset
	})

	for _, s := range segments {
		if total <= e.policy.MaxSizeBytes {
			break
		}
		if err := e.removeSegment(s); err != nil {
			slog.Warn("Retention size delete failed", "path", s.path, "error", err)
		} else {
			total -= s.size
			slog.Info("Retention: deleted segment for size limit", "path", s.path)
		}
	}
	return nil
}

// removeSegment deletes a segment file and its matching sparse index file.
func (e *Enforcer) removeSegment(s segmentInfo) error {
	if err := os.Remove(s.path); err != nil && !os.IsNotExist(err) {
		return err
	}
	indexPath := filepath.Join(filepath.Dir(s.path), "..", "index", filepath.Base(s.path)+".index")
	indexPath = filepath.Clean(indexPath)
	if err := os.Remove(indexPath); err != nil && !os.IsNotExist(err) {
		slog.Warn("Retention: failed to delete index file", "path", indexPath, "error", err)
	}
	return nil
}

func fallbackSegmentMeta(path string, info os.FileInfo) (int64, int64, bool) {
	base := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	firstOffset, err := strconv.ParseInt(base, 16, 64)
	if err != nil {
		return 0, 0, false
	}
	return firstOffset, info.ModTime().UnixMilli(), true
}

func isProtectedDir(name string) bool {
	return name == "raft" || name == "pebble" || name == "dedup" || name == "offsets" || name == "scheduler" || name == "cold_store" || name == "index" || name == "backups" || name == "consumer_offsets" || name == "consumer_groups"
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

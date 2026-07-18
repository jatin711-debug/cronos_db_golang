package delivery

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
)

// DLQEntry is a durable record of a delivery that exhausted retries (or a tombstone).
type DLQEntry struct {
	// Event is the failed event payload (nil on tombstone records).
	Event *types.Event `json:"event"`
	// DeliveryID correlates this entry with the original delivery attempt.
	DeliveryID string `json:"delivery_id"`
	// Attempts is how many delivery attempts were made before DLQ.
	Attempts int32 `json:"attempts"`
	// LastError describes the final failure reason.
	LastError string `json:"last_error"`
	// FailedAt is when the entry (or tombstone) was written (Unix ms).
	FailedAt int64 `json:"failed_at"`
	// Subscriber is the subscription ID that failed, if known.
	Subscriber string `json:"subscriber"`
	// PartitionID is the event's partition.
	PartitionID int32 `json:"partition_id"`
	// Tombstone marks a log entry that deletes/retracts a prior DLQ entry with
	// the same DeliveryID. It is used by Remove/Retry so the action survives a
	// restart; load() skips the original entry when a tombstone is present.
	Tombstone bool `json:"tombstone,omitempty"`
}

// DeadLetterQueue stores failed deliveries in memory and on append-only segments.
// Remove/Retry append tombstones so actions survive restart; a background
// retry worker can re-drive entries via a registered callback.
type DeadLetterQueue struct {
	mu      sync.RWMutex
	entries []*DLQEntry
	dataDir string
	maxSize int // max in-memory entries; oldest are dropped on overflow
	writer  *DLQSegmentWriter

	// Background retry worker
	retryQuit chan struct{}
	retryFn   func(entry *DLQEntry) // callback to re-queue for delivery
}

// NewDeadLetterQueue creates a DLQ under dataDir/dlq, loading existing segments.
// A non-positive maxSize defaults to 10000 in-memory entries.
func NewDeadLetterQueue(dataDir string, maxSize int) (*DeadLetterQueue, error) {
	dlqDir := filepath.Join(dataDir, "dlq")
	if err := os.MkdirAll(dlqDir, 0755); err != nil {
		return nil, fmt.Errorf("create dlq dir: %w", err)
	}

	if maxSize <= 0 {
		maxSize = 10000 // Default max entries
	}

	writer, err := NewDLQSegmentWriter(dlqDir)
	if err != nil {
		return nil, fmt.Errorf("create dlq segment writer: %w", err)
	}

	dlq := &DeadLetterQueue{
		entries: make([]*DLQEntry, 0),
		dataDir: dlqDir,
		maxSize: maxSize,
		writer:  writer,
	}

	// Load existing entries from segments
	if err := dlq.load(); err != nil {
		// Log but don't fail - start fresh
		fmt.Printf("[DLQ] Failed to load existing entries: %v\n", err)
	}

	return dlq, nil
}

// Add records a failed delivery in memory and appends it to the segment log.
func (d *DeadLetterQueue) Add(event *types.Event, deliveryID string, attempts int32, lastError string, subscriber string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	entry := &DLQEntry{
		Event:       event,
		DeliveryID:  deliveryID,
		Attempts:    attempts,
		LastError:   lastError,
		FailedAt:    time.Now().UnixMilli(),
		Subscriber:  subscriber,
		PartitionID: event.GetPartitionId(),
	}

	// Evict oldest if at capacity
	if len(d.entries) >= d.maxSize {
		d.entries = d.entries[1:]
	}

	d.entries = append(d.entries, entry)

	// Persist to segment
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal dlq entry: %w", err)
	}
	return d.writer.WriteEntry(data)
}

// Get returns a copy of all in-memory DLQ entries.
func (d *DeadLetterQueue) Get() []*DLQEntry {
	d.mu.RLock()
	defer d.mu.RUnlock()

	entries := make([]*DLQEntry, len(d.entries))
	copy(entries, d.entries)
	return entries
}

// GetByPartition returns in-memory DLQ entries for the given partition.
func (d *DeadLetterQueue) GetByPartition(partitionID int32) []*DLQEntry {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var entries []*DLQEntry
	for _, entry := range d.entries {
		if entry.PartitionID == partitionID {
			entries = append(entries, entry)
		}
	}
	return entries
}

// Remove removes an entry from the DLQ by appending a tombstone record.
func (d *DeadLetterQueue) Remove(deliveryID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	for i, entry := range d.entries {
		if entry.DeliveryID == deliveryID {
			d.entries = append(d.entries[:i], d.entries[i+1:]...)
			return d.writeTombstone(deliveryID)
		}
	}
	return fmt.Errorf("entry %s not found", deliveryID)
}

// Retry moves an entry back to the delivery queue and appends a tombstone
// record so the entry is not reloaded after a restart.
func (d *DeadLetterQueue) Retry(deliveryID string) (*DLQEntry, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for i, entry := range d.entries {
		if entry.DeliveryID == deliveryID {
			// Remove from DLQ
			d.entries = append(d.entries[:i], d.entries[i+1:]...)
			if err := d.writeTombstone(deliveryID); err != nil {
				return nil, err
			}
			return entry, nil
		}
	}
	return nil, fmt.Errorf("entry %s not found", deliveryID)
}

// writeTombstone appends a tombstone record for the given delivery ID to the
// segment log. This makes Remove/Retry durable across restarts.
func (d *DeadLetterQueue) writeTombstone(deliveryID string) error {
	tombstone := &DLQEntry{
		DeliveryID: deliveryID,
		Tombstone:  true,
		FailedAt:   time.Now().UnixMilli(),
	}
	data, err := json.Marshal(tombstone)
	if err != nil {
		return fmt.Errorf("marshal dlq tombstone: %w", err)
	}
	return d.writer.WriteEntry(data)
}

// Count returns the number of in-memory DLQ entries.
func (d *DeadLetterQueue) Count() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.entries)
}

// Clear removes all in-memory entries and compacts segment files to empty.
func (d *DeadLetterQueue) Clear() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.entries = make([]*DLQEntry, 0)
	if d.writer != nil {
		return d.writer.Compact(nil)
	}
	return nil
}

// load reads DLQ from segment files, filtering out entries whose DeliveryID
// has a tombstone record in the log.
func (d *DeadLetterQueue) load() error {
	if d.writer == nil {
		return nil
	}
	records, err := d.writer.Scan()
	if err != nil {
		return fmt.Errorf("scan dlq segments: %w", err)
	}

	tombstones := make(map[string]bool)
	for _, record := range records {
		var marker DLQEntry
		if err := json.Unmarshal(record, &marker); err != nil {
			continue // Skip corrupt entries
		}
		if marker.Tombstone {
			tombstones[marker.DeliveryID] = true
		}
	}

	for _, record := range records {
		var entry DLQEntry
		if err := json.Unmarshal(record, &entry); err != nil {
			continue // Skip corrupt entries
		}
		if entry.Tombstone {
			continue
		}
		if tombstones[entry.DeliveryID] {
			continue // This entry was removed/retried before the restart.
		}
		d.entries = append(d.entries, &entry)
	}
	return nil
}

// DLQStats summarizes dead-letter queue occupancy and entry ages.
type DLQStats struct {
	// TotalEntries is the number of in-memory DLQ entries.
	TotalEntries int `json:"total_entries"`
	// OldestEntryAge is now minus the oldest entry's FailedAt, in milliseconds.
	OldestEntryAge int64 `json:"oldest_entry_age_ms"`
	// NewestEntryAge is now minus the newest entry's FailedAt, in milliseconds.
	NewestEntryAge int64 `json:"newest_entry_age_ms"`
}

// SetRetryCallback registers a function that will be called for each DLQ entry
// during background retry. The function should attempt to re-deliver the event.
func (d *DeadLetterQueue) SetRetryCallback(fn func(entry *DLQEntry)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.retryFn = fn
}

// StartRetryWorker starts a background goroutine that retries DLQ entries
// at the given interval. Call StopRetryWorker to shut it down.
func (d *DeadLetterQueue) StartRetryWorker(interval time.Duration) {
	d.mu.Lock()
	if d.retryQuit != nil {
		d.mu.Unlock()
		return // Already running
	}
	d.retryQuit = make(chan struct{})
	d.mu.Unlock()

	utils.GoSafe("dlq-retry-worker", func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				d.retryAll()
			case <-d.retryQuit:
				return
			}
		}
	})
}

// StopRetryWorker stops the background retry worker.
func (d *DeadLetterQueue) StopRetryWorker() {
	d.mu.Lock()
	if d.retryQuit != nil {
		close(d.retryQuit)
		d.retryQuit = nil
	}
	d.mu.Unlock()
}

func (d *DeadLetterQueue) retryAll() {
	d.mu.RLock()
	entries := make([]*DLQEntry, len(d.entries))
	copy(entries, d.entries)
	retryFn := d.retryFn
	d.mu.RUnlock()

	if retryFn == nil {
		return
	}

	for _, entry := range entries {
		retryFn(entry)
	}
}

// Close closes the DLQ and releases file handles.
func (d *DeadLetterQueue) Close() error {
	d.StopRetryWorker()
	if d.writer != nil {
		return d.writer.Close()
	}
	return nil
}

// GetStats returns occupancy and age statistics for in-memory DLQ entries.
func (d *DeadLetterQueue) GetStats() *DLQStats {
	d.mu.RLock()
	defer d.mu.RUnlock()

	stats := &DLQStats{
		TotalEntries: len(d.entries),
	}

	now := time.Now().UnixMilli()
	if len(d.entries) > 0 {
		stats.OldestEntryAge = now - d.entries[0].FailedAt
		stats.NewestEntryAge = now - d.entries[len(d.entries)-1].FailedAt
	}

	return stats
}

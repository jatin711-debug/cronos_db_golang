package delivery

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"cronos_db/pkg/types"
)

// DLQEntry represents a dead-letter queue entry
type DLQEntry struct {
	Event       *types.Event `json:"event"`
	DeliveryID  string       `json:"delivery_id"`
	Attempts    int32        `json:"attempts"`
	LastError   string       `json:"last_error"`
	FailedAt    int64        `json:"failed_at"`
	Subscriber  string       `json:"subscriber"`
	PartitionID int32        `json:"partition_id"`
}

// DeadLetterQueue manages failed deliveries
type DeadLetterQueue struct {
	mu       sync.RWMutex
	entries  []*DLQEntry
	dataDir  string
	maxSize  int
	filePath string
}

// NewDeadLetterQueue creates a new DLQ
func NewDeadLetterQueue(dataDir string, maxSize int) (*DeadLetterQueue, error) {
	dlqDir := filepath.Join(dataDir, "dlq")
	if err := os.MkdirAll(dlqDir, 0755); err != nil {
		return nil, fmt.Errorf("create dlq dir: %w", err)
	}

	if maxSize <= 0 {
		maxSize = 10000 // Default max entries
	}

	dlq := &DeadLetterQueue{
		entries:  make([]*DLQEntry, 0),
		dataDir:  dlqDir,
		maxSize:  maxSize,
		filePath: filepath.Join(dlqDir, "dlq.json"),
	}

	// Load existing entries
	if err := dlq.load(); err != nil {
		// Log but don't fail - start fresh
		fmt.Printf("[DLQ] Failed to load existing entries: %v\n", err)
	}

	return dlq, nil
}

// Add adds a failed delivery to the DLQ
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

	// Persist to disk
	return d.persist()
}

// Get returns all DLQ entries
func (d *DeadLetterQueue) Get() []*DLQEntry {
	d.mu.RLock()
	defer d.mu.RUnlock()

	entries := make([]*DLQEntry, len(d.entries))
	copy(entries, d.entries)
	return entries
}

// GetByPartition returns DLQ entries for a specific partition
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

// Remove removes an entry from the DLQ
func (d *DeadLetterQueue) Remove(deliveryID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	for i, entry := range d.entries {
		if entry.DeliveryID == deliveryID {
			d.entries = append(d.entries[:i], d.entries[i+1:]...)
			return d.persist()
		}
	}
	return fmt.Errorf("entry %s not found", deliveryID)
}

// Retry moves an entry back to the delivery queue
func (d *DeadLetterQueue) Retry(deliveryID string) (*DLQEntry, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for i, entry := range d.entries {
		if entry.DeliveryID == deliveryID {
			// Remove from DLQ
			d.entries = append(d.entries[:i], d.entries[i+1:]...)
			if err := d.persist(); err != nil {
				return nil, err
			}
			return entry, nil
		}
	}
	return nil, fmt.Errorf("entry %s not found", deliveryID)
}

// Count returns the number of entries in the DLQ
func (d *DeadLetterQueue) Count() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.entries)
}

// Clear removes all entries from the DLQ
func (d *DeadLetterQueue) Clear() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.entries = make([]*DLQEntry, 0)
	return d.persist()
}

// persist saves DLQ to disk
func (d *DeadLetterQueue) persist() error {
	data, err := json.MarshalIndent(d.entries, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal dlq: %w", err)
	}

	tmpPath := d.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("write dlq: %w", err)
	}

	return os.Rename(tmpPath, d.filePath)
}

// load reads DLQ from disk
func (d *DeadLetterQueue) load() error {
	data, err := os.ReadFile(d.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No existing DLQ
		}
		return fmt.Errorf("read dlq: %w", err)
	}

	var entries []*DLQEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return fmt.Errorf("unmarshal dlq: %w", err)
	}

	d.entries = entries
	return nil
}

// DLQStats represents DLQ statistics
type DLQStats struct {
	TotalEntries   int   `json:"total_entries"`
	OldestEntryAge int64 `json:"oldest_entry_age_ms"`
	NewestEntryAge int64 `json:"newest_entry_age_ms"`
}

// GetStats returns DLQ statistics
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

package dedup

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/jatin711-debug/cronos_db_golang/internal/metrics"
)

// PebbleStore is a PebbleDB-backed dedup store
type PebbleStore struct {
	db          *pebble.DB
	dataDir     string
	partitionID int32
	ttlHours    int32
}

// NewPebbleStore creates a new PebbleStore
func NewPebbleStore(dataDir string, partitionID int32, ttlHours int32, cache *pebble.Cache) (*PebbleStore, error) {
	// Create data directory
	dir := filepath.Join(dataDir, fmt.Sprintf("dedup_%d", partitionID))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create dedup dir: %w", err)
	}

	// Open PebbleDB with optimized settings for high throughput
	opts := &pebble.Options{
		Logger:                      nil,
		MemTableSize:                64 * 1024 * 1024,        // 64MB memtable (default 4MB)
		MemTableStopWritesThreshold: 10,                      // Allow more memtables before stalling (increased from 4)
		L0CompactionThreshold:       2,                       // Trigger L0 compaction earlier (reduced from 4)
		L0StopWritesThreshold:       20,                      // Allow more L0 files before stalling (increased from 12)
		MaxConcurrentCompactions:    func() int { return 3 }, // Increase parallel compaction (increased from 2)
		DisableWAL:                  true,                    // Disable PebbleDB WAL (we have our own)
	}
	if cache != nil {
		opts.Cache = cache
	}

	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("open pebble db: %w", err)
	}

	return &PebbleStore{
		db:          db,
		dataDir:     dir,
		partitionID: partitionID,
		ttlHours:    ttlHours,
	}, nil
}

// CheckAndStore checks if message ID exists, and stores it if not
func (p *PebbleStore) CheckAndStore(messageID string, offset int64) (bool, error) {
	start := time.Now()
	defer func() {
		metrics.ObserveDedupCheck(strconv.FormatInt(int64(p.partitionID), 10), "pebble_slow", time.Since(start))
	}()

	key := []byte(messageID)

	// Check if exists
	_, closer, err := p.db.Get(key)
	if err == nil {
		closer.Close()
		return true, nil // Already exists
	}
	if err != pebble.ErrNotFound {
		return false, fmt.Errorf("check key: %w", err)
	}

	// Store new entry - use NoSync for performance (WAL provides durability)
	expirationTS := time.Now().UnixMilli() + int64(p.ttlHours)*60*60*1000
	value := p.buildValue(offset, expirationTS, time.Now().UnixNano())

	if err := p.db.Set(key, value, pebble.NoSync); err != nil {
		return false, fmt.Errorf("set key: %w", err)
	}

	return false, nil // Not a duplicate
}

// StoreOnly stores a message ID without checking if it exists
// Used by BloomPebbleStore when bloom filter confirms key is new
func (p *PebbleStore) StoreOnly(messageID string, offset int64) error {
	key := []byte(messageID)
	expirationTS := time.Now().UnixMilli() + int64(p.ttlHours)*60*60*1000
	value := p.buildValue(offset, expirationTS, time.Now().UnixNano())

	if err := p.db.Set(key, value, pebble.NoSync); err != nil {
		return fmt.Errorf("set key: %w", err)
	}
	return nil
}

// StoreBatch stores multiple message IDs in a single PebbleDB batch commit.
// indices specifies which elements from messageIDs/offsets to store.
// This is much faster than individual StoreOnly calls because it uses a single
// write batch with one commit instead of N individual Set calls.
func (p *PebbleStore) StoreBatch(messageIDs []string, offsets []int64, indices []int) error {
	batch := p.db.NewBatch()
	defer batch.Close()

	expirationTS := time.Now().UnixMilli() + int64(p.ttlHours)*60*60*1000
	nowNano := time.Now().UnixNano()

	for _, idx := range indices {
		key := []byte(messageIDs[idx])
		value := p.buildValue(offsets[idx], expirationTS, nowNano)
		if err := batch.Set(key, value, nil); err != nil {
			return fmt.Errorf("batch set key: %w", err)
		}
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("batch commit: %w", err)
	}
	return nil
}

// GetOffset returns stored offset for message ID
func (p *PebbleStore) GetOffset(messageID string) (int64, bool, error) {
	key := []byte(messageID)
	value, closer, err := p.db.Get(key)
	if err == pebble.ErrNotFound {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("get key: %w", err)
	}
	defer closer.Close()

	offset, _, _, err := p.parseValue(value)
	return offset, true, err
}

// Exists checks if message ID exists
func (p *PebbleStore) Exists(messageID string) (bool, error) {
	key := []byte(messageID)
	_, closer, err := p.db.Get(key)
	if err == pebble.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check key: %w", err)
	}
	closer.Close()
	return true, nil
}

// PruneExpired removes expired entries
func (p *PebbleStore) PruneExpired() (int, error) {
	now := time.Now().UnixMilli()
	pruned := 0

	iter, err := p.db.NewIter(nil)
	if err != nil {
		return 0, fmt.Errorf("create iterator: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		value := iter.Value()
		_, expirationTS, _, err := p.parseValue(value)
		if err != nil {
			log.Printf("Failed to parse dedup value: %v", err)
			continue
		}

		if expirationTS <= now {
			if err := p.db.Delete(iter.Key(), pebble.NoSync); err != nil {
				return pruned, fmt.Errorf("delete expired key: %w", err)
			}
			pruned++
		}
	}

	return pruned, nil
}

// GetTimestamp returns stored timestamp for message ID
func (p *PebbleStore) GetTimestamp(messageID string) (time.Time, bool, error) {
	key := []byte(messageID)
	value, closer, err := p.db.Get(key)
	if err == pebble.ErrNotFound {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("get key: %w", err)
	}
	defer closer.Close()

	_, _, createdTS, err := p.parseValue(value)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("parse value: %w", err)
	}
	return time.Unix(0, createdTS), true, nil
}

// Put inserts or overwrites an entry directly with a given created timestamp
func (p *PebbleStore) Put(messageID string, offset int64, createdTS int64) error {
	key := []byte(messageID)
	expirationTS := time.Now().UnixMilli() + int64(p.ttlHours)*60*60*1000
	value := p.buildValue(offset, expirationTS, createdTS)

	if err := p.db.Set(key, value, pebble.NoSync); err != nil {
		return fmt.Errorf("set key: %w", err)
	}
	return nil
}

// GetStats returns store statistics.
// This is used for observability and test verification, so we compute key count
// exactly via iteration rather than using file-size based approximations.
func (p *PebbleStore) GetStats() (*DedupStats, error) {
	iter, err := p.db.NewIter(nil)
	if err != nil {
		return nil, fmt.Errorf("create iterator: %w", err)
	}
	defer iter.Close()

	count := int64(0)
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterate keys: %w", err)
	}

	return &DedupStats{
		ApproximateCount: count,
		TTLHours:         p.ttlHours,
		LastPruneTS:      time.Now().UnixMilli(),
	}, nil
}

// Close closes the store
func (p *PebbleStore) Close() error {
	if err := p.db.Flush(); err != nil {
		return fmt.Errorf("flush before close: %w", err)
	}
	return p.db.Close()
}

// Checkpoint creates a PebbleDB checkpoint of the dedup store at destDir.
// The checkpoint is a consistent, point-in-time snapshot that can be used for
// backups or seeding a new replica.
func (p *PebbleStore) Checkpoint(destDir string) error {
	if err := p.db.Checkpoint(destDir); err != nil {
		return fmt.Errorf("dedup checkpoint: %w", err)
	}
	log.Printf("[DEDUP] Checkpoint created at %s for partition %d", destDir, p.partitionID)
	return nil
}

// buildValue builds storage value
func (p *PebbleStore) buildValue(offset int64, expirationTS int64, createdTS int64) []byte {
	value := make([]byte, 24)
	// Offset (8 bytes)
	binary.BigEndian.PutUint64(value[0:8], uint64(offset))
	// Expiration timestamp (8 bytes)
	binary.BigEndian.PutUint64(value[8:16], uint64(expirationTS))
	// Created timestamp (8 bytes)
	binary.BigEndian.PutUint64(value[16:24], uint64(createdTS))
	return value
}

// parseValue parses storage value
func (p *PebbleStore) parseValue(value []byte) (int64, int64, int64, error) {
	if len(value) < 24 {
		if len(value) >= 16 {
			offset := int64(binary.BigEndian.Uint64(value[0:8]))
			expirationTS := int64(binary.BigEndian.Uint64(value[8:16]))
			return offset, expirationTS, 0, nil
		}
		return 0, 0, 0, fmt.Errorf("invalid value size")
	}
	offset := int64(binary.BigEndian.Uint64(value[0:8]))
	expirationTS := int64(binary.BigEndian.Uint64(value[8:16]))
	createdTS := int64(binary.BigEndian.Uint64(value[16:24]))
	return offset, expirationTS, createdTS, nil
}

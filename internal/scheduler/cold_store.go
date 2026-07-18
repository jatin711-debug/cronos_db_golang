package scheduler

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
)

// ColdStore persists timer references for far-future events using PebbleDB.
// It stores only offsets, not full events — the WAL remains the single source of truth.
// Key format: [schedule_ts:be64][offset:be64] → empty value.
// Entries are scanned by schedule time range when the hydrator promotes them
// into the hot timing wheel.
type ColdStore struct {
	db      *pebble.DB
	dataDir string
	count   atomic.Int64 // Approximate entry count for metrics (updated on Store/Delete)
}

// NewColdStore creates a new cold store for far-future scheduled events.
func NewColdStore(dataDir string, cache *pebble.Cache) (*ColdStore, error) {
	dir := filepath.Join(dataDir, "cold_schedule")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create cold store dir: %w", err)
	}

	opts := &pebble.Options{
		Logger:       nil,
		MemTableSize: 32 * 1024 * 1024, // 32MB — smaller than dedup since this is scan-heavy
		DisableWAL:   true,             // CronosDB WAL provides durability
	}
	if cache != nil {
		opts.Cache = cache
	}

	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("open cold store pebble db: %w", err)
	}

	cs := &ColdStore{
		db:      db,
		dataDir: dir,
	}
	cs.initCount()
	return cs, nil
}

// Store stores an offset reference keyed by its scheduled timestamp.
func (cs *ColdStore) Store(offset int64, scheduleTS int64) error {
	key := cs.buildKey(scheduleTS, offset)
	if err := cs.db.Set(key, nil, pebble.NoSync); err != nil {
		return fmt.Errorf("cold store set: %w", err)
	}
	cs.count.Add(1)
	return nil
}

// StoreBatch stores multiple offset references atomically in a single Pebble batch.
func (cs *ColdStore) StoreBatch(entries []struct {
	Offset     int64
	ScheduleTS int64
}) error {
	if len(entries) == 0 {
		return nil
	}

	batch := cs.db.NewBatch()
	for _, e := range entries {
		key := cs.buildKey(e.ScheduleTS, e.Offset)
		if err := batch.Set(key, nil, nil); err != nil {
			batch.Close()
			return fmt.Errorf("cold store batch set: %w", err)
		}
	}

	if err := cs.db.Apply(batch, pebble.NoSync); err != nil {
		batch.Close()
		return fmt.Errorf("cold store batch apply: %w", err)
	}
	batch.Close()
	cs.count.Add(int64(len(entries)))
	return nil
}

// ScanRange returns all offsets with schedule_ts in [startTS, endTS].
// The returned offsets are sorted by schedule_ts ascending.
func (cs *ColdStore) ScanRange(startTS, endTS int64) ([]int64, error) {
	startKey := cs.buildKey(startTS, 0)
	// Upper bound is the maximum possible key at endTS (max offset = max uint64)
	endKey := cs.buildKey(endTS, 0)
	// Set offset portion to max uint64 so all offsets at endTS are included
	binary.BigEndian.PutUint64(endKey[8:16], ^uint64(0))
	// Append one more 0xff to make upper bound exclusive for any key beyond endTS
	endKey = append(endKey, 0xff)

	iter, err := cs.db.NewIter(&pebble.IterOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	if err != nil {
		return nil, fmt.Errorf("cold store iter: %w", err)
	}
	defer iter.Close()

	var offsets []int64
	for iter.First(); iter.Valid(); iter.Next() {
		offset := cs.parseOffsetFromKey(iter.Key())
		offsets = append(offsets, offset)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("cold store scan: %w", err)
	}
	return offsets, nil
}

// Delete removes a stored offset reference.
func (cs *ColdStore) Delete(offset int64, scheduleTS int64) error {
	key := cs.buildKey(scheduleTS, offset)
	if err := cs.db.Delete(key, pebble.NoSync); err != nil {
		return fmt.Errorf("cold store delete: %w", err)
	}
	cs.count.Add(-1)
	return nil
}

// DeleteBatch removes multiple offset references atomically in a single Pebble batch.
func (cs *ColdStore) DeleteBatch(entries []struct {
	Offset     int64
	ScheduleTS int64
}) error {
	if len(entries) == 0 {
		return nil
	}

	batch := cs.db.NewBatch()
	for _, e := range entries {
		key := cs.buildKey(e.ScheduleTS, e.Offset)
		if err := batch.Delete(key, nil); err != nil {
			batch.Close()
			return fmt.Errorf("cold store batch delete: %w", err)
		}
	}

	if err := cs.db.Apply(batch, pebble.NoSync); err != nil {
		batch.Close()
		return fmt.Errorf("cold store batch apply delete: %w", err)
	}
	batch.Close()
	cs.count.Add(-int64(len(entries)))
	return nil
}

// Count returns the approximate number of entries in the cold store.
func (cs *ColdStore) Count() int64 {
	return cs.count.Load()
}

// Close closes the underlying PebbleDB.
func (cs *ColdStore) Close() error {
	return cs.db.Close()
}

// buildKey encodes schedule_ts + offset as a sortable Pebble key.
// Big-endian encoding preserves lexicographic sort order.
func (cs *ColdStore) buildKey(scheduleTS int64, offset int64) []byte {
	key := make([]byte, 16)
	binary.BigEndian.PutUint64(key[0:8], uint64(scheduleTS))
	binary.BigEndian.PutUint64(key[8:16], uint64(offset))
	return key
}

// parseOffsetFromKey extracts the offset from a 16-byte key.
func (cs *ColdStore) parseOffsetFromKey(key []byte) int64 {
	if len(key) < 16 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(key[8:16]))
}

// initCount initializes the atomic count from an initial scan.
func (cs *ColdStore) initCount() {
	iter, err := cs.db.NewIter(nil)
	if err != nil {
		return
	}
	defer iter.Close()

	var n int64
	for iter.First(); iter.Valid(); iter.Next() {
		n++
	}
	cs.count.Store(n)
}

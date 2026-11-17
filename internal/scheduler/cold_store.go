package scheduler

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/jatin711-debug/cronos_db_golang/internal/pebblemem"
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

	// The hydrator scans from the earliest schedule time on every pass, and
	// deletes what it takes. A delete leaves a tombstone until compaction, so
	// each scan stepped over the tombstone of every entry taken before it: in
	// the soak, a third of a node's CPU went to that once a backlog of delayed
	// events fell due. floor is a schedule time below which the store holds
	// nothing, so scans start there.
	//
	// Writes lower the floor below what they store, after storing it. A scan
	// raises it to its first live entry, but not above anything stored while
	// it ran (lowestDuringScan), which its snapshot of the store may not show.
	// Deletes cannot break it. Guarded by floorMu.
	floorMu          sync.Mutex
	floor            int64
	floorSet         bool
	scansActive      int
	lowestDuringScan int64
}

// NewColdStore creates a new cold store for far-future scheduled events.
func NewColdStore(dataDir string, mem *pebblemem.Budget) (*ColdStore, error) {
	dir := filepath.Join(dataDir, "cold_schedule")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create cold store dir: %w", err)
	}

	opts := &pebble.Options{
		Logger:     nil,
		DisableWAL: true, // CronosDB WAL provides durability
	}
	// Up to 32 MiB, following the node's memory: smaller than dedup's,
	// since this store is scanned more than written.
	mem.Apply(pebblemem.Cold, opts)

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
	cs.noteStored(scheduleTS)
	return nil
}

// noteStored keeps the scan floor at or below a schedule time just stored.
func (cs *ColdStore) noteStored(scheduleTS int64) {
	cs.floorMu.Lock()
	if cs.floorSet && scheduleTS < cs.floor {
		cs.floor = scheduleTS
	}
	if cs.scansActive > 0 && scheduleTS < cs.lowestDuringScan {
		cs.lowestDuringScan = scheduleTS
	}
	cs.floorMu.Unlock()
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
	lowest := entries[0].ScheduleTS
	for _, e := range entries[1:] {
		lowest = min(lowest, e.ScheduleTS)
	}
	cs.noteStored(lowest)
	return nil
}

// ScanRange returns all offsets with schedule_ts in [startTS, endTS].
// The returned offsets are sorted by schedule_ts ascending.
func (cs *ColdStore) ScanRange(startTS, endTS int64) ([]int64, error) {
	return cs.ScanRangeLimit(startTS, endTS, 0)
}

// ScanRangeLimit bounds the reference slice; zero retains the legacy API.
func (cs *ColdStore) ScanRangeLimit(startTS, endTS int64, limit int) ([]int64, error) {
	// Start at the floor when it is above startTS. The floor can be raised
	// after the scan only if the scan covered everything from it.
	// A negative startTS keys after every other (keys are unsigned), so such
	// a scan neither uses nor moves the floor.
	cs.floorMu.Lock()
	from, canRaise := startTS, startTS == 0
	if cs.floorSet && startTS >= 0 {
		from, canRaise = max(startTS, cs.floor), startTS <= cs.floor
	}
	if cs.scansActive == 0 {
		cs.lowestDuringScan = math.MaxInt64
	}
	cs.scansActive++
	cs.floorMu.Unlock()

	offsets, firstTS, found, err := cs.scan(from, endTS, limit)

	cs.floorMu.Lock()
	cs.scansActive--
	if err == nil && canRaise && from <= endTS {
		next := endTS + 1 // nothing in [from, endTS]
		if found {
			next = firstTS
		}
		cs.floor, cs.floorSet = min(next, cs.lowestDuringScan), true
	}
	cs.floorMu.Unlock()
	return offsets, err
}

// scan returns the offsets with schedule_ts in [startTS, endTS], at most
// limit of them (zero is no limit), and the schedule time of the first.
func (cs *ColdStore) scan(startTS, endTS int64, limit int) (offsets []int64, firstTS int64, found bool, err error) {
	if startTS > endTS {
		return nil, 0, false, nil
	}
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
		return nil, 0, false, fmt.Errorf("cold store iter: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if !found && len(key) >= 8 {
			firstTS, found = int64(binary.BigEndian.Uint64(key[0:8])), true
		}
		offsets = append(offsets, cs.parseOffsetFromKey(key))
		if limit > 0 && len(offsets) >= limit {
			break
		}
	}
	if err := iter.Error(); err != nil {
		return nil, 0, false, fmt.Errorf("cold store scan: %w", err)
	}
	return offsets, firstTS, found, nil
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

// Contains reports whether the store holds the reference.
func (cs *ColdStore) Contains(offset, scheduleTS int64) (bool, error) {
	_, closer, err := cs.db.Get(cs.buildKey(scheduleTS, offset))
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, closer.Close()
}

// DeleteIfPresent is used under the scheduler's hydration gate; unlike Delete,
// it does not decrement the approximate count for a reference that is absent.
func (cs *ColdStore) DeleteIfPresent(offset, scheduleTS int64) error {
	key := cs.buildKey(scheduleTS, offset)
	_, closer, err := cs.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	closer.Close()
	return cs.Delete(offset, scheduleTS)
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

// Clear removes every stored reference.
func (cs *ColdStore) Clear() error {
	end := make([]byte, 17)
	for i := range end {
		end[i] = 0xff
	}
	if err := cs.db.DeleteRange([]byte{}, end, pebble.NoSync); err != nil {
		return fmt.Errorf("cold store clear: %w", err)
	}
	cs.count.Store(0)
	// Empty, and one range tombstone to skip: the next scan starts over.
	cs.floorMu.Lock()
	cs.floorSet = false
	cs.floorMu.Unlock()
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

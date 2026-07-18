package dedup

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/jatin711-debug/cronos_db_golang/internal/metrics"
)

// PebbleStore is a PebbleDB-backed dedup store with an in-memory pending-write
// buffer. New entries are accumulated in memory and flushed to PebbleDB either
// when the buffer reaches 1k entries or every 1ms. This collapses many small
// writes into larger batches and dramatically reduces L0 pressure under high
// throughput.
type PebbleStore struct {
	db          *pebble.DB
	dataDir     string
	partitionID int32
	ttlHours    int32 // entry TTL applied at flush time

	pendingMu sync.RWMutex
	pending   map[string]int64 // messageID -> offset (not yet durable)
	claimMu   sync.Mutex       // serializes check-and-buffer and flush

	quit      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

// NewPebbleStore opens (or creates) a per-partition Pebble dedup database under
// dataDir/dedup_<partitionID>. cache may be shared across partitions; pass nil
// to use Pebble's default caching.
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

	store := &PebbleStore{
		db:          db,
		dataDir:     dir,
		partitionID: partitionID,
		ttlHours:    ttlHours,
		pending:     make(map[string]int64),
		quit:        make(chan struct{}),
	}

	store.startFlushLoop()

	return store, nil
}

// CheckAndStore checks if message ID exists, and stores it if not.
// The pending-write buffer is scanned first so duplicates that were recently
// inserted but not yet flushed are detected.
func (p *PebbleStore) CheckAndStore(messageID string, offset int64) (bool, error) {
	p.claimMu.Lock()
	defer p.claimMu.Unlock()

	start := time.Now()
	defer func() {
		metrics.ObserveDedupCheck(strconv.FormatInt(int64(p.partitionID), 10), "pebble_slow", time.Since(start))
	}()

	key := []byte(messageID)

	// 1. Check pending buffer first.
	p.pendingMu.RLock()
	if _, exists := p.pending[messageID]; exists {
		p.pendingMu.RUnlock()
		return true, nil
	}
	p.pendingMu.RUnlock()

	// 2. Check PebbleDB.
	_, closer, err := p.db.Get(key)
	if err == nil {
		closer.Close()
		return true, nil // Already exists
	}
	if err != pebble.ErrNotFound {
		return false, fmt.Errorf("check key: %w", err)
	}

	// 3. Buffer the new entry.
	if err := p.bufferWrite(messageID, offset); err != nil {
		return false, err
	}
	return false, nil // Not a duplicate
}

// bufferWrite adds a message ID to the pending buffer and triggers a flush if
// the buffer is full.
func (p *PebbleStore) bufferWrite(messageID string, offset int64) error {
	p.pendingMu.Lock()
	p.pending[messageID] = offset
	full := len(p.pending) >= 1000
	var toFlush map[string]int64
	if full {
		toFlush = p.pending
		p.pending = make(map[string]int64)
	}
	p.pendingMu.Unlock()

	if full {
		if err := p.flushPendingMap(toFlush); err != nil {
			p.requeuePending(toFlush)
			return fmt.Errorf("flush pending entry: %w", err)
		}
	}
	return nil
}

// requeuePending puts failed flushes back into the live buffer. A newer value
// already present in pending wins, so an older failed flush cannot overwrite a
// subsequent claim.
func (p *PebbleStore) requeuePending(entries map[string]int64) {
	p.pendingMu.Lock()
	defer p.pendingMu.Unlock()
	for id, offset := range entries {
		if _, exists := p.pending[id]; !exists {
			p.pending[id] = offset
		}
	}
}

// StoreOnly stores a message ID without checking if it exists.
// Used by BloomPebbleStore when bloom filter confirms key is new.
func (p *PebbleStore) StoreOnly(messageID string, offset int64) error {
	p.claimMu.Lock()
	defer p.claimMu.Unlock()
	return p.bufferWrite(messageID, offset)
}

// StoreBatch stores multiple message IDs in a single buffered write.
// indices specifies which elements from messageIDs/offsets to store.
func (p *PebbleStore) StoreBatch(messageIDs []string, offsets []int64, indices []int) error {
	p.claimMu.Lock()
	defer p.claimMu.Unlock()

	p.pendingMu.Lock()
	for _, idx := range indices {
		p.pending[messageIDs[idx]] = offsets[idx]
	}
	full := len(p.pending) >= 1000
	var toFlush map[string]int64
	if full {
		toFlush = p.pending
		p.pending = make(map[string]int64)
	}
	p.pendingMu.Unlock()

	if full {
		if err := p.flushPendingMap(toFlush); err != nil {
			p.requeuePending(toFlush)
			return fmt.Errorf("flush pending batch: %w", err)
		}
	}
	return nil
}

// RollbackClaim removes previously-claimed message IDs from the store. It undoes
// a dedup claim when the durable write that was supposed to follow it failed, so
// that a client retry of the same message ID is not incorrectly rejected as a
// duplicate. IDs are removed from both the in-memory pending buffer and PebbleDB
// (deleting an absent key is a no-op, so IDs still only in the pending buffer are
// handled correctly). claimMu is held so this cannot interleave with a concurrent
// flush of the pending buffer.
func (p *PebbleStore) RollbackClaim(messageIDs []string) error {
	if len(messageIDs) == 0 {
		return nil
	}
	p.claimMu.Lock()
	defer p.claimMu.Unlock()

	p.pendingMu.Lock()
	for _, id := range messageIDs {
		delete(p.pending, id)
	}
	p.pendingMu.Unlock()

	batch := p.db.NewBatch()
	defer batch.Close()
	for _, id := range messageIDs {
		if err := batch.Delete([]byte(id), nil); err != nil {
			return fmt.Errorf("rollback delete key: %w", err)
		}
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("rollback commit: %w", err)
	}
	return nil
}

// GetOffset returns the stored WAL offset for messageID, checking pending first.
func (p *PebbleStore) GetOffset(messageID string) (int64, bool, error) {
	p.pendingMu.RLock()
	if offset, ok := p.pending[messageID]; ok {
		p.pendingMu.RUnlock()
		return offset, true, nil
	}
	p.pendingMu.RUnlock()

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

// Exists reports whether messageID is in the pending buffer or PebbleDB.
func (p *PebbleStore) Exists(messageID string) (bool, error) {
	p.pendingMu.RLock()
	if _, ok := p.pending[messageID]; ok {
		p.pendingMu.RUnlock()
		return true, nil
	}
	p.pendingMu.RUnlock()

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

// startFlushLoop runs a 1ms ticker that flushes the pending buffer.
func (p *PebbleStore) startFlushLoop() {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := p.flushPending(); err != nil {
					log.Printf("[DEDUP-%d] pending flush failed: %v", p.partitionID, err)
				}
			case <-p.quit:
				return
			}
		}
	}()
}

// flushPending moves all buffered entries into PebbleDB as one batch.
func (p *PebbleStore) flushPending() error {
	p.claimMu.Lock()
	defer p.claimMu.Unlock()

	p.pendingMu.Lock()
	if len(p.pending) == 0 {
		p.pendingMu.Unlock()
		return nil
	}
	toFlush := p.pending
	p.pending = make(map[string]int64)
	p.pendingMu.Unlock()

	if err := p.flushPendingMap(toFlush); err != nil {
		p.requeuePending(toFlush)
		return err
	}
	return nil
}

// flushPendingMap writes the provided pending map to PebbleDB in a single batch.
func (p *PebbleStore) flushPendingMap(pending map[string]int64) error {
	if len(pending) == 0 {
		return nil
	}

	batch := p.db.NewBatch()
	defer batch.Close()

	expirationTS := time.Now().UnixMilli() + int64(p.ttlHours)*60*60*1000
	nowNano := time.Now().UnixNano()

	for id, offset := range pending {
		value := p.buildValue(offset, expirationTS, nowNano)
		if err := batch.Set([]byte(id), value, nil); err != nil {
			return fmt.Errorf("batch set key: %w", err)
		}
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("batch commit: %w", err)
	}
	return nil
}

// PruneExpired flushes pending writes then deletes TTL-expired Pebble keys.
func (p *PebbleStore) PruneExpired() (int, error) {
	if err := p.flushPending(); err != nil {
		return 0, fmt.Errorf("flush pending before prune: %w", err)
	}
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

// GetTimestamp returns the creation time for messageID.
// Pending (not yet flushed) entries report time.Now() as an approximation.
func (p *PebbleStore) GetTimestamp(messageID string) (time.Time, bool, error) {
	p.pendingMu.RLock()
	if _, ok := p.pending[messageID]; ok {
		p.pendingMu.RUnlock()
		return time.Now(), true, nil
	}
	p.pendingMu.RUnlock()

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

// Put inserts or overwrites an entry directly with a given created timestamp.
// This bypasses the pending buffer to ensure the entry is durable immediately.
func (p *PebbleStore) Put(messageID string, offset int64, createdTS int64) error {
	if err := p.flushPending(); err != nil {
		return fmt.Errorf("flush pending before put: %w", err)
	}

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
	p.pendingMu.RLock()
	pendingCount := int64(len(p.pending))
	p.pendingMu.RUnlock()

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
		ApproximateCount: count + pendingCount,
		TTLHours:         p.ttlHours,
		LastPruneTS:      time.Now().UnixMilli(),
	}, nil
}

// Close stops the flush loop, flushes pending writes, and closes PebbleDB.
// Safe to call multiple times; subsequent calls return the first close error.
func (p *PebbleStore) Close() error {
	p.closeOnce.Do(func() {
		close(p.quit)
		p.wg.Wait()
		var errs []error
		if err := p.flushPending(); err != nil {
			errs = append(errs, fmt.Errorf("flush pending before close: %w", err))
		}
		if err := p.db.Flush(); err != nil {
			errs = append(errs, fmt.Errorf("flush before close: %w", err))
		}
		if err := p.db.Close(); err != nil {
			errs = append(errs, err)
		}
		p.closeErr = errors.Join(errs...)
	})
	return p.closeErr
}

// Checkpoint creates a PebbleDB checkpoint of the dedup store at destDir.
// Pending writes are flushed before the checkpoint so it is consistent.
func (p *PebbleStore) Checkpoint(destDir string) error {
	if err := p.flushPending(); err != nil {
		return fmt.Errorf("flush pending before checkpoint: %w", err)
	}
	if err := p.db.Checkpoint(destDir); err != nil {
		return fmt.Errorf("dedup checkpoint: %w", err)
	}
	log.Printf("[DEDUP] Checkpoint created at %s for partition %d", destDir, p.partitionID)
	return nil
}

// buildValue encodes offset, expiration (ms), and created (ns) as 24 big-endian bytes.
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

// parseValue decodes a stored value into offset, expirationTS, and createdTS.
// Legacy 16-byte values without createdTS return createdTS=0.
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

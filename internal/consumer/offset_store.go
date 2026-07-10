package consumer

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
)

type offsetKey struct {
	groupID     string
	partitionID int32
}

const (
	offsetKeyPrefix    = "off:"
	commitIDKeyPrefix  = "cid:"
	groupMetaKeyPrefix = "grp:"
	commitIDCapacity   = 1_000_000
)

// commitIDEntryValue returns an 8-byte big-endian Unix-nano timestamp.
func commitIDEntryValue() []byte {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, uint64(time.Now().UnixNano()))
	return v
}

func parseCommitIDValue(v []byte) int64 {
	if len(v) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(v))
}

func (s *OffsetStore) offsetKey(groupID string, partitionID int32) []byte {
	key := make([]byte, len(offsetKeyPrefix)+4+len(groupID)+4)
	offset := 0
	copy(key[offset:], offsetKeyPrefix)
	offset += len(offsetKeyPrefix)
	binary.BigEndian.PutUint32(key[offset:], uint32(partitionID))
	offset += 4
	copy(key[offset:], groupID)
	offset += len(groupID)
	key[offset] = 0
	return key
}

func (s *OffsetStore) commitIDKey(commitID string) []byte {
	return append([]byte(commitIDKeyPrefix), []byte(commitID)...)
}

func (s *OffsetStore) groupMetaKey(groupID string) []byte {
	return append([]byte(groupMetaKeyPrefix), []byte(groupID)...)
}

// PersistGroup saves group metadata to the pending flush queue.
func (s *OffsetStore) PersistGroup(group *types.ConsumerGroup) error {
	s.groupMu.Lock()
	s.groups[group.GroupID] = group
	s.pendingGroups[group.GroupID] = group
	delete(s.pendingGroupDels, group.GroupID)
	s.groupMu.Unlock()
	s.dirty.Store(true)
	return nil
}

// DeletePersistedGroup marks a group for deletion from the store.
func (s *OffsetStore) DeletePersistedGroup(groupID string) error {
	s.groupMu.Lock()
	delete(s.groups, groupID)
	delete(s.pendingGroups, groupID)
	s.pendingGroupDels[groupID] = struct{}{}
	s.groupMu.Unlock()
	s.dirty.Store(true)
	return nil
}

// LoadGroups returns all persisted groups.
func (s *OffsetStore) LoadGroups() map[string]*types.ConsumerGroup {
	s.groupMu.RLock()
	defer s.groupMu.RUnlock()

	result := make(map[string]*types.ConsumerGroup, len(s.groups))
	for id, g := range s.groups {
		result[id] = g
	}
	return result
}

// GetGroup returns a persisted group by ID.
func (s *OffsetStore) GetGroup(groupID string) (*types.ConsumerGroup, bool) {
	s.groupMu.RLock()
	defer s.groupMu.RUnlock()
	g, ok := s.groups[groupID]
	return g, ok
}

// OffsetStore stores consumer group offsets, exactly-once commit IDs, and group metadata.
type OffsetStore struct {
	db          *pebble.DB
	dataDir     string
	partitionID int32
	dirty       atomic.Bool // Set on write, skip flush when clean
	quit        chan struct{}
	wg          sync.WaitGroup

	pendingMu sync.RWMutex
	pending   map[offsetKey]int64 // offset >= 0 is a commit. offset == -2 means deleted.

	// Exactly-once commit dedup: in-memory LRU of recent commit IDs.
	// Protected by commitMu. Capacity ~1M entries per partition.
	exactlyOnce       bool
	commitMu          sync.RWMutex
	recentCommits     map[string]struct{}
	commitQueue       []string            // FIFO for eviction
	pendingCommitIDs  map[string]struct{} // commit IDs waiting to be persisted
	pendingCommitDels map[string]struct{} // commit IDs waiting to be deleted from DB

	groupMu          sync.RWMutex
	groups           map[string]*types.ConsumerGroup
	pendingGroups    map[string]*types.ConsumerGroup
	pendingGroupDels map[string]struct{}
}

// NewOffsetStore creates a new offset store
func NewOffsetStore(dataDir string, partitionID int32, cache *pebble.Cache) (*OffsetStore, error) {
	// Create data directory
	dir := filepath.Join(dataDir, "consumer_offsets")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create offsets dir: %w", err)
	}

	// Open PebbleDB
	opts := &pebble.Options{
		Logger: nil, // Use default logger
	}
	if cache != nil {
		opts.Cache = cache
	}

	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("open pebble db: %w", err)
	}

	store := &OffsetStore{
		db:                db,
		dataDir:           dataDir,
		partitionID:       partitionID,
		quit:              make(chan struct{}),
		pending:           make(map[offsetKey]int64),
		recentCommits:     make(map[string]struct{}),
		commitQueue:       make([]string, 0, 1_000_000),
		pendingCommitIDs:  make(map[string]struct{}),
		pendingCommitDels: make(map[string]struct{}),
		groups:            make(map[string]*types.ConsumerGroup),
		pendingGroups:     make(map[string]*types.ConsumerGroup),
		pendingGroupDels:  make(map[string]struct{}),
	}

	// Load persisted state so in-memory maps are consistent after restart.
	if err := store.loadPersistedCommitIDs(); err != nil {
		db.Close()
		return nil, fmt.Errorf("load commit ids: %w", err)
	}
	if err := store.loadPersistedGroups(); err != nil {
		db.Close()
		return nil, fmt.Errorf("load groups: %w", err)
	}

	store.startFlushLoop()

	return store, nil
}

func (s *OffsetStore) startFlushLoop() {
	s.wg.Add(1)
	utils.GoSafe("offset-store-flush", func() {
		defer s.wg.Done()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Only flush if something was written since last flush
				if s.dirty.CompareAndSwap(true, false) {
					s.flushPending()
				}
			case <-s.quit:
				return
			}
		}
	})
}

func (s *OffsetStore) loadPersistedCommitIDs() error {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(commitIDKeyPrefix),
		UpperBound: []byte(commitIDKeyPrefix + "~"),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	type entry struct {
		id string
		ts int64
	}
	var entries []entry
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		id := key[len(commitIDKeyPrefix):]
		if id == "" {
			continue
		}
		ts := parseCommitIDValue(iter.Value())
		entries = append(entries, entry{id: id, ts: ts})
	}

	// Keep only the most recent commitIDCapacity entries by timestamp.
	if len(entries) > commitIDCapacity {
		sort.Slice(entries, func(i, j int) bool { return entries[i].ts < entries[j].ts })
		for _, e := range entries[:len(entries)-commitIDCapacity] {
			s.pendingCommitDels[e.id] = struct{}{}
		}
		entries = entries[len(entries)-commitIDCapacity:]
	}

	for _, e := range entries {
		s.recentCommits[e.id] = struct{}{}
		s.commitQueue = append(s.commitQueue, e.id)
	}
	return nil
}

func (s *OffsetStore) loadPersistedGroups() error {
	s.groupMu.Lock()
	defer s.groupMu.Unlock()

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(groupMetaKeyPrefix),
		UpperBound: []byte(groupMetaKeyPrefix + "~"),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		data, err := iter.ValueAndErr()
		if err != nil {
			continue
		}
		value := make([]byte, len(data))
		copy(value, data)

		var group types.ConsumerGroup
		if err := json.Unmarshal(value, &group); err != nil {
			continue
		}
		// Rebuild map pointers (json leaves nil maps if empty).
		if group.CommittedOffsets == nil {
			group.CommittedOffsets = make(map[int32]int64)
		}
		if group.MemberOffsets == nil {
			group.MemberOffsets = make(map[string]int64)
		}
		if group.Members == nil {
			group.Members = make(map[string]*types.ConsumerMember)
		}
		s.groups[group.GroupID] = &group
	}
	return nil
}

func (s *OffsetStore) flushPending() {
	s.pendingMu.Lock()
	toFlush := s.pending
	s.pending = make(map[offsetKey]int64)
	s.pendingMu.Unlock()

	s.commitMu.Lock()
	toCommitIDs := s.pendingCommitIDs
	s.pendingCommitIDs = make(map[string]struct{})
	toCommitDels := s.pendingCommitDels
	s.pendingCommitDels = make(map[string]struct{})
	s.commitMu.Unlock()

	s.groupMu.Lock()
	toGroups := s.pendingGroups
	s.pendingGroups = make(map[string]*types.ConsumerGroup)
	toGroupDels := s.pendingGroupDels
	s.pendingGroupDels = make(map[string]struct{})
	s.groupMu.Unlock()

	if len(toFlush) == 0 && len(toCommitIDs) == 0 && len(toCommitDels) == 0 && len(toGroups) == 0 && len(toGroupDels) == 0 {
		return
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	for k, val := range toFlush {
		dbKey := s.buildKey(k.groupID, k.partitionID)
		if val == -2 {
			_ = batch.Delete(dbKey, pebble.NoSync)
		} else {
			dbValue := s.buildValue(val)
			_ = batch.Set(dbKey, dbValue, pebble.NoSync)
		}
	}

	for id := range toCommitIDs {
		_ = batch.Set(s.commitIDKey(id), commitIDEntryValue(), pebble.NoSync)
	}
	for id := range toCommitDels {
		_ = batch.Delete(s.commitIDKey(id), pebble.NoSync)
	}
	for id, group := range toGroups {
		data, err := json.Marshal(group)
		if err != nil {
			fmt.Printf("[OFFSET_STORE] Failed to marshal group %s: %v\n", id, err)
			continue
		}
		_ = batch.Set(s.groupMetaKey(id), data, pebble.NoSync)
	}
	for id := range toGroupDels {
		_ = batch.Delete(s.groupMetaKey(id), pebble.NoSync)
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		fmt.Printf("[OFFSET_STORE] Failed to commit batch: %v\n", err)
	}

	_ = s.db.Flush()
}

// CommitOffset commits an offset for a consumer group.
func (s *OffsetStore) CommitOffset(groupID string, partitionID int32, offset int64) error {
	s.pendingMu.Lock()
	s.pending[offsetKey{groupID: groupID, partitionID: partitionID}] = offset
	s.pendingMu.Unlock()
	s.dirty.Store(true)
	return nil
}

// CommitOffsetExactlyOnce commits an offset with a commit ID for exactly-once semantics.
// Returns true if the commit was applied, false if it was a duplicate.
func (s *OffsetStore) CommitOffsetExactlyOnce(groupID string, partitionID int32, offset int64, commitID string) (bool, error) {
	if commitID == "" {
		// Fall back to at-least-once if no commit ID provided
		return true, s.CommitOffset(groupID, partitionID, offset)
	}

	s.commitMu.Lock()
	if _, exists := s.recentCommits[commitID]; exists {
		s.commitMu.Unlock()
		return false, nil // Duplicate commit ID
	}
	// Check persistent store for commit IDs that fell out of the in-memory cache.
	if _, closer, err := s.db.Get(s.commitIDKey(commitID)); err == nil {
		closer.Close()
		s.recentCommits[commitID] = struct{}{}
		s.commitQueue = append(s.commitQueue, commitID)
		s.evictCommitIDsIfNeededLocked()
		s.commitMu.Unlock()
		return false, nil
	}

	// Record commit ID in memory and queue it for persistence.
	s.recentCommits[commitID] = struct{}{}
	s.commitQueue = append(s.commitQueue, commitID)
	s.pendingCommitIDs[commitID] = struct{}{}
	s.evictCommitIDsIfNeededLocked()
	s.commitMu.Unlock()

	// Persist commit ID with offset
	s.pendingMu.Lock()
	s.pending[offsetKey{groupID: groupID, partitionID: partitionID}] = offset
	s.pendingMu.Unlock()
	s.dirty.Store(true)
	return true, nil
}

func (s *OffsetStore) evictCommitIDsIfNeededLocked() {
	if len(s.commitQueue) <= commitIDCapacity {
		return
	}
	oldest := s.commitQueue[0]
	s.commitQueue = s.commitQueue[1:]
	delete(s.recentCommits, oldest)
	// If the evicted ID was queued to be persisted but not flushed yet, drop it
	// from the insert queue; otherwise schedule a deletion from disk.
	if _, pending := s.pendingCommitIDs[oldest]; pending {
		delete(s.pendingCommitIDs, oldest)
	} else {
		s.pendingCommitDels[oldest] = struct{}{}
	}
}

// GetOffset gets committed offset for a consumer group.
func (s *OffsetStore) GetOffset(groupID string, partitionID int32) (int64, error) {
	key := offsetKey{groupID: groupID, partitionID: partitionID}
	s.pendingMu.RLock()
	val, ok := s.pending[key]
	s.pendingMu.RUnlock()

	if ok {
		if val == -2 {
			return -1, nil // Deletion pending
		}
		return val, nil
	}

	dbKey := s.buildKey(groupID, partitionID)
	value, closer, err := s.db.Get(dbKey)
	if err == pebble.ErrNotFound {
		return -1, nil // No committed offset
	}
	if err != nil {
		return 0, fmt.Errorf("get offset: %w", err)
	}
	defer closer.Close()

	return s.parseValue(value)
}

// DeleteOffset deletes committed offset for a consumer group
func (s *OffsetStore) DeleteOffset(groupID string, partitionID int32) error {
	s.pendingMu.Lock()
	s.pending[offsetKey{groupID: groupID, partitionID: partitionID}] = -2
	s.pendingMu.Unlock()
	s.dirty.Store(true)
	return nil
}

// buildKey builds storage key
func (s *OffsetStore) buildKey(groupID string, partitionID int32) []byte {
	key := make([]byte, 4+len(groupID)+4)
	offset := 0

	// Partition ID (4 bytes) - FIXED to use binary.BigEndian
	binary.BigEndian.PutUint32(key[offset:], uint32(partitionID))
	offset += 4

	// Group ID
	copy(key[offset:], groupID)
	offset += len(groupID)

	// Separator
	key[offset] = 0

	return key
}

// buildValue builds storage value
func (s *OffsetStore) buildValue(offset int64) []byte {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, uint64(offset))
	return value
}

// buildValueExactlyOnce builds storage value with commit ID
func (s *OffsetStore) buildValueExactlyOnce(offset int64, commitID string) []byte {
	idBytes := uuid.MustParse(commitID)
	value := make([]byte, 8+16)
	binary.BigEndian.PutUint64(value[0:8], uint64(offset))
	copy(value[8:24], idBytes[:])
	return value
}

// parseValue parses storage value
func (s *OffsetStore) parseValue(value []byte) (int64, error) {
	if len(value) < 8 {
		return 0, fmt.Errorf("invalid value size")
	}

	offset := int64(binary.BigEndian.Uint64(value[0:8]))
	return offset, nil
}

// Close closes the offset store
func (s *OffsetStore) Close() error {
	close(s.quit)
	s.wg.Wait()
	s.flushPending()
	return s.db.Close()
}

// Checkpoint creates a PebbleDB checkpoint of the offset store at destDir.
// Pending in-memory commits are flushed before the checkpoint so it is
// consistent with the latest committed offsets.
func (s *OffsetStore) Checkpoint(destDir string) error {
	s.flushPending()
	if err := s.db.Checkpoint(destDir); err != nil {
		return fmt.Errorf("offset checkpoint: %w", err)
	}
	return nil
}

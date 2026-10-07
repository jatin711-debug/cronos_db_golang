package consumer

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// A group's progress on a partition has two parts. The completion floor is
// the offset below which every event is complete; it only moves over events
// whose completion the server validated. Above the floor, completion is
// recorded per event, because schedule order need not match WAL order and a
// later delivery must not carry the floor over an unfinished timer. Records the
// floor has passed are redundant and are removed as it advances.
//
// The floor is distinct from the group's committed offset, which older APIs
// can set directly: an offset committed that way proves nothing about the
// events below it, so it never counts as completion.
//
// Completion is recorded by offset, so it is only as good as the log entry at
// that offset. A replica's log can lose its end: a follower drops what a
// replaced leader wrote, a node that crashed comes back without what it had
// not synced. The offsets are then given to other events, and a record that
// outlived its entry would make the new event count as finished before it
// was ever delivered. So a replica keeps no completion at or beyond the end
// of its own log: ForgetCompletionsFrom removes it when the log is cut and
// when a partition starts, and ApplyReplicatedProgress takes a leader's
// progress only as far as the local log reaches.

func completionKey(group string, partition int32, offset int64) string {
	return fmt.Sprintf("done:%d:%s:%d:%020d", len(group), group, partition, offset)
}

func completionPrefix(group string, partition int32) string {
	return fmt.Sprintf("done:%d:%s:%d:", len(group), group, partition)
}

func floorKey(group string, partition int32) string {
	return floorKeyPrefix + completionPrefix(group, partition)
}

// floorLocked returns the group's completion floor on a partition. The caller
// holds g.mu. Nothing below the start of the log is left to do, for a group
// created after the log was cut as for any other.
func (g *GroupManager) floorLocked(group string, partition int32) int64 {
	return max(g.floors[completionPrefix(group, partition)], g.logStart[partition])
}

// SetLogStart records where a partition's log now starts. The entries below
// were released because every group that takes them had finished them, so from
// here on they count as complete for every group, and each group's completion
// floor is raised to the new start: a floor left below it would wait for
// entries that no longer exist. The start only moves forward.
func (g *GroupManager) SetLogStart(partitionID int32, offset int64) error {
	if offset <= 0 {
		return nil
	}
	g.commitMu.Lock()
	defer g.commitMu.Unlock()

	type raise struct {
		group string
		from  int64
	}
	g.mu.RLock()
	if g.logStart[partitionID] >= offset {
		g.mu.RUnlock()
		return nil
	}
	var raises []raise
	for id, group := range g.groups {
		if _, tracked := group.CommittedOffsets[partitionID]; !tracked {
			continue
		}
		if floor := g.floors[completionPrefix(id, partitionID)]; floor < offset {
			raises = append(raises, raise{id, floor})
		}
	}
	store := g.offsetStore
	g.mu.RUnlock()

	if store != nil && len(raises) > 0 {
		// Not synced: the log start is read from the log again at startup.
		err := store.withDB(func(db *pebble.DB) error {
			batch := db.NewBatch()
			defer batch.Close()
			for _, r := range raises {
				if err := setFloorInBatch(batch, r.group, partitionID, r.from, offset); err != nil {
					return err
				}
			}
			return batch.Commit(pebble.NoSync)
		})
		if err != nil {
			return fmt.Errorf("raise completion floors to log start %d: %w", offset, err)
		}
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if g.logStart == nil {
		g.logStart = make(map[int32]int64)
	}
	g.logStart[partitionID] = offset
	for _, r := range raises {
		group := g.groups[r.group]
		if group == nil {
			continue
		}
		prefix := completionPrefix(r.group, partitionID)
		if store == nil {
			for key := range g.completed {
				if strings.HasPrefix(key, prefix) {
					if done, ok := parseCompletionOffset(key, prefix); ok && done < offset {
						delete(g.completed, key)
					}
				}
			}
		}
		g.setFloorLocked(r.group, partitionID, offset)
		if offset > group.CommittedOffsets[partitionID] {
			group.CommittedOffsets[partitionID] = offset
			if store != nil {
				_ = store.CommitOffset(r.group, partitionID, offset)
			}
		}
		g.raiseHighestCompleted(prefix, offset-1)
	}
	g.progressVersion.Add(1)
	return nil
}

func (g *GroupManager) setFloorLocked(group string, partition int32, floor int64) {
	if g.floors == nil {
		g.floors = make(map[string]int64)
	}
	g.floors[completionPrefix(group, partition)] = floor
}

func (g *GroupManager) completedLocked(group string, partition int32, offset int64) bool {
	if offset < g.floorLocked(group, partition) {
		return true
	}
	key := completionKey(group, partition, offset)
	if g.offsetStore == nil {
		return g.completed[key]
	}
	return g.offsetStore.withDB(func(db *pebble.DB) error {
		_, closer, err := db.Get([]byte(key))
		if err != nil {
			return err
		}
		return closer.Close()
	}) == nil
}

func (g *GroupManager) IsCompleted(group string, partition int32, offset int64) bool {
	// Newly published events sit above every completion. Answering those from
	// memory keeps a lock and a store lookup off the live delivery path.
	if offset > g.highestCompleted(group, partition) {
		return false
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.completedLocked(group, partition, offset)
}

// highestCompleted returns the largest offset known complete for the group on
// this partition (by record or by floor), or -1 when there is none.
func (g *GroupManager) highestCompleted(group string, partition int32) int64 {
	prefix := completionPrefix(group, partition)
	g.completedMaxMu.Lock()
	highest, known := g.completedMax[prefix]
	g.completedMaxMu.Unlock()
	if known {
		return highest
	}

	// First question for this group since startup: derive it from the stored
	// state. A completion committed meanwhile only raises the cached value.
	g.mu.RLock()
	belowFloor := g.floorLocked(group, partition) - 1
	g.mu.RUnlock()
	return g.raiseHighestCompleted(prefix, max(belowFloor, g.loadHighestCompleted(prefix)))
}

func (g *GroupManager) raiseHighestCompleted(prefix string, offset int64) int64 {
	g.completedMaxMu.Lock()
	defer g.completedMaxMu.Unlock()
	if g.completedMax == nil {
		g.completedMax = make(map[string]int64)
	}
	if current, known := g.completedMax[prefix]; known && current >= offset {
		return current
	}
	g.completedMax[prefix] = offset
	return offset
}

// lowerHighestCompleted brings the cached value down to offset. A value that
// is too low only costs a lookup of the stored state.
func (g *GroupManager) lowerHighestCompleted(prefix string, offset int64) {
	g.completedMaxMu.Lock()
	if current, known := g.completedMax[prefix]; known && current > offset {
		g.completedMax[prefix] = offset
	}
	g.completedMaxMu.Unlock()
}

// ForgetCompletionsFrom removes what is recorded as complete at offset and
// beyond on a partition, for every group: the log does not hold those entries
// any more, or never did. Each group's floor and committed offset are brought
// down to offset if they were past it. It reports how many groups had
// something to forget.
func (g *GroupManager) ForgetCompletionsFrom(partitionID int32, offset int64) (int, error) {
	offset = max(offset, 0)
	g.commitMu.Lock()
	defer g.commitMu.Unlock()

	g.mu.RLock()
	var tracked []string
	for id, group := range g.groups {
		if _, ok := group.CommittedOffsets[partitionID]; ok {
			tracked = append(tracked, id)
		}
	}
	store := g.offsetStore
	g.mu.RUnlock()

	// floors holds, for each group with something to forget, its stored floor.
	floors := make(map[string]int64)
	for _, id := range tracked {
		highest := g.highestCompleted(id, partitionID)
		g.mu.RLock()
		floor := g.floors[completionPrefix(id, partitionID)]
		committed := g.groups[id].CommittedOffsets[partitionID]
		g.mu.RUnlock()
		if highest >= offset || floor > offset || committed > offset {
			floors[id] = floor
		}
	}
	if len(floors) == 0 {
		return 0, nil
	}

	if store != nil {
		err := store.withDB(func(db *pebble.DB) error {
			batch := db.NewBatch()
			defer batch.Close()
			for id, floor := range floors {
				prefix := completionPrefix(id, partitionID)
				if err := batch.DeleteRange([]byte(completionKey(id, partitionID, offset)), []byte(prefix+":"), nil); err != nil {
					return err
				}
				if floor > offset {
					value := make([]byte, 8)
					binary.BigEndian.PutUint64(value, uint64(offset))
					if err := batch.Set([]byte(floorKey(id, partitionID)), value, nil); err != nil {
						return err
					}
				}
			}
			return batch.Commit(pebble.Sync)
		})
		if err != nil {
			return 0, fmt.Errorf("forget completion from offset %d: %w", offset, err)
		}
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	for id, floor := range floors {
		group := g.groups[id]
		if group == nil {
			continue
		}
		prefix := completionPrefix(id, partitionID)
		if store == nil {
			for key := range g.completed {
				if strings.HasPrefix(key, prefix) {
					if done, ok := parseCompletionOffset(key, prefix); ok && done >= offset {
						delete(g.completed, key)
					}
				}
			}
		}
		if floor > offset {
			g.setFloorLocked(id, partitionID, offset)
		}
		if group.CommittedOffsets[partitionID] > offset {
			group.CommittedOffsets[partitionID] = offset
			group.UpdatedTS = time.Now().UnixMilli()
			if store != nil {
				_ = store.CommitOffset(id, partitionID, offset)
			}
			// The group record carries the committed offset too, and the
			// higher of the two is taken when it is read back.
			g.persistGroup(group)
		}
		g.lowerHighestCompleted(prefix, offset-1)
	}
	g.progressVersion.Add(1)
	return len(floors), nil
}

func parseCompletionOffset(key, prefix string) (int64, bool) {
	offset, err := strconv.ParseInt(strings.TrimPrefix(key, prefix), 10, 64)
	return offset, err == nil
}

func (g *GroupManager) loadHighestCompleted(prefix string) int64 {
	highest := int64(-1)
	if g.offsetStore == nil {
		g.mu.RLock()
		for key := range g.completed {
			if strings.HasPrefix(key, prefix) {
				if offset, ok := parseCompletionOffset(key, prefix); ok && offset > highest {
					highest = offset
				}
			}
		}
		g.mu.RUnlock()
		return highest
	}
	// Offsets are zero-padded digits, so the last key under the prefix is the
	// highest, and ':' (the byte after '9') bounds them from above.
	_ = g.offsetStore.withDB(func(db *pebble.DB) error {
		iter, err := db.NewIter(&pebble.IterOptions{
			LowerBound: []byte(prefix),
			UpperBound: []byte(prefix + ":"),
		})
		if err != nil {
			return err
		}
		defer iter.Close()
		if iter.Last() {
			if offset, ok := parseCompletionOffset(string(iter.Key()), prefix); ok {
				highest = offset
			}
		}
		return nil
	})
	return highest
}

// WithRetentionCheck gives a caller that verifies and prunes WAL segments the
// test an event has to pass. At least one group must own the event, and every
// matching group must have completed it: the event is below the group's
// completion floor or has its own completion record. A committed offset alone
// is not enough.
//
// The lock is taken for each event, not for the whole pass. A pass reads
// whole segments, and held throughout it stopped every acknowledgement to the
// partition, and with them deliveries, until the pass was over. A group that
// is created during a pass may find that the entries the pass removes are
// gone; it would have found the same had it been created a moment later.
func (g *GroupManager) WithRetentionCheck(fn func(func(*types.Event) bool) (int, error)) (int, error) {
	return fn(func(event *types.Event) bool {
		g.mu.RLock()
		defer g.mu.RUnlock()
		matched := false
		for _, group := range g.groups {
			if group.Topic != event.Topic {
				continue
			}
			for _, id := range group.Partitions {
				if id != event.PartitionId {
					continue
				}
				matched = true
				if !g.completedLocked(group.GroupID, id, event.Offset) {
					return false
				}
				break
			}
		}
		return matched
	})
}

// DeliveryCommit is one acknowledged delivery: the group that received it and
// the server-validated records it carried.
type DeliveryCommit struct {
	GroupID string
	Events  []*types.Event
}

// CommitDelivery accepts server-validated delivery records, never a client cursor.
func (g *GroupManager) CommitDelivery(groupID string, partitionID int32, events []*types.Event) error {
	return g.CommitDeliveries(partitionID, []DeliveryCommit{{GroupID: groupID, Events: events}})[0]
}

// CommitDeliveries records completion for several deliveries of one partition
// with a single durable write, so acks that arrive together share one fsync.
// The same write advances each group's completion floor over its completed
// prefix and drops the records the floor has passed. It returns one error slot
// per commit, in order.
func (g *GroupManager) CommitDeliveries(partitionID int32, commits []DeliveryCommit) []error {
	errs := make([]error, len(commits))

	// One commit at a time: each derives the new floor from the previous one.
	// g.mu is not held across the durable write, so completion lookups and the
	// delivery path keep running during the fsync.
	g.commitMu.Lock()
	defer g.commitMu.Unlock()

	g.mu.RLock()
	cursors := make(map[string]int64)
	for i, commit := range commits {
		group, ok := g.groups[commit.GroupID]
		if !ok {
			errs[i] = fmt.Errorf("group %s not found", commit.GroupID)
			continue
		}
		for _, event := range commit.Events {
			if event == nil || event.Offset < 0 || event.PartitionId != partitionID || event.Topic != group.Topic {
				errs[i] = fmt.Errorf("delivery does not belong to consumer group")
				break
			}
		}
		if errs[i] == nil {
			cursors[commit.GroupID] = g.floorLocked(commit.GroupID, partitionID)
		}
	}
	store := g.offsetStore
	g.mu.RUnlock()

	// added holds, per group, the offsets this call completes.
	added := make(map[string]map[int64]struct{}, len(cursors))
	for i, commit := range commits {
		if errs[i] != nil {
			continue
		}
		offsets := added[commit.GroupID]
		if offsets == nil {
			offsets = make(map[int64]struct{}, len(commit.Events))
			added[commit.GroupID] = offsets
		}
		for _, event := range commit.Events {
			offsets[event.Offset] = struct{}{}
		}
	}

	advanced := make(map[string]int64, len(cursors))
	if store != nil {
		err := store.withDB(func(db *pebble.DB) error {
			batch := db.NewBatch()
			defer batch.Close()
			for group, offsets := range added {
				for offset := range offsets {
					if offset < cursors[group] {
						continue // already covered by the floor
					}
					if err := batch.Set([]byte(completionKey(group, partitionID, offset)), []byte{1}, nil); err != nil {
						return err
					}
				}
				recorded := func(offset int64) bool {
					_, closer, err := db.Get([]byte(completionKey(group, partitionID, offset)))
					if err != nil {
						return false
					}
					closer.Close()
					return true
				}
				cursor := advanceCursor(cursors[group], offsets, recorded)
				advanced[group] = cursor
				if cursor > cursors[group] {
					if err := setFloorInBatch(batch, group, partitionID, cursors[group], cursor); err != nil {
						return err
					}
				}
			}
			if batch.Empty() {
				return nil
			}
			return batch.Commit(pebble.Sync)
		})
		if err != nil {
			err = fmt.Errorf("persist delivery completion: %w", err)
			for i := range errs {
				if errs[i] == nil {
					errs[i] = err
				}
			}
			return errs
		}
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	for groupID, offsets := range added {
		group, ok := g.groups[groupID]
		if !ok {
			continue // removed meanwhile; reported per commit below
		}
		if store == nil {
			if g.completed == nil {
				g.completed = make(map[string]bool)
			}
			for offset := range offsets {
				g.completed[completionKey(groupID, partitionID, offset)] = true
			}
			advanced[groupID] = advanceCursor(cursors[groupID], offsets, func(offset int64) bool {
				return g.completed[completionKey(groupID, partitionID, offset)]
			})
			for offset := cursors[groupID]; offset < advanced[groupID]; offset++ {
				delete(g.completed, completionKey(groupID, partitionID, offset))
			}
		}
		highest := advanced[groupID] - 1
		for offset := range offsets {
			highest = max(highest, offset)
		}
		g.raiseHighestCompleted(completionPrefix(groupID, partitionID), highest)

		floor := advanced[groupID]
		if floor > cursors[groupID] {
			g.setFloorLocked(groupID, partitionID, floor)
		}
		if floor > group.CommittedOffsets[partitionID] {
			group.CommittedOffsets[partitionID] = floor
			group.UpdatedTS = time.Now().UnixMilli()
			if store != nil {
				_ = store.CommitOffset(groupID, partitionID, floor)
			}
		}
	}
	for i, commit := range commits {
		if errs[i] == nil {
			if _, ok := g.groups[commit.GroupID]; !ok {
				errs[i] = fmt.Errorf("group %s not found", commit.GroupID)
			}
		}
	}
	g.progressVersion.Add(1)
	return errs
}

// floorKeyPrefix prefixes the stored completion floor of a group on a partition.
const floorKeyPrefix = "floor:"

// setFloorInBatch raises a group's stored completion floor from old to floor
// and removes the completion records it now covers, in the caller's batch.
func setFloorInBatch(batch *pebble.Batch, group string, partition int32, old, floor int64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, uint64(floor))
	if err := batch.Set([]byte(floorKey(group, partition)), value, nil); err != nil {
		return err
	}
	return batch.DeleteRange([]byte(completionKey(group, partition, old)), []byte(completionKey(group, partition, floor)), nil)
}

// loadFloors reads every stored completion floor, keyed by completion prefix.
func (s *OffsetStore) loadFloors() (map[string]int64, error) {
	floors := make(map[string]int64)
	err := s.withDB(func(db *pebble.DB) error {
		iter, err := db.NewIter(&pebble.IterOptions{
			LowerBound: []byte(floorKeyPrefix),
			UpperBound: []byte(floorKeyPrefix + "~"),
		})
		if err != nil {
			return err
		}
		defer iter.Close()
		for iter.First(); iter.Valid(); iter.Next() {
			value, err := iter.ValueAndErr()
			if err != nil || len(value) < 8 {
				continue
			}
			floors[strings.TrimPrefix(string(iter.Key()), floorKeyPrefix)] = int64(binary.BigEndian.Uint64(value))
		}
		return nil
	})
	return floors, err
}

// advanceCursor moves a completion floor over the contiguous run of completed
// offsets that starts at it. An offset is complete when this call adds it or a
// record for it already exists.
func advanceCursor(cursor int64, added map[int64]struct{}, recorded func(int64) bool) int64 {
	for {
		if _, ok := added[cursor]; !ok && !recorded(cursor) {
			return cursor
		}
		cursor++
	}
}

// maxExportedCompletions bounds the per-group completion set sent to followers.
const maxExportedCompletions = 100_000

// ExportProgress returns every group's progress on a partition, in the form a
// follower needs to continue from: the completion floor and the completed
// offsets above it. version changes whenever progress does, so a caller can
// skip unchanged state.
func (g *GroupManager) ExportProgress(partitionID int32) (version uint64, groups []*types.ConsumerGroupProgress) {
	version = g.progressVersion.Load()
	g.mu.RLock()
	defer g.mu.RUnlock()
	for _, group := range g.groups {
		if _, tracked := group.CommittedOffsets[partitionID]; !tracked {
			continue
		}
		cursor := g.floorLocked(group.GroupID, partitionID)
		groups = append(groups, &types.ConsumerGroupProgress{
			GroupId:          group.GroupID,
			Topic:            group.Topic,
			CommittedOffset:  cursor,
			CompletedOffsets: g.completedFromLocked(group.GroupID, partitionID, cursor),
		})
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].GroupId < groups[j].GroupId })
	return version, groups
}

// completedFromLocked lists the offsets at or above from that have completion
// records, in ascending order. The caller holds g.mu.
func (g *GroupManager) completedFromLocked(group string, partition int32, from int64) []int64 {
	prefix := completionPrefix(group, partition)
	var offsets []int64
	if g.offsetStore == nil {
		for key := range g.completed {
			if strings.HasPrefix(key, prefix) {
				if offset, ok := parseCompletionOffset(key, prefix); ok && offset >= from {
					offsets = append(offsets, offset)
				}
			}
		}
		sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
		if len(offsets) > maxExportedCompletions {
			offsets = offsets[:maxExportedCompletions]
		}
		return offsets
	}
	_ = g.offsetStore.withDB(func(db *pebble.DB) error {
		iter, err := db.NewIter(&pebble.IterOptions{
			LowerBound: []byte(completionKey(group, partition, from)),
			UpperBound: []byte(prefix + ":"),
		})
		if err != nil {
			return err
		}
		defer iter.Close()
		for iter.First(); iter.Valid() && len(offsets) < maxExportedCompletions; iter.Next() {
			if offset, ok := parseCompletionOffset(string(iter.Key()), prefix); ok {
				offsets = append(offsets, offset)
			}
		}
		return nil
	})
	return offsets
}

// ApplyReplicatedProgress installs the leader's consumer progress on a
// follower. Progress only moves forward: a floor behind the local one is
// ignored, and completions are added, never removed. The write is not synced;
// the leader re-sends its full progress, so a lost tail is repaired by the
// next round.
//
// logEnd is where the follower's log ends: the offset its next entry gets.
// Progress at or beyond it is left for a later round. It is about entries
// this replica does not hold, and may never get: if it leads next, those
// offsets go to new events, which must not count as finished.
func (g *GroupManager) ApplyReplicatedProgress(partitionID int32, logEnd int64, groups []*types.ConsumerGroupProgress) error {
	g.commitMu.Lock()
	defer g.commitMu.Unlock()

	g.mu.RLock()
	cursors := make(map[string]int64, len(groups))
	for _, progress := range groups {
		if existing := g.groups[progress.GetGroupId()]; existing != nil && existing.Topic != progress.GetTopic() {
			g.mu.RUnlock()
			return fmt.Errorf("consumer group %s belongs to topic %q, not %q", progress.GetGroupId(), existing.Topic, progress.GetTopic())
		}
		cursors[progress.GetGroupId()] = g.floorLocked(progress.GetGroupId(), partitionID)
	}
	store := g.offsetStore
	g.mu.RUnlock()

	if store != nil {
		err := store.withDB(func(db *pebble.DB) error {
			batch := db.NewBatch()
			defer batch.Close()
			for _, progress := range groups {
				group, cursor := progress.GetGroupId(), max(min(progress.GetCommittedOffset(), logEnd), cursors[progress.GetGroupId()])
				for _, offset := range progress.GetCompletedOffsets() {
					if offset < cursor || offset >= logEnd {
						continue
					}
					if err := batch.Set([]byte(completionKey(group, partitionID, offset)), []byte{1}, nil); err != nil {
						return err
					}
				}
				if cursor > cursors[group] {
					if err := setFloorInBatch(batch, group, partitionID, cursors[group], cursor); err != nil {
						return err
					}
				}
			}
			if batch.Empty() {
				return nil
			}
			return batch.Commit(pebble.NoSync)
		})
		if err != nil {
			return fmt.Errorf("persist replicated consumer progress: %w", err)
		}
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now().UnixMilli()
	for _, progress := range groups {
		groupID := progress.GetGroupId()
		group := g.groups[groupID]
		changed := false
		if group == nil {
			group = &types.ConsumerGroup{
				GroupID:          groupID,
				Topic:            progress.GetTopic(),
				CommittedOffsets: make(map[int32]int64),
				MemberOffsets:    make(map[string]int64),
				Members:          make(map[string]*types.ConsumerMember),
				CreatedTS:        now,
			}
			g.groups[groupID] = group
			changed = true
		}
		if _, tracked := group.CommittedOffsets[partitionID]; !tracked {
			group.Partitions = append(group.Partitions, partitionID)
			group.CommittedOffsets[partitionID] = -1
			changed = true
		}
		cursor := max(min(progress.GetCommittedOffset(), logEnd), cursors[groupID])
		highest := cursor - 1
		for _, offset := range progress.GetCompletedOffsets() {
			if offset < cursor || offset >= logEnd {
				continue
			}
			highest = max(highest, offset)
			if store == nil {
				if g.completed == nil {
					g.completed = make(map[string]bool)
				}
				g.completed[completionKey(groupID, partitionID, offset)] = true
			}
		}
		if cursor > cursors[groupID] {
			if store == nil {
				for offset := cursors[groupID]; offset < cursor; offset++ {
					delete(g.completed, completionKey(groupID, partitionID, offset))
				}
			}
			g.setFloorLocked(groupID, partitionID, cursor)
		}
		if cursor > group.CommittedOffsets[partitionID] {
			if store != nil {
				_ = store.CommitOffset(groupID, partitionID, cursor)
			}
			group.CommittedOffsets[partitionID] = cursor
			changed = true
		}
		g.raiseHighestCompleted(completionPrefix(groupID, partitionID), highest)
		if changed {
			group.UpdatedTS = now
			g.persistGroup(group)
		}
	}
	return nil
}

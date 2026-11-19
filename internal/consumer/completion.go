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
// recorded per event, as a bit in a block of offsets (see completion_set.go),
// because schedule order need not match WAL order and a later delivery must
// not carry the floor over an unfinished timer. Bits the floor has passed are
// redundant and are cleared as it advances.
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

func completionPrefix(group string, partition int32) string {
	return string(appendCompletionPrefix(make([]byte, 0, len(group)+24), group, partition))
}

// appendCompletionPrefix appends completionPrefix(group, partition) to dst.
func appendCompletionPrefix(dst []byte, group string, partition int32) []byte {
	dst = append(dst, "done:"...)
	dst = strconv.AppendInt(dst, int64(len(group)), 10)
	dst = append(dst, ':')
	dst = append(dst, group...)
	dst = append(dst, ':')
	dst = strconv.AppendInt(dst, int64(partition), 10)
	return append(dst, ':')
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

	g.mu.RLock()
	if g.logStart[partitionID] >= offset {
		g.mu.RUnlock()
		return nil
	}
	var plans []*completionPlan // for groups whose floor is below the new start
	for id, group := range g.groups {
		if _, tracked := group.CommittedOffsets[partitionID]; !tracked {
			continue
		}
		if g.floors[completionPrefix(id, partitionID)] < offset {
			plans = append(plans, g.planLocked(id, partitionID, nil, offset, -1, false))
		}
	}
	store := g.offsetStore
	g.mu.RUnlock()

	if store != nil && len(plans) > 0 {
		// Not synced: the log start is read from the log again at startup.
		err := store.withDB(func(db *pebble.DB) error {
			batch := db.NewBatch()
			defer batch.Close()
			for _, p := range plans {
				if err := writePlan(batch, partitionID, p); err != nil {
					return err
				}
			}
			return batch.Commit(pebble.NoSync)
		})
		if err != nil {
			return fmt.Errorf("raise completion floors to log start %d: %w", offset, err)
		}
		if progressWritten != nil {
			progressWritten()
		}
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if g.logStart == nil {
		g.logStart = make(map[int32]int64)
	}
	g.logStart[partitionID] = offset
	for _, p := range plans {
		group := g.groups[p.group]
		if group == nil {
			continue
		}
		g.installPlanLocked(partitionID, p)
		if offset > group.CommittedOffsets[partitionID] {
			group.CommittedOffsets[partitionID] = offset
			if store != nil {
				_ = store.CommitOffset(p.group, partitionID, offset)
			}
		}
		g.raiseHighestCompleted(p.prefix, offset-1)
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
	if g.offsetStore != nil && g.offsetStore.isClosed() {
		return false // a closed store vouches for nothing above the floor
	}
	return g.sets[completionPrefix(group, partition)].has(offset)
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
	// state, and take any raise made while it was being read.
	g.mu.RLock()
	belowFloor := g.floorLocked(group, partition) - 1
	g.mu.RUnlock()
	loaded := max(belowFloor, g.loadHighestCompleted(prefix))

	g.completedMaxMu.Lock()
	defer g.completedMaxMu.Unlock()
	if current, known := g.completedMax[prefix]; known {
		highest = max(current, loaded)
	} else {
		highest = loaded
		if raised, ok := g.completedRaised[prefix]; ok {
			highest = max(highest, raised)
		}
	}
	delete(g.completedRaised, prefix)
	if g.completedMax == nil {
		g.completedMax = make(map[string]int64)
	}
	g.completedMax[prefix] = highest
	return highest
}

// raiseHighestCompleted notes that offset is complete. The caller has already
// recorded it, in the store or in memory.
//
// Until the cached value has been read from the stored state it is not set
// here. It used to be: a partition that started raised its groups' floors to
// the start of its log, and the floor became the cached value before anything
// had read the completions recorded above it. Every event completed above the
// floor then read as not complete and was delivered again; a group whose
// floor cannot pass another topic's entries keeps all its progress there.
// The raise is kept until the first read, which takes it into account.
func (g *GroupManager) raiseHighestCompleted(prefix string, offset int64) {
	g.completedMaxMu.Lock()
	defer g.completedMaxMu.Unlock()
	if current, known := g.completedMax[prefix]; known {
		if offset > current {
			g.completedMax[prefix] = offset
		}
		return
	}
	if raised, ok := g.completedRaised[prefix]; ok && raised >= offset {
		return
	}
	if g.completedRaised == nil {
		g.completedRaised = make(map[string]int64)
	}
	g.completedRaised[prefix] = offset
}

// lowerHighestCompleted brings the cached value down to offset. A value that
// is too high only costs a lookup of the stored state; one that is too low
// makes completed events look new.
func (g *GroupManager) lowerHighestCompleted(prefix string, offset int64) {
	g.completedMaxMu.Lock()
	if current, known := g.completedMax[prefix]; known && current > offset {
		g.completedMax[prefix] = offset
	}
	if raised, ok := g.completedRaised[prefix]; ok && raised > offset {
		g.completedRaised[prefix] = offset
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

	// Whatever was read of the log before now may be replaced.
	g.NoteLogCut(partitionID)

	g.mu.RLock()
	var tracked []string
	for id, group := range g.groups {
		if _, ok := group.CommittedOffsets[partitionID]; ok {
			tracked = append(tracked, id)
		}
	}
	store := g.offsetStore
	g.mu.RUnlock()

	var plans []*completionPlan // for each group with something to forget
	for _, id := range tracked {
		highest := g.highestCompleted(id, partitionID)
		g.mu.RLock()
		prefix := completionPrefix(id, partitionID)
		floor := g.floors[prefix]
		committed := g.groups[id].CommittedOffsets[partitionID]
		if highest >= offset || floor > offset || committed > offset {
			plans = append(plans, g.forgetPlanLocked(id, prefix, floor, offset))
		}
		g.mu.RUnlock()
	}
	if len(plans) == 0 {
		return 0, nil
	}

	if store != nil {
		err := store.withDB(func(db *pebble.DB) error {
			batch := db.NewBatch()
			defer batch.Close()
			for _, p := range plans {
				if err := writePlan(batch, partitionID, p); err != nil {
					return err
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
	for _, p := range plans {
		group := g.groups[p.group]
		if group == nil {
			continue
		}
		g.installPlanLocked(partitionID, p)
		if group.CommittedOffsets[partitionID] > offset {
			group.CommittedOffsets[partitionID] = offset
			group.UpdatedTS = time.Now().UnixMilli()
			if store != nil {
				_ = store.CommitOffset(p.group, partitionID, offset)
			}
			// The group record carries the committed offset too, and the
			// higher of the two is taken when it is read back.
			g.persistGroup(group)
		}
		g.lowerHighestCompleted(p.prefix, offset-1)
	}
	// The journal may name offsets that are gone now; followers get the
	// full copy next.
	g.journal.reset(partitionID, g.progressVersion.Add(1))
	return len(plans), nil
}

// forgetPlanLocked makes the plan that removes a group's completions at and
// beyond offset and brings its stored floor down to offset if it was past it.
// The caller holds commitMu and at least a read lock on mu.
func (g *GroupManager) forgetPlanLocked(group, prefix string, floor, offset int64) *completionPlan {
	p := &completionPlan{group: group, prefix: prefix, before: floor, floor: min(floor, offset), put: make(map[int64]*completionBlock)}
	if set := g.sets[prefix]; set != nil {
		for block, held := range set.blocks {
			switch {
			case block > blockOf(offset):
				p.drop = append(p.drop, block)
			case block == blockOf(offset):
				kept := *held
				kept.clearFrom(offset)
				if kept.empty() {
					p.drop = append(p.drop, block)
				} else if kept != *held {
					p.put[block] = &kept
				}
			}
		}
	}
	return p
}

// loadHighestCompleted returns the highest offset recorded complete above
// the floor for the group and partition of prefix, or -1.
func (g *GroupManager) loadHighestCompleted(prefix string) int64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.sets[prefix].highest()
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
// prefix; the records the floor has passed are dropped once it is raised in
// memory as well (see setFloorInBatch). It returns one error slot per commit,
// in order.
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
	// added holds, per group, the offsets this call completes.
	added := make(map[string][]int64, len(cursors))
	for i, commit := range commits {
		if errs[i] != nil {
			continue
		}
		for _, event := range commit.Events {
			added[commit.GroupID] = append(added[commit.GroupID], event.Offset)
		}
	}
	plans := make(map[string]*completionPlan, len(added))
	for group, offsets := range added {
		plans[group] = g.planLocked(group, partitionID, offsets, 0, -1, true)
	}
	store := g.offsetStore
	g.mu.RUnlock()

	if store != nil {
		err := store.withDB(func(db *pebble.DB) error {
			batch := db.NewBatch()
			defer batch.Close()
			for _, p := range plans {
				if err := writePlan(batch, partitionID, p); err != nil {
					return err
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
		if progressWritten != nil {
			progressWritten()
		}
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	// Followers that hold the progress so far are sent only these.
	journaled := make(map[string][]int64, len(added))
	for groupID, p := range plans {
		group, ok := g.groups[groupID]
		if !ok {
			continue // removed meanwhile; reported per commit below
		}
		g.installPlanLocked(partitionID, p)
		highest := p.floor - 1
		for _, offset := range added[groupID] {
			highest = max(highest, offset)
			if offset >= cursors[groupID] {
				journaled[groupID] = append(journaled[groupID], offset)
			}
		}
		g.raiseHighestCompleted(p.prefix, highest)
		if p.floor > group.CommittedOffsets[partitionID] {
			group.CommittedOffsets[partitionID] = p.floor
			group.UpdatedTS = time.Now().UnixMilli()
			if store != nil {
				_ = store.CommitOffset(groupID, partitionID, p.floor)
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
	g.noteCompletions(partitionID, journaled)
	return errs
}

// progressWritten, when a test sets it, runs between a durable progress write
// and the update of the in-memory state that follows it: where a concurrent
// reader finds the store ahead of memory.
var progressWritten func()

// floorKeyPrefix prefixes the stored completion floor of a group on a partition.
const floorKeyPrefix = "floor:"

// setFloorInBatch stores a group's completion floor in the caller's batch.
func setFloorInBatch(batch *pebble.Batch, group string, partition int32, floor int64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, uint64(floor))
	return batch.Set([]byte(floorKey(group, partition)), value, nil)
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

// completedFromLocked lists the offsets at or above from that are recorded
// complete, in ascending order. The caller holds g.mu.
func (g *GroupManager) completedFromLocked(group string, partition int32, from int64) []int64 {
	return g.sets[completionPrefix(group, partition)].ascend(from, maxExportedCompletions)
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
	plans := make(map[string]*completionPlan, len(groups))
	for _, progress := range groups {
		groupID := progress.GetGroupId()
		if existing := g.groups[groupID]; existing != nil && existing.Topic != progress.GetTopic() {
			g.mu.RUnlock()
			return fmt.Errorf("consumer group %s belongs to topic %q, not %q", groupID, existing.Topic, progress.GetTopic())
		}
		plans[groupID] = g.planLocked(groupID, partitionID, progress.GetCompletedOffsets(), min(progress.GetCommittedOffset(), logEnd), logEnd, false)
	}
	store := g.offsetStore
	g.mu.RUnlock()

	if store != nil {
		err := store.withDB(func(db *pebble.DB) error {
			batch := db.NewBatch()
			defer batch.Close()
			for _, p := range plans {
				if err := writePlan(batch, partitionID, p); err != nil {
					return err
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
		if progressWritten != nil {
			progressWritten()
		}
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now().UnixMilli()
	for _, progress := range groups {
		groupID := progress.GetGroupId()
		p := plans[groupID]
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
		g.installPlanLocked(partitionID, p)
		highest := p.floor - 1
		for _, offset := range progress.GetCompletedOffsets() {
			if offset >= p.floor && offset < logEnd {
				highest = max(highest, offset)
			}
		}
		if p.floor > group.CommittedOffsets[partitionID] {
			if store != nil {
				_ = store.CommitOffset(groupID, partitionID, p.floor)
			}
			group.CommittedOffsets[partitionID] = p.floor
			changed = true
		}
		g.raiseHighestCompleted(p.prefix, highest)
		if changed {
			group.UpdatedTS = now
			g.persistGroup(group)
		}
	}
	return nil
}

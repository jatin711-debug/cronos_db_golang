package consumer

import (
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
)

// A partition's log holds entries a group does not take: events of the other
// topics that hash to the partition, and the log's own records (a term start,
// written at every change of leader, has no topic). A group never completes
// them, and its floor, which moves over completed offsets only, used to stop at
// the first of them until the entry left the log with its segment. Everything
// the group completed above it was kept as a record per event, and the
// records, the progress sent to followers and the range the redelivery sweep
// reads grew for as long as the entry stayed.
//
// The redelivery sweep reads the log from the floor anyway. When it finds that
// every offset from the floor up to some point holds an entry the group does
// not take or one the group has completed, it passes them (PassOtherEntries).

// CompletionFloor returns a group's completion floor on a partition: every
// offset below it is complete for the group.
func (g *GroupManager) CompletionFloor(groupID string, partitionID int32) int64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.floorLocked(groupID, partitionID)
}

// LogCuts returns how many times the end of a partition's log has been cut
// away (see ForgetCompletionsFrom and NoteLogCut). A caller that reads the log
// notes it first, so that PassOtherEntries can tell whether the entries it
// read may have been replaced since.
func (g *GroupManager) LogCuts(partitionID int32) uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.cuts[partitionID]
}

// NoteLogCut records that the end of a partition's log was cut away.
// ForgetCompletionsFrom does so before the entries go; the caller that cuts
// them says so again once they are gone, so that no read made in between
// counts as current.
func (g *GroupManager) NoteLogCut(partitionID int32) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.noteLogCutLocked(partitionID)
}

func (g *GroupManager) noteLogCutLocked(partitionID int32) {
	if g.cuts == nil {
		g.cuts = make(map[int32]uint64)
	}
	g.cuts[partitionID]++
}

// PassOtherEntries raises a group's completion floor on a partition to to. The
// caller has read the log from the floor, having noted LogCuts as cuts first,
// and found that every offset below to holds either an entry the group does
// not take or one of the group's own, whose offsets are in own.
//
// Nothing changes when the floor has gone below from or reached to since, when
// the log has been cut since the caller noted cuts, or when one of the group's
// own entries is not complete: an entry the group takes is passed only once it
// is finished. It reports whether the floor rose.
func (g *GroupManager) PassOtherEntries(groupID string, partitionID int32, from, to int64, own []int64, cuts uint64) (bool, error) {
	g.commitMu.Lock()
	defer g.commitMu.Unlock()

	g.mu.RLock()
	floor := g.floorLocked(groupID, partitionID)
	pass := g.cuts[partitionID] == cuts && floor >= from && floor < to
	if group, ok := g.groups[groupID]; !ok {
		pass = false
	} else if _, tracked := group.CommittedOffsets[partitionID]; !tracked {
		pass = false
	}
	for _, offset := range own {
		if !pass {
			break
		}
		if offset >= floor && offset < to && !g.completedLocked(groupID, partitionID, offset) {
			pass = false
		}
	}
	store := g.offsetStore
	g.mu.RUnlock()
	if !pass {
		return false, nil
	}

	if store != nil {
		// Not synced: a floor that is lost is raised again by the next sweep,
		// and the records it covers are removed only after it is raised here.
		err := store.withDB(func(db *pebble.DB) error {
			batch := db.NewBatch()
			defer batch.Close()
			if err := setFloorInBatch(batch, groupID, partitionID, to); err != nil {
				return err
			}
			return batch.Commit(pebble.NoSync)
		})
		if err != nil {
			return false, fmt.Errorf("raise completion floor over other entries to %d: %w", to, err)
		}
		if progressWritten != nil {
			progressWritten()
		}
	}

	g.mu.Lock()
	group := g.groups[groupID]
	if group == nil {
		g.mu.Unlock()
		return false, nil // removed meanwhile
	}
	if store == nil {
		for offset := floor; offset < to; offset++ {
			delete(g.completed, completionKey(groupID, partitionID, offset))
		}
	}
	g.setFloorLocked(groupID, partitionID, to)
	if to > group.CommittedOffsets[partitionID] {
		group.CommittedOffsets[partitionID] = to
		group.UpdatedTS = time.Now().UnixMilli()
		if store != nil {
			_ = store.CommitOffset(groupID, partitionID, to)
		}
	}
	g.raiseHighestCompleted(completionPrefix(groupID, partitionID), to-1)
	g.progressVersion.Add(1)
	g.mu.Unlock()
	g.dropCompletionsBelow(store, partitionID, map[string]int64{groupID: to})
	return true, nil
}

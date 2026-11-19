package consumer

import (
	"sort"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

const progressPageSize = 4096

func (g *GroupManager) ProgressVersion() uint64 { return g.progressVersion.Load() }

// ExportProgressPage returns one bounded page. Floors and completions only
// advance; additions behind the cursor are sent on the next versioned round.
// The wire format stays compatible with older followers' additive apply.
func (g *GroupManager) ExportProgressPage(partitionID int32, cursor types.ProgressCursor) ([]*types.ConsumerGroupProgress, types.ProgressCursor, bool, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	ids := make([]string, 0, len(g.groups))
	for id, group := range g.groups {
		if _, tracked := group.CommittedOffsets[partitionID]; tracked && (id > cursor.Group || id == cursor.Group && cursor.NextOffset >= 0) {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil, types.ProgressCursor{}, true, nil
	}
	sort.Strings(ids)
	id := ids[0]
	floor := g.floorLocked(id, partitionID)
	from := floor
	if id == cursor.Group {
		from = max(from, cursor.NextOffset)
	}
	offsets, err := g.completionPageLocked(id, partitionID, from, progressPageSize)
	if err != nil {
		return nil, cursor, false, err
	}
	next := types.ProgressCursor{Group: id, NextOffset: -1}
	done := len(ids) == 1 && len(offsets) < progressPageSize
	if len(offsets) == progressPageSize {
		next.NextOffset = offsets[len(offsets)-1] + 1
	}
	return []*types.ConsumerGroupProgress{{GroupId: id, Topic: g.groups[id].Topic, CommittedOffset: floor, CompletedOffsets: offsets}}, next, done, nil
}

func (g *GroupManager) completionPageLocked(group string, partition int32, from int64, limit int) ([]int64, error) {
	return g.sets[completionPrefix(group, partition)].ascend(from, limit), nil
}

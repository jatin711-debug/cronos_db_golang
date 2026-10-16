package consumer

import (
	"sort"
	"strings"

	"github.com/cockroachdb/pebble"
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
		if len(offsets) > limit {
			offsets = offsets[:limit]
		}
		return offsets, nil
	}
	err := g.offsetStore.withDB(func(db *pebble.DB) error {
		iter, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte(completionKey(group, partition, from)), UpperBound: []byte(prefix + ":")})
		if err != nil {
			return err
		}
		defer iter.Close()
		for iter.First(); iter.Valid() && len(offsets) < limit; iter.Next() {
			if offset, ok := parseCompletionOffset(string(iter.Key()), prefix); ok {
				offsets = append(offsets, offset)
			}
		}
		return iter.Error()
	})
	return offsets, err
}

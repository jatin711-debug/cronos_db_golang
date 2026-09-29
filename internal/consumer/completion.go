package consumer

import (
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// Completion is recorded per event: schedule order need not match WAL order.
// A later delivery must not advance the replay cursor over an unfinished timer.
func completionKey(group string, partition int32, offset int64) string {
	return fmt.Sprintf("done:%d:%s:%d:%020d", len(group), group, partition, offset)
}

func (g *GroupManager) completedLocked(group string, partition int32, offset int64) bool {
	key := completionKey(group, partition, offset)
	if g.offsetStore == nil {
		return g.completed[key]
	}
	_, closer, err := g.offsetStore.db.Get([]byte(key))
	if err != nil {
		return false
	}
	closer.Close()
	return true
}

func (g *GroupManager) IsCompleted(group string, partition int32, offset int64) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.completedLocked(group, partition, offset)
}

// WithRetentionCheck holds membership stable while a caller verifies and prunes
// WAL segments. At least one group must own the event, and every matching group
// must have a durable per-event completion record. A committed cursor alone is
// insufficient because timers can complete out of WAL order.
func (g *GroupManager) WithRetentionCheck(fn func(func(*types.Event) bool) (int, error)) (int, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return fn(func(event *types.Event) bool {
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

// CommitDelivery accepts server-validated delivery records, never a client cursor.
func (g *GroupManager) CommitDelivery(groupID string, partitionID int32, events []*types.Event) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	group, ok := g.groups[groupID]
	if !ok {
		return fmt.Errorf("group %s not found", groupID)
	}
	for _, event := range events {
		if event == nil || event.Offset < 0 || event.PartitionId != partitionID || event.Topic != group.Topic {
			return fmt.Errorf("delivery does not belong to consumer group")
		}
	}
	if g.offsetStore != nil {
		batch := g.offsetStore.db.NewBatch()
		defer batch.Close()
		for _, event := range events {
			if err := batch.Set([]byte(completionKey(groupID, partitionID, event.Offset)), []byte{1}, nil); err != nil {
				return err
			}
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			return fmt.Errorf("persist delivery completion: %w", err)
		}
	} else {
		if g.completed == nil {
			g.completed = make(map[string]bool)
		}
		for _, event := range events {
			g.completed[completionKey(groupID, partitionID, event.Offset)] = true
		}
	}
	cursor := group.CommittedOffsets[partitionID]
	if cursor < 0 {
		cursor = 0
	}
	for g.completedLocked(groupID, partitionID, cursor) {
		cursor++
	}
	group.CommittedOffsets[partitionID] = cursor
	if g.offsetStore != nil {
		return g.offsetStore.CommitOffset(groupID, partitionID, cursor)
	}
	return nil
}

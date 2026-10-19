// Package logcontrol describes internal WAL records that are never user events.
package logcontrol

import (
	"fmt"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

const TermStartKey = "cronos.internal.term_start"
const TermStartMarker = "term-start.version"
const TermStartFormat uint32 = 1

func IsTermStart(e *types.Event) bool {
	_, found := e.GetMeta()[TermStartKey]
	return found
}

func NewTermStart(partition int32) *types.Event {
	return &types.Event{PartitionId: partition, Meta: map[string]string{TermStartKey: "1"}}
}

// ValidateTermStart also rejects a forged ordinary event carrying the marker.
// Offset, term and created time are assigned by the WAL before validation.
func ValidateTermStart(e *types.Event) error {
	if e == nil || !IsTermStart(e) || len(e.Meta) != 1 || e.Meta[TermStartKey] != "1" || e.PartitionId < 0 || e.Offset < 0 || e.Term <= 0 || e.MessageId != "" || e.Topic != "" || len(e.Payload) != 0 || e.ScheduleTs != 0 || e.Checksum != 0 {
		return fmt.Errorf("invalid term-start record")
	}
	return nil
}

// FilterTermStarts preserves the input slice when it contains no controls.
// Callers advance their physical cursor through the unfiltered input.
func FilterTermStarts(events []*types.Event) ([]*types.Event, error) {
	out := events
	filtered := false
	for n, event := range events {
		if IsTermStart(event) {
			if err := ValidateTermStart(event); err != nil {
				return nil, err
			}
			if !filtered {
				out = make([]*types.Event, n, len(events)-1)
				copy(out, events[:n])
				filtered = true
			}
			continue
		}
		if filtered {
			out = append(out, event)
		}
	}
	return out, nil
}

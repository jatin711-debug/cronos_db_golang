package consumer

import (
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func orderAt(offset int64) *types.Event {
	return &types.Event{Topic: "orders", PartitionId: 0, Offset: offset}
}

// Entries are removed from the start of a log once every group that takes
// them has finished them. From then on they count as complete for every
// group, whatever its own records say, and a group's floor moves up to the
// new start: left below it, the floor would wait for entries that are gone.
func TestSetLogStart_CountsWhatWasRemovedAsComplete(t *testing.T) {
	for _, stored := range []bool{false, true} {
		name := "in memory"
		if stored {
			name = "stored"
		}
		t.Run(name, func(t *testing.T) {
			gm := NewGroupManager()
			if stored {
				store, err := NewOffsetStore(t.TempDir(), 0, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer store.Close()
				gm = NewGroupManagerWithStore(store)
			}
			if err := gm.CreateGroup("workers", "orders", []int32{0}); err != nil {
				t.Fatal(err)
			}
			// Offsets 0 and 1 are finished, 2 is not, 5 is.
			if err := gm.CommitDelivery("workers", 0, []*types.Event{orderAt(0), orderAt(1), orderAt(5)}); err != nil {
				t.Fatal(err)
			}
			if _, groups := gm.ExportProgress(0); len(groups) != 1 || groups[0].GetCommittedOffset() != 2 {
				t.Fatalf("setup: progress %+v, want a floor of 2", groups)
			}

			if err := gm.SetLogStart(0, 4); err != nil {
				t.Fatal(err)
			}
			for offset := int64(0); offset < 4; offset++ {
				if !gm.IsCompleted("workers", 0, offset) {
					t.Fatalf("offset %d lies below the start of the log and does not count as complete", offset)
				}
			}
			if gm.IsCompleted("workers", 0, 4) {
				t.Fatal("offset 4 is in the log, was never acknowledged, and counts as complete")
			}
			_, groups := gm.ExportProgress(0)
			if len(groups) != 1 || groups[0].GetCommittedOffset() != 4 {
				t.Fatalf("progress after the log start moved: %+v, want a floor of 4", groups)
			}
			if done := groups[0].GetCompletedOffsets(); len(done) != 1 || done[0] != 5 {
				t.Fatalf("completions above the floor: %v, want [5]", done)
			}
			if committed, err := gm.GetCommittedOffset("workers", 0); err != nil || committed != 4 {
				t.Fatalf("committed offset %d (%v), want 4", committed, err)
			}

			// The floor carries on from the new start.
			if err := gm.CommitDelivery("workers", 0, []*types.Event{orderAt(4)}); err != nil {
				t.Fatal(err)
			}
			if _, groups := gm.ExportProgress(0); groups[0].GetCommittedOffset() != 6 {
				t.Fatalf("floor after finishing offset 4: %d, want 6", groups[0].GetCommittedOffset())
			}

			// The start does not move back.
			if err := gm.SetLogStart(0, 1); err != nil {
				t.Fatal(err)
			}
			if !gm.IsCompleted("workers", 0, 3) {
				t.Fatal("a lower log start undid a higher one")
			}

			// A group created afterwards has nothing to do below the start.
			if err := gm.CreateGroup("auditors", "orders", []int32{0}); err != nil {
				t.Fatal(err)
			}
			if !gm.IsCompleted("auditors", 0, 3) || gm.IsCompleted("auditors", 0, 4) {
				t.Fatal("a group created after the log was cut does not start at the start of the log")
			}
		})
	}
}

package consumer

import (
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func wantCompleted(t *testing.T, manager *GroupManager, when string, want map[int64]bool) {
	t.Helper()
	for offset, done := range want {
		if got := manager.IsCompleted("g", 0, offset); got != done {
			t.Errorf("%s: IsCompleted(%d) = %v, want %v", when, offset, got, done)
		}
	}
}

// A replica that loses the end of its log gives those offsets to other
// events. Completion is recorded by offset, and what was recorded for the
// entries that went used to stay: the events that took their places counted
// as finished on this replica, and were never delivered once it led.
func TestCompletionIsForgottenWithTheEntriesItWasFor(t *testing.T) {
	for _, stored := range []bool{true, false} {
		name := "in memory"
		if stored {
			name = "stored"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			open := func() *GroupManager {
				if stored {
					return newStoreBackedManager(t, dir)
				}
				return NewGroupManager()
			}
			manager := open()
			if err := manager.CreateGroup("g", "orders", []int32{0}); err != nil {
				t.Fatal(err)
			}
			if err := manager.CommitDelivery("g", 0, completionEvents("orders", 0, 1, 2, 3, 4, 5, 8)); err != nil {
				t.Fatal(err)
			}
			if committed, _ := manager.GetCommittedOffset("g", 0); committed != 6 {
				t.Fatalf("committed offset = %d, want 6", committed)
			}
			before, _ := manager.ExportProgress(0)

			// The log is cut at offset 4.
			if groups, err := manager.ForgetCompletionsFrom(0, 4); err != nil || groups != 1 {
				t.Fatalf("ForgetCompletionsFrom = %d groups, %v; want 1, nil", groups, err)
			}
			after := map[int64]bool{0: true, 3: true, 4: false, 5: false, 6: false, 8: false}
			wantCompleted(t, manager, "after the cut", after)
			if committed, _ := manager.GetCommittedOffset("g", 0); committed != 4 {
				t.Fatalf("committed offset after the cut = %d, want 4", committed)
			}
			if version, progress := manager.ExportProgress(0); version == before ||
				len(progress) != 1 || progress[0].GetCommittedOffset() != 4 || len(progress[0].GetCompletedOffsets()) != 0 {
				t.Fatalf("exported progress after the cut = %v (version changed: %v), want floor 4 and nothing above it", progress, version != before)
			}
			// Nothing is left to forget, which must cost nothing: this runs
			// whenever a partition starts.
			if groups, err := manager.ForgetCompletionsFrom(0, 4); err != nil || groups != 0 {
				t.Fatalf("second ForgetCompletionsFrom = %d groups, %v; want 0, nil", groups, err)
			}

			if stored {
				if err := manager.Close(); err != nil {
					t.Fatal(err)
				}
				manager = open()
				wantCompleted(t, manager, "after a restart", after)
				if committed, _ := manager.GetCommittedOffset("g", 0); committed != 4 {
					t.Fatalf("committed offset after a restart = %d, want 4", committed)
				}
			}
			defer manager.Close()

			// The events that take the offsets are delivered and recorded
			// like any other.
			if err := manager.CommitDelivery("g", 0, completionEvents("orders", 4)); err != nil {
				t.Fatal(err)
			}
			wantCompleted(t, manager, "after offset 4 is finished again", map[int64]bool{4: true, 5: false})
			if committed, _ := manager.GetCommittedOffset("g", 0); committed != 5 {
				t.Fatalf("committed offset = %d, want 5", committed)
			}
		})
	}
}

// A follower can be behind its leader's log and still be sent the whole of
// its consumer progress. What it took of that beyond the end of its own log
// was about entries it did not hold. If it led next, with those entries lost
// to it, the events published at their offsets counted as finished.
func TestReplicatedProgressIsTakenOnlyAsFarAsTheLogReaches(t *testing.T) {
	leader := newStoreBackedManager(t, t.TempDir())
	defer leader.Close()
	if err := leader.CreateGroup("g", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	if err := leader.CommitDelivery("g", 0, completionEvents("orders", 0, 1, 2, 3, 4, 5, 8)); err != nil {
		t.Fatal(err)
	}
	_, progress := leader.ExportProgress(0)

	for _, stored := range []bool{true, false} {
		follower := NewGroupManager()
		if stored {
			follower = newStoreBackedManager(t, t.TempDir())
		}
		defer follower.Close()

		// The follower holds offsets 0 to 3.
		if err := follower.ApplyReplicatedProgress(0, 4, progress); err != nil {
			t.Fatal(err)
		}
		wantCompleted(t, follower, "with a log that ends at 3", map[int64]bool{0: true, 3: true, 4: false, 5: false, 8: false})
		if committed, _ := follower.GetCommittedOffset("g", 0); committed != 4 {
			t.Fatalf("committed offset with a log that ends at 3 = %d, want 4", committed)
		}

		// Caught up, it takes the rest.
		if err := follower.ApplyReplicatedProgress(0, 9, progress); err != nil {
			t.Fatal(err)
		}
		wantCompleted(t, follower, "with the whole log", map[int64]bool{4: true, 5: true, 6: false, 7: false, 8: true})
		if committed, _ := follower.GetCommittedOffset("g", 0); committed != 6 {
			t.Fatalf("committed offset with the whole log = %d, want 6", committed)
		}

		// A later round for a shorter log takes nothing back.
		if err := follower.ApplyReplicatedProgress(0, 2, []*types.ConsumerGroupProgress{{GroupId: "g", Topic: "orders", CommittedOffset: 6}}); err != nil {
			t.Fatal(err)
		}
		wantCompleted(t, follower, "after a round for a shorter log", map[int64]bool{5: true, 8: true})
	}
}

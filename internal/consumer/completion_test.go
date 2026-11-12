package consumer

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func completionEvents(topic string, offsets ...int64) []*types.Event {
	events := make([]*types.Event, len(offsets))
	for i, offset := range offsets {
		events[i] = &types.Event{Topic: topic, PartitionId: 0, Offset: offset}
	}
	return events
}

func newStoreBackedManager(t *testing.T, dir string) *GroupManager {
	t.Helper()
	store, err := NewOffsetStore(dir, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	return NewGroupManagerWithStore(store)
}

// Several acks recorded together must each get their own result, and an
// invalid one must not stop the others from being recorded.
func TestCommitDeliveriesRecordsEachDelivery(t *testing.T) {
	for name, manager := range map[string]*GroupManager{
		"in-memory": NewGroupManager(),
		"store":     newStoreBackedManager(t, t.TempDir()),
	} {
		t.Run(name, func(t *testing.T) {
			defer manager.Close()
			if err := manager.CreateGroup("g", "orders", []int32{0}); err != nil {
				t.Fatal(err)
			}

			errs := manager.CommitDeliveries(0, []DeliveryCommit{
				{GroupID: "g", Events: completionEvents("orders", 0, 1)},
				{GroupID: "missing", Events: completionEvents("orders", 2)},
				{GroupID: "g", Events: completionEvents("another-topic", 3)},
				{GroupID: "g", Events: completionEvents("orders", 4)},
			})
			if errs[0] != nil || errs[3] != nil {
				t.Fatalf("valid deliveries were rejected: %v, %v", errs[0], errs[3])
			}
			if errs[1] == nil || errs[2] == nil {
				t.Fatal("a delivery for an unknown group or another topic was accepted")
			}

			for offset, want := range map[int64]bool{0: true, 1: true, 2: false, 3: false, 4: true, 5: false} {
				if got := manager.IsCompleted("g", 0, offset); got != want {
					t.Errorf("IsCompleted(%d) = %v, want %v", offset, got, want)
				}
			}
			// The committed cursor stops at the first gap, not at the highest ack.
			if committed, err := manager.GetCommittedOffset("g", 0); err != nil || committed != 2 {
				t.Fatalf("committed offset = %d, %v; want 2", committed, err)
			}
		})
	}
}

// The in-memory mark of the highest completed offset is rebuilt from the
// records after a restart; without it every completed event would look new.
func TestCompletionSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	manager := newStoreBackedManager(t, dir)
	if err := manager.CreateGroup("g", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	if err := manager.CommitDelivery("g", 0, completionEvents("orders", 0, 1, 7)); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	manager = newStoreBackedManager(t, dir)
	defer manager.Close()
	for offset, want := range map[int64]bool{0: true, 1: true, 2: false, 7: true, 8: false} {
		if got := manager.IsCompleted("g", 0, offset); got != want {
			t.Errorf("after restart IsCompleted(%d) = %v, want %v", offset, got, want)
		}
	}
	if manager.IsCompleted("other-group", 0, 0) {
		t.Error("another group's events must not read as completed")
	}
}

// A group whose first event comes after another topic's entries cannot move
// its floor over them, so its progress is all in per-event records. A
// partition that starts raises the floor to the start of its log before
// anything has read those records; the raise used to become the cached highest
// completion, and every recorded event read as new and was delivered again.
// So did the events of a restart's first ack, which set the cache to its own
// offset.
func TestCompletionsAboveTheFloorSurviveRestart(t *testing.T) {
	acked := []int64{312, 319, 313, 320, 327, 314, 321, 328, 335, 315, 322, 329}
	for name, firstAfterRestart := range map[string]func(*testing.T, *GroupManager){
		"partition starts": func(t *testing.T, m *GroupManager) {
			if err := m.SetLogStart(0, 184); err != nil {
				t.Fatal(err)
			}
		},
		"ack comes first": func(t *testing.T, m *GroupManager) {
			if err := m.CommitDelivery("late", 0, completionEvents("later", 316)); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			m := newStoreBackedManager(t, dir)
			if err := m.SetLogStart(0, 184); err != nil {
				t.Fatal(err)
			}
			if err := m.CreateGroup("earlier", "before", []int32{0}); err != nil {
				t.Fatal(err)
			}
			if err := m.CreateGroup("late", "later", []int32{0}); err != nil {
				t.Fatal(err)
			}
			for _, offset := range acked {
				if err := m.CommitDelivery("late", 0, completionEvents("later", offset)); err != nil {
					t.Fatal(err)
				}
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}

			m = newStoreBackedManager(t, dir)
			defer m.Close()
			firstAfterRestart(t, m)
			for _, offset := range acked {
				if !m.IsCompleted("late", 0, offset) {
					t.Errorf("after restart IsCompleted(%d) = false; it was acknowledged", offset)
				}
			}
			for _, offset := range []int64{184, 311, 317, 336} {
				if m.IsCompleted("late", 0, offset) {
					t.Errorf("after restart IsCompleted(%d) = true; it was never acknowledged", offset)
				}
			}
		})
	}
}

// A floor that rises over completion records must not make them look
// unfinished to a reader that comes in while it rises. The records used to be
// removed in the same durable write that stored the new floor, before the
// floor was raised in memory, and a reader in between found neither: the
// redelivery scan then sent completed events again, all at once.
func TestCompletionsStayCompleteWhileTheFloorRises(t *testing.T) {
	for name, tc := range map[string]struct {
		setup, raise func(*testing.T, *GroupManager)
		complete     []int64
		left         int // completion records above the risen floor
	}{
		"an ack closes the gap": {
			setup: func(t *testing.T, m *GroupManager) {
				if err := m.CommitDelivery("g", 0, completionEvents("orders", 1, 2, 3, 4, 5)); err != nil {
					t.Fatal(err)
				}
			},
			raise: func(t *testing.T, m *GroupManager) {
				if err := m.CommitDelivery("g", 0, completionEvents("orders", 0)); err != nil {
					t.Fatal(err)
				}
			},
			complete: []int64{1, 2, 3, 4, 5},
			left:     0,
		},
		"the log start moves up": {
			setup: func(t *testing.T, m *GroupManager) {
				if err := m.CommitDelivery("g", 0, completionEvents("orders", 3, 4, 5, 6)); err != nil {
					t.Fatal(err)
				}
			},
			raise: func(t *testing.T, m *GroupManager) {
				if err := m.SetLogStart(0, 5); err != nil {
					t.Fatal(err)
				}
			},
			complete: []int64{3, 4, 5, 6},
			left:     2,
		},
		"the leader's floor is replicated": {
			setup: func(t *testing.T, m *GroupManager) {
				progress := []*types.ConsumerGroupProgress{{GroupId: "g", Topic: "orders", CompletedOffsets: []int64{1, 2, 3, 4, 5}}}
				if err := m.ApplyReplicatedProgress(0, 10, progress); err != nil {
					t.Fatal(err)
				}
			},
			raise: func(t *testing.T, m *GroupManager) {
				progress := []*types.ConsumerGroupProgress{{GroupId: "g", Topic: "orders", CommittedOffset: 6}}
				if err := m.ApplyReplicatedProgress(0, 10, progress); err != nil {
					t.Fatal(err)
				}
			},
			complete: []int64{1, 2, 3, 4, 5},
			left:     0,
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := newStoreBackedManager(t, t.TempDir())
			defer m.Close()
			if err := m.CreateGroup("g", "orders", []int32{0}); err != nil {
				t.Fatal(err)
			}
			tc.setup(t, m)
			for _, offset := range tc.complete {
				if !m.IsCompleted("g", 0, offset) {
					t.Fatalf("IsCompleted(%d) = false before the floor rises", offset)
				}
			}

			var seen []int64
			progressWritten = func() {
				for _, offset := range tc.complete {
					if !m.IsCompleted("g", 0, offset) {
						seen = append(seen, offset)
					}
				}
			}
			defer func() { progressWritten = nil }()
			tc.raise(t, m)
			progressWritten = nil

			if len(seen) > 0 {
				t.Errorf("completed offsets %v read as unfinished while the floor rose", seen)
			}
			for _, offset := range tc.complete {
				if !m.IsCompleted("g", 0, offset) {
					t.Errorf("IsCompleted(%d) = false after the floor rose", offset)
				}
			}
			// The records the floor has passed are still removed.
			if got := completionRecordCount(t, m, "g"); got != tc.left {
				t.Errorf("%d completion records left, want %d", got, tc.left)
			}
		})
	}
}

// An ack or redelivery scan still in flight at shutdown must get an error or a
// negative answer, not a panic from the closed store.
func TestCompletionAfterCloseDoesNotPanic(t *testing.T) {
	manager := newStoreBackedManager(t, t.TempDir())
	if err := manager.CreateGroup("g", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	// Offset 2 stays above the floor, so answering for it needs the store.
	if err := manager.CommitDelivery("g", 0, completionEvents("orders", 0, 2)); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	if err := manager.CommitDelivery("g", 0, completionEvents("orders", 1)); err == nil {
		t.Fatal("recording a completion on a closed store must fail")
	}
	if manager.IsCompleted("g", 0, 2) {
		t.Fatal("a closed store cannot vouch for a completion")
	}
}

func completionRecordCount(t *testing.T, manager *GroupManager, group string) int {
	t.Helper()
	count := 0
	err := manager.offsetStore.withDB(func(db *pebble.DB) error {
		prefix := completionPrefix(group, 0)
		iter, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte(prefix), UpperBound: []byte(prefix + ":")})
		if err != nil {
			return err
		}
		defer iter.Close()
		for iter.First(); iter.Valid(); iter.Next() {
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return count
}

// The floor advances over the completed prefix, the records it has passed are
// removed, and both survive a restart. Without that the completion state would
// grow by one record per event forever.
func TestCompletionFloorAdvancesAndPrunesRecords(t *testing.T) {
	dir := t.TempDir()
	manager := newStoreBackedManager(t, dir)
	if err := manager.CreateGroup("g", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	// Out of order: 1 and 2 first leave the floor at 0.
	if err := manager.CommitDelivery("g", 0, completionEvents("orders", 1, 2)); err != nil {
		t.Fatal(err)
	}
	if got := completionRecordCount(t, manager, "g"); got != 2 {
		t.Fatalf("%d completion records above an unmoved floor, want 2", got)
	}
	if committed, _ := manager.GetCommittedOffset("g", 0); committed > 0 {
		t.Fatalf("committed offset %d moved past the unfinished offset 0", committed)
	}
	// 0 closes the gap; 5 stays above it.
	if err := manager.CommitDelivery("g", 0, completionEvents("orders", 0, 5)); err != nil {
		t.Fatal(err)
	}
	if committed, _ := manager.GetCommittedOffset("g", 0); committed != 3 {
		t.Fatalf("committed offset = %d, want 3", committed)
	}
	if got := completionRecordCount(t, manager, "g"); got != 1 {
		t.Fatalf("%d completion records left, want only the one above the floor", got)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	manager = newStoreBackedManager(t, dir)
	defer manager.Close()
	for offset, want := range map[int64]bool{0: true, 1: true, 2: true, 3: false, 4: false, 5: true, 6: false} {
		if got := manager.IsCompleted("g", 0, offset); got != want {
			t.Errorf("after restart IsCompleted(%d) = %v, want %v", offset, got, want)
		}
	}
	if committed, _ := manager.GetCommittedOffset("g", 0); committed != 3 {
		t.Fatalf("committed offset after restart = %d, want 3", committed)
	}
}

// An offset committed directly says nothing about the events below it and
// must not make them count as complete.
func TestCommittedOffsetOverrideIsNotCompletion(t *testing.T) {
	manager := newStoreBackedManager(t, t.TempDir())
	defer manager.Close()
	if err := manager.CreateGroup("g", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	if err := manager.CommitOffset("g", 0, 100); err != nil {
		t.Fatal(err)
	}
	if manager.IsCompleted("g", 0, 7) {
		t.Fatal("an overridden committed offset was treated as proof of completion")
	}
}

// A follower that applies the leader's exported progress must give the same
// answers as the leader, keep them across a restart, and never move backwards.
func TestReplicatedProgressMatchesLeader(t *testing.T) {
	leader := newStoreBackedManager(t, t.TempDir())
	defer leader.Close()
	if err := leader.CreateGroup("g", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	before, _ := leader.ExportProgress(0)
	if err := leader.CommitDelivery("g", 0, completionEvents("orders", 0, 1, 2, 5, 9)); err != nil {
		t.Fatal(err)
	}
	version, progress := leader.ExportProgress(0)
	if version == before {
		t.Fatal("progress version did not change after an ack")
	}
	if len(progress) != 1 || progress[0].GetCommittedOffset() != 3 || len(progress[0].GetCompletedOffsets()) != 2 {
		t.Fatalf("exported progress = %v, want floor 3 with completions 5 and 9", progress)
	}

	followerDir := t.TempDir()
	follower := newStoreBackedManager(t, followerDir)
	if err := follower.ApplyReplicatedProgress(0, 12, progress); err != nil {
		t.Fatal(err)
	}
	check := func(manager *GroupManager, when string) {
		t.Helper()
		for offset := int64(0); offset < 12; offset++ {
			if got, want := manager.IsCompleted("g", 0, offset), leader.IsCompleted("g", 0, offset); got != want {
				t.Errorf("%s: follower IsCompleted(%d) = %v, leader says %v", when, offset, got, want)
			}
		}
		if committed, _ := manager.GetCommittedOffset("g", 0); committed != 3 {
			t.Errorf("%s: follower committed offset = %d, want 3", when, committed)
		}
	}
	check(follower, "after apply")

	// A stale round must not undo newer progress.
	stale := []*types.ConsumerGroupProgress{{GroupId: "g", Topic: "orders", CommittedOffset: 1}}
	if err := follower.ApplyReplicatedProgress(0, 12, stale); err != nil {
		t.Fatal(err)
	}
	check(follower, "after stale round")

	if err := follower.Close(); err != nil {
		t.Fatal(err)
	}
	follower = newStoreBackedManager(t, followerDir)
	defer follower.Close()
	check(follower, "after restart")

	// The promoted follower continues from the replicated state.
	if err := follower.CommitDelivery("g", 0, completionEvents("orders", 3, 4)); err != nil {
		t.Fatal(err)
	}
	if committed, _ := follower.GetCommittedOffset("g", 0); committed != 6 {
		t.Fatalf("after continuing on the follower committed offset = %d, want 6", committed)
	}
}

// Group records are written by a background flush. Membership changes during
// a flush must not race with it.
func TestGroupPersistenceDuringMembershipChanges(t *testing.T) {
	manager := newStoreBackedManager(t, t.TempDir())
	defer manager.Close()
	deadline := time.Now().Add(600 * time.Millisecond) // spans several flushes
	for i := 0; time.Now().Before(deadline); i++ {
		member := fmt.Sprintf("member-%d", i%8)
		if err := manager.JoinGroup("g", member, "addr", "orders", 0); err != nil {
			t.Fatal(err)
		}
		if i%3 == 0 {
			_ = manager.LeaveGroup("g", member)
		}
	}
}

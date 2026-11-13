package consumer

import (
	"testing"
)

// A group's floor stops at an entry it does not take, another topic's or a
// term start. Before PassOtherEntries it stayed there, and every completion
// above was kept as a record of its own until the entry left the log.
func TestPassOtherEntries_RaisesTheFloorOverEntriesOfOthers(t *testing.T) {
	for name, newManager := range map[string]func(*testing.T) *GroupManager{
		"in-memory": func(*testing.T) *GroupManager { return NewGroupManager() },
		"store":     func(t *testing.T) *GroupManager { return newStoreBackedManager(t, t.TempDir()) },
	} {
		t.Run(name, func(t *testing.T) {
			manager := newManager(t)
			defer manager.Close()
			if err := manager.CreateGroup("g", "orders", []int32{0}); err != nil {
				t.Fatal(err)
			}
			// orders at 0, 2, 3, 5; another topic at 1 and 4.
			if err := manager.CommitDelivery("g", 0, completionEvents("orders", 0, 2, 3, 5)); err != nil {
				t.Fatal(err)
			}
			if floor := manager.CompletionFloor("g", 0); floor != 1 {
				t.Fatalf("floor %d, want 1: it stops at the other topic's entry", floor)
			}

			raised, err := manager.PassOtherEntries("g", 0, 1, 6, []int64{2, 3, 5}, manager.LogCuts(0))
			if err != nil || !raised {
				t.Fatalf("raised=%v err=%v, want the floor raised", raised, err)
			}
			if floor := manager.CompletionFloor("g", 0); floor != 6 {
				t.Fatalf("floor %d, want 6", floor)
			}
			if committed, _ := manager.GetCommittedOffset("g", 0); committed != 6 {
				t.Fatalf("committed offset %d, want 6", committed)
			}
			for offset := int64(0); offset < 6; offset++ {
				if !manager.IsCompleted("g", 0, offset) {
					t.Errorf("offset %d is not complete below the floor", offset)
				}
			}
			if manager.IsCompleted("g", 0, 6) {
				t.Error("offset 6, above the floor, counts as complete")
			}
			if manager.offsetStore != nil {
				if got := completionRecordCount(t, manager, "g"); got != 0 {
					t.Fatalf("%d completion records left below the floor, want 0", got)
				}
			}
			_, groups := manager.ExportProgress(0)
			if len(groups) != 1 || groups[0].GetCommittedOffset() != 6 || len(groups[0].GetCompletedOffsets()) != 0 {
				t.Fatalf("followers are sent %+v, want floor 6 and no records", groups)
			}
		})
	}
}

// An event of the group's own topic is passed only once it is complete.
func TestPassOtherEntries_DoesNotPassAnUnfinishedEvent(t *testing.T) {
	manager := newStoreBackedManager(t, t.TempDir())
	defer manager.Close()
	if err := manager.CreateGroup("g", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	if err := manager.CommitDelivery("g", 0, completionEvents("orders", 0, 3)); err != nil {
		t.Fatal(err)
	}
	// 2 is an order that is not complete.
	if raised, err := manager.PassOtherEntries("g", 0, 1, 4, []int64{2, 3}, manager.LogCuts(0)); err != nil || raised {
		t.Fatalf("raised=%v err=%v, want nothing raised over an unfinished event", raised, err)
	}
	if floor := manager.CompletionFloor("g", 0); floor != 1 {
		t.Fatalf("floor %d, want 1", floor)
	}
}

// The floor is raised only from where the caller read the log, and not over a
// log that has been cut since: the entries read may have been replaced.
func TestPassOtherEntries_RefusesAStaleRead(t *testing.T) {
	manager := newStoreBackedManager(t, t.TempDir())
	defer manager.Close()
	if err := manager.CreateGroup("g", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	if err := manager.CommitDelivery("g", 0, completionEvents("orders", 0, 1)); err != nil {
		t.Fatal(err)
	}
	cuts := manager.LogCuts(0)
	if raised, _ := manager.PassOtherEntries("g", 0, 3, 5, nil, cuts); raised {
		t.Fatal("raised the floor from offset 3, above where it is (2)")
	}

	if _, err := manager.ForgetCompletionsFrom(0, 10); err != nil {
		t.Fatal(err)
	}
	if raised, _ := manager.PassOtherEntries("g", 0, 2, 5, nil, cuts); raised {
		t.Fatal("raised the floor on a read made before the log was cut")
	}
	if raised, _ := manager.PassOtherEntries("g", 0, 2, 5, nil, manager.LogCuts(0)); !raised {
		t.Fatal("did not raise the floor on a read made after the cut")
	}

	manager.NoteLogCut(0)
	if raised, _ := manager.PassOtherEntries("g", 0, 5, 7, nil, cuts); raised {
		t.Fatal("raised the floor on a read made before a cut was noted")
	}
}

// The raised floor is kept across a restart.
func TestPassOtherEntries_FloorSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	manager := newStoreBackedManager(t, dir)
	if err := manager.CreateGroup("g", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	if err := manager.CommitDelivery("g", 0, completionEvents("orders", 0, 2)); err != nil {
		t.Fatal(err)
	}
	if raised, err := manager.PassOtherEntries("g", 0, 1, 3, []int64{2}, manager.LogCuts(0)); err != nil || !raised {
		t.Fatalf("raised=%v err=%v", raised, err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	manager = newStoreBackedManager(t, dir)
	defer manager.Close()
	if floor := manager.CompletionFloor("g", 0); floor != 3 {
		t.Fatalf("floor after restart %d, want 3", floor)
	}
	if manager.IsCompleted("g", 0, 3) {
		t.Fatal("offset 3 counts as complete after restart")
	}
}

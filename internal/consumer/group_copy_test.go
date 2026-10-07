package consumer

import (
	"sync"
	"testing"
)

// A group handed out by the manager is the caller's own. Snapshots and the
// admin API read its maps while acknowledgements move its offsets and
// consumers join and leave; reading the manager's object there is a
// concurrent map read and write, which ends the process.
func TestGroupManager_HandsOutCopies(t *testing.T) {
	gm := NewGroupManager()
	if err := gm.CreateGroup("workers", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	if err := gm.JoinGroup("workers", "member-0", "", "orders", 0); err != nil {
		t.Fatal(err)
	}

	group, ok := gm.GetGroup("workers")
	if !ok {
		t.Fatal("group not found")
	}
	group.CommittedOffsets[0] = 999
	delete(group.Members, "member-0")
	if offset, err := gm.GetCommittedOffset("workers", 0); err != nil || offset == 999 {
		t.Fatalf("changing a returned group changed the manager's: offset %d, err %v", offset, err)
	}
	if again, _ := gm.GetGroup("workers"); len(again.Members) != 1 {
		t.Fatalf("changing a returned group changed the manager's members: %v", again.Members)
	}

	// Under the race detector this is what fails when a live group escapes.
	var readers sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 2; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, listed := range gm.ListGroups() {
					_ = listed.CommittedOffsets[0]
					_ = len(listed.Members)
				}
				if got, ok := gm.GetGroup("workers"); ok {
					_ = got.CommittedOffsets[0]
				}
			}
		}()
	}
	for offset := int64(0); offset < 2000; offset++ {
		if err := gm.CommitOffset("workers", 0, offset); err != nil {
			t.Fatal(err)
		}
		if offset%100 == 0 {
			_ = gm.JoinGroup("workers", "member-1", "", "orders", 0)
			_ = gm.LeaveGroup("workers", "member-1")
		}
	}
	close(stop)
	readers.Wait()
}

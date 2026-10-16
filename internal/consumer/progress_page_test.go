package consumer

import (
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestProgressPagesTransferAllSparseCompletions(t *testing.T) {
	leader := newStoreBackedManager(t, t.TempDir())
	defer leader.Close()
	followerDir := t.TempDir()
	follower := newStoreBackedManager(t, followerDir)
	if err := leader.CreateGroup("g", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	const count = 100_005
	for base := 0; base < count; base += 1000 {
		events := make([]*types.Event, 0, 1000)
		for i := base; i < min(base+1000, count); i++ {
			events = append(events, &types.Event{Topic: "orders", PartitionId: 0, Offset: int64(2*i + 1)})
		}
		if err := leader.CommitDelivery("g", 0, events); err != nil {
			t.Fatal(err)
		}
	}
	cursor := types.ProgressCursor{}
	transferred := 0
	for pages := 0; pages < 100; pages++ {
		groups, next, done, err := leader.ExportProgressPage(0, cursor)
		if err != nil {
			t.Fatal(err)
		}
		for _, group := range groups {
			if len(group.CompletedOffsets) > progressPageSize {
				t.Fatal("unbounded page")
			}
			transferred += len(group.CompletedOffsets)
		}
		if err := follower.ApplyReplicatedProgress(0, 2*count+1, groups); err != nil {
			t.Fatal(err)
		}
		cursor = next
		if done {
			break
		}
	}
	if transferred != count {
		t.Fatalf("transferred %d of %d", transferred, count)
	}
	if err := follower.Close(); err != nil {
		t.Fatal(err)
	}
	follower = newStoreBackedManager(t, followerDir)
	defer follower.Close()
	for _, offset := range []int64{1, 199999, 200009} {
		if !follower.IsCompleted("g", 0, offset) {
			t.Fatalf("completion %d missing after restart", offset)
		}
	}
	if follower.IsCompleted("g", 0, 0) || follower.IsCompleted("g", 0, 200008) {
		t.Fatal("unrelated/incomplete offset was acknowledged")
	}
}

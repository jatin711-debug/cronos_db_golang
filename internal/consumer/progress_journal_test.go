package consumer

import (
	"slices"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func commitOffsets(t *testing.T, g *GroupManager, group string, offsets ...int64) {
	t.Helper()
	events := make([]*types.Event, 0, len(offsets))
	for _, offset := range offsets {
		events = append(events, &types.Event{Topic: "orders", PartitionId: 0, Offset: offset})
	}
	if err := g.CommitDelivery(group, 0, events); err != nil {
		t.Fatal(err)
	}
}

func deltaFor(t *testing.T, groups []*types.ConsumerGroupProgress, group string) *types.ConsumerGroupProgress {
	t.Helper()
	for _, progress := range groups {
		if progress.GetGroupId() == group {
			return progress
		}
	}
	t.Fatalf("group %s is not in the delta", group)
	return nil
}

// A follower that holds the progress up to some version is sent only what was
// recorded after it, with every group's current floor. The full copy is every
// completion above the floor, and a floor held by one pending event leaves
// all later completions on record: sent each time, it grew without bound.
func TestProgressDeltaHasOnlyWhatChanged(t *testing.T) {
	g := newStoreBackedManager(t, t.TempDir())
	defer g.Close()
	for _, group := range []string{"a", "b"} {
		if err := g.CreateGroup(group, "orders", []int32{0}); err != nil {
			t.Fatal(err)
		}
	}
	g.ResetProgressJournal(0)

	// Offset 0 stays pending, so the floor stays at 0 and every completion
	// above it is kept on record.
	commitOffsets(t, g, "a", 1, 2, 3)
	commitOffsets(t, g, "b", 5)
	held := g.ProgressVersion()
	commitOffsets(t, g, "a", 4, 7)

	groups, version, ok := g.ExportProgressDelta(0, held)
	if !ok {
		t.Fatal("no delta from a version the journal holds")
	}
	if version != g.ProgressVersion() {
		t.Fatalf("delta brings the follower to version %d, want %d", version, g.ProgressVersion())
	}
	if got := deltaFor(t, groups, "a").GetCompletedOffsets(); !slices.Equal(got, []int64{4, 7}) {
		t.Fatalf("group a delta = %v, want [4 7]", got)
	}
	if got := deltaFor(t, groups, "b"); len(got.GetCompletedOffsets()) != 0 || got.GetCommittedOffset() != 0 {
		t.Fatalf("group b delta = %v, want its floor and nothing else", got)
	}

	// Once offset 0 is done, the floor passes everything up to 4: the delta
	// says so with the floor and names only what lies above it.
	commitOffsets(t, g, "a", 0)
	groups, _, ok = g.ExportProgressDelta(0, version)
	if !ok {
		t.Fatal("no delta")
	}
	a := deltaFor(t, groups, "a")
	if a.GetCommittedOffset() != 5 || len(a.GetCompletedOffsets()) != 0 {
		t.Fatalf("group a delta = floor %d, completed %v; want floor 5 and nothing else", a.GetCommittedOffset(), a.GetCompletedOffsets())
	}
}

// A version from before the journal began, or one it no longer reaches back
// to, gets no delta: the follower needs the full copy.
func TestProgressDeltaNeedsTheJournalToReachBack(t *testing.T) {
	g := newStoreBackedManager(t, t.TempDir())
	defer g.Close()
	if err := g.CreateGroup("a", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	before := g.ProgressVersion()
	commitOffsets(t, g, "a", 1)
	g.ResetProgressJournal(0)
	if _, _, ok := g.ExportProgressDelta(0, before); ok {
		t.Fatal("a delta from before the journal was reset")
	}
	reset := g.ProgressVersion()
	commitOffsets(t, g, "a", 2)
	if _, _, ok := g.ExportProgressDelta(0, reset); !ok {
		t.Fatal("no delta from the version the journal was reset at")
	}

	// Cutting the log forgets completions; the journal may name them.
	if _, err := g.ForgetCompletionsFrom(0, 2); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := g.ExportProgressDelta(0, reset); ok {
		t.Fatal("a delta that may name completions the log no longer has")
	}

	// Beyond its bound, the journal forgets its oldest entries.
	g.ResetProgressJournal(0)
	start := g.ProgressVersion()
	offset := int64(10)
	for recorded := 0; recorded <= maxJournalOffsets; recorded += 4096 {
		batch := make([]int64, 4096)
		for i := range batch {
			batch[i] = offset
			offset++
		}
		g.noteCompletions(0, map[string][]int64{"a": batch})
	}
	if _, _, ok := g.ExportProgressDelta(0, start); ok {
		t.Fatal("a delta from a version the journal no longer reaches back to")
	}
}

// The deltas a follower applies, after one full copy, leave it with exactly
// the leader's progress.
func TestProgressDeltasKeepAFollowerEqualToTheLeader(t *testing.T) {
	leader := newStoreBackedManager(t, t.TempDir())
	defer leader.Close()
	follower := newStoreBackedManager(t, t.TempDir())
	defer follower.Close()
	if err := leader.CreateGroup("a", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	leader.ResetProgressJournal(0)
	const logEnd = 10_000

	commitOffsets(t, leader, "a", 3, 5, 9)
	held := leader.ProgressVersion()
	for cursor := (types.ProgressCursor{}); ; {
		groups, next, done, err := leader.ExportProgressPage(0, cursor)
		if err != nil {
			t.Fatal(err)
		}
		if err := follower.ApplyReplicatedProgress(0, logEnd, groups); err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
		cursor = next
	}

	for round := 0; round < 20; round++ {
		base := int64(100 + round*50)
		commitOffsets(t, leader, "a", base, base+2, base+7)
		if round == 10 {
			commitOffsets(t, leader, "a", 0, 1, 2, 4) // the floor rises to 6
		}
		groups, version, ok := leader.ExportProgressDelta(0, held)
		if !ok {
			t.Fatalf("round %d: no delta", round)
		}
		if err := follower.ApplyReplicatedProgress(0, logEnd, groups); err != nil {
			t.Fatal(err)
		}
		held = version
	}

	for offset := int64(0); offset < 1200; offset++ {
		if want, got := leader.IsCompleted("a", 0, offset), follower.IsCompleted("a", 0, offset); want != got {
			t.Fatalf("offset %d: leader complete=%v, follower complete=%v", offset, want, got)
		}
	}
}

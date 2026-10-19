package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

type indexTestLog struct {
	first, last int64
	read        func(int64, int64, int64) ([]*types.Event, error)
}

func (l indexTestLog) GetFirstOffset() int64 { return l.first }
func (l indexTestLog) GetLastOffset() int64  { return l.last }
func (l indexTestLog) ReadEventsWithin(from, to, bytes int64) ([]*types.Event, error) {
	return l.read(from, to, bytes)
}

func newTestIndex(t *testing.T) *Index {
	t.Helper()
	i, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := i.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := i.Rebuild(context.Background(), indexTestLog{last: -1}); err != nil {
		t.Fatal(err)
	}
	return i
}

func indexOriginal() *types.Event {
	return &types.Event{PartitionId: 7, Offset: 10, Term: 1, MessageId: "original", Topic: "a", ScheduleTs: 100, Payload: []byte("payload")}
}

func indexReplacement(t *testing.T, previous *types.Event, kind string, offset, term int64) *types.Event {
	t.Helper()
	at := previous.ScheduleTs + 100
	request := fmt.Sprint(offset)
	if kind == Cancel {
		at = 0
	} else if kind == Relocate {
		at, request = previous.ScheduleTs, ""
	}
	e, err := Replacement(previous, kind, request, at)
	if err != nil {
		t.Fatal(err)
	}
	e.Offset, e.Term = offset, term
	if _, err := Decode(e); err != nil {
		t.Fatal(err)
	}
	return e
}

func TestIndexStagedSuccessorFreezesDeliveryWithoutProvingRetention(t *testing.T) {
	for _, kind := range []string{Cancel, Reschedule, Relocate} {
		t.Run(kind, func(t *testing.T) {
			i := newTestIndex(t)
			original := indexOriginal()
			next := indexReplacement(t, original, kind, 20, 2)
			if active, err := i.DeliverableThrough(original, original.Offset); err != nil || !active {
				t.Fatalf("original before mutation: active=%v error=%v", active, err)
			}
			if err := i.Apply(next); err != nil {
				t.Fatal(err)
			}
			for _, e := range []*types.Event{original, next} {
				if active, err := i.DeliverableThrough(e, original.Offset); err != nil || active {
					t.Fatalf("delivered while successor was staged: offset=%d active=%v error=%v", e.Offset, active, err)
				}
				if terminal, err := i.TerminalThrough(e, original.Offset); err != nil || terminal {
					t.Fatalf("pruned before successor was accepted: offset=%d terminal=%v error=%v", e.Offset, terminal, err)
				}
			}
			if terminal, err := i.TerminalThrough(original, next.Offset); err != nil || !terminal {
				t.Fatalf("accepted successor did not release original: %v %v", terminal, err)
			}
			if active, err := i.DeliverableThrough(next, next.Offset); err != nil || active != (kind != Cancel) {
				t.Fatalf("accepted successor delivery: %v %v", active, err)
			}
			if terminal, err := i.TerminalThrough(next, next.Offset); err != nil || terminal != (kind == Cancel) {
				t.Fatalf("active replacement lost its consumer obligation: %v %v", terminal, err)
			}
		})
	}
}

func TestIndexAcceptedHistorySurvivesLaterStagedMutationsAndReorderedObservations(t *testing.T) {
	i := newTestIndex(t)
	original := indexOriginal()
	rescheduled := indexReplacement(t, original, Reschedule, 20, 2)
	relocated := indexReplacement(t, rescheduled, Relocate, 30, 2)
	canceled := indexReplacement(t, relocated, Cancel, 40, 3)
	for _, e := range []*types.Event{canceled, rescheduled, relocated, rescheduled} {
		if err := i.Apply(e); err != nil {
			t.Fatal(err)
		}
	}
	if at, found, err := i.Latest(Reference(original)); err != nil || !found || at != canceled.Offset {
		t.Fatalf("late observation moved the head backwards: %d %v %v", at, found, err)
	}
	for _, test := range []struct{ through, want int64 }{{-1, -1}, {19, -1}, {20, 20}, {29, 20}, {30, 30}, {39, 30}, {40, 40}, {math.MaxInt64, 40}} {
		at, found, err := i.LatestThrough(Reference(original), test.through)
		if err != nil || found != (test.want >= 0) || (found && at != test.want) {
			t.Fatalf("through %d: latest=%d found=%v error=%v, want %d", test.through, at, found, err, test.want)
		}
	}
	if terminal, err := i.TerminalThrough(rescheduled, relocated.Offset); err != nil || !terminal {
		t.Fatalf("new tentative head hid earlier accepted successor: %v %v", terminal, err)
	}
	if terminal, err := i.TerminalThrough(relocated, canceled.Offset-1); err != nil || terminal {
		t.Fatalf("tentative cancel released last accepted copy: %v %v", terminal, err)
	}
	// Serialized identities with embedded delimiters must have disjoint history.
	other := indexOriginal()
	other.MessageId = "original\x00\""
	if at, found, err := i.LatestThrough(Reference(other), math.MaxInt64); err != nil || found {
		t.Fatalf("another identity inherited history: %d %v %v", at, found, err)
	}
}

func TestIndexCleanupKeepsRetainedHeadAndRejectsOldWorkerReferences(t *testing.T) {
	i := newTestIndex(t)
	original := indexOriginal()
	current := original
	// Cross the 128-record cleanup batch size twice.
	for offset := int64(11); offset <= 270; offset++ {
		current = indexReplacement(t, current, Relocate, offset, 2)
		if err := i.Apply(current); err != nil {
			t.Fatal(err)
		}
	}
	if err := i.DropBefore(current.Offset); err != nil {
		t.Fatal(err)
	}
	if at, found, err := i.LatestThrough(Reference(original), current.Offset); err != nil || !found || at != current.Offset {
		t.Fatalf("collected retained head: %d %v %v", at, found, err)
	}
	if at, found, err := i.LatestThrough(Reference(original), current.Offset-1); err != nil || found {
		t.Fatalf("pruned history remained: %d %v %v", at, found, err)
	}
	if err := i.DropBefore(current.Offset + 1); err != nil {
		t.Fatal(err)
	}
	if err := i.Apply(current); err != nil {
		t.Fatal(err)
	}
	if at, found, err := i.Latest(Reference(original)); err != nil || found {
		t.Fatalf("late observation resurrected pruned head: %d %v %v", at, found, err)
	}
	for _, e := range []*types.Event{original, current} {
		if active, err := i.Active(e); err != nil || active {
			t.Fatalf("old worker reference became active: offset=%d %v %v", e.Offset, active, err)
		}
	}
	it, err := i.db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	if it.First() || it.Error() != nil {
		t.Fatalf("collected index still contains keys or failed: %v", it.Error())
	}
}

func TestIndexSuffixReplacementFencesCachedMutationAtReusedOffset(t *testing.T) {
	i := newTestIndex(t)
	original := indexOriginal()
	old := indexReplacement(t, original, Reschedule, 20, 2)
	if err := i.Apply(old); err != nil {
		t.Fatal(err)
	}
	// A new generation has the original, but not the old tentative suffix.
	log := indexTestLog{first: original.Offset, last: original.Offset, read: func(_, _, _ int64) ([]*types.Event, error) {
		return []*types.Event{original}, nil
	}}
	if err := i.Rebuild(context.Background(), log); err != nil {
		t.Fatal(err)
	}
	if active, err := i.Active(original); err != nil || !active {
		t.Fatalf("suffix cut did not restore the original: %v %v", active, err)
	}
	if active, err := i.Active(old); err != nil || active {
		t.Fatalf("discarded worker reference failed the rebuilt generation: %v %v", active, err)
	}
	new := indexReplacement(t, original, Reschedule, old.Offset, old.Term+1)
	if err := i.Apply(new); err != nil {
		t.Fatal(err)
	}
	if active, err := i.Active(old); err != nil || active {
		t.Fatalf("cached old term matched reused physical offset: %v %v", active, err)
	}
	if active, err := i.Active(new); err != nil || !active {
		t.Fatalf("replacement generation did not become active: %v %v", active, err)
	}
}

func requireIncompleteIndex(t *testing.T, i *Index, original *types.Event) {
	t.Helper()
	if _, _, err := i.Latest(Reference(original)); !errors.Is(err, ErrIncompleteIndex) {
		t.Fatalf("head lookup trusted incomplete state: %v", err)
	}
	if _, _, err := i.LatestThrough(Reference(original), math.MaxInt64); !errors.Is(err, ErrIncompleteIndex) {
		t.Fatalf("accepted lookup trusted incomplete state: %v", err)
	}
	if _, err := i.Active(original); !errors.Is(err, ErrIncompleteIndex) {
		t.Fatalf("delivery trusted incomplete state: %v", err)
	}
	if _, err := i.TerminalThrough(original, math.MaxInt64); !errors.Is(err, ErrIncompleteIndex) {
		t.Fatalf("retention trusted incomplete state: %v", err)
	}
	if err := i.DropBefore(100); !errors.Is(err, ErrIncompleteIndex) {
		t.Fatalf("cleanup changed incomplete state: %v", err)
	}
	if err := i.Apply(indexReplacement(t, original, Cancel, 20, 2)); !errors.Is(err, ErrIncompleteIndex) {
		t.Fatalf("append trusted incomplete state: %v", err)
	}
}

func TestIndexFailedOrCanceledRebuildFailsClosedUntilCompleteRecovery(t *testing.T) {
	for _, failure := range []string{"before_scan", "partial_scan"} {
		t.Run(failure, func(t *testing.T) {
			i := newTestIndex(t)
			original := indexOriginal()
			next := indexReplacement(t, original, Reschedule, 20, 2)
			if err := i.Apply(next); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			log := indexTestLog{first: 10, last: 2000, read: func(from, to, bytes int64) ([]*types.Event, error) {
				if to-from >= 1024 || bytes != 4<<20 {
					t.Errorf("unbounded recovery read: %d-%d bytes=%d", from, to, bytes)
				}
				cancel()
				return []*types.Event{next}, nil
			}}
			if failure == "before_scan" {
				cancel()
			}
			if err := i.Rebuild(ctx, log); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled rebuild returned %v", err)
			}
			requireIncompleteIndex(t, i, original)
			log.last = next.Offset
			if err := i.Rebuild(context.Background(), log); err != nil {
				t.Fatalf("complete recovery failed: %v", err)
			}
			if active, err := i.Active(next); err != nil || !active {
				t.Fatalf("complete recovery did not reopen lookups: %v %v", active, err)
			}
		})
	}
}

func TestIndexRefusesPersistedGenerationUntilRebuilt(t *testing.T) {
	dir := t.TempDir()
	i, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := i.Rebuild(context.Background(), indexTestLog{last: -1}); err != nil {
		t.Fatal(err)
	}
	if err := i.Apply(indexReplacement(t, indexOriginal(), Cancel, 20, 2)); err != nil {
		t.Fatal(err)
	}
	if err := i.Close(); err != nil {
		t.Fatal(err)
	}
	i, err = Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	requireIncompleteIndex(t, i, indexOriginal())
}

func TestIndexConflictingObservationRequiresRebuild(t *testing.T) {
	for _, conflict := range []string{"term", "identity"} {
		t.Run(conflict, func(t *testing.T) {
			i := newTestIndex(t)
			original := indexOriginal()
			if err := i.Apply(indexReplacement(t, original, Reschedule, 20, 2)); err != nil {
				t.Fatal(err)
			}
			other := indexOriginal()
			term := int64(3)
			if conflict == "identity" {
				other.MessageId, term = "other", 2
			}
			if err := i.Apply(indexReplacement(t, other, Reschedule, 20, term)); err == nil {
				t.Fatal("replaced an indexed term or identity without rebuilding")
			}
			requireIncompleteIndex(t, i, original)
		})
	}
}

func TestIndexUnindexedMutationCannotBeDeliveredOrPruned(t *testing.T) {
	i := newTestIndex(t)
	for _, kind := range []string{Cancel, Reschedule, Relocate} {
		next := indexReplacement(t, indexOriginal(), kind, 20, 2)
		if active, err := i.Active(next); active || err != nil {
			t.Fatalf("unindexed %s reached delivery: %v %v", kind, active, err)
		}
		if terminal, err := i.TerminalThrough(next, next.Offset); terminal || !errors.Is(err, ErrIncompleteIndex) {
			t.Fatalf("unindexed %s proved retention: %v %v", kind, terminal, err)
		}
	}
}

func TestIndexPartialReadAndCleanupFailureRequireRebuild(t *testing.T) {
	for _, failure := range []string{"read", "cleanup"} {
		t.Run(failure, func(t *testing.T) {
			i := newTestIndex(t)
			original := indexOriginal()
			current := original
			for offset := int64(11); offset <= 270; offset++ {
				current = indexReplacement(t, current, Relocate, offset, 2)
				if err := i.Apply(current); err != nil {
					t.Fatal(err)
				}
			}
			if failure == "read" {
				broken := errors.New("WAL read failed")
				log := indexTestLog{first: 10, last: 2000, read: func(from, _, _ int64) ([]*types.Event, error) {
					if from == 10 {
						return []*types.Event{current}, nil
					}
					return nil, broken
				}}
				if err := i.Rebuild(context.Background(), log); !errors.Is(err, broken) {
					t.Fatalf("read failure returned %v", err)
				}
			} else {
				// Fail after one cleanup batch has already committed. Exposing
				// the resulting partial generation could resurrect old work.
				if err := i.db.Set(offsetKey(200), []byte{'h'}, pebble.NoSync); err != nil {
					t.Fatal(err)
				}
				if err := i.DropBefore(271); err == nil {
					t.Fatal("cleanup accepted a corrupt reverse key")
				}
			}
			requireIncompleteIndex(t, i, original)
			// A retained self-contained replacement is enough to recover.
			log := indexTestLog{first: current.Offset, last: current.Offset, read: func(_, _, _ int64) ([]*types.Event, error) {
				return []*types.Event{current}, nil
			}}
			if err := i.Rebuild(context.Background(), log); err != nil {
				t.Fatal(err)
			}
			if active, err := i.Active(current); err != nil || !active {
				t.Fatalf("retained replacement did not recover: %v %v", active, err)
			}
		})
	}
}

func TestIndexReadersWaitForCompleteRebuildGeneration(t *testing.T) {
	i := newTestIndex(t)
	original := indexOriginal()
	next := indexReplacement(t, original, Reschedule, 20, 2)
	entered, release := make(chan struct{}), make(chan struct{})
	log := indexTestLog{first: 10, last: 20, read: func(_, _, _ int64) ([]*types.Event, error) {
		close(entered)
		<-release
		return []*types.Event{original, next}, nil
	}}
	rebuilt := make(chan error, 1)
	go func() { rebuilt <- i.Rebuild(context.Background(), log) }()
	<-entered
	lookup := make(chan error, 1)
	go func() {
		at, found, err := i.LatestThrough(Reference(original), next.Offset)
		if err == nil && (!found || at != next.Offset) {
			err = fmt.Errorf("reader observed incomplete generation: offset=%d found=%v", at, found)
		}
		lookup <- err
	}()
	select {
	case err := <-lookup:
		close(release)
		<-rebuilt
		t.Fatalf("reader crossed rebuild: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-rebuilt; err != nil {
		t.Fatal(err)
	}
	if err := <-lookup; err != nil {
		t.Fatal(err)
	}
}

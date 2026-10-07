package partition

import (
	"fmt"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/dedup"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func unacceptedTestConfig(t *testing.T) *types.Config {
	return &types.Config{DataDir: t.TempDir(), PartitionCount: 1, TickMS: 10, WheelSize: 100, SegmentSizeBytes: 1 << 20, IndexInterval: 1, FsyncMode: "periodic", FlushIntervalMS: 50, DedupTTLHours: 24, BloomCapacity: 1000}
}

// Held publishes are scheduled and accepted in log order, only as far as the
// caller vouches for, and never twice.
func TestHeldPublishesAreAcceptedInOrderAndOnce(t *testing.T) {
	pm := NewPartitionManager("node-1", unacceptedTestConfig(t))
	defer pm.StopAllPartitions()
	if err := pm.CreatePartition(0, "orders"); err != nil {
		t.Fatal(err)
	}
	if err := pm.StartPartition(0); err != nil {
		t.Fatal(err)
	}
	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatal(err)
	}

	// Three batches of two reach the log. The first two claimed their message
	// IDs; the third was published with duplicates allowed.
	due := time.Now().Add(time.Minute).UnixMilli()
	var batches [3][]*types.Event
	for b := range batches {
		for i := 0; i < 2; i++ {
			batches[b] = append(batches[b], &types.Event{MessageId: fmt.Sprintf("m%d", b*2+i), Topic: "orders", Payload: []byte("p"), ScheduleTs: due})
		}
		if b < 2 {
			ids := []string{batches[b][0].MessageId, batches[b][1].MessageId}
			if _, err := p.DedupStore.IsDuplicateBatch(ids, []int64{dedup.ClaimedOffset, dedup.ClaimedOffset}); err != nil {
				t.Fatal(err)
			}
		}
		if err := p.Wal.AppendBatch(batches[b]); err != nil {
			t.Fatal(err)
		}
		p.HoldUnaccepted(batches[b], b < 2)
	}
	if got := len(p.held); got != 2 {
		t.Fatalf("%d held ranges, want 2: consecutive batches of the same kind are one range (%+v)", got, p.held)
	}
	outcome := func(id string) (dedup.Outcome, bool) {
		t.Helper()
		o, _, found, err := p.DedupStore.Outcome(id)
		if err != nil {
			t.Fatal(err)
		}
		return o, found
	}
	if got := p.Scheduler.GetTimingWheelDepth(); got != 0 {
		t.Fatalf("%d timers scheduled while everything is held", got)
	}
	if o, found := outcome("m0"); !found || o != dedup.Appended {
		t.Fatalf("held publish recorded as %v (found=%v), want appended", o, found)
	}

	for attempt := 0; attempt < 2; attempt++ {
		if err := p.AcceptThrough(2); err != nil {
			t.Fatal(err)
		}
		if got := p.Scheduler.GetTimingWheelDepth(); got != 3 {
			t.Fatalf("attempt %d: %d timers after accepting through offset 2, want 3", attempt, got)
		}
	}
	for id, want := range map[string]dedup.Outcome{"m0": dedup.Accepted, "m2": dedup.Accepted, "m3": dedup.Appended} {
		if o, found := outcome(id); !found || o != want {
			t.Fatalf("%s recorded as %v (found=%v), want %v", id, o, found, want)
		}
	}
	if !p.HasUnaccepted() {
		t.Fatal("offsets 3-5 should still be held")
	}

	if err := p.AcceptThrough(100); err != nil {
		t.Fatal(err)
	}
	if got := p.Scheduler.GetTimingWheelDepth(); got != 6 {
		t.Fatalf("%d timers after accepting everything, want 6", got)
	}
	if p.HasUnaccepted() {
		t.Fatalf("still holding %+v", p.held)
	}
	if o, found := outcome("m3"); !found || o != dedup.Accepted {
		t.Fatalf("m3 recorded as %v (found=%v), want accepted", o, found)
	}
	if _, found := outcome("m4"); found {
		t.Fatal("a publish that allowed duplicates was given a dedup record")
	}
}

// Held ranges stay ordered and joined however the failures arrive.
func TestHeldRangesMergeInAnyOrder(t *testing.T) {
	at := func(from, to int64) []*types.Event {
		return []*types.Event{{Offset: from}, {Offset: to}}
	}
	p := &Partition{}
	for _, r := range [][2]int64{{10, 19}, {30, 39}, {0, 9}, {20, 29}, {50, 59}} {
		p.HoldUnaccepted(at(r[0], r[1]), false)
	}
	want := []heldRange{{from: 0, to: 39}, {from: 50, to: 59}}
	if len(p.held) != len(want) {
		t.Fatalf("held = %+v, want %+v", p.held, want)
	}
	for i := range want {
		if p.held[i] != want[i] {
			t.Fatalf("held = %+v, want %+v", p.held, want)
		}
	}
	if !p.HasUnaccepted() {
		t.Fatal("HasUnaccepted is false with ranges held")
	}
	p.dropHeld()
	if p.HasUnaccepted() || len(p.held) != 0 {
		t.Fatalf("dropHeld left %+v", p.held)
	}
}

// Recovery records where a logged event is instead of a bare claim, so that a
// retry can be answered from the log.
func TestRecoverDedupFromWALRecordsLogPosition(t *testing.T) {
	pm := NewPartitionManager("node-1", unacceptedTestConfig(t))
	defer pm.StopAllPartitions()
	if err := pm.CreatePartition(0, "orders"); err != nil {
		t.Fatal(err)
	}
	p, _ := pm.GetInternalPartition(0)

	now := time.Now().UnixMilli()
	events := make([]*types.Event, 3)
	for i := range events {
		events[i] = &types.Event{MessageId: fmt.Sprintf("m%d", i), Topic: "orders", Payload: []byte("p"), ScheduleTs: now + 60_000, CreatedTs: now}
	}
	if err := p.Wal.AppendBatch(events); err != nil {
		t.Fatal(err)
	}
	// m0 was accepted, m1 only claimed, m2 has no record at all.
	if err := p.DedupStore.Put("m0", 0, now); err != nil {
		t.Fatal(err)
	}
	if _, err := p.DedupStore.IsDuplicate("m1", dedup.ClaimedOffset); err != nil {
		t.Fatal(err)
	}

	pm.recoverDedupFromWAL(p)

	want := []struct {
		outcome dedup.Outcome
		offset  int64
	}{{dedup.Accepted, 0}, {dedup.Appended, 1}, {dedup.Appended, 2}}
	for i, w := range want {
		outcome, offset, found, err := p.DedupStore.Outcome(fmt.Sprintf("m%d", i))
		if err != nil || !found || outcome != w.outcome || offset != w.offset {
			t.Errorf("m%d after recovery: %v at %d (found=%v err=%v), want %v at %d", i, outcome, offset, found, err, w.outcome, w.offset)
		}
	}
}

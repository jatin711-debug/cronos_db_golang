package consumer

import (
	"fmt"
	"sync"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func ackedEvents(partition int32, offsets ...int64) []*types.Event {
	events := make([]*types.Event, len(offsets))
	for i, offset := range offsets {
		events[i] = &types.Event{Topic: "orders", PartitionId: partition, Offset: offset}
	}
	return events
}

// Many consumer streams bring their acks at once, each in a call of its own,
// for several groups and partitions. The calls that wait while one write is
// under way are recorded together by the next: each still gets its own
// results, an invalid delivery among them fails alone, and everything
// acknowledged is recorded, and still is after a restart.
func TestCommitDeliveriesFromManyCallers(t *testing.T) {
	dir := t.TempDir()
	g := newStoreBackedManager(t, dir)
	groups := []string{"a", "b", "c"}
	for _, group := range groups {
		if err := g.CreateGroup(group, "orders", []int32{0, 1}); err != nil {
			t.Fatal(err)
		}
	}

	const callers, calls, perCall = 12, 40, 3
	var wg sync.WaitGroup
	failures := make(chan error, callers)
	for caller := 0; caller < callers; caller++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			group := groups[caller%len(groups)]
			partition := int32(caller % 2)
			for call := 0; call < calls; call++ {
				// Offsets no other caller of this group and partition names.
				base := int64(caller*calls*perCall + call*perCall)
				commits := []DeliveryCommit{
					{GroupID: group, Events: ackedEvents(partition, base, base+1)},
					{GroupID: "nobody", Events: ackedEvents(partition, base)},
					{GroupID: group, Events: ackedEvents(partition, base+2)},
				}
				errs := g.CommitDeliveries(partition, commits)
				if len(errs) != len(commits) || errs[0] != nil || errs[1] == nil || errs[2] != nil {
					failures <- fmt.Errorf("caller %d, call %d: results %v", caller, call, errs)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}

	check := func(g *GroupManager, when string) {
		t.Helper()
		for caller := 0; caller < callers; caller++ {
			group := groups[caller%len(groups)]
			partition := int32(caller % 2)
			for offset := int64(caller * calls * perCall); offset < int64((caller+1)*calls*perCall); offset++ {
				if !g.IsCompleted(group, partition, offset) {
					t.Fatalf("%s: group %s, partition %d, offset %d was acknowledged and is not recorded", when, group, partition, offset)
				}
			}
		}
	}
	check(g, "after the acks")
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := newStoreBackedManager(t, dir)
	defer reopened.Close()
	check(reopened, "after a restart")
}

// What a partition's consumers do to its record of completions: each brings
// a few acks at a time, and each ack is durable before it is answered.
func BenchmarkCommitDeliveriesFromManyAckers(b *testing.B) {
	for _, ackers := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("ackers=%d", ackers), func(b *testing.B) {
			store, err := NewOffsetStore(b.TempDir(), 0, nil)
			if err != nil {
				b.Fatal(err)
			}
			g := NewGroupManagerWithStore(store)
			defer g.Close()
			for acker := 0; acker < ackers; acker++ {
				if err := g.CreateGroup(fmt.Sprintf("g%d", acker), "orders", []int32{0}); err != nil {
					b.Fatal(err)
				}
			}
			var wg sync.WaitGroup
			failures := make(chan error, ackers)
			b.ResetTimer()
			for acker := 0; acker < ackers; acker++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					group := fmt.Sprintf("g%d", acker)
					next := int64(0)
					for i := acker; i < b.N; i += ackers {
						errs := g.CommitDeliveries(0, []DeliveryCommit{{GroupID: group, Events: ackedEvents(0, next, next+1, next+2, next+3)}})
						next += 4
						if errs[0] != nil {
							failures <- errs[0]
							return
						}
					}
				}()
			}
			wg.Wait()
			b.StopTimer()
			close(failures)
			for err := range failures {
				b.Fatal(err)
			}
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ack-batches/s")
		})
	}
}

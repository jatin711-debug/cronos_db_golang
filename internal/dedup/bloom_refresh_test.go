package dedup

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newRefreshTestStore(t *testing.T, capacity uint64) (*BloomPebbleStore, *Manager) {
	t.Helper()
	store, err := NewBloomPebbleStore(t.TempDir(), 0, 168, capacity, 0.01, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	store.rebuildWG.Wait()
	return store, NewManager(store)
}

// accept claims id and records it as appended at offset, as a publish does.
func accept(t *testing.T, m *Manager, id string, offset int64) {
	t.Helper()
	if dup, err := m.IsDuplicate(id, ClaimedOffset); err != nil || dup {
		t.Fatalf("claim %s: dup=%v err=%v", id, dup, err)
	}
	if err := m.PutBatch([]string{id}, []int64{offset}, []int64{time.Now().UnixNano()}); err != nil {
		t.Fatal(err)
	}
}

// A rebuilt filter must hold every ID the store still holds. The hourly prune
// used to empty a full filter and leave it empty, and the next retry of an
// accepted ID was claimed as new.
func TestAcceptedIDStaysDuplicateAfterFilterRebuild(t *testing.T) {
	store, m := newRefreshTestStore(t, 1000)
	accept(t, m, "order-42", 7)
	for i := 0; i < 20000; i++ {
		if _, err := m.IsDuplicate(fmt.Sprintf("new-%d", i), ClaimedOffset); err != nil {
			t.Fatal(err)
		}
	}
	if !store.bloomSaturated() {
		t.Fatal("precondition: 20000 IDs in a filter sized for 1000 should be judged full")
	}
	if _, err := m.PruneExpired(); err != nil {
		t.Fatal(err)
	}
	if !store.bloomReady.Load() {
		t.Fatal("the filter was not ready again after the rebuild")
	}
	if dup, err := m.IsDuplicate("order-42", ClaimedOffset); err != nil || !dup {
		t.Fatalf("an accepted ID was claimed again after the filter was rebuilt: dup=%v err=%v", dup, err)
	}
	if outcome, offset, found, _ := m.Outcome("order-42"); !found || offset != 7 {
		t.Fatalf("the accepted record was overwritten: outcome=%v offset=%d found=%v", outcome, offset, found)
	}
}

// Only a filter that answers "maybe" for many absent IDs is rebuilt. With
// ordinary traffic nearly every "maybe" is false, and the rate used to be taken
// over those answers alone, so a healthy filter was judged full.
func TestHealthyFilterIsNotJudgedFull(t *testing.T) {
	store, m := newRefreshTestStore(t, 1_000_000)
	for i := 0; i < 20000; i++ {
		if _, err := m.IsDuplicate(fmt.Sprintf("id-%d", i), ClaimedOffset); err != nil {
			t.Fatal(err)
		}
	}
	// Under 1% false positives, as a healthy filter gives, and no duplicates:
	// the old ratio counted that as 100%.
	atomic.AddUint64(&store.bloomFalsePos, 100)
	if store.bloomSaturated() {
		t.Fatalf("a filter with %d false positives in %d absent checks was judged full",
			atomic.LoadUint64(&store.bloomFalsePos), atomic.LoadUint64(&store.bloomHits)+atomic.LoadUint64(&store.bloomFalsePos))
	}
}

// Claims go on while the filter is rebuilt, and none of them may take an
// accepted ID as new.
func TestClaimsDuringFilterRebuildKeepDuplicates(t *testing.T) {
	store, m := newRefreshTestStore(t, 100_000)
	const accepted = 2000
	for i := 0; i < accepted; i++ {
		accept(t, m, fmt.Sprintf("acc-%d", i), int64(i))
	}
	var wrong atomic.Int64
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				// A retry of an accepted ID, and a new ID that is then retried.
				if dup, err := m.IsDuplicate(fmt.Sprintf("acc-%d", (i*7+w)%accepted), ClaimedOffset); err != nil || !dup {
					wrong.Add(1)
				}
				id := fmt.Sprintf("w%d-%d", w, i)
				if dup, err := m.IsDuplicate(id, ClaimedOffset); err != nil || dup {
					wrong.Add(1)
				}
				if dup, err := m.IsDuplicate(id, ClaimedOffset); err != nil || !dup {
					wrong.Add(1)
				}
			}
		}(w)
	}
	for r := 0; r < 20; r++ {
		store.ResetBloom()
	}
	close(stop)
	wg.Wait()
	if n := wrong.Load(); n > 0 {
		t.Fatalf("%d claims were answered wrongly while the filter was rebuilt", n)
	}
}

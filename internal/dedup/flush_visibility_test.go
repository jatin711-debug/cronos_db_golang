package dedup

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func assertBufferedIDVisible(t *testing.T, store *PebbleStore) {
	t.Helper()
	if exists, err := store.Exists("flushing"); err != nil || !exists {
		t.Errorf("Exists lost buffered ID: exists=%v, error=%v", exists, err)
	}
	if offset, found, err := store.GetOffset("flushing"); err != nil || !found || offset != 42 {
		t.Errorf("GetOffset lost buffered ID: offset=%d, found=%v, error=%v", offset, found, err)
	}
	if created, found, err := store.GetTimestamp("flushing"); err != nil || !found || created.IsZero() {
		t.Errorf("GetTimestamp lost buffered ID: created=%v, found=%v, error=%v", created, found, err)
	}
}

// A lookup must see the pending ID until the Pebble commit is visible. Pausing
// the write makes the handoff window reproducible without scheduler timing.
func TestPebbleStore_LookupsDuringFlush(t *testing.T) {
	for _, failWrite := range []bool{false, true} {
		name := "successful write"
		if failWrite {
			name = "failed write"
		}
		t.Run(name, func(t *testing.T) {
			store, err := NewPebbleStore(t.TempDir(), 0, 1, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()

			// Exclude the background flush while arranging the buffered entry.
			store.claimMu.Lock()
			if err := store.bufferWrite("flushing", 42); err != nil {
				store.claimMu.Unlock()
				t.Fatal(err)
			}
			writing := make(chan struct{})
			resume := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(resume) })
			defer unblock()
			finished := make(chan error, 1)
			writeErr := errors.New("simulated Pebble commit failure")
			go func() {
				defer store.claimMu.Unlock()
				finished <- store.flushPendingLocked(func(entries map[string]int64) error {
					close(writing)
					<-resume
					if failWrite {
						return writeErr
					}
					return store.flushPendingMap(entries)
				})
			}()
			select {
			case <-writing:
			case <-time.After(5 * time.Second):
				t.Fatal("flush did not reach the write")
			}
			assertBufferedIDVisible(t, store)
			unblock()
			select {
			case err := <-finished:
				if failWrite && !errors.Is(err, writeErr) || !failWrite && err != nil {
					t.Fatalf("flush error: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("flush did not finish")
			}
			assertBufferedIDVisible(t, store)
		})
	}
}

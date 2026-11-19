package consumer

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
)

// The completion prefix is part of every stored floor key, so its layout must
// not change.
func TestCompletionPrefixKeepsItsFormat(t *testing.T) {
	for _, group := range []string{"", "g", "soak-slow", "groupe-é", strings.Repeat("x", 300), "a:b:c"} {
		for _, partition := range []int32{0, 1, 7, 1023, math.MaxInt32, -1} {
			want := fmt.Sprintf("done:%d:%s:%d:", len(group), group, partition)
			if got := completionPrefix(group, partition); got != want {
				t.Fatalf("completionPrefix(%q, %d) = %q, want %q", group, partition, got, want)
			}
		}
	}
}

// A block key gives back its group, partition and number; a group's blocks
// sort by number and stay inside the range its prefix bounds.
func TestBlockKeys(t *testing.T) {
	for _, group := range []string{"", "g", "a:b:c", "groupe-é", strings.Repeat("x", 300)} {
		for _, partition := range []int32{0, 1, 12, math.MaxInt32} {
			prefix := completionPrefix(group, partition)
			lower, upper := prefix, prefix+":"
			var keys []string
			for _, block := range []int64{0, 1, 9, 10, 4095, 1 << 40, math.MaxInt64 / completionBlockBits} {
				key := string(appendBlockKey(nil, prefix, block))
				gotPrefix, gotBlock, ok := parseBlockKey(key)
				if !ok || gotPrefix != prefix || gotBlock != block {
					t.Fatalf("parseBlockKey(%q) = %q, %d, %v; want %q, %d", key, gotPrefix, gotBlock, ok, prefix, block)
				}
				if key < lower || key >= upper {
					t.Fatalf("block key %q is outside [%q, %q)", key, lower, upper)
				}
				keys = append(keys, key)
			}
			if !sort.StringsAreSorted(keys) {
				t.Fatalf("block keys of %q do not sort by number: %q", prefix, keys)
			}
		}
	}
	// Partition 1's range must not take in partition 12's blocks.
	one := completionPrefix("g", 1)
	twelve := string(appendBlockKey(nil, completionPrefix("g", 12), 3))
	if twelve >= one && twelve < one+":" {
		t.Fatalf("partition 12's block %q falls in partition 1's range", twelve)
	}
}

func TestBlockEncoding(t *testing.T) {
	var b completionBlock
	if got := len(b.encode()); got != 0 {
		t.Fatalf("an empty block encodes to %d bytes", got)
	}
	for _, offset := range []int64{0, 63, 64, 1000} {
		b.set(offset)
	}
	encoded := b.encode()
	if len(encoded) != 16*8 {
		t.Fatalf("a block set up to offset 1000 encodes to %d bytes, want the 16 words that hold it", len(encoded))
	}
	decoded, err := decodeBlock(encoded)
	if err != nil || *decoded != b {
		t.Fatalf("decoded %v, %v", decoded, err)
	}
	if _, err := decodeBlock(make([]byte, 7)); err == nil {
		t.Fatal("a value that is not whole words decoded")
	}
	if got := b.highest(2); got != 2*completionBlockBits+1000 {
		t.Fatalf("highest = %d", got)
	}
}

// The floor moves over completed offsets a word at a time, across words and
// blocks, and stops at the first one missing.
func TestPlanAdvancesOverCompletedOffsets(t *testing.T) {
	g := NewGroupManager()
	if err := g.CreateGroup("g", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	var add []int64
	for offset := int64(0); offset < 3*completionBlockBits+70; offset++ {
		if offset != 2*completionBlockBits+5 {
			add = append(add, offset)
		}
	}
	g.commitMu.Lock()
	g.mu.RLock()
	plan := g.planLocked("g", 0, add, 0, -1, true)
	g.mu.RUnlock()
	g.commitMu.Unlock()
	if want := int64(2*completionBlockBits + 5); plan.floor != want {
		t.Fatalf("floor advanced to %d, want %d", plan.floor, want)
	}
	// What is left above the floor: the rest of block 2, and block 3.
	if len(plan.put) != 2 || plan.put[2] == nil || plan.put[3] == nil {
		t.Fatalf("plan keeps blocks %v, want 2 and 3", keysOf(plan.put))
	}
}

func keysOf(m map[int64]*completionBlock) []int64 {
	var keys []int64
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// storedCompletionKeys counts the stored blocks and the per-offset records
// of earlier releases.
func storedCompletionKeys(t *testing.T, manager *GroupManager) (blocks, legacy int) {
	t.Helper()
	err := manager.offsetStore.withDB(func(db *pebble.DB) error {
		iter, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte(completionKeyPrefix), UpperBound: []byte("done;")})
		if err != nil {
			return err
		}
		defer iter.Close()
		for iter.First(); iter.Valid(); iter.Next() {
			if _, _, ok := parseBlockKey(string(iter.Key())); ok {
				blocks++
			} else {
				legacy++
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return blocks, legacy
}

func storedBlocks(t *testing.T, manager *GroupManager) int {
	t.Helper()
	blocks, _ := storedCompletionKeys(t, manager)
	return blocks
}

// The soak's case: one event far ahead keeps the floor where it is while
// 100,000 later events complete. Up to v0.13.1 that left 100,000 records; now
// it is a block per 4,096 offsets. The floor then passes all of them once the
// event completes, and nothing stays stored.
func TestCompletionsBehindAHeldFloorTakeABlockPer4096Offsets(t *testing.T) {
	dir := t.TempDir()
	manager := newStoreBackedManager(t, dir)
	if err := manager.CreateGroup("g", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	const total = 100_000
	for start := int64(1); start <= total; start += 500 {
		var offsets []int64
		for offset := start; offset < start+500 && offset <= total; offset++ {
			offsets = append(offsets, offset)
		}
		if err := manager.CommitDelivery("g", 0, completionEvents("orders", offsets...)); err != nil {
			t.Fatal(err)
		}
	}
	if floor := manager.CompletionFloor("g", 0); floor != 0 {
		t.Fatalf("floor %d passed the unfinished offset 0", floor)
	}
	if got, want := storedBlocks(t, manager), total/completionBlockBits+1; got != want {
		t.Fatalf("%d blocks stored for %d completions, want %d", got, total, want)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	manager = newStoreBackedManager(t, dir)
	defer manager.Close()
	for _, offset := range []int64{1, 4095, 4096, total} {
		if !manager.IsCompleted("g", 0, offset) {
			t.Fatalf("offset %d is not complete after a restart", offset)
		}
	}
	if manager.IsCompleted("g", 0, 0) || manager.IsCompleted("g", 0, total+1) {
		t.Fatal("an offset never completed reads as complete after a restart")
	}
	if err := manager.CommitDelivery("g", 0, completionEvents("orders", 0)); err != nil {
		t.Fatal(err)
	}
	if floor := manager.CompletionFloor("g", 0); floor != total+1 {
		t.Fatalf("floor %d after the held event completed, want %d", floor, total+1)
	}
	if got := storedBlocks(t, manager); got != 0 {
		t.Fatalf("%d blocks left stored below the floor", got)
	}
}

// A store written by an earlier release holds a record per completion. It is
// converted to blocks when opened, and the old records are removed.
func TestLegacyCompletionRecordsAreConverted(t *testing.T) {
	dir := t.TempDir()
	manager := newStoreBackedManager(t, dir)
	if err := manager.CreateGroup("g", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	// Floor 3; offsets 1 (below it) and 5, 4100 recorded as an earlier
	// release recorded them.
	if err := manager.CommitDelivery("g", 0, completionEvents("orders", 0, 1, 2)); err != nil {
		t.Fatal(err)
	}
	err := manager.offsetStore.withDB(func(db *pebble.DB) error {
		batch := db.NewBatch()
		defer batch.Close()
		for _, offset := range []int64{1, 5, 4100} {
			key := fmt.Sprintf("done:%d:%s:%d:%020d", len("g"), "g", 0, offset)
			if err := batch.Set([]byte(key), []byte{1}, nil); err != nil {
				return err
			}
		}
		return batch.Commit(pebble.Sync)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	manager = newStoreBackedManager(t, dir)
	for offset, want := range map[int64]bool{2: true, 3: false, 4: false, 5: true, 4099: false, 4100: true} {
		if got := manager.IsCompleted("g", 0, offset); got != want {
			t.Errorf("IsCompleted(%d) = %v after conversion, want %v", offset, got, want)
		}
	}
	if _, legacy := storedCompletionKeys(t, manager); legacy != 0 {
		t.Fatalf("records of the old format are left: %d", legacy)
	}
	if got := storedBlocks(t, manager); got != 2 {
		t.Fatalf("%d blocks after conversion, want 2 (offsets 5 and 4100)", got)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	// And the blocks are what the next start reads.
	manager = newStoreBackedManager(t, dir)
	defer manager.Close()
	if !manager.IsCompleted("g", 0, 4100) || manager.IsCompleted("g", 0, 4) {
		t.Fatal("the converted completions did not survive another restart")
	}
}

// After a rollback, an earlier release deletes completion records by range:
// from the offset where its log is cut to the end of the group's range, and
// below its floor up to the floor. Those deletions must leave no block that
// holds an offset at or after the cut, so that a later upgrade never finds
// completions of entries that the cut replaced, and must keep every block
// that holds an offset at or above the floor.
func TestBlocksFollowRangeDeletionsOfEarlierReleases(t *testing.T) {
	dir := t.TempDir()
	manager := newStoreBackedManager(t, dir)
	if err := manager.CreateGroup("g", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	// Floor 0 held; completions in blocks 0, 1 and 2.
	if err := manager.CommitDelivery("g", 0, completionEvents("orders", 10, 4090, 4100, 5000, 9000)); err != nil {
		t.Fatal(err)
	}
	prefix := completionPrefix("g", 0)
	legacyKey := func(offset int64) []byte {
		return []byte(fmt.Sprintf("%s%020d", prefix, offset))
	}
	err := manager.offsetStore.withDB(func(db *pebble.DB) error {
		batch := db.NewBatch()
		defer batch.Close()
		// The earlier release cuts its log at 5000 ...
		if err := batch.DeleteRange(legacyKey(5000), []byte(prefix+":"), nil); err != nil {
			return err
		}
		// ... and its floor rises to 20.
		if err := batch.DeleteRange([]byte(prefix), legacyKey(20), nil); err != nil {
			return err
		}
		if err := setFloorInBatch(batch, "g", 0, 20); err != nil {
			return err
		}
		return batch.Commit(pebble.Sync)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	manager = newStoreBackedManager(t, dir)
	defer manager.Close()
	for offset, want := range map[int64]bool{4090: true, 4100: false, 5000: false, 9000: false} {
		if got := manager.IsCompleted("g", 0, offset); got != want {
			t.Errorf("IsCompleted(%d) = %v after the earlier release's deletions, want %v", offset, got, want)
		}
	}
}

// A group deleted and created again under its name starts from nothing.
func TestDeletedGroupLeavesNoProgress(t *testing.T) {
	for name, manager := range map[string]*GroupManager{
		"in-memory": NewGroupManager(),
		"store":     newStoreBackedManager(t, t.TempDir()),
	} {
		t.Run(name, func(t *testing.T) {
			defer manager.Close()
			if err := manager.CreateGroup("g", "orders", []int32{0}); err != nil {
				t.Fatal(err)
			}
			if err := manager.CommitDelivery("g", 0, completionEvents("orders", 0, 1, 5)); err != nil {
				t.Fatal(err)
			}
			if err := manager.DeleteGroup("g"); err != nil {
				t.Fatal(err)
			}
			if err := manager.CreateGroup("g", "orders", []int32{0}); err != nil {
				t.Fatal(err)
			}
			if floor := manager.CompletionFloor("g", 0); floor != 0 {
				t.Fatalf("the new group starts at floor %d, the old group's", floor)
			}
			if manager.IsCompleted("g", 0, 5) {
				t.Fatal("the new group inherited the old group's completion of offset 5")
			}
			if manager.offsetStore != nil {
				if got := storedBlocks(t, manager); got != 0 {
					t.Fatalf("%d blocks of the deleted group are left stored", got)
				}
			}
		})
	}
}

// BenchmarkCommitBehindAHeldFloor measures acks completing events one at a
// time while the floor is held back, the catch-up case of the soak.
func BenchmarkCommitBehindAHeldFloor(b *testing.B) {
	store, err := NewOffsetStore(b.TempDir(), 0, nil)
	if err != nil {
		b.Fatal(err)
	}
	manager := NewGroupManagerWithStore(store)
	defer manager.Close()
	if err := manager.CreateGroup("g", "orders", []int32{0}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := int64(i + 1)
		if err := manager.CommitDelivery("g", 0, completionEvents("orders", offset)); err != nil {
			b.Fatal(err)
		}
		if !manager.IsCompleted("g", 0, offset) {
			b.Fatal("not complete")
		}
	}
}

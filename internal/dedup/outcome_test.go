package dedup

import "testing"

func TestStoredOffsetKeepsOutcomeAndOffsetApart(t *testing.T) {
	for _, offset := range []int64{0, 1, 7, 1 << 40} {
		if outcome, got := DecodeOffset(offset); outcome != Accepted || got != offset {
			t.Errorf("DecodeOffset(%d) = %v, %d; want accepted at %d", offset, outcome, got, offset)
		}
		if outcome, got := DecodeOffset(AppendedAt(offset)); outcome != Appended || got != offset {
			t.Errorf("DecodeOffset(AppendedAt(%d)) = %v, %d; want appended at %d", offset, outcome, got, offset)
		}
	}
	if outcome, got := DecodeOffset(ClaimedOffset); outcome != Claimed || got != -1 {
		t.Errorf("DecodeOffset(ClaimedOffset) = %v, %d; want claimed, -1", outcome, got)
	}
}

// A record may only be released by a caller that read its current value;
// otherwise a retry could remove a claim another publish has made since.
func TestBloomPebbleStore_DeleteIfRemovesOnlyTheValueRead(t *testing.T) {
	store, err := NewBloomPebbleStore(t.TempDir(), 0, 1, 100000, 0.01, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.rebuildWG.Wait()
	mgr := NewManager(store)

	// Still in the write buffer.
	if dup, err := store.CheckAndStore("buffered", ClaimedOffset); err != nil || dup {
		t.Fatalf("claim: dup=%v err=%v", dup, err)
	}
	if released, err := mgr.ReleaseIf("buffered", AppendedAt(3)); err != nil || released {
		t.Fatalf("release with the wrong value: released=%v err=%v", released, err)
	}
	if released, err := mgr.ReleaseIf("buffered", ClaimedOffset); err != nil || !released {
		t.Fatalf("release with the value read: released=%v err=%v", released, err)
	}

	// Written through to Pebble.
	if err := store.Put("stored", AppendedAt(3), 1); err != nil {
		t.Fatal(err)
	}
	if released, err := mgr.ReleaseIf("stored", ClaimedOffset); err != nil || released {
		t.Fatalf("release with the wrong value: released=%v err=%v", released, err)
	}
	if outcome, offset, found, err := mgr.Outcome("stored"); err != nil || !found || outcome != Appended || offset != 3 {
		t.Fatalf("record after a refused release: %v at %d (found=%v err=%v)", outcome, offset, found, err)
	}
	if released, err := mgr.ReleaseIf("stored", AppendedAt(3)); err != nil || !released {
		t.Fatalf("release with the value read: released=%v err=%v", released, err)
	}

	for _, id := range []string{"buffered", "stored"} {
		if _, _, found, err := mgr.Outcome(id); err != nil || found {
			t.Fatalf("%s still recorded after release (err=%v)", id, err)
		}
		if dup, err := store.CheckAndStore(id, ClaimedOffset); err != nil || dup {
			t.Fatalf("%s cannot be claimed after release: dup=%v err=%v", id, dup, err)
		}
	}
}

// A claim written by a run that ended before its publish reached the log can
// never be completed or released. It is dropped when the store is reopened;
// records that point into the log are kept.
func TestBloomPebbleStore_AbandonedClaimsAreDroppedOnReopen(t *testing.T) {
	dir := t.TempDir()
	store, err := NewBloomPebbleStore(dir, 0, 1, 100000, 0.01, nil)
	if err != nil {
		t.Fatal(err)
	}
	if dup, err := store.CheckAndStore("claimed", ClaimedOffset); err != nil || dup {
		t.Fatalf("claim: dup=%v err=%v", dup, err)
	}
	if err := store.Put("appended", AppendedAt(7), 1); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("accepted", 3, 1); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = NewBloomPebbleStore(dir, 0, 1, 100000, 0.01, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.rebuildWG.Wait()

	if _, found, err := store.GetOffset("claimed"); err != nil || found {
		t.Fatalf("abandoned claim survived the restart (err=%v)", err)
	}
	if stored, found, err := store.GetOffset("appended"); err != nil || !found || stored != AppendedAt(7) {
		t.Fatalf("appended record after restart: %d (found=%v err=%v)", stored, found, err)
	}
	if stored, found, err := store.GetOffset("accepted"); err != nil || !found || stored != 3 {
		t.Fatalf("accepted record after restart: %d (found=%v err=%v)", stored, found, err)
	}

	// The ID can be published again, and the claim this run makes for it is in
	// flight rather than abandoned: it is honoured.
	if dup, err := store.CheckAndStore("claimed", ClaimedOffset); err != nil || dup {
		t.Fatalf("ID of an abandoned claim cannot be published again: dup=%v err=%v", dup, err)
	}
	if dup, err := store.CheckAndStore("claimed", ClaimedOffset); err != nil || !dup {
		t.Fatalf("live claim not honoured: dup=%v err=%v", dup, err)
	}
}

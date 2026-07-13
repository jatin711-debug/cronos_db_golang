package delivery

import "testing"

func TestDeliveryExpiry_PopsInDeadlineOrderAndStopsAtFuture(t *testing.T) {
	e := newDeliveryExpiry()
	e.add("c", 300)
	e.add("a", 100)
	e.add("b", 200)
	e.add("d", 400)

	// now=250: entries a(100) and b(200) are due, in deadline order; c(300) is not.
	got := []string{}
	for {
		id, ok := e.popExpired(250)
		if !ok {
			break
		}
		got = append(got, id)
	}
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("expected [a b] in deadline order, got %v", got)
	}
	if e.len() != 2 {
		t.Fatalf("expected 2 entries remaining, got %d", e.len())
	}
}

func TestDeliveryExpiry_RemoveBeforeExpiry(t *testing.T) {
	e := newDeliveryExpiry()
	e.add("a", 100)
	e.add("b", 200)
	e.remove("a")
	if e.len() != 1 {
		t.Fatalf("expected 1 after remove, got %d", e.len())
	}
	id, ok := e.popExpired(1000)
	if !ok || id != "b" {
		t.Fatalf("expected only b to remain, got id=%q ok=%v", id, ok)
	}
	if _, ok := e.popExpired(1000); ok {
		t.Fatal("expected empty after popping b")
	}
}

func TestDeliveryExpiry_ReTrackUpdatesDeadlineInPlace(t *testing.T) {
	e := newDeliveryExpiry()
	e.add("a", 100)
	// A retry re-tracks the same id with a later deadline; must not leak a
	// second entry and must not expire at the old deadline.
	e.add("a", 500)
	if e.len() != 1 {
		t.Fatalf("expected single entry after re-track, got %d", e.len())
	}
	if _, ok := e.popExpired(200); ok {
		t.Fatal("entry should not be due at the old deadline after re-track")
	}
	id, ok := e.popExpired(600)
	if !ok || id != "a" {
		t.Fatalf("entry should be due at the new deadline, got id=%q ok=%v", id, ok)
	}
}

func TestDeliveryExpiry_RemoveIsIdempotent(t *testing.T) {
	e := newDeliveryExpiry()
	e.add("a", 100)
	e.remove("a")
	e.remove("a") // no-op, must not panic
	e.remove("missing")
	if e.len() != 0 {
		t.Fatalf("expected empty, got %d", e.len())
	}
}

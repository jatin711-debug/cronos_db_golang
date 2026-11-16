package delivery

import (
	"slices"
	"testing"
)

// takeSpan takes the cursor's requested ranges and returns the lowest and
// highest offset they cover.
func takeSpan(c *RedriveCursor) (low, high int64, ok bool) {
	ranges := c.Take()
	if len(ranges) == 0 {
		return 0, 0, false
	}
	return ranges[0].Low, ranges[len(ranges)-1].High, true
}

func TestOffsetRanges_MergesOnlyWhatIsClose(t *testing.T) {
	var s OffsetRanges
	s.Add(1000, 1000)
	s.Add(10, 12)
	s.Add(13, 20)                     // adjacent: one range
	s.Add(20+rangeMergeGap, 200)      // within the gap: still one
	s.Add(5000, 5010)                 // far: separate
	s.Add(999, 1001)                  // overlaps 1000
	s.Add(5011+rangeMergeGap+1, 6000) // just beyond the gap: separate

	want := []OffsetRange{{10, 200}, {999, 1001}, {5000, 5010}, {5011 + rangeMergeGap + 1, 6000}}
	if got := s.Ranges(); !slices.Equal(got, want) {
		t.Fatalf("ranges = %v, want %v", got, want)
	}

	// A range spanning several joins them all.
	s.Add(150, 5500)
	if got, want := s.Ranges(), []OffsetRange{{10, 6000}}; !slices.Equal(got, want) {
		t.Fatalf("ranges = %v, want %v", got, want)
	}
}

func TestOffsetRanges_RemoveBelowKeepsTheRest(t *testing.T) {
	var s OffsetRanges
	s.Add(0, 9)
	s.Add(1000, 1009)
	s.Add(5000, 5009)

	s.RemoveBelow(5)
	if got, want := s.Ranges(), []OffsetRange{{5, 9}, {1000, 1009}, {5000, 5009}}; !slices.Equal(got, want) {
		t.Fatalf("after RemoveBelow(5): %v, want %v", got, want)
	}
	s.RemoveBelow(10)
	if got, want := s.Ranges(), []OffsetRange{{1000, 1009}, {5000, 5009}}; !slices.Equal(got, want) {
		t.Fatalf("after RemoveBelow(10): %v, want %v", got, want)
	}
	s.RemoveBelow(6000)
	if !s.Empty() {
		t.Fatalf("after RemoveBelow(6000): %v, want empty", s.Ranges())
	}
}

// However many separate offsets are added, the set stays bounded, and it
// still covers every one of them.
func TestOffsetRanges_StaysBounded(t *testing.T) {
	var s OffsetRanges
	const n = 10 * maxOffsetRanges
	for i := int64(0); i < n; i++ {
		s.Add(i*1000, i*1000)
	}
	if s.Len() > maxOffsetRanges {
		t.Fatalf("%d ranges kept, want at most %d", s.Len(), maxOffsetRanges)
	}
	ranges := s.Ranges()
	for i := int64(0); i < n; i++ {
		offset := i * 1000
		j, _ := slices.BinarySearchFunc(ranges, offset, func(r OffsetRange, o int64) int {
			switch {
			case r.High < o:
				return -1
			case r.Low > o:
				return 1
			}
			return 0
		})
		if j >= len(ranges) || ranges[j].Low > offset || ranges[j].High < offset {
			t.Fatalf("offset %d is no longer covered", offset)
		}
	}
}

// One pass can hold back an event written long ago next to one just written:
// scheduled events become due in schedule order, not log order. The redrive
// request must name both, not everything written between them; a reader sent
// through the whole span started over each time an older event was held back
// and never reached the newer ones.
func TestDispatcher_HeldBackEventsFarApartStaySeparate(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()

	stream := newMockStream()
	sub := makeTestSubscription("sub-1", 0, stream)
	sub.MaxCredits = 1
	if err := d.Subscribe(sub); err != nil {
		t.Fatal(err)
	}
	cursor := d.RegisterRedrive(sub.ConsumerGroup)
	defer d.UnregisterRedrive(cursor)

	// One credit: offset 99 goes out, the rest is held back.
	events := append(testEvents(99, 100), testEvents(250000, 250001)...)
	if err := d.DispatchBatch(events); err != nil {
		t.Fatal(err)
	}
	if got := stream.lastMsg.Event.GetOffset(); got != 99 {
		t.Fatalf("delivered offset %d, want 99", got)
	}
	want := []OffsetRange{{100, 100}, {250000, 250001}}
	if got := cursor.Take(); !slices.Equal(got, want) {
		t.Fatalf("redrive request = %v, want %v", got, want)
	}
}

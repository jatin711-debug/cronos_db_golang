package delivery

import (
	"sort"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// OffsetRange is an inclusive span of WAL offsets.
type OffsetRange struct {
	Low, High int64
}

const (
	// rangeMergeGap is how far apart two offsets may be and still be kept in
	// one range. Reading the few entries between them costs less than a
	// second read of the log.
	rangeMergeGap = 128
	// maxOffsetRanges bounds how many separate ranges a set keeps. Beyond it
	// the two closest ranges are merged, which costs a read of the entries
	// between them and nothing else.
	maxOffsetRanges = 1024
)

// OffsetRanges is a set of offsets kept as sorted, disjoint ranges.
//
// It replaces a single [lowest, highest] span. Events held back together are
// often far apart in the log: scheduled events become due in schedule order,
// not in the order they were written, so one pass can hold back an event
// written hours ago next to one written a second ago. A single span then
// covers everything written in between, and a reader working through it
// starts again from the bottom every time an older event joins it, without
// ever reaching the newer ones. Separate ranges keep each one's own progress.
//
// The zero value is an empty set. It is not safe for concurrent use.
type OffsetRanges struct {
	ranges []OffsetRange
}

// Add puts [low, high] into the set, merging it with the ranges it overlaps or
// comes within rangeMergeGap of.
func (s *OffsetRanges) Add(low, high int64) {
	if high < low {
		return
	}
	// The first range that could merge: one that ends no lower than the gap
	// before low.
	i := sort.Search(len(s.ranges), func(i int) bool { return s.ranges[i].High+rangeMergeGap >= low })
	j := i
	for j < len(s.ranges) && s.ranges[j].Low <= high+rangeMergeGap {
		low, high = min(low, s.ranges[j].Low), max(high, s.ranges[j].High)
		j++
	}
	switch {
	case i == j:
		s.ranges = append(s.ranges, OffsetRange{})
		copy(s.ranges[i+1:], s.ranges[i:])
		s.ranges[i] = OffsetRange{Low: low, High: high}
	default:
		s.ranges[i] = OffsetRange{Low: low, High: high}
		s.ranges = append(s.ranges[:i+1], s.ranges[j:]...)
	}
	for len(s.ranges) > maxOffsetRanges {
		s.mergeClosest()
	}
}

// AddEvents puts the offsets of events into the set.
func (s *OffsetRanges) AddEvents(events ...*types.Event) {
	for _, event := range events {
		if event != nil {
			s.Add(event.Offset, event.Offset)
		}
	}
}

// AddAll puts every range of other into the set.
func (s *OffsetRanges) AddAll(other []OffsetRange) {
	for _, r := range other {
		s.Add(r.Low, r.High)
	}
}

// mergeClosest joins the two neighbouring ranges with the smallest gap.
func (s *OffsetRanges) mergeClosest() {
	if len(s.ranges) < 2 {
		return
	}
	best := 0
	for i := 1; i < len(s.ranges)-1; i++ {
		if s.ranges[i+1].Low-s.ranges[i].High < s.ranges[best+1].Low-s.ranges[best].High {
			best = i
		}
	}
	s.ranges[best].High = s.ranges[best+1].High
	s.ranges = append(s.ranges[:best+1], s.ranges[best+2:]...)
}

// Empty reports whether the set holds no offsets.
func (s *OffsetRanges) Empty() bool { return len(s.ranges) == 0 }

// Len returns how many separate ranges the set holds.
func (s *OffsetRanges) Len() int { return len(s.ranges) }

// First returns the lowest range.
func (s *OffsetRanges) First() (OffsetRange, bool) {
	if len(s.ranges) == 0 {
		return OffsetRange{}, false
	}
	return s.ranges[0], true
}

// RemoveBelow drops every offset below offset.
func (s *OffsetRanges) RemoveBelow(offset int64) {
	i := 0
	for i < len(s.ranges) && s.ranges[i].High < offset {
		i++
	}
	s.ranges = s.ranges[i:]
	if len(s.ranges) > 0 && s.ranges[0].Low < offset {
		s.ranges[0].Low = offset
	}
	if len(s.ranges) == 0 {
		s.ranges = nil
	}
}

// Ranges returns a copy of the ranges, lowest first.
func (s *OffsetRanges) Ranges() []OffsetRange {
	if len(s.ranges) == 0 {
		return nil
	}
	return append([]OffsetRange(nil), s.ranges...)
}

// Take returns the ranges, lowest first, and empties the set.
func (s *OffsetRanges) Take() []OffsetRange {
	ranges := s.ranges
	s.ranges = nil
	return ranges
}

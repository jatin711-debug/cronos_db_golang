package scheduler

import "github.com/jatin711-debug/cronos_db_golang/pkg/types"

// The events a budgeted scheduler holds in memory, ordered latest due first.
//
// When an event that is due earlier needs room, the latest one held goes back
// to the cold store. Finding it used to mean a scan of every event held: with
// the budget full, that ran for most publishes and hydration passes.

type retainedItem struct {
	event *types.Event
	index int
}

// retainedHeap is a max-heap by schedule time; ties put the higher offset on
// top. The caller keeps each item's index up to date through it.
type retainedHeap []*retainedItem

func (h retainedHeap) later(i, j int) bool {
	a, b := h[i].event, h[j].event
	if a.ScheduleTs != b.ScheduleTs {
		return a.ScheduleTs > b.ScheduleTs
	}
	return a.Offset > b.Offset
}

func (h retainedHeap) swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}

func (h retainedHeap) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !h.later(i, parent) {
			return
		}
		h.swap(i, parent)
		i = parent
	}
}

func (h retainedHeap) down(i int) {
	for {
		top, left, right := i, 2*i+1, 2*i+2
		if left < len(h) && h.later(left, top) {
			top = left
		}
		if right < len(h) && h.later(right, top) {
			top = right
		}
		if top == i {
			return
		}
		h.swap(i, top)
		i = top
	}
}

// push adds item.
func (h *retainedHeap) push(item *retainedItem) {
	item.index = len(*h)
	*h = append(*h, item)
	h.up(item.index)
}

// remove takes item out, wherever it is.
func (h *retainedHeap) remove(item *retainedItem) {
	i, last := item.index, len(*h)-1
	if i < 0 || i > last || (*h)[i] != item {
		return
	}
	if i != last {
		h.swap(i, last)
	}
	(*h)[last] = nil
	*h = (*h)[:last]
	if i < last {
		h.down(i)
		h.up(i)
	}
	item.index = -1
}

// latest returns the event due last, or nil.
func (h retainedHeap) latest() *types.Event {
	if len(h) == 0 {
		return nil
	}
	return h[0].event
}

package delivery

import (
	"container/heap"
	"time"
)

// RetryEntry is a delivery scheduled for a future retry attempt.
type RetryEntry struct {
	active  *ActiveDelivery // delivery state to re-send
	retryAt int64           // earliest retry time (Unix ms)
	index   int             // heap index maintained by container/heap
}

// RetryHeap is a min-heap of RetryEntry ordered by retryAt.
// It provides O(log N) push/pop and O(1) peek for the dispatcher timeout loop.
type RetryHeap struct {
	entries []*RetryEntry
}

// NewRetryHeap creates a new retry heap.
func NewRetryHeap() *RetryHeap {
	h := &RetryHeap{
		entries: make([]*RetryEntry, 0),
	}
	heap.Init(h)
	return h
}

// PushEntry adds a retry entry to the heap.
func (h *RetryHeap) PushEntry(entry *RetryEntry) {
	heap.Push(h, entry)
}

// PopEntry removes and returns the earliest retry entry.
func (h *RetryHeap) PopEntry() *RetryEntry {
	if h.Len() == 0 {
		return nil
	}
	return heap.Pop(h).(*RetryEntry)
}

// Peek returns the earliest retry entry without removing it.
func (h *RetryHeap) Peek() *RetryEntry {
	if h.Len() == 0 {
		return nil
	}
	return h.entries[0]
}

// Due returns all entries with retryAt <= now, removing them from the heap.
func (h *RetryHeap) Due(now int64) []*RetryEntry {
	var due []*RetryEntry
	for {
		entry := h.Peek()
		if entry == nil || entry.retryAt > now {
			break
		}
		due = append(due, h.PopEntry())
	}
	return due
}

// Len returns the number of entries in the heap.
func (h *RetryHeap) Len() int {
	return len(h.entries)
}

// --- heap.Interface implementation ---

func (h *RetryHeap) Less(i, j int) bool {
	return h.entries[i].retryAt < h.entries[j].retryAt
}

func (h *RetryHeap) Swap(i, j int) {
	h.entries[i], h.entries[j] = h.entries[j], h.entries[i]
	h.entries[i].index = i
	h.entries[j].index = j
}

// Push implements heap.Interface. Do not call directly; use PushEntry.
func (h *RetryHeap) Push(x interface{}) {
	n := len(h.entries)
	entry := x.(*RetryEntry)
	entry.index = n
	h.entries = append(h.entries, entry)
}

// Pop implements heap.Interface. Do not call directly; use PopEntry.
func (h *RetryHeap) Pop() interface{} {
	old := h.entries
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil // avoid memory leak
	entry.index = -1
	h.entries = old[:n-1]
	return entry
}

// NewRetryEntry creates a retry entry with the given backoff duration.
func NewRetryEntry(active *ActiveDelivery, backoff time.Duration) *RetryEntry {
	return &RetryEntry{
		active:  active,
		retryAt: time.Now().UnixMilli() + backoff.Milliseconds(),
	}
}

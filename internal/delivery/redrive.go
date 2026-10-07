package delivery

import (
	"sync"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// RedriveCursor is one subscription's view of redelivery requests for its
// consumer group. Events that could not be delivered when they became ready
// (no credits, in-flight cap, a full worker queue, a dropped stream) stay in
// the WAL; the dispatcher records their offset range here so the subscription
// re-reads exactly that range instead of polling the whole log.
type RedriveCursor struct {
	group string
	wake  chan struct{}

	mu        sync.Mutex
	requested bool
	low, high int64
}

// Take returns the offset range requested since the previous Take and clears
// it. ok is false when nothing was requested.
func (c *RedriveCursor) Take() (low, high int64, ok bool) {
	c.mu.Lock()
	low, high, ok = c.low, c.high, c.requested
	c.requested = false
	c.mu.Unlock()
	return low, high, ok
}

// Wake is signalled when a redrive is requested or the group regains credits.
func (c *RedriveCursor) Wake() <-chan struct{} {
	return c.wake
}

func (c *RedriveCursor) notify() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *RedriveCursor) request(low, high int64) {
	c.mu.Lock()
	if !c.requested {
		c.low, c.high, c.requested = low, high, true
	} else {
		c.low, c.high = min(c.low, low), max(c.high, high)
	}
	c.mu.Unlock()
	c.notify()
}

// RegisterRedrive subscribes to redelivery requests for group. The caller must
// UnregisterRedrive the cursor when its subscription ends.
func (d *Dispatcher) RegisterRedrive(group string) *RedriveCursor {
	cursor := &RedriveCursor{group: group, wake: make(chan struct{}, 1)}
	d.redriveMu.Lock()
	if d.redriveCursors == nil {
		d.redriveCursors = make(map[*RedriveCursor]struct{})
	}
	d.redriveCursors[cursor] = struct{}{}
	d.redriveMu.Unlock()
	return cursor
}

// UnregisterRedrive removes a cursor returned by RegisterRedrive.
func (d *Dispatcher) UnregisterRedrive(cursor *RedriveCursor) {
	d.redriveMu.Lock()
	delete(d.redriveCursors, cursor)
	d.redriveMu.Unlock()
}

// RequestRedrive asks the subscriptions of group to re-read WAL offsets
// [low, high]. An empty group addresses every group on the partition.
func (d *Dispatcher) RequestRedrive(group string, low, high int64) {
	if high < low {
		return
	}
	d.redriveMu.RLock()
	for cursor := range d.redriveCursors {
		if group == "" || cursor.group == group {
			cursor.request(low, high)
		}
	}
	d.redriveMu.RUnlock()
}

// requestRedriveOf asks group to re-read the offsets spanned by events.
func (d *Dispatcher) requestRedriveOf(group string, events []*types.Event) {
	var span offsetSpan
	span.add(events...)
	if span.set {
		d.RequestRedrive(group, span.low, span.high)
	}
}

// wakeRedrive nudges the group's subscriptions without requesting a range,
// for example when an ack has returned credits.
func (d *Dispatcher) wakeRedrive(group string) {
	d.redriveMu.RLock()
	for cursor := range d.redriveCursors {
		if cursor.group == group {
			cursor.notify()
		}
	}
	d.redriveMu.RUnlock()
}

// offsetSpan accumulates the lowest and highest offset of a set of events.
type offsetSpan struct {
	set       bool
	low, high int64
}

func (s *offsetSpan) add(events ...*types.Event) {
	for _, event := range events {
		if event == nil {
			continue
		}
		if !s.set {
			s.low, s.high, s.set = event.Offset, event.Offset, true
			continue
		}
		s.low, s.high = min(s.low, event.Offset), max(s.high, event.Offset)
	}
}

// RedriveGroup offers retained events to one consumer group. It reports how
// many were handed to a subscriber and whether any eligible event was held
// back by flow control; events already completed or in flight are skipped.
func (d *Dispatcher) RedriveGroup(group string, events []*types.Event) (dispatched int, blocked bool) {
	if len(events) == 0 {
		return 0, false
	}
	dispatched, blocked, _ = d.dispatchGroupBatch(events[0].PartitionId, events, group)
	return dispatched, blocked
}

func (d *Dispatcher) isPending(group string, offset int64) bool {
	d.pendingMu.Lock()
	_, pending := d.pending[group][offset]
	d.pendingMu.Unlock()
	return pending
}

func (d *Dispatcher) markPending(group string, offset int64) {
	d.pendingMu.Lock()
	offsets := d.pending[group]
	if offsets == nil {
		if d.pending == nil {
			d.pending = make(map[string]map[int64]struct{})
		}
		offsets = make(map[int64]struct{})
		d.pending[group] = offsets
	}
	offsets[offset] = struct{}{}
	d.pendingMu.Unlock()
}

func (d *Dispatcher) releasePending(group string, events []*types.Event) {
	d.pendingMu.Lock()
	if offsets := d.pending[group]; offsets != nil {
		for _, event := range events {
			delete(offsets, event.Offset)
		}
		if len(offsets) == 0 {
			delete(d.pending, group)
		}
	}
	d.pendingMu.Unlock()
}

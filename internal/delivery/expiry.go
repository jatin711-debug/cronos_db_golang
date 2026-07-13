package delivery

import "container/heap"

// expiryItem is one delivery's ack deadline in a shard's expiry index.
// index is maintained by container/heap so the item can be removed in O(log n)
// when the delivery is acked before it expires.
type expiryItem struct {
	id       string
	deadline int64 // ack deadline as Unix nanoseconds
	index    int   // position within the heap; -1 once popped
}

// expiryPQ is a min-heap of expiryItem ordered by deadline. It implements
// container/heap.Interface. Do not use it directly; go through deliveryExpiry,
// which keeps the id->item lookup in sync.
type expiryPQ []*expiryItem

func (pq expiryPQ) Len() int { return len(pq) }

func (pq expiryPQ) Less(i, j int) bool { return pq[i].deadline < pq[j].deadline }

func (pq expiryPQ) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *expiryPQ) Push(x any) {
	it := x.(*expiryItem)
	it.index = len(*pq)
	*pq = append(*pq, it)
}

func (pq *expiryPQ) Pop() any {
	old := *pq
	n := len(old)
	it := old[n-1]
	old[n-1] = nil // avoid retaining the item for GC
	it.index = -1
	*pq = old[:n-1]
	return it
}

// deliveryExpiry is a per-shard index of in-flight deliveries ordered by ack
// deadline. It replaces the previous O(active) full-map scan: expiry becomes
// O(k log n) for the k entries that are actually due, and removal on ack keeps
// the structure bounded to the current in-flight count (not throughput × timeout).
//
// It is not internally synchronized; callers hold the owning shard's lock.
type deliveryExpiry struct {
	pq     expiryPQ
	lookup map[string]*expiryItem
}

func newDeliveryExpiry() *deliveryExpiry {
	return &deliveryExpiry{lookup: make(map[string]*expiryItem)}
}

// add inserts or updates the deadline for a delivery id. A retry re-tracks the
// same delivery id with a later deadline; that updates the existing entry in
// place rather than leaking a stale one.
func (e *deliveryExpiry) add(id string, deadlineNano int64) {
	if it, ok := e.lookup[id]; ok {
		it.deadline = deadlineNano
		heap.Fix(&e.pq, it.index)
		return
	}
	it := &expiryItem{id: id, deadline: deadlineNano}
	heap.Push(&e.pq, it)
	e.lookup[id] = it
}

// remove drops a delivery id from the index (called when it is acked or its
// subscription is removed). No-op if absent.
func (e *deliveryExpiry) remove(id string) {
	if it, ok := e.lookup[id]; ok {
		heap.Remove(&e.pq, it.index)
		delete(e.lookup, id)
	}
}

// popExpired removes and returns the earliest delivery id whose deadline is at
// or before nowNano. It returns ok=false when the earliest entry is not yet due
// (so the caller can stop) or the index is empty.
func (e *deliveryExpiry) popExpired(nowNano int64) (string, bool) {
	if len(e.pq) == 0 || e.pq[0].deadline > nowNano {
		return "", false
	}
	it := heap.Pop(&e.pq).(*expiryItem)
	delete(e.lookup, it.id)
	return it.id, true
}

// len returns the number of tracked deadlines.
func (e *deliveryExpiry) len() int { return len(e.pq) }

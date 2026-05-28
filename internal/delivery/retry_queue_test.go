package delivery

import (
	"testing"
	"time"
)

func TestRetryHeap_PushPop(t *testing.T) {
	h := NewRetryHeap()

	// Push entries with different retry times
	now := time.Now().UnixMilli()

	entry1 := &RetryEntry{retryAt: now + 100}
	entry2 := &RetryEntry{retryAt: now + 50}
	entry3 := &RetryEntry{retryAt: now + 200}

	h.PushEntry(entry1)
	h.PushEntry(entry2)
	h.PushEntry(entry3)

	if h.Len() != 3 {
		t.Errorf("expected len 3, got %d", h.Len())
	}

	// Pop should return earliest first
	popped := h.PopEntry()
	if popped != entry2 {
		t.Errorf("expected entry2 first, got %v", popped)
	}

	popped = h.PopEntry()
	if popped != entry1 {
		t.Errorf("expected entry1 second, got %v", popped)
	}

	popped = h.PopEntry()
	if popped != entry3 {
		t.Errorf("expected entry3 last, got %v", popped)
	}

	if h.Len() != 0 {
		t.Errorf("expected empty heap, got len %d", h.Len())
	}
}

func TestRetryHeap_PopEmpty(t *testing.T) {
	h := NewRetryHeap()

	result := h.PopEntry()
	if result != nil {
		t.Errorf("expected nil for empty pop, got %v", result)
	}
}

func TestRetryHeap_Peek(t *testing.T) {
	h := NewRetryHeap()

	result := h.Peek()
	if result != nil {
		t.Error("expected nil peek on empty heap")
	}

	now := time.Now().UnixMilli()
	entry := &RetryEntry{retryAt: now + 100}
	h.PushEntry(entry)

	peeked := h.Peek()
	if peeked != entry {
		t.Errorf("expected same entry, got %v", peeked)
	}

	// Peek should NOT remove
	if h.Len() != 1 {
		t.Errorf("expected len 1 after peek, got %d", h.Len())
	}
}

func TestRetryHeap_Due(t *testing.T) {
	h := NewRetryHeap()
	now := time.Now().UnixMilli()

	// Entry due now
	dueNow := &RetryEntry{retryAt: now - 10}
	// Entry due in 100ms
	dueLater := &RetryEntry{retryAt: now + 100}
	// Entry due in 200ms
	dueEvenLater := &RetryEntry{retryAt: now + 200}

	h.PushEntry(dueLater)
	h.PushEntry(dueNow)
	h.PushEntry(dueEvenLater)

	// Only due now entries should be returned
	due := h.Due(now)
	if len(due) != 1 {
		t.Errorf("expected 1 due entry, got %d", len(due))
	}
	if due[0] != dueNow {
		t.Errorf("expected dueNow entry")
	}

	// After 150ms, dueLater should also be due
	due2 := h.Due(now + 150)
	if len(due2) != 1 {
		t.Errorf("expected 1 due entry at 150ms, got %d", len(due2))
	}
	if due2[0] != dueLater {
		t.Errorf("expected dueLater entry")
	}

	// No more due entries
	due3 := h.Due(now + 150)
	if len(due3) != 0 {
		t.Errorf("expected 0 due entries, got %d", len(due3))
	}
}

func TestRetryHeap_Due_Multiple(t *testing.T) {
	h := NewRetryHeap()
	now := time.Now().UnixMilli()

	// All due now
	entries := make([]*RetryEntry, 5)
	for i := 0; i < 5; i++ {
		entries[i] = &RetryEntry{retryAt: now - int64(i)}
		h.PushEntry(entries[i])
	}

	due := h.Due(now)
	if len(due) != 5 {
		t.Errorf("expected 5 due entries, got %d", len(due))
	}
}

func TestRetryHeap_Less(t *testing.T) {
	h := NewRetryHeap()
	now := int64(1000)

	// Test with actual entries
	e1 := &RetryEntry{retryAt: now + 10}
	e2 := &RetryEntry{retryAt: now + 20}
	h.entries = []*RetryEntry{e1, e2}

	if !h.Less(0, 1) {
		t.Error("expected e1 retryAt < e2 retryAt")
	}
	if h.Less(1, 0) {
		t.Error("expected e2 retryAt > e1 retryAt")
	}
}

func TestRetryHeap_Swap(t *testing.T) {
	h := NewRetryHeap()
	e1 := &RetryEntry{index: 0}
	e2 := &RetryEntry{index: 1}
	h.entries = []*RetryEntry{e1, e2}

	h.Swap(0, 1)

	if h.entries[0] != e2 || h.entries[1] != e1 {
		t.Error("Swap did not exchange entries correctly")
	}
	if h.entries[0].index != 0 || h.entries[1].index != 1 {
		t.Error("Swap did not update indices correctly")
	}
}

func TestNewRetryEntry(t *testing.T) {
	active := &ActiveDelivery{}
	backoff := 100 * time.Millisecond

	before := time.Now().UnixMilli()
	entry := NewRetryEntry(active, backoff)
	after := time.Now().UnixMilli()

	expectedMin := before + backoff.Milliseconds()
	expectedMax := after + backoff.Milliseconds()

	if entry.retryAt < expectedMin || entry.retryAt > expectedMax {
		t.Errorf("retryAt %d not in expected range [%d, %d]", entry.retryAt, expectedMin, expectedMax)
	}

	if entry.active != active {
		t.Error("entry.active should point to the ActiveDelivery")
	}
}

func TestRetryHeap_OrderAfterMultiplePushes(t *testing.T) {
	h := NewRetryHeap()
	now := time.Now().UnixMilli()

	// Push in random order
	order := []int64{500, 100, 300, 50, 400, 200}
	entries := make([]*RetryEntry, len(order))
	for i, delay := range order {
		entries[i] = &RetryEntry{retryAt: now + delay}
		h.PushEntry(entries[i])
	}

	// Pop all and verify order
	var prev int64 = 0
	for i := 0; i < len(order); i++ {
		entry := h.PopEntry()
		if entry.retryAt < prev {
			t.Errorf("heap order violated: %d < %d", entry.retryAt, prev)
		}
		prev = entry.retryAt
	}
}
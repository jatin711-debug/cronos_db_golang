package consumer

import (
	"testing"

	"github.com/cockroachdb/pebble"
)

// BenchmarkCatchUpBehindAHeldFloor completes events in acks of 50 while one
// event far ahead holds the floor, the way the soak's slow group caught up,
// checking each delivered event first as the dispatcher does. It reports what
// the offset store wrote: its WAL, its flushes and compactions.
func BenchmarkCatchUpBehindAHeldFloor(b *testing.B) {
	store, err := NewOffsetStore(b.TempDir(), 0, nil)
	if err != nil {
		b.Fatal(err)
	}
	manager := NewGroupManagerWithStore(store)
	defer manager.Close()
	if err := manager.CreateGroup("g", "orders", []int32{0}); err != nil {
		b.Fatal(err)
	}
	const ack = 50
	offsets := make([]int64, ack)
	b.ReportAllocs()
	b.ResetTimer()
	next := int64(1) // offset 0 is the event far ahead
	for i := 0; i < b.N; i++ {
		for j := range offsets {
			offsets[j] = next
			if manager.IsCompleted("g", 0, next) {
				b.Fatal("an event not yet delivered reads as complete")
			}
			next++
		}
		if err := manager.CommitDelivery("g", 0, completionEvents("orders", offsets...)); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	_ = store.withDB(func(db *pebble.DB) error {
		if err := db.Flush(); err != nil {
			return err
		}
		m := db.Metrics()
		total := m.Total()
		events := float64(b.N * ack)
		b.ReportMetric(float64(m.WAL.BytesWritten)/events, "wal-B/event")
		b.ReportMetric(float64(total.BytesFlushed+total.BytesCompacted)/events, "flush+compact-B/event")
		return nil
	})
}

// BenchmarkLookupBehindAHeldFloor checks events completed above a held floor,
// as the redelivery sweep does each time it reads the log from the floor.
func BenchmarkLookupBehindAHeldFloor(b *testing.B) {
	store, err := NewOffsetStore(b.TempDir(), 0, nil)
	if err != nil {
		b.Fatal(err)
	}
	manager := NewGroupManagerWithStore(store)
	defer manager.Close()
	if err := manager.CreateGroup("g", "orders", []int32{0}); err != nil {
		b.Fatal(err)
	}
	const total = 100_000
	for start := int64(1); start <= total; start += 1000 {
		offsets := make([]int64, 0, 1000)
		for offset := start; offset < start+1000; offset++ {
			offsets = append(offsets, offset)
		}
		if err := manager.CommitDelivery("g", 0, completionEvents("orders", offsets...)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !manager.IsCompleted("g", 0, int64(i%total)+1) {
			b.Fatal("not complete")
		}
	}
}

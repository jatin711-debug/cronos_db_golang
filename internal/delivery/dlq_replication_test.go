package delivery

import (
	"fmt"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestDeadLetterPagesIncludeEvictedEntriesAndDurableRemovals(t *testing.T) {
	source, err := NewDeadLetterQueue(t.TempDir(), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	dir := t.TempDir()
	replica, err := NewDeadLetterQueue(dir, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 270; i++ {
		if err := source.Add(&types.Event{MessageId: fmt.Sprint(i), Offset: int64(i), Payload: []byte("failed payload")}, fmt.Sprint(i), 3, "failure", "s"); err != nil {
			t.Fatal(err)
		}
	}
	transfer := func() int {
		t.Helper()
		var cursor types.DeadLetterCursor
		count := 0
		for pages := 0; pages < 10; pages++ {
			records, next, done, err := source.ExportPage(cursor)
			if err != nil {
				t.Fatal(err)
			}
			if len(records) > 256 {
				t.Fatal("unbounded page")
			}
			if err := replica.ApplyReplica(0, records); err != nil {
				t.Fatal(err)
			}
			count += len(records)
			cursor = next
			if done {
				return count
			}
		}
		t.Fatal("transfer did not finish")
		return 0
	}
	if n := transfer(); n != 270 || replica.Count() != 270 {
		t.Fatalf("journal=%d replica=%d", n, replica.Count())
	}
	transfer()
	if replica.Count() != 270 {
		t.Fatal("retry duplicated entries")
	}
	if err := source.Remove("269"); err != nil {
		t.Fatal(err)
	}
	transfer()
	if err := replica.Close(); err != nil {
		t.Fatal(err)
	}
	replica, err = NewDeadLetterQueue(dir, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	transfer()
	if replica.Count() != 269 {
		t.Fatalf("restart/retry resurrected entries: %d", replica.Count())
	}
	if err := source.Clear(); err != nil {
		t.Fatal(err)
	}
	transfer()
	if replica.Count() != 0 {
		t.Fatalf("clear missed evicted entries: %d", replica.Count())
	}
}

func TestDeadLetterFailedWriteIsNotVisible(t *testing.T) {
	d, err := NewDeadLetterQueue(t.TempDir(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.Add(&types.Event{MessageId: "x"}, "d", 1, "error", "s"); err == nil {
		t.Fatal("expected closed writer failure")
	}
	if d.Count() != 0 || d.Version() != 0 {
		t.Fatal("failed write became visible")
	}
}

func TestDeadLetterRetryPreservesOtherEventsFromSameBatch(t *testing.T) {
	dir := t.TempDir()
	d, err := NewDeadLetterQueue(dir, 10)
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 2; i++ {
		if err := d.Add(&types.Event{Offset: i, MessageId: fmt.Sprint(i)}, "batch", 2, "failed", "s"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.Retry("batch"); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = NewDeadLetterQueue(dir, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if d.Count() != 1 || d.Get()[0].Event.Offset != 1 {
		t.Fatal("retry lost another event in the batch")
	}
}

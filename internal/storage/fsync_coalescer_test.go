package storage

import (
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestFsyncCoalescer_BasicFlush(t *testing.T) {
	coalescer := NewFsyncCoalescer(25 * time.Millisecond)
	defer coalescer.Close()

	tmpDir := t.TempDir()
	config := &WALConfig{
		SegmentSizeBytes: 64 * 1024 * 1024,
		IndexInterval:    100,
		FsyncMode:        "batch",
		FlushIntervalMS:  25,
	}

	wal, err := NewWALWithCoalescer(tmpDir, 0, config, nil, coalescer)
	if err != nil {
		t.Fatalf("NewWALWithCoalescer failed: %v", err)
	}
	defer wal.Close()

	event := &types.Event{
		MessageId:  "coalesce-1",
		ScheduleTs: time.Now().UnixMilli(),
		Payload:    []byte("payload"),
		Topic:      "test",
	}
	if err := wal.AppendEvent(event); err != nil {
		t.Fatalf("AppendEvent failed: %v", err)
	}

	// Wait for at least one coalesced sweep to run.
	time.Sleep(100 * time.Millisecond)

	if wal.GetNextOffset() != 1 {
		t.Errorf("expected next offset 1, got %d", wal.GetNextOffset())
	}

	// Recovery should see the event since the coalescer flushed/synced it.
	wal2, err := NewWAL(tmpDir, 0, config, nil)
	if err != nil {
		t.Fatalf("reopen WAL failed: %v", err)
	}
	defer wal2.Close()

	if wal2.GetNextOffset() != 1 {
		t.Errorf("expected recovered next offset 1, got %d", wal2.GetNextOffset())
	}
}

func TestFsyncCoalescer_MultipleWALs(t *testing.T) {
	coalescer := NewFsyncCoalescer(25 * time.Millisecond)
	defer coalescer.Close()

	config := &WALConfig{
		SegmentSizeBytes: 64 * 1024 * 1024,
		IndexInterval:    100,
		FsyncMode:        "batch",
		FlushIntervalMS:  25,
	}

	const numWALs = 4
	wals := make([]*WAL, numWALs)
	dirs := make([]string, numWALs)
	for i := 0; i < numWALs; i++ {
		dirs[i] = t.TempDir()
		wal, err := NewWALWithCoalescer(dirs[i], int32(i), config, nil, coalescer)
		if err != nil {
			t.Fatalf("NewWALWithCoalescer failed: %v", err)
		}
		wals[i] = wal
	}
	defer func() {
		for _, wal := range wals {
			wal.Close()
		}
	}()

	for i, wal := range wals {
		event := &types.Event{
			MessageId:  "wal-event",
			ScheduleTs: time.Now().UnixMilli(),
			Payload:    []byte("payload"),
			Topic:      "test",
		}
		if err := wal.AppendEvent(event); err != nil {
			t.Fatalf("AppendEvent on WAL-%d failed: %v", i, err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	for i, wal := range wals {
		if wal.GetNextOffset() != 1 {
			t.Errorf("WAL-%d expected next offset 1, got %d", i, wal.GetNextOffset())
		}
	}
}

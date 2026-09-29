package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestMaintenanceBackupFailurePreservesCompletedGeneration(t *testing.T) {
	dir := t.TempDir()
	fail := false
	bs := NewCheckpointBackupScheduler(dir, time.Hour, time.Hour, func(dest string) error {
		if fail {
			return errors.New("injected checkpoint failure")
		}
		return os.WriteFile(filepath.Join(dest, "verified"), []byte("checkpoint"), 0600)
	})
	if err := bs.runBackup(); err != nil {
		t.Fatal(err)
	}
	fail = true
	if err := bs.runBackup(); err == nil {
		t.Fatal("checkpoint failure reported success")
	}
	complete, _ := filepath.Glob(filepath.Join(dir, "backup-*"))
	incomplete, _ := filepath.Glob(filepath.Join(dir, ".incomplete-*"))
	if len(complete) != 1 || len(incomplete) != 0 {
		t.Fatalf("published incomplete backup: complete=%v incomplete=%v", complete, incomplete)
	}
	if _, err := os.Stat(filepath.Join(complete[0], "verified")); err != nil {
		t.Fatal(err)
	}
}

func TestMaintenanceBackupStopWaitsForCheckpoint(t *testing.T) {
	entered, release, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
	bs := NewCheckpointBackupScheduler(t.TempDir(), time.Millisecond, time.Hour, func(string) error {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return nil
	})
	bs.Start()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("backup did not start")
	}
	go func() { bs.Stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Error("Stop returned while checkpoint was active")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not finish")
	}
	bs.Stop()  // idempotent
	bs.Start() // must not restart a stopped scheduler
}

func TestMaintenancePruneRejectsCorruptionAndCancellation(t *testing.T) {
	w, err := NewWAL(t.TempDir(), 0, &WALConfig{SegmentSizeBytes: 1024, IndexInterval: 1, FsyncMode: "batch", FlushIntervalMS: 10}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.AppendEvent(&types.Event{MessageId: "zero", Topic: "a", Payload: make([]byte, 2048), ScheduleTs: 1}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := w.Prune(ctx, PruneOptions{AllCompleted: true}, func(*types.Event) bool { return true }); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	seg := w.GetSegments()[0]
	if _, err := seg.segmentFile.WriteAt([]byte{255}, 75); err != nil {
		t.Fatal(err)
	}
	if n, err := w.Prune(context.Background(), PruneOptions{AllCompleted: true}, func(*types.Event) bool { return true }); n != 0 || err == nil {
		t.Fatalf("corrupt segment pruned: %d %v", n, err)
	}
	if len(w.GetSegments()) != 2 {
		t.Fatal("corrupt segment removed from WAL")
	}
}

func TestMaintenanceLegacyBackupIncludesOffsetZero(t *testing.T) {
	dir, dest := t.TempDir(), t.TempDir()
	w, err := NewWAL(dir, 0, &WALConfig{SegmentSizeBytes: 1024, IndexInterval: 1, FsyncMode: "batch", FlushIntervalMS: 10}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.AppendEvent(&types.Event{MessageId: "zero", Payload: make([]byte, 2048), ScheduleTs: 1}); err != nil {
		t.Fatal(err)
	}
	if err := BackupWAL(dir, dest); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dest, "segments", "00000000000000000000.log")); err != nil {
		t.Fatalf("offset zero omitted from backup: %v", err)
	}
}

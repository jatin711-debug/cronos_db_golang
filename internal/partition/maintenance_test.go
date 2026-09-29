package partition

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/compliance"
	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func maintenanceManager(t *testing.T, encrypted bool) *PartitionManager {
	t.Helper()
	cfg := &types.Config{DataDir: t.TempDir(), PartitionCount: 2, TickMS: 10, WheelSize: 100, SegmentSizeBytes: 1024, IndexInterval: 1, FsyncMode: "batch", FlushIntervalMS: 10, DedupTTLHours: 24, BloomCapacity: 1000}
	if encrypted {
		cfg.EncryptionEnabled = true
		cfg.EncryptionKeyFile = filepath.Join(t.TempDir(), "key")
		if err := os.WriteFile(cfg.EncryptionKeyFile, make([]byte, 32), 0600); err != nil {
			t.Fatal(err)
		}
	}
	pm := NewPartitionManager("maintenance", cfg)
	t.Cleanup(func() {
		if err := pm.Close(); err != nil {
			t.Error(err)
		}
	})
	for id := int32(0); id < 2; id++ {
		if err := pm.CreatePartition(id, "a"); err != nil {
			t.Fatal(err)
		}
	}
	return pm
}

func appendMaintenanceEvent(t *testing.T, p *Partition, id, topic string, at time.Time, size int) *types.Event {
	t.Helper()
	e := &types.Event{MessageId: id, Topic: topic, Payload: make([]byte, size), ScheduleTs: at.UnixMilli()}
	if err := p.Wal.AppendEvent(e); err != nil {
		t.Fatal(err)
	}
	return e
}

func TestMaintenanceBackupsRestoreEveryPartitionAndActiveTail(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint("encrypted=", encrypted), func(t *testing.T) {
			pm := maintenanceManager(t, encrypted)
			backups := []string{t.TempDir(), t.TempDir()}
			for generation, dest := range backups {
				for _, p := range pm.ListPartitions() {
					appendMaintenanceEvent(t, p, fmt.Sprintf("closed-%d-%d", p.ID, generation), "a", time.Now(), 2048)
					appendMaintenanceEvent(t, p, fmt.Sprintf("tail-%d-%d", p.ID, generation), "a", time.Now().Add(time.Hour), 32)
				}
				if err := pm.BackupWALs(dest); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(filepath.Join(dest, "backup.json")); err != nil {
					t.Fatal(err)
				}
			}
			// Restore both generations AFTER subsequent source writes. Each must be
			// independent, including offset zero, rotated segments, and the active tail.
			for generation, backup := range backups {
				for id := int32(0); id < 2; id++ {
					dest := t.TempDir()
					if err := storage.RestoreWAL(filepath.Join(backup, "partitions", fmt.Sprint(id)), dest); err != nil {
						t.Fatal(err)
					}
					var cipher *storage.SegmentCipher
					if encrypted {
						var err error
						cipher, err = storage.NewSegmentCipher(make([]byte, 32), id)
						if err != nil {
							t.Fatal(err)
						}
					}
					w, err := storage.NewWAL(dest, id, &storage.WALConfig{SegmentSizeBytes: 1024, IndexInterval: 1, FsyncMode: "batch", FlushIntervalMS: 10}, cipher)
					if err != nil {
						t.Fatal(err)
					}
					events, readErr := w.ReadEvents(0, int64(2*(generation+1)-1))
					closeErr := w.Close()
					if readErr != nil || closeErr != nil {
						t.Fatalf("restore: read=%v close=%v", readErr, closeErr)
					}
					if len(events) != 2*(generation+1) {
						t.Fatalf("generation %d partition %d: got %d events", generation, id, len(events))
					}
					for index, event := range events {
						kind := "closed"
						if index%2 == 1 {
							kind = "tail"
						}
						want := fmt.Sprintf("%s-%d-%d", kind, id, index/2)
						if event.MessageId != want {
							t.Fatalf("got %s, want %s", event.MessageId, want)
						}
					}
				}
			}
		})
	}
}

func TestMaintenanceRetentionProtectsPendingUnownedAndFutureEvents(t *testing.T) {
	pm := maintenanceManager(t, false)
	p, _ := pm.GetInternalPartition(0)
	for _, group := range []string{"g1", "g2"} {
		if err := p.ConsumerGroup.CreateGroup(group, "a", []int32{0}); err != nil {
			t.Fatal(err)
		}
	}
	future := appendMaintenanceEvent(t, p, "future", "a", time.Now().Add(time.Hour), 2048)
	done := appendMaintenanceEvent(t, p, "done", "a", time.Now().Add(-time.Hour), 2048)
	unowned := appendMaintenanceEvent(t, p, "unowned", "b", time.Now().Add(-time.Hour), 2048)
	pending := appendMaintenanceEvent(t, p, "pending", "a", time.Now().Add(-time.Hour), 2048)
	for _, group := range []string{"g1", "g2"} {
		if err := p.ConsumerGroup.CommitDelivery(group, 0, []*types.Event{done, future}); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.ConsumerGroup.CommitDelivery("g1", 0, []*types.Event{pending}); err != nil {
		t.Fatal(err)
	}
	// A cursor override must never substitute for the missing per-event ACK.
	if err := p.ConsumerGroup.CommitOffset("g2", 0, 100); err != nil {
		t.Fatal(err)
	}
	enforcer := compliance.NewManagedEnforcer(pm.config.DataDir, compliance.RetentionPolicy{MaxSizeBytes: 1}, pm.RemoveRetainedSegment)
	stats, err := enforcer.RunWithStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.SegmentsDeleted != 1 {
		t.Fatalf("deleted %d segments, want only the completed historical event", stats.SegmentsDeleted)
	}
	for _, event := range []*types.Event{future, unowned, pending} {
		got, err := p.Wal.ReadEvents(event.Offset, event.Offset)
		if err != nil || len(got) != 1 || got[0].MessageId != event.MessageId {
			t.Fatalf("protected event %s lost: %v %v", event.MessageId, got, err)
		}
	}
	if err := p.ConsumerGroup.CommitDelivery("g2", 0, []*types.Event{pending}); err != nil {
		t.Fatal(err)
	}
	n, err := p.PruneWAL(context.Background(), storage.PruneOptions{AllCompleted: true})
	if err != nil || n != 1 {
		t.Fatalf("completed segment not released: deleted=%d err=%v", n, err)
	}
}

func TestMaintenanceReplicatedPruningFailsClosed(t *testing.T) {
	pm := maintenanceManager(t, false)
	p, _ := pm.GetInternalPartition(0)
	p.retentionBlocked = true
	appendMaintenanceEvent(t, p, "replicated", "a", time.Now(), 2048)
	if n, err := p.PruneWAL(context.Background(), storage.PruneOptions{AllCompleted: true}); err == nil || n != 0 {
		t.Fatalf("replicated pruning: %d %v", n, err)
	}
}

func TestMaintenanceRetentionUsesOwningPartitionIdentity(t *testing.T) {
	pm := maintenanceManager(t, false)
	p, _ := pm.GetInternalPartition(1)
	if err := p.ConsumerGroup.CreateGroup("g", "a", []int32{1}); err != nil {
		t.Fatal(err)
	}
	e := appendMaintenanceEvent(t, p, "partition-one", "a", time.Now().Add(-time.Minute), 2048)
	if err := p.ConsumerGroup.CommitDelivery("g", 1, []*types.Event{e}); err != nil {
		t.Fatal(err)
	}
	n, err := p.PruneWAL(context.Background(), storage.PruneOptions{AllCompleted: true})
	if err != nil || n != 1 {
		t.Fatalf("partition 1 completion ignored: %d %v", n, err)
	}
}

func TestMaintenanceCheckpointDuringAppendsRestoresContiguousPrefix(t *testing.T) {
	pm := maintenanceManager(t, false)
	p, _ := pm.GetInternalPartition(0)
	errors := make(chan error, 1)
	go func() {
		for i := 0; i < 40; i++ {
			if err := p.Wal.AppendEvent(&types.Event{MessageId: fmt.Sprint(i), Topic: "a", ScheduleTs: 1, Payload: make([]byte, 256)}); err != nil {
				errors <- err
				return
			}
		}
		errors <- nil
	}()
	dest := t.TempDir()
	backupErr := pm.BackupWALs(dest)
	if err := <-errors; err != nil {
		t.Fatal(err)
	}
	if backupErr != nil {
		t.Fatal(backupErr)
	}
	restore := t.TempDir()
	if err := storage.RestoreWAL(filepath.Join(dest, "partitions", "0"), restore); err != nil {
		t.Fatal(err)
	}
	w, err := storage.NewWAL(restore, 0, &storage.WALConfig{SegmentSizeBytes: 1024, IndexInterval: 1, FsyncMode: "batch", FlushIntervalMS: 10}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if w.GetNextOffset() > 40 {
		t.Fatalf("invalid restored end: %d", w.GetNextOffset())
	}
	if w.GetNextOffset() > 0 {
		events, err := w.ReadEvents(0, w.GetNextOffset()-1)
		if err != nil || int64(len(events)) != w.GetNextOffset() {
			t.Fatalf("checkpoint lost records: %d %v", len(events), err)
		}
		for i, event := range events {
			if event.MessageId != fmt.Sprint(i) || event.Offset != int64(i) {
				t.Fatalf("noncontiguous checkpoint: %+v", event)
			}
		}
	}
}

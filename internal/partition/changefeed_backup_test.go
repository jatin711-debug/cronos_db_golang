package partition

import (
	"path/filepath"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
)

// A failed CDC destination has not accepted any of the three committed
// events. A backup did not hold the feed's position, and the restored
// partition took its feed for newly switched on: it started at the end of the
// log, and the three events were never exported.
func TestBackupKeepsPendingChangeFeedExports(t *testing.T) {
	sink := &feedSink{}
	sink.refuse.Store(true)
	cfg := unacceptedTestConfig(t)
	cfg.FsyncMode = "every_event"
	source := feedManager(t, cfg, sink)
	p := startedPartition(t, source, 0)
	publish(t, p, 3)
	if position, _ := p.ChangeFeedPosition(); position != -1 {
		t.Fatalf("precondition: source feed cursor is %d, want -1", position)
	}
	backup := filepath.Join(t.TempDir(), "backup")
	if err := source.Backup(backup); err != nil {
		t.Fatal(err)
	}
	restoredDir := t.TempDir()
	if _, err := storage.RestoreBackup(backup, restoredDir, storage.RestoreOptions{}); err != nil {
		t.Fatal(err)
	}
	restoredCfg := *cfg
	restoredCfg.DataDir = restoredDir
	restoredSink := &feedSink{}
	restoredSink.refuse.Store(true)
	restored := startedPartition(t, feedManager(t, &restoredCfg, restoredSink), 0)
	events, err := restored.Wal.ReadEvents(0, 2)
	if err != nil || len(events) != 3 {
		t.Fatalf("restored WAL has %d events, err=%v", len(events), err)
	}
	if position, _ := restored.ChangeFeedPosition(); position != -1 {
		t.Fatalf("backup restored all 3 accepted WAL events but advanced the CDC cursor from -1 to %d without exporting them; sink received %s", position, restoredSink.got(0))
	}
	restoredSink.refuse.Store(false)
	restoredSink.expect(t, 0, "[0 1 2]")
}

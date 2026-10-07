package partition

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/dedup"
	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func backupTestConfig(dataDir string) *types.Config {
	return &types.Config{DataDir: dataDir, PartitionCount: 2, TickMS: 10, WheelSize: 100, SegmentSizeBytes: 1024, IndexInterval: 1, FsyncMode: "periodic", FlushIntervalMS: 20, DedupTTLHours: 24, BloomCapacity: 1000}
}

func startBackupTestNode(t *testing.T, dataDir string) *PartitionManager {
	t.Helper()
	pm := NewPartitionManager("node-1", backupTestConfig(dataDir))
	t.Cleanup(func() { pm.Close() })
	for id := int32(0); id < 2; id++ {
		if err := pm.CreatePartition(id, "orders"); err != nil {
			t.Fatal(err)
		}
		if err := pm.StartPartition(id); err != nil {
			t.Fatal(err)
		}
	}
	return pm
}

// accept appends events to the partition as an accepted publish does: in the
// log, with their message IDs recorded as accepted.
func accept(t *testing.T, p *Partition, events []*types.Event) {
	t.Helper()
	if err := p.Wal.AppendBatch(events); err != nil {
		t.Fatal(err)
	}
	ids, offsets, created := make([]string, len(events)), make([]int64, len(events)), make([]int64, len(events))
	for i, event := range events {
		ids[i], offsets[i], created[i] = event.MessageId, event.Offset, event.CreatedTs
	}
	if err := p.DedupStore.PutBatch(ids, offsets, created); err != nil {
		t.Fatal(err)
	}
}

// populate gives a partition state in every store a backup covers and returns
// the six events in its log: three due now and three due in ten minutes.
func populate(t *testing.T, p *Partition) []*types.Event {
	t.Helper()
	now := time.Now()
	events := make([]*types.Event, 6)
	for i := range events {
		due := now.Add(-time.Second)
		if i >= 3 {
			due = now.Add(10 * time.Minute)
		}
		events[i] = &types.Event{MessageId: fmt.Sprintf("p%d-order-%d", p.ID, i), Topic: "orders", Payload: make([]byte, 300), ScheduleTs: due.UnixMilli(), CreatedTs: now.UnixMilli()}
	}
	accept(t, p, events)

	// A consumer group finished offsets 0, 1 and, out of order, 3.
	if err := p.ConsumerGroup.CreateGroup("workers", "orders", []int32{p.ID}); err != nil {
		t.Fatal(err)
	}
	if err := p.ConsumerGroup.CommitDelivery("workers", p.ID, []*types.Event{events[0], events[1], events[3]}); err != nil {
		t.Fatal(err)
	}
	// One delivery is dead-lettered for good; another was dead-lettered and
	// then removed by an operator.
	if err := p.DLQ.Add(events[4], "delivery-4", 5, "handler failed", "worker-1"); err != nil {
		t.Fatal(err)
	}
	if err := p.DLQ.Add(events[5], "delivery-5", 5, "handler failed", "worker-1"); err != nil {
		t.Fatal(err)
	}
	if err := p.DLQ.Remove("delivery-5"); err != nil {
		t.Fatal(err)
	}
	if err := p.AcceptLeadership(7, "node-1"); err != nil {
		t.Fatal(err)
	}
	return events
}

// A backup taken from a running node, restored into an empty data directory,
// gives a node that knows everything the original did at that moment: the
// events, what consumers finished, which message IDs were published, what was
// dead-lettered, and which leadership epoch it had accepted.
func TestBackupRestoresWholePartitionState(t *testing.T) {
	source := startBackupTestNode(t, t.TempDir())
	want := make(map[int32][]*types.Event)
	for _, p := range source.ListPartitions() {
		want[p.ID] = populate(t, p)
	}

	backup := filepath.Join(t.TempDir(), "backup")
	if err := source.Backup(backup); err != nil {
		t.Fatalf("backup: %v", err)
	}

	// The node keeps working after the backup. None of this may show up in
	// the restored state.
	for _, p := range source.ListPartitions() {
		late := []*types.Event{{MessageId: fmt.Sprintf("p%d-late", p.ID), Topic: "orders", Payload: make([]byte, 300), ScheduleTs: time.Now().Add(10 * time.Minute).UnixMilli()}}
		accept(t, p, late)
		if err := p.ConsumerGroup.CommitDelivery("workers", p.ID, []*types.Event{want[p.ID][2], want[p.ID][4]}); err != nil {
			t.Fatal(err)
		}
		if err := p.DLQ.Add(late[0], "delivery-late", 5, "handler failed", "worker-1"); err != nil {
			t.Fatal(err)
		}
	}

	restoredDir := t.TempDir()
	manifest, err := storage.RestoreBackup(backup, restoredDir, storage.RestoreOptions{})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if len(manifest.Partitions) != 2 {
		t.Fatalf("manifest lists %d partitions, want 2", len(manifest.Partitions))
	}
	for _, entry := range manifest.Partitions {
		if entry.LastOffset != 5 || len(entry.Files) == 0 {
			t.Fatalf("manifest entry for partition %d: last offset %d, %d files", entry.PartitionID, entry.LastOffset, len(entry.Files))
		}
		if got := strings.Join(entry.Components, ","); got != "dedup,consumer_offsets,dlq,epoch" {
			t.Fatalf("partition %d backed up components %q", entry.PartitionID, got)
		}
	}

	restored := startBackupTestNode(t, restoredDir)
	for _, p := range restored.ListPartitions() {
		events := want[p.ID]

		// The log.
		if got := p.Wal.GetLastOffset(); got != 5 {
			t.Fatalf("partition %d: restored log ends at offset %d, want 5", p.ID, got)
		}
		logged, err := p.Wal.ReadEvents(0, 5)
		if err != nil || len(logged) != 6 {
			t.Fatalf("partition %d: read restored log: %d events, %v", p.ID, len(logged), err)
		}
		for i, event := range logged {
			if event.MessageId != events[i].MessageId || event.ScheduleTs != events[i].ScheduleTs || len(event.Payload) != 300 {
				t.Fatalf("partition %d offset %d: restored %q, want %q", p.ID, i, event.MessageId, events[i].MessageId)
			}
		}

		// Consumer progress, as of the backup.
		for offset := int64(0); offset < 6; offset++ {
			wantDone := offset == 0 || offset == 1 || offset == 3
			if got := p.ConsumerGroup.IsCompleted("workers", p.ID, offset); got != wantDone {
				t.Errorf("partition %d: restored IsCompleted(%d) = %v, want %v", p.ID, offset, got, wantDone)
			}
		}
		if committed, err := p.ConsumerGroup.GetCommittedOffset("workers", p.ID); err != nil || committed != 2 {
			t.Errorf("partition %d: restored group resumes at offset %d (err=%v), want 2", p.ID, committed, err)
		}

		// Published message IDs: still accepted, so a retry is a duplicate.
		for offset, event := range events {
			outcome, at, found, err := p.DedupStore.Outcome(event.MessageId)
			if err != nil || !found || outcome != dedup.Accepted || at != int64(offset) {
				t.Errorf("partition %d: restored record of %s: %v at %d (found=%v err=%v), want accepted at %d", p.ID, event.MessageId, outcome, at, found, err, offset)
			}
		}
		if _, _, found, _ := p.DedupStore.Outcome(fmt.Sprintf("p%d-late", p.ID)); found {
			t.Errorf("partition %d: an ID published after the backup is known to the restored node", p.ID)
		}

		// The dead-letter queue, without the entry an operator removed.
		entries := p.DLQ.Get()
		if len(entries) != 1 || entries[0].DeliveryID != "delivery-4" || entries[0].Event.GetMessageId() != events[4].MessageId {
			t.Errorf("partition %d: restored dead-letter queue holds %d entries: %+v", p.ID, len(entries), entries)
		}

		// Fencing.
		if p.Epoch() != 7 || p.EpochLeader() != "node-1" {
			t.Errorf("partition %d: restored epoch %d held by %q, want 7 held by node-1", p.ID, p.Epoch(), p.EpochLeader())
		}

		// Timers are not in a backup; they come back from the log.
		if got := p.Scheduler.GetTimingWheelDepth(); got != 3 {
			t.Errorf("partition %d: %d timers pending after restore, want the 3 future events", p.ID, got)
		}
	}
}

// Restore must not turn a damaged or incomplete backup into a node that looks
// healthy, and must not write over data that is already there.
func TestRestoreRefusesWhatItCannotVouchFor(t *testing.T) {
	source := startBackupTestNode(t, t.TempDir())
	for _, p := range source.ListPartitions() {
		populate(t, p)
	}
	good := filepath.Join(t.TempDir(), "backup")
	if err := source.Backup(good); err != nil {
		t.Fatal(err)
	}

	// copyBackup returns a private copy of the good backup to damage.
	copyBackup := func(t *testing.T) string {
		t.Helper()
		dst := filepath.Join(t.TempDir(), "backup")
		if err := os.CopyFS(dst, os.DirFS(good)); err != nil {
			t.Fatal(err)
		}
		return dst
	}
	firstSegment := func(t *testing.T, backup string, partition int) string {
		t.Helper()
		segments, err := filepath.Glob(filepath.Join(backup, "partitions", fmt.Sprint(partition), "segments", "*.log"))
		if err != nil || len(segments) == 0 {
			t.Fatalf("no segments in backup of partition %d: %v", partition, err)
		}
		return segments[0]
	}

	cases := []struct {
		name   string
		damage func(t *testing.T, backup string)
		want   string
	}{
		{"a flipped byte in a log segment", func(t *testing.T, backup string) {
			path := firstSegment(t, backup, 1)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			data[len(data)/2] ^= 0xff
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
		}, "does not match the backup manifest"},
		{"a truncated file", func(t *testing.T, backup string) {
			path := firstSegment(t, backup, 1)
			if err := os.Truncate(path, 100); err != nil {
				t.Fatal(err)
			}
		}, "does not match the backup manifest"},
		{"a missing file", func(t *testing.T, backup string) {
			if err := os.Remove(firstSegment(t, backup, 1)); err != nil {
				t.Fatal(err)
			}
		}, "backup is missing"},
		{"a manifest that points outside the partition", func(t *testing.T, backup string) {
			path := filepath.Join(backup, storage.BackupManifestName)
			manifest, err := storage.ReadBackupManifest(backup)
			if err != nil {
				t.Fatal(err)
			}
			manifest.Partitions[0].Files[0].Path = "../../outside"
			data, _ := json.Marshal(manifest)
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
		}, "outside partition"},
		{"no manifest", func(t *testing.T, backup string) {
			if err := os.Remove(filepath.Join(backup, storage.BackupManifestName)); err != nil {
				t.Fatal(err)
			}
		}, "read backup manifest"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backup := copyBackup(t)
			tc.damage(t, backup)
			target := t.TempDir()
			_, err := storage.RestoreBackup(backup, target, storage.RestoreOptions{})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("restore error = %v, want one containing %q", err, tc.want)
			}
			// Partition 0 was intact in every case, and still must not appear:
			// a restore is all of the backup or nothing.
			left, _ := filepath.Glob(filepath.Join(target, "partitions", "*"))
			if len(left) != 0 {
				t.Fatalf("failed restore left %v behind", left)
			}
		})
	}

	t.Run("a data directory that already holds a partition", func(t *testing.T) {
		target := t.TempDir()
		existing := filepath.Join(target, "partitions", "1", "segments")
		if err := os.MkdirAll(existing, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(existing, "00000000000000000000.log"), []byte("live data"), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := storage.RestoreBackup(good, target, storage.RestoreOptions{})
		if err == nil || !strings.Contains(err.Error(), "already holds data") {
			t.Fatalf("restore over existing data: %v", err)
		}
		if data, _ := os.ReadFile(filepath.Join(existing, "00000000000000000000.log")); string(data) != "live data" {
			t.Fatal("restore touched the existing partition")
		}
		if _, err := os.Stat(filepath.Join(target, "partitions", "0")); !os.IsNotExist(err) {
			t.Fatal("restore wrote partition 0 although it refused the backup")
		}
	})

	// The undamaged backup still restores.
	if _, err := storage.RestoreBackup(good, t.TempDir(), storage.RestoreOptions{}); err != nil {
		t.Fatalf("restore of the good backup: %v", err)
	}
}

// A backup of an encrypted log says which key it needs without containing it.
// Restore can check a key against that before it touches the data directory:
// the wrong key is refused there, not found out when the node first reads.
func TestBackupOfEncryptedLogIdentifiesItsKey(t *testing.T) {
	writeKey := func(text string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "master.key")
		if err := os.WriteFile(path, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	const keyText = "0123456789abcdef0123456789abcdef"
	keyFile, wrongKeyFile := writeKey(keyText), writeKey("ffffffffffffffffffffffffffffffff")

	start := func(dataDir string) *PartitionManager {
		t.Helper()
		cfg := backupTestConfig(dataDir)
		cfg.PartitionCount = 1
		cfg.EncryptionEnabled, cfg.EncryptionKeyFile = true, keyFile
		pm := NewPartitionManager("node-1", cfg)
		t.Cleanup(func() { pm.Close() })
		if err := pm.CreatePartition(0, "orders"); err != nil {
			t.Fatal(err)
		}
		if err := pm.StartPartition(0); err != nil {
			t.Fatal(err)
		}
		return pm
	}
	source := start(t.TempDir())
	p, _ := source.GetInternalPartition(0)
	want := populate(t, p)

	backup := filepath.Join(t.TempDir(), "backup")
	if err := source.Backup(backup); err != nil {
		t.Fatalf("backup: %v", err)
	}
	manifest, err := storage.ReadBackupManifest(backup)
	if err != nil {
		t.Fatal(err)
	}
	key, err := storage.LoadMasterKey(keyFile)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Encryption == nil || manifest.Encryption.KeyCheck != storage.KeyCheckValue(key) {
		t.Fatalf("manifest of an encrypted backup identifies its key as %+v", manifest.Encryption)
	}
	if manifest.CutAt.IsZero() || manifest.CutAt.Before(manifest.Created) {
		t.Fatalf("manifest says the logs were cut at %v, the backup having started at %v", manifest.CutAt, manifest.Created)
	}
	// Nothing in the backup holds the key or the payloads in the clear.
	err = filepath.Walk(backup, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(data), keyText) {
			t.Errorf("%s contains the encryption key", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	target := t.TempDir()
	if _, err := storage.RestoreBackup(backup, target, storage.RestoreOptions{EncryptionKeyFile: wrongKeyFile}); err == nil || !strings.Contains(err.Error(), "not the key") {
		t.Fatalf("restore with the wrong key: %v", err)
	}
	if entries, _ := os.ReadDir(target); len(entries) != 0 {
		t.Fatalf("a restore that was refused left %d entries in the data directory", len(entries))
	}
	if _, err := storage.RestoreBackup(backup, target, storage.RestoreOptions{EncryptionKeyFile: keyFile}); err != nil {
		t.Fatalf("restore with the right key: %v", err)
	}
	restored := start(target)
	rp, _ := restored.GetInternalPartition(0)
	got, err := rp.Wal.ReadEvents(0, rp.Wal.GetLastOffset())
	if err != nil || len(got) != len(want) {
		t.Fatalf("restored log has %d readable events, want %d (err=%v)", len(got), len(want), err)
	}
	for i := range want {
		if got[i].MessageId != want[i].MessageId || string(got[i].Payload) != string(want[i].Payload) {
			t.Fatalf("restored event %d is %q, want %q", i, got[i].MessageId, want[i].MessageId)
		}
	}

	// A key offered for a backup that is not encrypted is a mistake worth
	// stopping for: it is probably the wrong backup.
	plain := startBackupTestNode(t, t.TempDir())
	plainBackup := filepath.Join(t.TempDir(), "backup")
	if err := plain.Backup(plainBackup); err != nil {
		t.Fatal(err)
	}
	if plainManifest, err := storage.ReadBackupManifest(plainBackup); err != nil || plainManifest.Encryption != nil {
		t.Fatalf("manifest of an unencrypted backup names a key: %+v (err=%v)", plainManifest, err)
	}
	if _, err := storage.RestoreBackup(plainBackup, t.TempDir(), storage.RestoreOptions{EncryptionKeyFile: keyFile}); err == nil || !strings.Contains(err.Error(), "not encrypted") {
		t.Fatalf("restore of an unencrypted backup with a key: %v", err)
	}
}

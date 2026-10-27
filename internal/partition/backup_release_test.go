package partition

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
)

// A scheduled backup runs while the cluster moves partitions and while the
// node shuts down. Pebble panics when a closed database is used, which took the
// whole node down, so a backup must either finish with a partition's stores
// open or leave that partition out.

func backedUpPartitions(t *testing.T, backup string) []int32 {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(backup, storage.BackupManifestName))
	if err != nil {
		t.Fatal(err)
	}
	var manifest storage.BackupManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	ids := make([]int32, 0, len(manifest.Partitions))
	for _, p := range manifest.Partitions {
		ids = append(ids, p.PartitionID)
	}
	slices.Sort(ids)
	return ids
}

// A partition that was stopped is still listed by the manager, as it is while
// the node shuts down. Its stores are closed, so the backup leaves it out, and
// what it does hold restores.
func TestBackupLeavesOutAPartitionThatWasStopped(t *testing.T) {
	pm := startBackupTestNode(t, t.TempDir())
	for _, p := range pm.ListPartitions() {
		populate(t, p)
	}
	if err := pm.StopPartition(1); err != nil {
		t.Fatal(err)
	}

	backup := filepath.Join(t.TempDir(), "backup")
	if err := pm.Backup(backup); err != nil {
		t.Fatalf("backup with a stopped partition: %v", err)
	}
	if got := backedUpPartitions(t, backup); !slices.Equal(got, []int32{0}) {
		t.Fatalf("backed-up partitions = %v, want [0]", got)
	}
	restored := t.TempDir()
	if _, err := storage.RestoreBackup(backup, restored, storage.RestoreOptions{}); err != nil {
		t.Fatalf("restore: %v", err)
	}

	if err := pm.StopPartition(0); err != nil {
		t.Fatal(err)
	}
	if err := pm.Backup(filepath.Join(t.TempDir(), "none")); err == nil {
		t.Fatal("a backup of a node whose partitions are all stopped succeeded")
	}
}

// A partition that is released while a backup is using it stays loaded until
// the backup is done with it, and the backup keeps it.
func TestReleaseWaitsForABackupThatIsUsingThePartition(t *testing.T) {
	pm := startBackupTestNode(t, t.TempDir())
	for _, p := range pm.ListPartitions() {
		populate(t, p)
	}
	first, _ := pm.GetInternalPartition(0)
	second, _ := pm.GetInternalPartition(1)

	// The backup takes the partitions in order. Holding the second one's lock
	// stops it there, after it has listed both and taken the first.
	second.ReplicateMu.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			second.ReplicateMu.Unlock()
		}
	}()
	backup := filepath.Join(t.TempDir(), "backup")
	backupDone := make(chan error, 1)
	go func() { backupDone <- pm.Backup(backup) }()
	deadline := time.Now().Add(10 * time.Second)
	for first.ReplicateMu.TryLock() {
		first.ReplicateMu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("the backup did not start")
		}
		time.Sleep(time.Millisecond)
	}

	releaseDone := make(chan error, 1)
	go func() { releaseDone <- pm.ReleasePartition(1) }()
	select {
	case err := <-releaseDone:
		t.Fatalf("partition 1 was released while a backup was using it (err=%v)", err)
	case <-time.After(300 * time.Millisecond):
	}

	second.ReplicateMu.Unlock()
	unlocked = true
	if err := <-backupDone; err != nil {
		t.Fatalf("backup: %v", err)
	}
	if err := <-releaseDone; err != nil {
		t.Fatalf("release: %v", err)
	}
	if got := backedUpPartitions(t, backup); !slices.Equal(got, []int32{0, 1}) {
		t.Fatalf("backed-up partitions = %v, want [0 1]", got)
	}
	if loaded(pm, 1) {
		t.Fatal("partition 1 is still loaded after its release")
	}
}

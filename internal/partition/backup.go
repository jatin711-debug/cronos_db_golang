package partition

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
)

// Backup writes the durable state of every loaded partition to dest: the event
// log including its active segment, consumer groups with their offsets and
// completion records, the dedup store, the dead-letter queue and the fencing
// epoch. storage.RestoreBackup puts it back into a fresh data directory.
//
// The node keeps serving while the backup is taken. The logs of all partitions
// are cut at one instant, recorded in the manifest: each backed-up log holds
// exactly the events appended before it, so the backup is one point in time
// across partitions. The other stores are captured just before that cut and
// therefore describe a slightly older moment than the logs, never a newer one.
// A restored node may then deliver an event again that had been completed just
// before the backup, which at-least-once delivery allows, but it cannot hold a
// completion or a dedup record for an offset its log does not contain, which
// would silently drop whatever event is given that offset next. The
// dead-letter queue is copied after the consumer state for the same reason: a
// dead-lettered event is also recorded as completed, and must not be completed
// yet missing from the queue.
//
// A publish is appended to one partition at a time, so a batch that spans
// partitions can be in the backup for some of them and not for others.
//
// A backup holds no key material. For an encrypted log the manifest records a
// value that identifies the master key, and restore can check the key against
// it. Credentials and TLS material are deployment configuration and are not
// part of a backup either.
func (pm *PartitionManager) Backup(dest string) error {
	// The manager lock is not held while files are copied: a writer waiting
	// for it would block every publish behind the backup.
	pm.mu.RLock()
	partitions := make([]*Partition, 0, len(pm.partitions))
	for _, p := range pm.partitions {
		partitions = append(partitions, p)
	}
	pm.mu.RUnlock()
	sort.Slice(partitions, func(i, j int) bool { return partitions[i].ID < partitions[j].ID })
	if len(partitions) == 0 {
		return fmt.Errorf("no loaded partitions to back up")
	}

	manifest := storage.BackupManifest{Version: storage.BackupVersionPartition, Scope: "partition", Created: time.Now().UTC(), NodeID: pm.nodeID}
	if pm.config.EncryptionEnabled && pm.config.EncryptionKeyFile != "" {
		key, err := storage.LoadMasterKey(pm.config.EncryptionKeyFile)
		if err != nil {
			return fmt.Errorf("identify encryption key: %w", err)
		}
		manifest.Encryption = &storage.BackupEncryption{KeyCheck: storage.KeyCheckValue(key)}
	}

	dirs := make([]string, len(partitions))
	wals := make([]*storage.WAL, len(partitions))
	for i, p := range partitions {
		dirs[i] = filepath.Join(dest, "partitions", fmt.Sprint(p.ID))
		wals[i] = p.Wal
		entry, err := p.backupStores(dirs[i])
		if err != nil {
			return fmt.Errorf("back up partition %d: %w", p.ID, err)
		}
		manifest.Partitions = append(manifest.Partitions, entry)
	}

	cuts, err := storage.CutCheckpoints(wals)
	if err != nil {
		return fmt.Errorf("cut partition logs: %w", err)
	}
	manifest.CutAt = time.Now().UTC()
	for i, cut := range cuts {
		entry := &manifest.Partitions[i]
		if _, entry.LastOffset, err = cut.CopyTo(dirs[i]); err == nil {
			entry.Files, err = storage.ListBackupFiles(dirs[i])
		}
		if err != nil {
			for _, rest := range cuts[i:] {
				rest.Release()
			}
			return fmt.Errorf("back up log of partition %d: %w", entry.PartitionID, err)
		}
	}

	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	if err := utils.AtomicWriteFile(filepath.Join(dest, storage.BackupManifestName), data, 0600); err != nil {
		return err
	}
	return storage.SyncDirectory(filepath.Join(dest, "partitions"))
}

// backupStores captures everything of this partition except its log into dir,
// laid out like the partition's data directory.
func (p *Partition) backupStores(dir string) (storage.BackupPartition, error) {
	entry := storage.BackupPartition{PartitionID: p.ID, LastOffset: -1}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return entry, err
	}
	if p.DedupStore != nil {
		if err := p.DedupStore.Checkpoint(filepath.Join(dir, fmt.Sprintf("dedup_%d", p.ID))); err != nil {
			return entry, fmt.Errorf("dedup store: %w", err)
		}
		entry.Components = append(entry.Components, "dedup")
	}
	if p.ConsumerGroup != nil {
		if err := p.ConsumerGroup.CheckpointOffsetStore(filepath.Join(dir, "consumer_offsets")); err != nil {
			return entry, fmt.Errorf("consumer state: %w", err)
		}
		entry.Components = append(entry.Components, "consumer_offsets")
	}
	if p.DLQ != nil {
		if err := p.DLQ.Checkpoint(filepath.Join(dir, "dlq")); err != nil {
			return entry, fmt.Errorf("dead-letter queue: %w", err)
		}
		entry.Components = append(entry.Components, "dlq")
	}
	switch err := copyIfPresent(filepath.Join(p.DataDir, "epoch.json"), filepath.Join(dir, "epoch.json")); {
	case err == nil:
		entry.Components = append(entry.Components, "epoch")
	case !os.IsNotExist(err):
		return entry, fmt.Errorf("epoch: %w", err)
	}
	return entry, nil
}

// copyIfPresent copies a small file and syncs the copy. It returns the error
// from opening src unchanged, so a missing file can be told apart.
func copyIfPresent(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	if copyErr == nil {
		copyErr = out.Sync()
	}
	if closeErr := out.Close(); copyErr == nil {
		copyErr = closeErr
	}
	return copyErr
}

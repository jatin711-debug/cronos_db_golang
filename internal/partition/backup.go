package partition

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
)

// BackupWALs checkpoints every loaded partition, including its active segment.
// Each partition is consistent independently; this is an event-log backup, not
// a cluster-wide transaction or a backup of consumer/Raft/credential state.
// Restore each partitions/<id> directory with storage.RestoreWAL into fresh
// storage, supplying the original encryption key if encryption was enabled.
func (pm *PartitionManager) BackupWALs(dest string) error {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	ids := make([]int32, 0, len(pm.partitions))
	for id := range pm.partitions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) == 0 {
		return fmt.Errorf("no loaded partitions to back up")
	}
	type entry struct {
		PartitionID int32 `json:"partition_id"`
		LastOffset  int64 `json:"last_offset"`
	}
	manifest := struct {
		Version    int       `json:"version"`
		Scope      string    `json:"scope"`
		Created    time.Time `json:"created"`
		Partitions []entry   `json:"partitions"`
	}{Version: 1, Scope: "partition-wal", Created: time.Now().UTC()}
	for _, id := range ids {
		p := pm.partitions[id]
		_, last, err := p.Wal.Checkpoint(filepath.Join(dest, "partitions", fmt.Sprint(id)))
		if err != nil {
			return fmt.Errorf("checkpoint partition %d: %w", id, err)
		}
		manifest.Partitions = append(manifest.Partitions, entry{id, last})
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	if err := utils.AtomicWriteFile(filepath.Join(dest, "backup.json"), data, 0600); err != nil {
		return err
	}
	return storage.SyncDirectory(filepath.Join(dest, "partitions"))
}

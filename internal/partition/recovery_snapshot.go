package partition

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/jatin711-debug/cronos_db_golang/internal/recoverystate"
	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// WriteRecoverySnapshot captures progress before dead letters, under the same
// append lock as the WAL checkpoint. Every captured completion thus has its
// log entry and any preceding durable dead letter in this generation.
func (p *Partition) WriteRecoverySnapshot(path string, last int64) error {
	accepted := min(p.AcceptedThrough(), last)
	if p.Replicated() && !p.IsLeader() {
		accepted = -1
	}
	m := recoverystate.Manifest{Partition: p.ID, Epoch: p.Epoch(), First: p.Wal.GetFirstOffset(), Last: last, AcceptedThrough: accepted}
	return recoverystate.Write(path, m, p.Wal.SnapshotCipher(), func(emit func(*types.ReplicationProgressRequest) error) error {
		cursor := types.ProgressCursor{}
		for {
			groups, next, done, err := p.ConsumerGroup.ExportProgressPage(p.ID, cursor)
			if err != nil {
				return err
			}
			for _, g := range groups {
				g.CommittedOffset = min(g.CommittedOffset, accepted+1)
				kept := g.CompletedOffsets[:0]
				for _, offset := range g.CompletedOffsets {
					if offset <= accepted {
						kept = append(kept, offset)
					}
				}
				g.CompletedOffsets = kept
			}
			if len(groups) > 0 {
				if err := emit(&types.ReplicationProgressRequest{PartitionId: p.ID, Term: m.Epoch, Groups: groups}); err != nil {
					return err
				}
			}
			if done {
				break
			}
			cursor = next
		}
		dlqCursor := types.DeadLetterCursor{}
		for {
			records, next, done, err := p.DLQ.ExportPage(dlqCursor)
			if err != nil {
				return err
			}
			if len(records) > 0 {
				if err := emit(&types.ReplicationProgressRequest{PartitionId: p.ID, Term: m.Epoch, DeadLetters: records}); err != nil {
					return err
				}
			}
			if done {
				return nil
			}
			dlqCursor = next
		}
	})
}

// applyRecoverySnapshot is called before the partition serves work. The file
// moves with segments in the WAL's journaled install, so restart selects the
// old WAL/state or the new WAL/state together. It stays until all state is
// durable; replaying a partially applied generation is safe.
func (p *Partition) applyRecoverySnapshot() (retErr error) {
	path := filepath.Join(p.DataDir, "segments", recoverystate.Filename)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			p.Wal.Fail(retErr)
		}
	}()
	m, err := recoverystate.Read(path, p.Wal.SnapshotCipher(), nil)
	if err != nil {
		return err
	}
	if m.Partition != p.ID || m.First != p.Wal.GetFirstOffset() || m.Last != p.Wal.GetLastOffset() {
		return fmt.Errorf("recovery state does not match installed WAL")
	}
	if err := p.ConsumerGroup.ResetSnapshotProgress(p.ID); err != nil {
		return err
	}
	// Dead letters must be durable before their completion records become visible.
	if _, err := recoverystate.Read(path, p.Wal.SnapshotCipher(), func(page *types.ReplicationProgressRequest) error {
		return p.DLQ.ApplyReplica(p.ID, page.DeadLetters)
	}); err != nil {
		return err
	}
	if _, err := recoverystate.Read(path, p.Wal.SnapshotCipher(), func(page *types.ReplicationProgressRequest) error {
		return p.ConsumerGroup.ApplyReplicatedProgress(p.ID, m.AcceptedThrough+1, page.Groups)
	}); err != nil {
		return err
	}
	if err := p.ConsumerGroup.SetLogStart(p.ID, m.First); err != nil {
		return err
	}
	if err := p.ConsumerGroup.SyncProgress(); err != nil {
		return err
	}
	if m.Epoch > p.Epoch() {
		if err := p.PersistEpoch(m.Epoch); err != nil {
			return err
		}
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	return storage.SyncDirectory(filepath.Dir(path))
}

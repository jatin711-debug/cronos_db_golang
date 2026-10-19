package partition

import (
	"fmt"
	"path/filepath"

	"github.com/jatin711-debug/cronos_db_golang/internal/logcontrol"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
)

func (pm *PartitionManager) SetTermStartRequirement(required func(int32) bool) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.termStartRequired = required
}

func (pm *PartitionManager) TermStartRequired(id int32) bool {
	pm.mu.RLock()
	required := pm.termStartRequired
	pm.mu.RUnlock()
	return required != nil && required(id)
}

func (p *Partition) TermStartEnabled() bool { return p.termStartMode.Load() }

// The caller serializes format changes and appends with ReplicateMu.
func (p *Partition) enableTermStart() error {
	if p.TermStartEnabled() {
		return nil
	}
	if err := utils.AtomicWriteFile(filepath.Join(p.DataDir, logcontrol.TermStartMarker), []byte("1\n"), 0600); err != nil {
		return err
	}
	// Stop legacy quorum counting before readers can observe the new mode
	// on a running leader. Its durable term-start offset is installed later.
	if leader := p.replQuorum.Load(); leader != nil {
		leader.SetTermStartFormat(logcontrol.TermStartFormat)
	}
	p.termStartMode.Store(true)
	return nil
}

// EnableReplicatedTermStart is also used before installing a versioned snapshot.
// The caller has checked the cluster requirement and holds p.ReplicateMu.
func (p *Partition) EnableReplicatedTermStart() error { return p.enableTermStart() }

// ensureTermStart appends once per local leadership term, before exposing that
// term to publishers. Followers fsync this record too; catch-up sends it even
// on an idle partition. The caller holds ReplicateMu and the manager lock.
func (p *Partition) ensureTermStart(epoch int64) error {
	if !p.TermStartEnabled() || p.ReplLeader == nil {
		return nil
	}
	p.ReplLeader.SetTermStartFormat(logcontrol.TermStartFormat)
	if p.termStartEpoch == epoch {
		p.ReplLeader.SetTermStartOffset(p.termStartOffset, epoch)
		return nil
	}
	// Ordinary periodic publishes may still be buffered when this running
	// partition first switches formats. Make the tail visible for validation.
	if err := p.Wal.Flush(); err != nil {
		p.Wal.Fail(err)
		return err
	}
	if last := p.Wal.GetLastOffset(); last >= p.Wal.GetFirstOffset() {
		term, err := p.Wal.GetTermForOffset(last)
		if err != nil {
			return err
		}
		if term > epoch {
			return fmt.Errorf("term %d cannot lead a log containing term %d", epoch, term)
		}
	}
	e := logcontrol.NewTermStart(p.ID)
	if err := p.Wal.AppendEvent(e); err != nil {
		return err
	}
	if err := p.Wal.Flush(); err != nil {
		p.Wal.Fail(err)
		return err
	}
	p.termStartEpoch = epoch
	p.termStartOffset = e.Offset
	p.ReplLeader.SetTermStartOffset(e.Offset, epoch)
	return nil
}

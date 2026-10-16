package dedup

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
)

// RecoveryPoint identifies a log entry whose preceding IDs have been flushed
// to stable dedup storage. Term and ID prevent reuse after log replacement.
type RecoveryPoint struct {
	Offset    int64  `json:"offset"`
	Term      int64  `json:"term"`
	MessageID string `json:"message_id"`
}

type recoveryStore interface {
	LoadRecoveryPoint() (*RecoveryPoint, error)
	SaveRecoveryPoint(RecoveryPoint) error
}

func (m *Manager) LoadRecoveryPoint() (*RecoveryPoint, error) {
	if store, ok := m.store.(recoveryStore); ok {
		return store.LoadRecoveryPoint()
	}
	return nil, nil
}

func (m *Manager) SaveRecoveryPoint(point RecoveryPoint) error {
	if store, ok := m.store.(recoveryStore); ok {
		return store.SaveRecoveryPoint(point)
	}
	return nil // memory-only stores must replay the whole retained log
}

func (p *PebbleStore) LoadRecoveryPoint() (*RecoveryPoint, error) {
	data, err := os.ReadFile(filepath.Join(p.dataDir, "recovery.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var point RecoveryPoint
	if err := json.Unmarshal(data, &point); err != nil {
		return nil, err
	}
	return &point, nil
}

func (p *PebbleStore) SaveRecoveryPoint(point RecoveryPoint) error {
	if err := p.flushPending(); err != nil {
		return err
	}
	// With DisableWAL, an atomic frontier file alone is insufficient: every
	// preceding ID must first be in an sstable referenced by the manifest.
	if err := p.db.Flush(); err != nil {
		return err
	}
	data, err := json.Marshal(point)
	if err != nil {
		return err
	}
	if err := utils.AtomicWriteFile(filepath.Join(p.dataDir, "recovery.json"), data, 0600); err != nil {
		return err
	}
	return utils.SyncDirectory(p.dataDir)
}

func (s *BloomPebbleStore) LoadRecoveryPoint() (*RecoveryPoint, error) {
	return s.pebble.LoadRecoveryPoint()
}

func (s *BloomPebbleStore) SaveRecoveryPoint(point RecoveryPoint) error {
	return s.pebble.SaveRecoveryPoint(point)
}

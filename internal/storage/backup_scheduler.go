package storage

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// BackupScheduler publishes complete backup generations and expires old ones
// only after a successful replacement. Stop waits for active checkpoint work.
type BackupScheduler struct {
	walDir    string        // source partition data directory (contains segments/)
	backupDir string        // root directory for timestamped backup subdirs
	interval  time.Duration // how often to run BackupWAL
	retention time.Duration // age after which backup subdirs are deleted
	quit      chan struct{} // closed by Stop to end the loop
	backup    func(string) error
	mu        sync.Mutex
	started   bool
	stopped   bool
	wg        sync.WaitGroup
	runMu     sync.Mutex
}

// NewBackupScheduler creates a scheduler that backs up walDir into backupDir
// every interval and deletes backups older than retention.
func NewBackupScheduler(walDir, backupDir string, interval, retention time.Duration) *BackupScheduler {
	bs := NewCheckpointBackupScheduler(backupDir, interval, retention, func(dest string) error {
		return BackupWAL(walDir, dest)
	})
	bs.walDir = walDir
	return bs
}

// NewCheckpointBackupScheduler schedules independently restorable checkpoints.
// backup must finish and sync all files before returning successfully.
func NewCheckpointBackupScheduler(backupDir string, interval, retention time.Duration, backup func(string) error) *BackupScheduler {
	return &BackupScheduler{
		backupDir: backupDir,
		interval:  interval,
		retention: retention,
		quit:      make(chan struct{}),
		backup:    backup,
	}
}

// Start begins the background backup loop.
func (bs *BackupScheduler) Start() {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if bs.started || bs.stopped {
		return
	}
	bs.started = true
	bs.wg.Add(1)
	go func() { defer bs.wg.Done(); bs.loop() }()
}

// Stop stops the backup loop.
func (bs *BackupScheduler) Stop() {
	bs.mu.Lock()
	if !bs.stopped {
		bs.stopped = true
		close(bs.quit)
	}
	bs.mu.Unlock()
	bs.wg.Wait()
}

func (bs *BackupScheduler) loop() {
	ticker := time.NewTicker(bs.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := bs.runBackup(); err != nil {
				slog.Error("Scheduled backup failed", "error", err)
				continue // Never expire the last good backup after a failed attempt.
			}
			if err := bs.purgeOldBackups(); err != nil {
				slog.Error("Backup purge failed", "error", err)
			}
		case <-bs.quit:
			return
		}
	}
}

func (bs *BackupScheduler) runBackup() error {
	bs.runMu.Lock()
	defer bs.runMu.Unlock()
	if err := os.MkdirAll(bs.backupDir, 0700); err != nil {
		return fmt.Errorf("create backup dir: %w", err)
	}
	staging, err := os.MkdirTemp(bs.backupDir, ".incomplete-")
	if err != nil {
		return err
	}
	// Failed generations cannot replace a good one or accumulate partial copies.
	defer os.RemoveAll(staging)
	if err := bs.backup(staging); err != nil {
		return err
	}
	if err := SyncDirectory(staging); err != nil {
		return err
	}
	dest := filepath.Join(bs.backupDir, "backup-"+strings.TrimPrefix(filepath.Base(staging), ".incomplete-"))
	if err := os.Rename(staging, dest); err != nil {
		return fmt.Errorf("publish backup: %w", err)
	}
	if err := SyncDirectory(bs.backupDir); err != nil {
		return err
	}
	slog.Info("Scheduled WAL backup completed", "destination", dest)
	return nil
}

func (bs *BackupScheduler) purgeOldBackups() error {
	if bs.retention <= 0 {
		return nil
	}
	entries, err := os.ReadDir(bs.backupDir)
	if err != nil {
		return err
	}
	cutoff := time.Now().Add(-bs.retention)
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "backup-") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			path := filepath.Join(bs.backupDir, entry.Name())
			if err := os.RemoveAll(path); err != nil {
				slog.Warn("Failed to purge old backup", "path", path, "error", err)
			} else {
				slog.Info("Purged old backup", "path", path)
			}
		}
	}
	return nil
}

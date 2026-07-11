package storage

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// BackupScheduler periodically backs up closed WAL segments.
type BackupScheduler struct {
	walDir    string
	backupDir string
	interval  time.Duration
	retention time.Duration
	quit      chan struct{}
}

// NewBackupScheduler creates a backup scheduler.
func NewBackupScheduler(walDir, backupDir string, interval, retention time.Duration) *BackupScheduler {
	return &BackupScheduler{
		walDir:    walDir,
		backupDir: backupDir,
		interval:  interval,
		retention: retention,
		quit:      make(chan struct{}),
	}
}

// Start begins the background backup loop.
func (bs *BackupScheduler) Start() {
	go bs.loop()
}

// Stop stops the backup loop.
func (bs *BackupScheduler) Stop() {
	close(bs.quit)
}

func (bs *BackupScheduler) loop() {
	ticker := time.NewTicker(bs.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := bs.runBackup(); err != nil {
				slog.Error("Scheduled backup failed", "error", err)
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
	dest := filepath.Join(bs.backupDir, time.Now().UTC().Format("20060102_150405"))
	if err := os.MkdirAll(dest, 0750); err != nil {
		return fmt.Errorf("create backup dir: %w", err)
	}
	if err := BackupWAL(bs.walDir, dest); err != nil {
		return err
	}
	slog.Info("Scheduled WAL backup completed", "destination", dest)
	return nil
}

func (bs *BackupScheduler) purgeOldBackups() error {
	entries, err := os.ReadDir(bs.backupDir)
	if err != nil {
		return err
	}
	cutoff := time.Now().Add(-bs.retention)
	for _, entry := range entries {
		if !entry.IsDir() {
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

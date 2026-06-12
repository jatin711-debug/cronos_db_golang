package compliance

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewEnforcer(t *testing.T) {
	tmpDir := t.TempDir()
	policy := RetentionPolicy{
		MaxAge:       24 * time.Hour,
		MaxSizeBytes: 1024 * 1024,
	}
	e := NewEnforcer(tmpDir, policy)
	if e == nil {
		t.Fatal("NewEnforcer should not return nil")
	}
	if e.dataDir != tmpDir {
		t.Errorf("expected dataDir %s, got %s", tmpDir, e.dataDir)
	}
}

func TestEnforcer_Run_NoPolicy(t *testing.T) {
	tmpDir := t.TempDir()
	segmentsDir := filepath.Join(tmpDir, "segments")
	os.MkdirAll(segmentsDir, 0755)
	segmentFile := filepath.Join(segmentsDir, "00000000000000000001.log")
	os.WriteFile(segmentFile, []byte("data"), 0644)

	e := NewEnforcer(tmpDir, RetentionPolicy{})
	if err := e.Run(context.Background()); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// File should still exist
	if _, err := os.Stat(segmentFile); os.IsNotExist(err) {
		t.Error("file should still exist with no policy")
	}
}

func TestEnforcer_Run_Age(t *testing.T) {
	tmpDir := t.TempDir()
	segmentsDir := filepath.Join(tmpDir, "segments")
	os.MkdirAll(segmentsDir, 0755)
	oldFile := filepath.Join(segmentsDir, "00000000000000000001.log")
	newFile := filepath.Join(segmentsDir, "00000000000000000002.log")

	os.WriteFile(oldFile, []byte("old"), 0644)
	os.WriteFile(newFile, []byte("new"), 0644)

	// Make old file actually old
	oldTime := time.Now().Add(-48 * time.Hour)
	os.Chtimes(oldFile, oldTime, oldTime)

	policy := RetentionPolicy{MaxAge: 24 * time.Hour}
	e := NewEnforcer(tmpDir, policy)
	if err := e.Run(context.Background()); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if _, err := os.Stat(oldFile); !os.IsNotExist(err) {
		t.Error("old file should be deleted")
	}
	if _, err := os.Stat(newFile); os.IsNotExist(err) {
		t.Error("new file should still exist")
	}
}

func TestEnforcer_Run_Age_ContextCancel(t *testing.T) {
	tmpDir := t.TempDir()
	segmentsDir := filepath.Join(tmpDir, "segments")
	os.MkdirAll(segmentsDir, 0755)
	oldFile := filepath.Join(segmentsDir, "00000000000000000001.log")
	os.WriteFile(oldFile, []byte("old"), 0644)
	oldTime := time.Now().Add(-48 * time.Hour)
	os.Chtimes(oldFile, oldTime, oldTime)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancel

	policy := RetentionPolicy{MaxAge: 24 * time.Hour}
	e := NewEnforcer(tmpDir, policy)
	err := e.Run(ctx)
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

func TestEnforcer_Run_Size(t *testing.T) {
	tmpDir := t.TempDir()
	segmentsDir := filepath.Join(tmpDir, "segments")
	os.MkdirAll(segmentsDir, 0755)
	file1 := filepath.Join(segmentsDir, "00000000000000000001.log")
	file2 := filepath.Join(segmentsDir, "00000000000000000002.log")

	// Write files with known sizes
	os.WriteFile(file1, make([]byte, 100), 0644)
	os.WriteFile(file2, make([]byte, 100), 0644)

	// Make file1 older
	oldTime := time.Now().Add(-2 * time.Hour)
	os.Chtimes(file1, oldTime, oldTime)

	policy := RetentionPolicy{MaxSizeBytes: 150}
	e := NewEnforcer(tmpDir, policy)
	if err := e.Run(context.Background()); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Oldest file should be deleted to get under 150 bytes
	if _, err := os.Stat(file1); !os.IsNotExist(err) {
		t.Error("oldest file should be deleted")
	}
	if _, err := os.Stat(file2); os.IsNotExist(err) {
		t.Error("newest file should still exist")
	}
}

func TestEnforcer_Run_Size_UnderLimit(t *testing.T) {
	tmpDir := t.TempDir()
	segmentsDir := filepath.Join(tmpDir, "segments")
	os.MkdirAll(segmentsDir, 0755)
	smallFile := filepath.Join(segmentsDir, "00000000000000000001.log")
	os.WriteFile(smallFile, []byte("x"), 0644)

	policy := RetentionPolicy{MaxSizeBytes: 10000}
	e := NewEnforcer(tmpDir, policy)
	if err := e.Run(context.Background()); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if _, err := os.Stat(smallFile); os.IsNotExist(err) {
		t.Error("file should still exist when under limit")
	}
}

func TestEnforcer_Run_Size_ContextCancel(t *testing.T) {
	tmpDir := t.TempDir()
	segmentsDir := filepath.Join(tmpDir, "segments")
	os.MkdirAll(segmentsDir, 0755)
	file1 := filepath.Join(segmentsDir, "00000000000000000001.log")
	file2 := filepath.Join(segmentsDir, "00000000000000000002.log")
	os.WriteFile(file1, make([]byte, 100), 0644)
	os.WriteFile(file2, make([]byte, 100), 0644)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	policy := RetentionPolicy{MaxSizeBytes: 1}
	e := NewEnforcer(tmpDir, policy)
	err := e.Run(ctx)
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

func TestEnforcer_Run_BothPolicies(t *testing.T) {
	tmpDir := t.TempDir()
	segmentsDir := filepath.Join(tmpDir, "segments")
	os.MkdirAll(segmentsDir, 0755)
	oldFile := filepath.Join(segmentsDir, "00000000000000000001.log")
	bigFile := filepath.Join(segmentsDir, "00000000000000000002.log")

	os.WriteFile(oldFile, []byte("old"), 0644)
	os.WriteFile(bigFile, make([]byte, 200), 0644)

	oldTime := time.Now().Add(-48 * time.Hour)
	os.Chtimes(oldFile, oldTime, oldTime)

	policy := RetentionPolicy{
		MaxAge:       24 * time.Hour,
		MaxSizeBytes: 150,
	}
	e := NewEnforcer(tmpDir, policy)
	if err := e.Run(context.Background()); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if _, err := os.Stat(oldFile); !os.IsNotExist(err) {
		t.Error("old file should be deleted by age policy")
	}
	if _, err := os.Stat(bigFile); !os.IsNotExist(err) {
		t.Error("big file should be deleted by size policy")
	}
}

func TestEnforcer_enforceAge_SkipDirs(t *testing.T) {
	tmpDir := t.TempDir()
	subDir := filepath.Join(tmpDir, "partition-1")
	segmentsDir := filepath.Join(subDir, "segments")
	os.MkdirAll(segmentsDir, 0755)
	oldFile := filepath.Join(segmentsDir, "00000000000000000001.log")
	os.WriteFile(oldFile, []byte("old"), 0644)
	oldTime := time.Now().Add(-48 * time.Hour)
	os.Chtimes(oldFile, oldTime, oldTime)

	policy := RetentionPolicy{MaxAge: 24 * time.Hour}
	e := NewEnforcer(tmpDir, policy)
	if err := e.Run(context.Background()); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if _, err := os.Stat(oldFile); !os.IsNotExist(err) {
		t.Error("old file in subdir should be deleted")
	}
}

func TestEnforcer_enforceAge_Unreadable(t *testing.T) {
	tmpDir := t.TempDir()
	policy := RetentionPolicy{MaxAge: 24 * time.Hour}
	e := NewEnforcer(tmpDir, policy)
	// Running on empty dir should not error
	if err := e.Run(context.Background()); err != nil {
		t.Fatalf("Run failed on empty dir: %v", err)
	}
}

func TestEnforcer_SafeExclusions(t *testing.T) {
	tmpDir := t.TempDir()

	// 1. Create protected directories
	raftDir := filepath.Join(tmpDir, "raft")
	pebbleDir := filepath.Join(tmpDir, "dedup")
	indexDir := filepath.Join(tmpDir, "index")
	os.MkdirAll(raftDir, 0755)
	os.MkdirAll(pebbleDir, 0755)
	os.MkdirAll(indexDir, 0755)

	// 2. Create actual WAL segments inside segments directory
	segmentsDir := filepath.Join(tmpDir, "segments")
	os.MkdirAll(segmentsDir, 0755)
	logFile := filepath.Join(segmentsDir, "00000000000000000001.log")
	os.WriteFile(logFile, make([]byte, 100), 0644)

	// 3. Create critical files in root and protected dirs
	raftDb := filepath.Join(raftDir, "raft.db")
	sstFile := filepath.Join(pebbleDir, "000012.sst")
	idxFile := filepath.Join(indexDir, "00000000000000000001.index")
	txLog := filepath.Join(tmpDir, "tx_log.json")
	checkpoint := filepath.Join(tmpDir, "timer_replay_checkpoint.json")

	os.WriteFile(raftDb, []byte("raft"), 0644)
	os.WriteFile(sstFile, []byte("pebble sst"), 0644)
	os.WriteFile(idxFile, []byte("index"), 0644)
	os.WriteFile(txLog, []byte("tx log"), 0644)
	os.WriteFile(checkpoint, []byte("checkpoint"), 0644)

	// Back-date ALL files (so both age and size policies would trigger on them)
	oldTime := time.Now().Add(-100 * time.Hour)
	os.Chtimes(logFile, oldTime, oldTime)
	os.Chtimes(raftDb, oldTime, oldTime)
	os.Chtimes(sstFile, oldTime, oldTime)
	os.Chtimes(idxFile, oldTime, oldTime)
	os.Chtimes(txLog, oldTime, oldTime)
	os.Chtimes(checkpoint, oldTime, oldTime)

	// Run enforcer with both policies to trigger complete deletion
	policy := RetentionPolicy{
		MaxAge:       24 * time.Hour,
		MaxSizeBytes: 10, // extremely small, forces size deletion
	}
	e := NewEnforcer(tmpDir, policy)
	if err := e.Run(context.Background()); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// 4. Verify results: WAL segment log should be deleted
	if _, err := os.Stat(logFile); !os.IsNotExist(err) {
		t.Error("WAL log file should be deleted")
	}

	// 5. Verify results: Protected system metadata files MUST NOT be deleted
	if _, err := os.Stat(raftDb); os.IsNotExist(err) {
		t.Error("raft.db was deleted!")
	}
	if _, err := os.Stat(sstFile); os.IsNotExist(err) {
		t.Error("pebble sst file was deleted!")
	}
	if _, err := os.Stat(idxFile); os.IsNotExist(err) {
		t.Error("index file was deleted!")
	}
	if _, err := os.Stat(txLog); os.IsNotExist(err) {
		t.Error("tx_log.json was deleted!")
	}
	if _, err := os.Stat(checkpoint); os.IsNotExist(err) {
		t.Error("timer_replay_checkpoint.json was deleted!")
	}
}

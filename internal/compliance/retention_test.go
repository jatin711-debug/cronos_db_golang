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
	// Write a file
	os.WriteFile(filepath.Join(tmpDir, "test.dat"), []byte("data"), 0644)

	e := NewEnforcer(tmpDir, RetentionPolicy{})
	if err := e.Run(context.Background()); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// File should still exist
	if _, err := os.Stat(filepath.Join(tmpDir, "test.dat")); os.IsNotExist(err) {
		t.Error("file should still exist with no policy")
	}
}

func TestEnforcer_Run_Age(t *testing.T) {
	tmpDir := t.TempDir()
	oldFile := filepath.Join(tmpDir, "old.dat")
	newFile := filepath.Join(tmpDir, "new.dat")

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
	oldFile := filepath.Join(tmpDir, "old.dat")
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
	file1 := filepath.Join(tmpDir, "1.dat")
	file2 := filepath.Join(tmpDir, "2.dat")

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
}

func TestEnforcer_Run_Size_UnderLimit(t *testing.T) {
	tmpDir := t.TempDir()
	os.WriteFile(filepath.Join(tmpDir, "small.dat"), []byte("x"), 0644)

	policy := RetentionPolicy{MaxSizeBytes: 10000}
	e := NewEnforcer(tmpDir, policy)
	if err := e.Run(context.Background()); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if _, err := os.Stat(filepath.Join(tmpDir, "small.dat")); os.IsNotExist(err) {
		t.Error("file should still exist when under limit")
	}
}

func TestEnforcer_Run_Size_ContextCancel(t *testing.T) {
	tmpDir := t.TempDir()
	os.WriteFile(filepath.Join(tmpDir, "1.dat"), make([]byte, 100), 0644)
	os.WriteFile(filepath.Join(tmpDir, "2.dat"), make([]byte, 100), 0644)

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
	oldFile := filepath.Join(tmpDir, "old.dat")
	bigFile := filepath.Join(tmpDir, "big.dat")

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
	subDir := filepath.Join(tmpDir, "subdir")
	os.Mkdir(subDir, 0755)
	oldFile := filepath.Join(subDir, "old.dat")
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

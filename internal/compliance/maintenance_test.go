package compliance

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMaintenanceAgeAndSizeDoNotDoubleCountDeletedFiles(t *testing.T) {
	dir := t.TempDir()
	segments := filepath.Join(dir, "segments")
	if err := os.Mkdir(segments, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"00000000000000000000.log", "00000000000000000001.log", "00000000000000000002.log"} {
		if err := os.WriteFile(filepath.Join(segments, name), make([]byte, 100), 0600); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(filepath.Join(segments, "00000000000000000000.log"), old, old); err != nil {
		t.Fatal(err)
	}
	e := NewEnforcer(dir, RetentionPolicy{MaxAge: 24 * time.Hour, MaxSizeBytes: 200})
	stats, err := e.RunWithStats(context.Background())
	if err != nil || stats.SegmentsDeleted != 1 || stats.BytesFreed != 100 {
		t.Fatalf("retention accounting: %+v %v", stats, err)
	}
	if _, err := os.Stat(filepath.Join(segments, "00000000000000000001.log")); err != nil {
		t.Fatalf("unnecessary deletion: %v", err)
	}
}

func TestMaintenanceManagedRetentionCannotBypassOwner(t *testing.T) {
	dir := t.TempDir()
	segments := filepath.Join(dir, "segments")
	if err := os.Mkdir(segments, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"00000000000000000000.log", "00000000000000000001.log"} {
		if err := os.WriteFile(filepath.Join(segments, name), make([]byte, 100), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, remove := range []func(context.Context, string) (bool, error){nil, func(context.Context, string) (bool, error) { return false, nil }} {
		e := NewManagedEnforcer(dir, RetentionPolicy{MaxSizeBytes: 1}, remove)
		e.Run(context.Background())
		files, _ := os.ReadDir(segments)
		if len(files) != 2 {
			t.Fatal("deleted segment without owner approval")
		}
	}
}

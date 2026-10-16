package dedup

import (
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestRecoveryPointSurvivesAbruptProcessExit(t *testing.T) {
	if dir := os.Getenv("CRONOS_TEST_DEDUP_CRASH_DIR"); dir != "" {
		store, err := NewBloomPebbleStore(dir, 0, 24, 1000, 0.01, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Put("durable", AppendedAt(10), time.Now().UnixMilli()); err != nil {
			t.Fatal(err)
		}
		if err := store.SaveRecoveryPoint(RecoveryPoint{Offset: 10, Term: 2, MessageID: "durable"}); err != nil {
			t.Fatal(err)
		}
		if err := store.Put("after-checkpoint", AppendedAt(11), time.Now().UnixMilli()); err != nil {
			t.Fatal(err)
		}
		os.Exit(0) // intentionally bypass every cleanup/close/flush
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestRecoveryPointSurvivesAbruptProcessExit$")
	cmd.Env = append(os.Environ(), "CRONOS_TEST_DEDUP_CRASH_DIR="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child failed: %v\n%s", err, output)
	}
	store, err := NewBloomPebbleStore(dir, 0, 24, 1000, 0.01, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	point, err := store.LoadRecoveryPoint()
	if err != nil || point == nil || point.Offset != 10 {
		t.Fatalf("point=%+v error=%v", point, err)
	}
	if offset, exists, err := store.GetOffset("durable"); err != nil || !exists || offset != AppendedAt(10) {
		t.Fatalf("checkpoint outlived its ID: offset=%d exists=%v error=%v", offset, exists, err)
	}
}

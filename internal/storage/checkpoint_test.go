package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// Small segments, so every WAL in these tests spans several files.
func checkpointTestConfig() *WALConfig {
	return &WALConfig{SegmentSizeBytes: 512, IndexInterval: 1, FsyncMode: "periodic", FlushIntervalMS: 0}
}

func appendTagged(t *testing.T, w *WAL, tag string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := w.AppendEvent(&types.Event{MessageId: fmt.Sprintf("%s-%d", tag, w.GetNextOffset()), Topic: "t", Payload: make([]byte, 64), ScheduleTs: 1}); err != nil {
			t.Fatalf("append %s event %d: %v", tag, i, err)
		}
	}
}

// buildWAL writes n events whose message IDs start with tag into a new WAL at
// dir and closes it.
func buildWAL(t *testing.T, dir, tag string, n int) {
	t.Helper()
	w, err := NewWAL(dir, 0, checkpointTestConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	appendTagged(t, w, tag, n)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// generation reports which WAL a set of events is: its tag and length. It
// fails the test if events from two WALs are mixed or an offset is missing.
func generation(t *testing.T, w *WAL) (tag string, count int) {
	t.Helper()
	last := w.GetLastOffset()
	if last < 0 {
		return "", 0
	}
	events, err := w.ReadEvents(0, last)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	for i, event := range events {
		eventTag, _, _ := strings.Cut(event.MessageId, "-")
		if event.Offset != int64(i) || event.MessageId != fmt.Sprintf("%s-%d", eventTag, i) {
			t.Fatalf("log entry %d is %q at offset %d", i, event.MessageId, event.Offset)
		}
		if tag != "" && eventTag != tag {
			t.Fatalf("log mixes generations %q and %q at offset %d", tag, eventTag, i)
		}
		tag = eventTag
	}
	return tag, len(events)
}

func mustRename(t *testing.T, from, to string) {
	t.Helper()
	if err := os.Rename(from, to); err != nil {
		t.Fatal(err)
	}
}

// Every state a crash can leave a snapshot install in must recover to one
// complete generation: the old log until the journal says "installed", the
// new one from then on. The states are built by performing the install's own
// steps on disk up to the point of the crash.
func TestSnapshotInstallRecoversFromEveryCrashPoint(t *testing.T) {
	const oldCount, newCount = 30, 45
	cases := []struct {
		name string
		// crash arranges root, which holds the old WAL, as the install would
		// have left it. fresh holds the new generation's segments and index.
		crash func(t *testing.T, root, fresh string)
		want  string
	}{
		{"journal written, nothing moved", func(t *testing.T, root, fresh string) {}, "old"},
		{"segments moved aside", func(t *testing.T, root, fresh string) {
			mustRename(t, filepath.Join(root, "segments"), filepath.Join(root, "segments.old"))
		}, "old"},
		{"both directories moved aside", func(t *testing.T, root, fresh string) {
			mustRename(t, filepath.Join(root, "segments"), filepath.Join(root, "segments.old"))
			mustRename(t, filepath.Join(root, "index"), filepath.Join(root, "index.old"))
		}, "old"},
		{"new segments in place, index not yet", func(t *testing.T, root, fresh string) {
			mustRename(t, filepath.Join(root, "segments"), filepath.Join(root, "segments.old"))
			mustRename(t, filepath.Join(root, "index"), filepath.Join(root, "index.old"))
			mustRename(t, filepath.Join(fresh, "segments"), filepath.Join(root, "segments"))
		}, "old"},
		{"both new directories in place, not yet verified", func(t *testing.T, root, fresh string) {
			mustRename(t, filepath.Join(root, "segments"), filepath.Join(root, "segments.old"))
			mustRename(t, filepath.Join(root, "index"), filepath.Join(root, "index.old"))
			mustRename(t, filepath.Join(fresh, "segments"), filepath.Join(root, "segments"))
			mustRename(t, filepath.Join(fresh, "index"), filepath.Join(root, "index"))
		}, "old"},
		{"rollback interrupted after restoring segments", func(t *testing.T, root, fresh string) {
			mustRename(t, filepath.Join(root, "index"), filepath.Join(root, "index.old"))
			mustRename(t, filepath.Join(fresh, "index"), filepath.Join(root, "index"))
		}, "old"},
		{"installed, old directories not yet removed", func(t *testing.T, root, fresh string) {
			mustRename(t, filepath.Join(root, "segments"), filepath.Join(root, "segments.old"))
			mustRename(t, filepath.Join(root, "index"), filepath.Join(root, "index.old"))
			mustRename(t, filepath.Join(fresh, "segments"), filepath.Join(root, "segments"))
			mustRename(t, filepath.Join(fresh, "index"), filepath.Join(root, "index"))
			if err := os.WriteFile(filepath.Join(root, "snapshot-install.state"), []byte("installed"), 0600); err != nil {
				t.Fatal(err)
			}
		}, "new"},
		{"installed, cleanup interrupted", func(t *testing.T, root, fresh string) {
			mustRename(t, filepath.Join(root, "index"), filepath.Join(root, "index.old"))
			if err := os.RemoveAll(filepath.Join(root, "segments")); err != nil {
				t.Fatal(err)
			}
			mustRename(t, filepath.Join(fresh, "segments"), filepath.Join(root, "segments"))
			mustRename(t, filepath.Join(fresh, "index"), filepath.Join(root, "index"))
			if err := os.WriteFile(filepath.Join(root, "snapshot-install.state"), []byte("installed"), 0600); err != nil {
				t.Fatal(err)
			}
		}, "new"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root, fresh := t.TempDir(), t.TempDir()
			buildWAL(t, root, "old", oldCount)
			buildWAL(t, fresh, "new", newCount)
			if err := os.WriteFile(filepath.Join(root, "snapshot-install.state"), []byte("prepared"), 0600); err != nil {
				t.Fatal(err)
			}
			tc.crash(t, root, fresh)

			// Recovery itself may be interrupted, so it must give the same
			// answer when it runs a second time.
			for pass := 0; pass < 2; pass++ {
				w, err := NewWAL(root, 0, checkpointTestConfig(), nil)
				if err != nil {
					t.Fatalf("pass %d: open after crash: %v", pass, err)
				}
				tag, count := generation(t, w)
				wantCount := oldCount
				if tc.want == "new" {
					wantCount = newCount
				}
				if tag != tc.want || count != wantCount {
					t.Fatalf("pass %d: recovered %d %q entries, want %d %q", pass, count, tag, wantCount, tc.want)
				}
				// The recovered log accepts appends where it ends.
				appendTagged(t, w, tc.want, 1)
				if got := w.GetLastOffset(); got != int64(wantCount) {
					t.Fatalf("pass %d: append after recovery landed at offset %d, want %d", pass, got, wantCount)
				}
				if _, err := w.TruncateToOffset(int64(wantCount)); err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				for _, leftover := range []string{"snapshot-install.state", "segments.old", "index.old"} {
					if _, err := os.Stat(filepath.Join(root, leftover)); !os.IsNotExist(err) {
						t.Fatalf("pass %d: %s left behind (err=%v)", pass, leftover, err)
					}
				}
			}
		})
	}
}

// A journal that is neither state means the directory cannot be trusted. The
// WAL refuses to open rather than guess which generation is current.
func TestSnapshotInstallJournalCorruptionFailsClosed(t *testing.T) {
	root := t.TempDir()
	buildWAL(t, root, "old", 10)
	if err := os.WriteFile(filepath.Join(root, "snapshot-install.state"), []byte("prep"), 0600); err != nil {
		t.Fatal(err)
	}
	if w, err := NewWAL(root, 0, checkpointTestConfig(), nil); err == nil {
		w.Close()
		t.Fatal("WAL opened with an unreadable snapshot install journal")
	}
}

func openWAL(t *testing.T, dir string) *WAL {
	t.Helper()
	w, err := NewWAL(dir, 0, checkpointTestConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

// checkpointOf returns a staging directory holding a checkpoint of a WAL with
// n events tagged tag, and the last offset in it.
func checkpointOf(t *testing.T, tag string, n int) (staging string, last int64) {
	t.Helper()
	source := openWAL(t, t.TempDir())
	appendTagged(t, source, tag, n)
	staging = t.TempDir()
	_, last, err := source.Checkpoint(staging)
	if err != nil {
		t.Fatal(err)
	}
	return staging, last
}

func TestInstallCheckpointReplacesTheLog(t *testing.T) {
	w := openWAL(t, t.TempDir())
	appendTagged(t, w, "old", 30)
	staging, last := checkpointOf(t, "new", 45)

	if err := w.InstallCheckpoint(staging, last); err != nil {
		t.Fatalf("install: %v", err)
	}
	if tag, count := generation(t, w); tag != "new" || count != 45 {
		t.Fatalf("log after install: %d %q entries, want 45 new", count, tag)
	}
	appendTagged(t, w, "new", 3)
	if tag, count := generation(t, w); tag != "new" || count != 48 {
		t.Fatalf("log after appending to the installed checkpoint: %d %q entries, want 48 new", count, tag)
	}
	for _, leftover := range []string{"snapshot-install.state", "segments.old", "index.old"} {
		if _, err := os.Stat(filepath.Join(w.GetDataDir(), leftover)); !os.IsNotExist(err) {
			t.Fatalf("%s left behind after a completed install (err=%v)", leftover, err)
		}
	}
}

// An install that fails part-way must leave the old log in place and usable.
func TestInstallCheckpointFailureKeepsTheOldLog(t *testing.T) {
	cases := []struct {
		name  string
		spoil func(t *testing.T, staging string, last int64) int64
	}{
		{"checkpoint ends at a different offset than announced", func(t *testing.T, staging string, last int64) int64 {
			return last + 5
		}},
		{"checkpoint has no index directory", func(t *testing.T, staging string, last int64) int64 {
			if err := os.RemoveAll(filepath.Join(staging, "index")); err != nil {
				t.Fatal(err)
			}
			return last
		}},
		{"checkpoint is missing its last segment", func(t *testing.T, staging string, last int64) int64 {
			segments, _ := filepath.Glob(filepath.Join(staging, "segments", "*.log"))
			sort.Strings(segments)
			// Skip a trailing segment that holds no events; removing it would
			// not change where the log ends.
			for i := len(segments) - 1; i >= 0; i-- {
				if info, err := os.Stat(segments[i]); err == nil && info.Size() > 64 {
					if err := os.Remove(segments[i]); err != nil {
						t.Fatal(err)
					}
					return last
				}
			}
			t.Fatal("checkpoint has no segment with events")
			return last
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := openWAL(t, t.TempDir())
			appendTagged(t, w, "old", 30)
			staging, last := checkpointOf(t, "new", 45)
			expected := tc.spoil(t, staging, last)

			if err := w.InstallCheckpoint(staging, expected); err == nil {
				t.Fatal("install of a bad checkpoint reported success")
			}
			if tag, count := generation(t, w); tag != "old" || count != 30 {
				t.Fatalf("log after the failed install: %d %q entries, want 30 old", count, tag)
			}
			appendTagged(t, w, "old", 2)
			if tag, count := generation(t, w); tag != "old" || count != 32 {
				t.Fatalf("log after appending post-failure: %d %q entries, want 32 old", count, tag)
			}
			for _, leftover := range []string{"snapshot-install.state", "segments.old", "index.old"} {
				if _, err := os.Stat(filepath.Join(w.GetDataDir(), leftover)); !os.IsNotExist(err) {
					t.Fatalf("%s left behind after a failed install (err=%v)", leftover, err)
				}
			}
		})
	}
}

// Taking a checkpoint must not stop the partition from accepting writes while
// the files are copied, and what is written during the copy must not leak into
// the checkpoint.
func TestCheckpointDoesNotBlockAppends(t *testing.T) {
	w := openWAL(t, t.TempDir())
	appendTagged(t, w, "log", 40)

	duringCopy := 0
	w.afterCheckpointCut = func() {
		// Appending here would deadlock if the WAL lock were still held.
		appendTagged(t, w, "log", 25)
		duringCopy = int(w.GetNextOffset())
	}
	staging := t.TempDir()
	files, last, err := w.Checkpoint(staging)
	w.afterCheckpointCut = nil
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if duringCopy != 65 {
		t.Fatalf("WAL held %d entries while the checkpoint was copying, want 65", duringCopy)
	}
	if last != 39 {
		t.Fatalf("checkpoint ends at offset %d, want 39: it must be the log as it was when it was cut", last)
	}
	if len(files) < 4 {
		t.Fatalf("checkpoint of a multi-segment WAL lists only %d files", len(files))
	}

	// The checkpoint is a complete log on its own: install it elsewhere.
	target := openWAL(t, t.TempDir())
	if err := target.InstallCheckpoint(staging, last); err != nil {
		t.Fatalf("install the checkpoint: %v", err)
	}
	if tag, count := generation(t, target); tag != "log" || count != 40 {
		t.Fatalf("installed checkpoint holds %d %q entries, want 40", count, tag)
	}
	if tag, count := generation(t, w); tag != "log" || count != 65 {
		t.Fatalf("source WAL holds %d %q entries after the checkpoint, want 65", count, tag)
	}
}

// Several logs are cut at one instant. A writer that alternates between two
// logs is never more than one event ahead on the first, so two cuts taken
// together differ by at most one event, however long each takes to copy.
// Cut one after the other, they would differ by whatever was written in
// between.
func TestCutCheckpointsCutsEveryLogAtOneInstant(t *testing.T) {
	// A cut syncs every segment file, so this test keeps the segments few.
	config := &WALConfig{SegmentSizeBytes: 32 << 10, IndexInterval: 1, FsyncMode: "periodic", FlushIntervalMS: 0}
	open := func() *WAL {
		w, err := NewWAL(t.TempDir(), 0, config, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { w.Close() })
		return w
	}
	a, b := open(), open()

	stop, stopped := make(chan struct{}), make(chan error, 1)
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				stopped <- nil
				return
			default:
			}
			for _, w := range []*WAL{a, b} {
				if err := w.AppendEvent(&types.Event{MessageId: fmt.Sprintf("pair-%d", i), Topic: "t", Payload: make([]byte, 64), ScheduleTs: 1}); err != nil {
					stopped <- err
					return
				}
			}
			time.Sleep(200 * time.Microsecond)
		}
	}()

	const rounds = 12
	for round := 0; round < rounds; round++ {
		cuts, err := CutCheckpoints([]*WAL{a, b})
		if err != nil {
			t.Fatal(err)
		}
		lastA, lastB := cuts[0].LastOffset(), cuts[1].LastOffset()
		if ahead := lastA - lastB; ahead < 0 || ahead > 1 {
			t.Fatalf("round %d: the logs were cut at offsets %d and %d; one instant allows a difference of 0 or 1", round, lastA, lastB)
		}
		// Each copy holds its log exactly as far as it was cut, although the
		// writer has moved on by the time the files are copied.
		for i, cut := range cuts {
			dir := t.TempDir()
			if _, last, err := cut.CopyTo(dir); err != nil || last != cut.LastOffset() {
				t.Fatalf("round %d: copy of log %d ends at offset %d, cut at %d (err=%v)", round, i, last, cut.LastOffset(), err)
			}
			copied, err := NewWAL(dir, 0, config, nil)
			if err != nil {
				t.Fatalf("round %d: open the copy of log %d: %v", round, i, err)
			}
			if got := copied.GetLastOffset(); got != cut.LastOffset() {
				t.Fatalf("round %d: the copy of log %d holds offsets through %d, cut at %d", round, i, got, cut.LastOffset())
			}
			copied.Close()
		}
	}
	close(stop)
	if err := <-stopped; err != nil {
		t.Fatalf("writer: %v", err)
	}
	if a.GetLastOffset() < rounds {
		t.Fatalf("the writer appended only %d events while %d cuts were taken", a.GetLastOffset()+1, rounds)
	}
}

// A cut holds back deletion and truncation of its log until it is copied or
// released, and can be used once.
func TestCheckpointCutIsReleasedOnce(t *testing.T) {
	w := openWAL(t, t.TempDir())
	appendTagged(t, w, "a", 20)

	cuts, err := CutCheckpoints([]*WAL{w})
	if err != nil {
		t.Fatal(err)
	}
	compacted := make(chan error, 1)
	go func() {
		_, err := w.CompactByOffset(10)
		compacted <- err
	}()
	select {
	case err := <-compacted:
		t.Fatalf("segments were removed while a cut of the log was outstanding (err=%v)", err)
	case <-time.After(150 * time.Millisecond):
	}

	cuts[0].Release()
	cuts[0].Release() // harmless
	if err := <-compacted; err != nil {
		t.Fatalf("compaction after the cut was released: %v", err)
	}
	if _, _, err := cuts[0].CopyTo(t.TempDir()); err == nil {
		t.Fatal("a released cut was copied")
	}
}

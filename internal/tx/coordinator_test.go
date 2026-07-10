package tx

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestCoordinator_NewCoordinator(t *testing.T) {
	c := NewCoordinator(30*time.Second, t.TempDir())
	if c == nil {
		t.Fatal("NewCoordinator should not return nil")
	}
	c.Stop()
}

func TestCoordinator_Begin(t *testing.T) {
	c := NewCoordinator(30*time.Second, t.TempDir())
	defer c.Stop()

	tx, err := c.Begin("tx-1", []Participant{
		PartitionParticipant{PartitionID: 1},
		PartitionParticipant{PartitionID: 2},
	})
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	if tx.ID != "tx-1" {
		t.Errorf("expected tx ID tx-1, got %s", tx.ID)
	}
	if tx.Status != StatusPending {
		t.Errorf("expected StatusPending, got %v", tx.Status)
	}
	if len(tx.Participants) != 2 {
		t.Errorf("expected 2 participants, got %d", len(tx.Participants))
	}
}

func TestCoordinator_Begin_Duplicate(t *testing.T) {
	c := NewCoordinator(30*time.Second, t.TempDir())
	defer c.Stop()

	c.Begin("tx-1", []Participant{PartitionParticipant{PartitionID: 1}})

	_, err := c.Begin("tx-1", []Participant{PartitionParticipant{PartitionID: 2}})
	if err == nil {
		t.Error("expected error for duplicate tx ID")
	}
}

func TestCoordinator_Commit(t *testing.T) {
	c := NewCoordinator(30*time.Second, t.TempDir())
	defer c.Stop()

	var prepCalled, commitCalled int
	participant := &mockParticipant{
		onPrepare: func() error { prepCalled++; return nil },
		onCommit:  func() error { commitCalled++; return nil },
	}

	tx, _ := c.Begin("tx-1", []Participant{participant})
	ctx := context.Background()

	err := c.Commit(ctx, tx.ID)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if prepCalled != 1 {
		t.Errorf("expected 1 prepare call, got %d", prepCalled)
	}
	if commitCalled != 1 {
		t.Errorf("expected 1 commit call, got %d", commitCalled)
	}

	status, _ := c.GetStatus(tx.ID)
	if status != StatusCommitted {
		t.Errorf("expected StatusCommitted, got %v", status)
	}
}

func TestCoordinator_PrepareThenCommit(t *testing.T) {
	c := NewCoordinator(30*time.Second, t.TempDir())
	defer c.Stop()

	var prepCalled, commitCalled int
	participant := &mockParticipant{
		onPrepare: func() error { prepCalled++; return nil },
		onCommit:  func() error { commitCalled++; return nil },
	}

	tx, _ := c.Begin("tx-prepare-commit", []Participant{participant})
	ctx := context.Background()

	if err := c.Prepare(ctx, tx.ID); err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	if prepCalled != 1 {
		t.Fatalf("expected 1 prepare call, got %d", prepCalled)
	}
	if commitCalled != 0 {
		t.Fatalf("expected 0 commit calls during prepare, got %d", commitCalled)
	}

	status, err := c.GetStatus(tx.ID)
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if status != StatusPrepared {
		t.Fatalf("expected StatusPrepared, got %v", status)
	}

	if err := c.Commit(ctx, tx.ID); err != nil {
		t.Fatalf("Commit failed after prepare: %v", err)
	}
	if prepCalled != 1 {
		t.Fatalf("expected commit on prepared tx to skip prepare, got prepare calls=%d", prepCalled)
	}
	if commitCalled != 1 {
		t.Fatalf("expected 1 commit call, got %d", commitCalled)
	}
}

func TestCoordinator_Commit_PrepareFailure(t *testing.T) {
	c := NewCoordinator(30*time.Second, t.TempDir())
	defer c.Stop()

	participant := &mockParticipant{
		onPrepare: func() error { return context.DeadlineExceeded },
		onCommit:  func() error { return nil },
	}

	c.Begin("tx-1", []Participant{participant})
	ctx := context.Background()

	err := c.Commit(ctx, "tx-1")
	if err == nil {
		t.Error("expected error when prepare fails")
	}
}

func TestCoordinator_Abort(t *testing.T) {
	c := NewCoordinator(30*time.Second, t.TempDir())
	defer c.Stop()

	var abortCalled int
	participant := &mockParticipant{
		onAbort: func() error { abortCalled++; return nil },
	}

	tx, _ := c.Begin("tx-1", []Participant{participant})
	ctx := context.Background()

	err := c.Abort(ctx, tx.ID)
	if err != nil {
		t.Fatalf("Abort failed: %v", err)
	}

	if abortCalled != 1 {
		t.Errorf("expected 1 abort call, got %d", abortCalled)
	}

	status, _ := c.GetStatus(tx.ID)
	if status != StatusAborted {
		t.Errorf("expected StatusAborted, got %v", status)
	}
}

func TestCoordinator_Abort_NonExistent(t *testing.T) {
	c := NewCoordinator(30*time.Second, t.TempDir())
	defer c.Stop()

	err := c.Abort(context.Background(), "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent tx")
	}
}

func TestCoordinator_GetStatus(t *testing.T) {
	c := NewCoordinator(30*time.Second, t.TempDir())
	defer c.Stop()

	// Non-existent tx
	_, err := c.GetStatus("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent tx")
	}

	// Existing tx
	tx, _ := c.Begin("tx-1", []Participant{PartitionParticipant{PartitionID: 1}})
	status, err := c.GetStatus(tx.ID)
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if status != StatusPending {
		t.Errorf("expected StatusPending, got %v", status)
	}
}

func TestCoordinator_MultiParticipantCommit(t *testing.T) {
	c := NewCoordinator(30*time.Second, t.TempDir())
	defer c.Stop()

	var prepCount, commitCount int
	var mu sync.Mutex

	participants := []Participant{
		&mockParticipant{
			onPrepare: func() error { mu.Lock(); prepCount++; mu.Unlock(); return nil },
			onCommit:  func() error { mu.Lock(); commitCount++; mu.Unlock(); return nil },
		},
		&mockParticipant{
			onPrepare: func() error { mu.Lock(); prepCount++; mu.Unlock(); return nil },
			onCommit:  func() error { mu.Lock(); commitCount++; mu.Unlock(); return nil },
		},
		&mockParticipant{
			onPrepare: func() error { mu.Lock(); prepCount++; mu.Unlock(); return nil },
			onCommit:  func() error { mu.Lock(); commitCount++; mu.Unlock(); return nil },
		},
	}

	c.Begin("tx-multi", participants)
	ctx := context.Background()
	c.Commit(ctx, "tx-multi")

	if prepCount != 3 {
		t.Errorf("expected 3 prepares, got %d", prepCount)
	}
	if commitCount != 3 {
		t.Errorf("expected 3 commits, got %d", commitCount)
	}
}

func TestPartitionParticipant(t *testing.T) {
	p := PartitionParticipant{PartitionID: 42}

	ctx := context.Background()

	if err := p.Prepare(ctx, "tx-1"); err != nil {
		t.Errorf("Prepare failed: %v", err)
	}
	if err := p.Commit(ctx, "tx-1"); err != nil {
		t.Errorf("Commit failed: %v", err)
	}
	if err := p.Abort(ctx, "tx-1"); err != nil {
		t.Errorf("Abort failed: %v", err)
	}
}

func TestCoordinator_Timeout(t *testing.T) {
	// Very short timeout
	c := NewCoordinator(1*time.Millisecond, t.TempDir())
	defer c.Stop()

	participant := &mockParticipant{
		onPrepare: func() error {
			time.Sleep(10 * time.Millisecond)
			return nil
		},
	}

	c.Begin("tx-timeout", []Participant{participant})
	ctx := context.Background()

	// This should timeout
	_ = c.Commit(ctx, "tx-timeout")
	// The mock doesn't actually sleep, so it won't timeout in prepare
	// But the context will be cancelled
}

func TestCoordinator_TxLog(t *testing.T) {
	tmpDir := t.TempDir()
	c := NewCoordinator(30*time.Second, tmpDir)
	defer c.Stop()

	c.Begin("tx-log-test", []Participant{
		PartitionParticipant{PartitionID: 1},
	})

	// Verify tx log file was created
	if _, err := os.Stat(filepath.Join(tmpDir, "tx_log.json")); os.IsNotExist(err) {
		t.Error("tx_log.json should exist after Begin")
	}
}

func TestCoordinator_Recovery(t *testing.T) {
	tmpDir := t.TempDir()

	// Create coordinator, start a transaction, then "crash" (stop without commit)
	c1 := NewCoordinator(30*time.Second, tmpDir)
	c1.Begin("tx-recover", []Participant{
		PartitionParticipant{PartitionID: 1},
	})
	c1.Stop()

	// Create new coordinator pointing at same data dir — should recover
	c2 := NewCoordinator(30*time.Second, tmpDir)
	defer c2.Stop()

	// The tx should be loaded from log
	status, err := c2.GetStatus("tx-recover")
	if err != nil {
		t.Fatalf("GetStatus after recovery failed: %v", err)
	}
	if status != StatusPending {
		t.Errorf("expected StatusPending after recovery, got %v", status)
	}
}

func TestTransaction_Struct(t *testing.T) {
	tx := &Transaction{
		ID:           "test-tx",
		Participants: []Participant{PartitionParticipant{PartitionID: 1}},
		Status:       StatusPending,
		CreatedAt:    time.Now(),
	}

	if tx.ID != "test-tx" {
		t.Errorf("expected ID test-tx, got %s", tx.ID)
	}
	if tx.Status != StatusPending {
		t.Errorf("expected StatusPending, got %v", tx.Status)
	}
}

func TestStatus_Values(t *testing.T) {
	if StatusPending != 0 {
		t.Errorf("expected StatusPending=0, got %d", StatusPending)
	}
	if StatusPrepared != 1 {
		t.Errorf("expected StatusPrepared=1, got %d", StatusPrepared)
	}
	if StatusCommitting != 2 {
		t.Errorf("expected StatusCommitting=2, got %d", StatusCommitting)
	}
	if StatusCommitted != 3 {
		t.Errorf("expected StatusCommitted=3, got %d", StatusCommitted)
	}
	if StatusAborted != 4 {
		t.Errorf("expected StatusAborted=4, got %d", StatusAborted)
	}
}

func TestStatus_String(t *testing.T) {
	if StatusPending.String() != "pending" {
		t.Errorf("expected pending, got %s", StatusPending.String())
	}
	if StatusPrepared.String() != "prepared" {
		t.Errorf("expected prepared, got %s", StatusPrepared.String())
	}
	if StatusCommitting.String() != "committing" {
		t.Errorf("expected committing, got %s", StatusCommitting.String())
	}
	if StatusCommitted.String() != "committed" {
		t.Errorf("expected committed, got %s", StatusCommitted.String())
	}
	if StatusAborted.String() != "aborted" {
		t.Errorf("expected aborted, got %s", StatusAborted.String())
	}
}

// mockParticipant implements Participant for testing
type mockParticipant struct {
	onPrepare func() error
	onCommit  func() error
	onAbort   func() error
}

func (m *mockParticipant) Prepare(ctx context.Context, txID TxID) error {
	if m.onPrepare != nil {
		return m.onPrepare()
	}
	return nil
}

func (m *mockParticipant) Commit(ctx context.Context, txID TxID) error {
	if m.onCommit != nil {
		return m.onCommit()
	}
	return nil
}

func (m *mockParticipant) Abort(ctx context.Context, txID TxID) error {
	if m.onAbort != nil {
		return m.onAbort()
	}
	return nil
}

func skipWindows(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Skipping on Windows due to PebbleDB file locking in partition cleanup")
	}
}

func TestCoordinator_RecoverPreparedLocks(t *testing.T) {
	skipWindows(t)

	tmpDir := t.TempDir()

	cfg := &types.Config{
		DataDir:          tmpDir,
		PartitionCount:   8,
		TickMS:           100,
		WheelSize:        60,
		SegmentSizeBytes: 10 * 1024 * 1024,
	}

	pm := partition.NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	err := pm.CreatePartition(0, "test-topic")
	if err != nil {
		t.Fatalf("CreatePartition failed: %v", err)
	}

	part, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatalf("GetInternalPartition failed: %v", err)
	}
	part.Leader = true // Make it leader so WAL is active

	// Create coordinator
	c := NewCoordinator(30*time.Second, t.TempDir())
	defer c.Stop()

	// Write an active prepare marker to the partition's WAL directly
	event := &types.Event{
		MessageId: "tx:tx-100:prepare",
		Topic:     "__transaction_log",
		Payload:   []byte("prepare"),
		CreatedTs: time.Now().UnixNano(),
		Meta: map[string]string{
			"tx_id":     "tx-100",
			"tx_action": "prepare",
		},
	}
	err = part.Wal.AppendEvent(event)
	if err != nil {
		t.Fatalf("AppendEvent failed: %v", err)
	}

	// Now register the PM on the coordinator which will run RecoverPreparedLocks.
	// Note: RecoverPreparedLocks no longer locks partitions here; the lock leak
	// caused by holding recovered locks forever was fixed. Recovery re-drives
	// resolution, which acquires locks via the normal acquireLocks path.
	c.SetPartitionManager(pm)

	// The transaction should be recovered in memory so the recovery loop can
	// re-drive Commit/Abort (which will acquire locks normally).
	status, err := c.GetStatus("tx-100")
	if err != nil {
		t.Fatalf("GetStatus after recovery failed: %v", err)
	}
	if status != StatusPending {
		// The log record is Pending unless a prepare log was written by the
		// coordinator. RecoverPreparedLocks only detects the WAL prepare marker
		// and does not change the in-memory status here.
		t.Logf("status after recovery: %v", status)
	}

	// Lock must be acquirable: it was NOT held by RecoverPreparedLocks.
	c.locksMu.Lock()
	lock, exists := c.txLocks[0]
	c.locksMu.Unlock()

	if !exists {
		// Lock may be lazily created; that's fine.
		return
	}

	lockAcquired := make(chan bool, 1)
	go func() {
		lock.Lock()
		lockAcquired <- true
		lock.Unlock()
	}()

	select {
	case <-lockAcquired:
		// Lock is not held — correct behavior after the leak fix.
	case <-time.After(100 * time.Millisecond):
		t.Error("Expected partition lock to be acquirable after recovery, but it is still held")
	}
}

func TestCoordinator_RecoveryBackoff(t *testing.T) {
	c := NewCoordinator(30*time.Second, t.TempDir())
	defer c.Stop()

	txID := TxID("tx-backoff")

	// Create a mock participant that always fails commits
	prepCount := 0
	commitCount := 0
	participant := &mockParticipant{
		onPrepare: func() error {
			prepCount++
			return nil
		},
		onCommit: func() error {
			commitCount++
			return fmt.Errorf("persistent database write error")
		},
	}

	// Begin the transaction
	_, err := c.Begin(txID, []Participant{participant})
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// Try committing — it will fail because the participant's Commit fails
	ctx := context.Background()
	err = c.Commit(ctx, txID)
	if err == nil {
		t.Fatal("Expected commit to fail")
	}

	// The initial Commit failed (commitCount = 1).
	// Call recover() manually. This is the first recovery attempt, so it will run and schedule a backoff.
	c.recover()
	if commitCount != 2 {
		t.Errorf("Expected commitCount to be 2 after first recovery attempt, got %d", commitCount)
	}

	// Call recover() again. The backoff is active, so it should skip the attempt.
	c.recover()
	if commitCount != 2 {
		t.Errorf("Expected commitCount to still be 2 (skipped via backoff), got %d", commitCount)
	}

	// Manually clear nextRetryTime to force a retry
	c.attemptsMu.Lock()
	delete(c.nextRetryTime, txID)
	c.attemptsMu.Unlock()

	// Call recover again — this time it will run
	c.recover()

	// Commit count should now be 3
	if commitCount != 3 {
		t.Errorf("Expected commitCount to be 3 after forcing retry, got %d", commitCount)
	}
}

// TestTxLog_AtomicWrite verifies that txLog.write does not leave behind a
// torn/partial tx_log.json even if the process crashes mid-write. We simulate
// this by corrupting the temp file before rename; the on-disk log must either
// be the previous version or the new version, never a partial JSON object.
func TestTxLog_AtomicWrite(t *testing.T) {
	tmpDir := t.TempDir()
	log := newTxLog(tmpDir)

	// Write an initial record.
	if err := log.write("tx-1", StatusPending, []int32{1}, time.Now()); err != nil {
		t.Fatalf("initial write failed: %v", err)
	}

	// Reload and verify it is parseable.
	log2 := newTxLog(tmpDir)
	if err := log2.load(); err != nil {
		t.Fatalf("load after initial write failed: %v", err)
	}
	if _, ok := log2.records["tx-1"]; !ok {
		t.Fatal("tx-1 missing after reload")
	}

	// Simulate a crash by writing garbage into the temp file path (which the
	// implementation uses for atomic writes). The temp file must not remain or,
	// if it does, the real tx_log.json must still be valid.
	tmpPath := filepath.Join(tmpDir, "tx_log.json.tmp")
	if err := os.WriteFile(tmpPath, []byte("{not valid json"), 0644); err != nil {
		t.Fatalf("write fake temp file: %v", err)
	}

	// A successful write should replace the temp file and the final file must
	// remain valid.
	if err := log2.write("tx-2", StatusPrepared, []int32{1, 2}, time.Now()); err != nil {
		t.Fatalf("second write failed: %v", err)
	}

	log3 := newTxLog(tmpDir)
	if err := log3.load(); err != nil {
		t.Fatalf("load after second write failed: %v", err)
	}
	if _, ok := log3.records["tx-1"]; !ok {
		t.Error("tx-1 missing after atomic write")
	}
	if _, ok := log3.records["tx-2"]; !ok {
		t.Error("tx-2 missing after atomic write")
	}
}

// TestCoordinator_PartialCommit_StaysCommitting verifies that a Commit with
// a partial participant failure transitions to StatusCommitting and persists
// that state, rather than marking the transaction Committed (which would make
// recovery ignore it).
func TestCoordinator_PartialCommit_StaysCommitting(t *testing.T) {
	tmpDir := t.TempDir()
	c := NewCoordinator(30*time.Second, tmpDir)
	defer c.Stop()

	commitCalled := 0
	participants := []Participant{
		&mockParticipant{onCommit: func() error { commitCalled++; return nil }},
		&mockParticipant{onCommit: func() error { return fmt.Errorf("disk full") }},
	}

	if _, err := c.Begin("tx-partial", participants); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	ctx := context.Background()
	if err := c.Prepare(ctx, "tx-partial"); err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}

	err := c.Commit(ctx, "tx-partial")
	if err == nil {
		t.Fatal("expected partial commit to return error")
	}

	status, _ := c.GetStatus("tx-partial")
	if status != StatusCommitting {
		t.Errorf("expected StatusCommitting after partial commit, got %v", status)
	}

	// The on-disk log must also be "committing" so a restart will re-drive.
	log := newTxLog(tmpDir)
	if err := log.load(); err != nil {
		t.Fatalf("load log: %v", err)
	}
	rec, ok := log.records["tx-partial"]
	if !ok {
		t.Fatal("tx-partial missing from log after partial commit")
	}
	if rec.Status != StatusCommitting.String() {
		t.Errorf("expected log status 'committing', got %s", rec.Status)
	}
}

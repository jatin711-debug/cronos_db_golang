package tx

import (
	"context"
	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"testing"
	"time"
)

func TestAuditCannotCommitAbortedTransaction(t *testing.T) {
	c := NewCoordinator(time.Second, t.TempDir())
	defer c.Stop()
	if _, err := c.Begin("aborted", []Participant{PartitionParticipant{PartitionID: 0}}); err != nil {
		t.Fatal(err)
	}
	if err := c.Abort(context.Background(), "aborted"); err != nil {
		t.Fatal(err)
	}
	if err := c.Commit(context.Background(), "aborted"); err == nil {
		t.Fatal("Commit accepted an aborted transaction")
	}
}
func TestAuditPrepareFailureReturnsWithoutDeadlock(t *testing.T) {
	pm := partition.NewPartitionManager("audit", &types.Config{DataDir: t.TempDir(), PartitionCount: 1})
	defer pm.Close()
	c := NewCoordinator(50*time.Millisecond, t.TempDir())
	defer c.Stop()
	c.SetPartitionManager(pm)
	if _, err := c.Begin("missing-partition", []Participant{PartitionParticipant{PartitionID: 0}}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- c.Prepare(context.Background(), "missing-partition") }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected missing partition failure")
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Prepare deadlocked after participant failure despite 50ms timeout")
	}
}

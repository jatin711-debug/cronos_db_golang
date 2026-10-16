package main

import (
	"context"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
)

// A fast consumer can receive an event before PublishBatch returns. Concurrent
// redeliveries must still produce one unique delivery for that accepted ID.
func TestLedgerReconcilesDeliveryBeforePublishResponse(t *testing.T) {
	l := &ledger{prefix: "trial-", states: make([]atomic.Uint32, 100)}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range l.states {
				l.delivery(&types.Event{MessageId: "trial-" + strconv.Itoa(i), ScheduleTs: time.Now().UnixMilli()})
			}
		}()
	}
	wg.Wait()
	for i := range l.states {
		l.states[i].Or(1)
	}
	l.accepted.Store(100)
	if l.delivered.Load() != 100 || l.duplicates.Load() != 700 {
		t.Fatalf("unique=%d duplicates=%d; want 100 and 700", l.delivered.Load(), l.duplicates.Load())
	}
	for i := range l.states {
		if l.states[i].Load() != 3 {
			t.Fatalf("accepted event %d lacks delivery evidence", i)
		}
	}
	for _, id := range []string{"old-trial-1", "trial--1", "trial-100", "trial-invalid"} {
		l.delivery(&types.Event{MessageId: id})
	}
	if l.delivered.Load() != 100 || l.duplicates.Load() != 700 {
		t.Fatal("unrelated or invalid IDs changed the ledger")
	}
}

func TestGeneratedPolicyAndRoutesPermitBenchmarkWork(t *testing.T) {
	dir := t.TempDir()
	if err := initialize(dir, []string{"node1", "node2", "node3"}); err != nil {
		t.Fatal(err)
	}
	policy, err := auth.NewPolicyFromFile(filepath.Join(dir, "policy.json"))
	if err != nil {
		t.Fatal(err)
	}
	claims := &auth.Claims{}
	claims.Subject = "peak"
	ctx := auth.WithClaims(context.Background(), claims)
	for _, operation := range []string{"publish", "subscribe"} {
		if err := auth.CheckTopicPermission(ctx, "peak", operation, policy); err != nil {
			t.Fatal(err)
		}
	}
	for partition := int32(0); partition < 3; partition++ {
		key, err := keyForPartition(partition, 3)
		if err != nil || utils.HashToPartitionID(key, 3) != partition {
			t.Fatalf("partition %d route: %s, %v", partition, key, err)
		}
	}
	if _, err := keyForPartition(3, 3); err == nil {
		t.Fatal("invalid metadata must not start an unbounded key search")
	}
}

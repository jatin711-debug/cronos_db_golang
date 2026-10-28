package api

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/internal/tenant"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// These tests began as the reproductions of the audit of 2026-10-16
// (docs/PRODUCTION_AUDIT_2026-10-16.md). Each asserts an invariant that the audited
// v0.6.0 broke.

// publishOne publishes e alone, through Publish or PublishBatch, and reports
// whether it was accepted, with a description of the response.
func publishOne(h *EventServiceHandler, e *types.Event, batch bool) (bool, string) {
	if batch {
		r, err := h.PublishBatch(context.Background(), &types.PublishBatchRequest{Events: []*types.Event{e}})
		return err == nil && r.GetSuccess() && r.GetErrorCount() == 0, fmt.Sprintf("%v / %v", r, err)
	}
	r, err := h.Publish(context.Background(), &types.PublishRequest{Event: e})
	return err == nil && r.GetSuccess(), fmt.Sprintf("%v / %v", r, err)
}

// idIsFree reports whether nothing is recorded for messageID.
func idIsFree(t *testing.T, p *partition.Partition, messageID string) bool {
	t.Helper()
	_, _, found, err := p.DedupStore.Outcome(messageID)
	if err != nil {
		t.Fatal(err)
	}
	return !found
}

// An event too large for a log record is refused before anything is claimed
// or written. v0.6.0 accepted it, and the record could not be read back.
func TestPublish_OversizedEventIsRefused(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			pm := auditManager(t)
			p, _ := pm.GetInternalPartition(0)
			h := NewEventServiceHandler(pm, p.DedupStore, p.ConsumerGroup)
			e := orders("large-metadata", 1)[0]
			e.Meta = make(map[string]string)
			for i := 0; i < 180; i++ {
				e.Meta[fmt.Sprintf("key-%03d", i)] = strings.Repeat("x", 60*1024)
			}
			if n := proto.Size(&types.PublishRequest{Event: e}); n >= DefaultConfig().MaxRecvMsgSize {
				t.Fatalf("event exceeds the transport limit (%d bytes), so it would not test the handler", n)
			}
			if ok, desc := publishOne(h, e, batch); ok || !strings.Contains(desc, storage.ErrRecordTooLarge.Error()) {
				t.Fatalf("oversized event: %s", desc)
			}
			if n := p.Wal.GetNextOffset(); n != 0 {
				t.Fatalf("refused event moved the next offset to %d", n)
			}
			if !idIsFree(t, p, e.MessageId) {
				t.Fatal("refused event left its message ID claimed")
			}

			// The same ID with an event that fits is an ordinary publish.
			e.Meta = nil
			if ok, desc := publishOne(h, e, batch); !ok {
				t.Fatalf("publish of the same ID after the refusal: %s", desc)
			}
			got, err := p.Wal.ReadEvent(e.Offset)
			if err != nil || got.MessageId != e.MessageId {
				t.Fatalf("accepted event does not read back: %v %v", got, err)
			}
		})
	}
}

// The term stored with an entry is the leader's, whatever the producer put in
// the event.
func TestPublish_ProducerCannotChooseLogTerm(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			pm := auditManager(t)
			p, _ := pm.GetInternalPartition(0)
			p.Wal.SetCurrentTerm(7)
			h := NewEventServiceHandler(pm, p.DedupStore, p.ConsumerGroup)
			e := orders("producer-term", 1)[0]
			e.Term = 999999
			if ok, desc := publishOne(h, e, batch); !ok {
				t.Fatalf("publish: %s", desc)
			}
			got, err := p.Wal.ReadEvent(e.Offset)
			if err != nil {
				t.Fatal(err)
			}
			if got.Term != 7 {
				t.Fatalf("stored term %d, the producer's; the log's is 7", got.Term)
			}
		})
	}
}

func TestPublish_ProducerTermDoesNotReachFollower(t *testing.T) {
	leader := newPublisher(t)
	follower := newReplicaLog(t, "term-follower")
	if err := leader.pm.AddFollower(0, "term-follower", follower.serveReplication()); err != nil {
		t.Fatal(err)
	}
	e := orders("replicated-producer-term", 1)[0]
	e.Term = 999999
	if r := leader.publish([]*types.Event{e}); !r.GetSuccess() {
		t.Fatalf("publish: %v", r)
	}
	got := follower.log()
	if len(got) != 1 || got[0].Term != leader.p.Epoch() {
		t.Fatalf("follower log %s; want one entry of the leader's epoch %d", describe(got), leader.p.Epoch())
	}
}

// A segment that cannot be started after an append does not fail that
// append. v0.6.0 reported the publish failed with the event in the log and
// released its ID, so the retry appended the event a second time.
func TestPublish_RotationFailureDoesNotAppendRetryTwice(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			pm := auditManager(t) // 1 MiB segments
			p, _ := pm.GetInternalPartition(0)
			h := NewEventServiceHandler(pm, p.DedupStore, p.ConsumerGroup)
			e := orders("rotation-failure", 1)[0]
			e.Payload = make([]byte, 1<<20)
			// The next segment cannot be made, inside this test's directory.
			blocked := filepath.Join(p.Wal.GetDataDir(), "segments", fmt.Sprintf("%020d.log", 1))
			if err := os.Mkdir(blocked, 0o755); err != nil {
				t.Fatal(err)
			}
			first, desc := publishOne(h, e, batch)
			if err := os.Remove(blocked); err != nil {
				t.Fatal(err)
			}
			if !first {
				t.Fatalf("publish that filled the segment: %s", desc)
			}
			publishOne(h, e, batch) // the client's retry
			got, err := p.Wal.ReadEvents(0, p.Wal.GetLastOffset())
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 {
				t.Fatalf("log holds %d entries of one message ID, want 1", len(got))
			}
		})
	}
}

// A publish refused for its tenant's quota leaves its message ID free for the
// retry.
func TestPublish_QuotaRefusalDoesNotClaimID(t *testing.T) {
	pm := auditManager(t)
	p, _ := pm.GetInternalPartition(0)
	h := NewEventServiceHandler(pm, p.DedupStore, p.ConsumerGroup)
	a := tenant.NewAccountant()
	a.SetLimits("default", tenant.Limits{MaxStorageBytes: 1})
	h.SetTenantAccountant(a)
	e := orders("quota-refusal", 1)[0]
	_, err := h.Publish(context.Background(), &types.PublishRequest{Event: e})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected quota refusal, got %v", err)
	}
	a.SetLimits("default", tenant.Limits{MaxStorageBytes: 10000})
	r, err := h.Publish(context.Background(), &types.PublishRequest{Event: e})
	if err != nil || !r.GetSuccess() {
		t.Fatalf("retry after the quota was raised: response=%v err=%v nextOffset=%d", r, err, p.Wal.GetNextOffset())
	}
}

// A partition whose log failed takes no publishes, claims nothing for them,
// and does not finish earlier ones.
func TestPublish_FailedLogTakesNoPublishes(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			pm := auditManager(t)
			p, _ := pm.GetInternalPartition(0)
			h := NewEventServiceHandler(pm, p.DedupStore, p.ConsumerGroup)
			if ok, desc := publishOne(h, orders("before", 1)[0], batch); !ok {
				t.Fatalf("publish before the failure: %s", desc)
			}
			_ = p.Wal.Fail(errors.New("injected for the test"))

			e := orders("after", 1)[0]
			if ok, desc := publishOne(h, e, batch); ok || !strings.Contains(desc, storage.ErrLogFailed.Error()) {
				t.Fatalf("publish to a failed log: %s", desc)
			}
			if !idIsFree(t, p, e.MessageId) {
				t.Fatal("refused publish left its message ID claimed")
			}
			if got := pm.FailedLogs(); len(got) != 1 || got[0] != 0 {
				t.Fatalf("FailedLogs = %v, want [0]", got)
			}
		})
	}
}

// A follower whose log failed acknowledges nothing, not even entries it
// already holds: it cannot say they are durable.
func TestReplication_FailedFollowerAcknowledgesNothing(t *testing.T) {
	follower := newReplicaLog(t, "failed-follower")
	batch := entries("leader", 1, 0, 2)
	if r := follower.send("node-a", 1, batch); !r.GetSuccess() {
		t.Fatalf("append before the failure: %v", r)
	}
	_ = follower.p.Wal.Fail(errors.New("injected for the test"))
	if r := follower.send("node-a", 1, batch); r.GetSuccess() {
		t.Fatalf("failed follower acknowledged entries it holds: %v", r)
	}
	if r := follower.send("node-a", 1, entries("leader", 1, 3, 3)); r.GetSuccess() {
		t.Fatalf("failed follower acknowledged a new entry: %v", r)
	}
}

// While the data disk is nearly full, publishes are refused before anything
// is claimed.
func TestPublish_RefusedWhileDiskIsFull(t *testing.T) {
	pm := auditManager(t)
	p, _ := pm.GetInternalPartition(0)
	h := NewEventServiceHandler(pm, p.DedupStore, p.ConsumerGroup)
	// Any disk in use is fuller than this.
	guard := partition.NewDiskGuard(t.TempDir(), 0.0001)
	guard.Start()
	t.Cleanup(guard.Stop)
	pm.SetDiskGuard(guard)

	e := orders("disk-full", 1)[0]
	_, err := h.Publish(context.Background(), &types.PublishRequest{Event: e})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("Publish with a full disk: %v", err)
	}
	_, err = h.PublishBatch(context.Background(), &types.PublishBatchRequest{Events: []*types.Event{e}})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("PublishBatch with a full disk: %v", err)
	}
	if !idIsFree(t, p, e.MessageId) || p.Wal.GetNextOffset() != 0 {
		t.Fatal("a refused publish claimed its ID or reached the log")
	}
}

func TestBatchQuotaChargesOnlyNewIDs(t *testing.T) {
	pm := auditManager(t)
	p, _ := pm.GetInternalPartition(0)
	h := NewEventServiceHandler(pm, p.DedupStore, p.ConsumerGroup)
	a := tenant.NewAccountant()
	a.SetLimits("default", tenant.Limits{MaxInFlight: 1})
	h.SetTenantAccountant(a)
	first := orders("quota-first", 1)[0]
	if ok, desc := publishOne(h, first, true); !ok {
		t.Fatal(desc)
	}
	beforeCount, beforeBytes := a.GetUsage("default")
	r, err := h.PublishBatch(context.Background(), &types.PublishBatchRequest{Events: []*types.Event{first}})
	if err != nil || r.GetErrorCount() != 0 || r.GetDuplicateCount() != 1 {
		t.Fatalf("duplicate-only retry charged quota: %v %v", r, err)
	}
	if n, b := a.GetUsage("default"); n != beforeCount || b != beforeBytes {
		t.Fatalf("usage changed on retry: %d %d", n, b)
	}
	next := orders("quota-next", 1)[0]
	r, err = h.PublishBatch(context.Background(), &types.PublishBatchRequest{Events: []*types.Event{first, next}})
	if err != nil || r.GetErrorCount() != 1 || r.GetDuplicateCount() != 1 {
		t.Fatalf("mixed batch: %v %v", r, err)
	}
	if !idIsFree(t, p, next.MessageId) {
		t.Fatal("quota refusal stranded new ID")
	}
	a.SetLimits("default", tenant.Limits{MaxInFlight: 2})
	if ok, desc := publishOne(h, next, true); !ok {
		t.Fatal(desc)
	}
}

func TestFullFollowerRefusesGrowthButAcknowledgesExistingEntries(t *testing.T) {
	f := newReplicaLog(t, "disk-full")
	batch := entries("leader", 1, 0, 1)
	if r := f.send("node-a", 1, batch); !r.GetSuccess() {
		t.Fatal(r)
	}
	guard := partition.NewDiskGuard(t.TempDir(), 0.0001)
	guard.Start()
	t.Cleanup(guard.Stop)
	f.pm.SetDiskGuard(guard)
	if r := f.send("node-a", 1, batch); !r.GetSuccess() {
		t.Fatalf("existing durable prefix refused: %v", r)
	}
	if r := f.send("node-a", 1, entries("leader", 1, 2, 2)); r.GetSuccess() {
		t.Fatal("full follower grew its log")
	}
	if f.p.Wal.GetNextOffset() != 2 {
		t.Fatal("refused append changed log end")
	}
}

func TestQuotaRefundedWhenNoReplicationLeaderCanAppend(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprint(batch), func(t *testing.T) {
			f := newReplicaLog(t, "follower")
			h := NewEventServiceHandler(f.pm, f.p.DedupStore, f.p.ConsumerGroup)
			a := tenant.NewAccountant()
			a.SetLimits("default", tenant.Limits{MaxInFlight: 1, MaxStorageBytes: 1000})
			h.SetTenantAccountant(a)
			event := orders("refund", 1)[0]
			if ok, desc := publishOne(h, event, batch); ok {
				t.Fatal(desc)
			}
			if n, b := a.GetUsage("default"); n != 0 || b != 0 {
				t.Fatalf("pre-append failure charged %d events / %d bytes", n, b)
			}
			if !idIsFree(t, f.p, event.MessageId) {
				t.Fatal("pre-append failure kept claim")
			}
		})
	}
}

package partition

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/delivery"
	"github.com/jatin711-debug/cronos_db_golang/internal/lifecycle"
	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func lifecycleConfig(t *testing.T) *types.Config {
	t.Helper()
	return &types.Config{DataDir: t.TempDir(), PartitionCount: 1, ReplicationFactor: 1, EventLifecycle: true, TickMS: 10, WheelSize: 64, SegmentSizeBytes: 1024, IndexInterval: 1, FsyncMode: "periodic", FlushIntervalMS: 10, DedupTTLHours: 24, BloomCapacity: 1000, HotWindowMinutes: 1}
}

func lifecyclePartition(t *testing.T, cfg *types.Config) (*PartitionManager, *Partition) {
	t.Helper()
	pm := NewPartitionManager("lifecycle", cfg)
	if err := pm.CreatePartition(0, "a"); err != nil {
		pm.Close()
		t.Fatal(err)
	}
	p, _ := pm.GetInternalPartition(0)
	return pm, p
}

func mutation(e *types.Event, request string, at int64) *types.EventMutationRequest {
	return &types.EventMutationRequest{Reference: lifecycle.Reference(e), RequestId: request, ExpectedRevisionOffset: e.Offset, ExpectedRevisionTerm: e.Term, ScheduleTs: at}
}

func TestLifecycleMutationsAndRevisionSurviveRelocation(t *testing.T) {
	pm, p := lifecyclePartition(t, lifecycleConfig(t))
	defer pm.Close()
	e := appendMaintenanceEvent(t, p, "m", "a", time.Now().Add(time.Hour), 2048)
	if err := p.Scheduler.Schedule(e); err != nil {
		t.Fatal(err)
	}
	req := mutation(e, "reschedule", time.Now().Add(2*time.Hour).UnixMilli())
	state, err := p.MutateEvent(context.Background(), req, lifecycle.Reschedule)
	if err != nil {
		t.Fatal(err)
	}
	if state.CurrentOffset == e.Offset || state.RevisionOffset != state.CurrentOffset || p.ActiveEvent(e) {
		t.Fatalf("bad new state: %v", state)
	}
	last := p.Wal.GetLastOffset()
	if retry, err := p.MutateEvent(context.Background(), req, lifecycle.Reschedule); err != nil || retry.CurrentOffset != last || p.Wal.GetLastOffset() != last {
		t.Fatalf("retry wrote again: %v %v", retry, err)
	}
	changed := proto.Clone(req).(*types.EventMutationRequest)
	changed.ScheduleTs++
	if _, err = p.MutateEvent(context.Background(), changed, lifecycle.Reschedule); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("changed retry: %v", err)
	}
	stale := mutation(e, "different", 0)
	if _, err = p.MutateEvent(context.Background(), stale, lifecycle.Cancel); status.Code(err) != codes.Aborted {
		t.Fatalf("stale revision: %v", err)
	}
	current, _ := p.Wal.ReadEvent(last)
	copy, err := lifecycle.Replacement(current, lifecycle.Relocate, "", current.ScheduleTs)
	if err != nil {
		t.Fatal(err)
	}
	p.lifecycleMu.Lock()
	err = p.appendLifecycle(copy, current)
	p.lifecycleMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	retry, err := p.MutateEvent(context.Background(), req, lifecycle.Reschedule)
	if err != nil || retry.CurrentOffset != copy.Offset || retry.RevisionOffset != last {
		t.Fatalf("retry after move: %v %v", retry, err)
	}
	if p.Scheduler.GetColdStoreCount() != 1 {
		t.Fatalf("old cold references remain: %d", p.Scheduler.GetColdStoreCount())
	}
	cancel := &types.EventMutationRequest{Reference: req.Reference, RequestId: "cancel", ExpectedRevisionOffset: retry.RevisionOffset, ExpectedRevisionTerm: retry.RevisionTerm}
	state, err = p.MutateEvent(context.Background(), cancel, lifecycle.Cancel)
	if err != nil || !state.Canceled || p.ActiveEvent(copy) {
		t.Fatalf("cancel: %v %v", state, err)
	}
	if p.Scheduler.GetColdStoreCount() != 0 {
		t.Fatal("canceled cold timer remains")
	}
	if _, err = p.MutateEvent(context.Background(), cancel, lifecycle.Cancel); err != nil {
		t.Fatalf("cancel retry: %v", err)
	}
}

func TestLifecycleReferenceChecksAndDueRejection(t *testing.T) {
	pm, p := lifecyclePartition(t, lifecycleConfig(t))
	defer pm.Close()
	e := appendMaintenanceEvent(t, p, "m", "a", time.Now().Add(time.Hour), 32)
	for name, alter := range map[string]func(*types.EventReference){"topic": func(r *types.EventReference) { r.Topic = "b" }, "message": func(r *types.EventReference) { r.MessageId = "other" }, "term": func(r *types.EventReference) { r.Term++ }, "offset": func(r *types.EventReference) { r.Offset++ }} {
		t.Run(name, func(t *testing.T) {
			req := mutation(e, name, 0)
			alter(req.Reference)
			if _, err := p.MutateEvent(context.Background(), req, lifecycle.Cancel); status.Code(err) != codes.NotFound {
				t.Fatalf("forged reference: %v", err)
			}
		})
	}
	due := appendMaintenanceEvent(t, p, "due", "a", time.Now().Add(-time.Second), 32)
	if _, err := p.MutateEvent(context.Background(), mutation(due, "cancel-due", 0), lifecycle.Cancel); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("changed a due event: %v", err)
	}
	if _, err := p.MutateEvent(context.Background(), mutation(e, "past", 1), lifecycle.Reschedule); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("accepted past schedule: %v", err)
	}
}

func TestLifecycleGenerationFencesReusedOffsetAndMessageID(t *testing.T) {
	cfg := lifecycleConfig(t)
	pm, p := lifecyclePartition(t, cfg)
	old := appendMaintenanceEvent(t, p, "same-id", "a", time.Now().Add(time.Hour), 32)
	// Model a lost periodic-fsync tail while retaining the durable generation.
	if _, err := p.Wal.TruncateToOffset(old.Offset); err != nil {
		t.Fatal(err)
	}
	if err := pm.Close(); err != nil {
		t.Fatal(err)
	}
	pm, p = lifecyclePartition(t, cfg)
	defer pm.Close()
	current := appendMaintenanceEvent(t, p, "same-id", "a", time.Now().Add(time.Hour), 32)
	if old.Offset != current.Offset || old.Term >= current.Term {
		t.Fatalf("generation was reused: old=%v current=%v", old, current)
	}
	if _, err := p.EventState(lifecycle.Reference(old)); status.Code(err) != codes.NotFound {
		t.Fatalf("old handle selected a reused offset: %v", err)
	}
	if _, err := p.EventState(lifecycle.Reference(current)); err != nil {
		t.Fatalf("new generation could not be resolved: %v", err)
	}
}

func TestLifecycleRefusesUnknownFormatBeforeOpeningStores(t *testing.T) {
	cfg := lifecycleConfig(t)
	marker := filepath.Join(cfg.DataDir, "partitions", "0", lifecycle.Marker)
	if err := os.MkdirAll(filepath.Dir(marker), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(marker, []byte("2\n"), 0600); err != nil {
		t.Fatal(err)
	}
	pm := NewPartitionManager("unknown-format", cfg)
	defer pm.Close()
	if err := pm.CreatePartition(0, "a"); err == nil {
		t.Fatal("opened an unknown lifecycle format")
	}
}

func TestLifecycleRestartAndBackupRebuild(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint(encrypted), func(t *testing.T) {
			cfg := lifecycleConfig(t)
			if encrypted {
				cfg.EncryptionEnabled = true
				cfg.EncryptionKeyFile = filepath.Join(t.TempDir(), "key")
				if err := os.WriteFile(cfg.EncryptionKeyFile, make([]byte, 32), 0600); err != nil {
					t.Fatal(err)
				}
			}
			pm, p := lifecyclePartition(t, cfg)
			defer func() {
				if pm != nil {
					pm.Close()
				}
			}()
			e := appendMaintenanceEvent(t, p, "pending", "a", time.Now().Add(time.Hour), 2048)
			req := mutation(e, "reschedule", time.Now().Add(2*time.Hour).UnixMilli())
			state, err := p.MutateEvent(context.Background(), req, lifecycle.Reschedule)
			if err != nil {
				t.Fatal(err)
			}
			canceled := appendMaintenanceEvent(t, p, "canceled", "a", time.Now().Add(time.Hour), 32)
			cancelReq := mutation(canceled, "cancel", 0)
			if _, err = p.MutateEvent(context.Background(), cancelReq, lifecycle.Cancel); err != nil {
				t.Fatal(err)
			}
			backup := t.TempDir()
			if err = pm.Backup(backup); err != nil {
				t.Fatal(err)
			}
			if err = pm.Close(); err != nil {
				t.Fatal(err)
			}
			pm = nil
			pm, p = lifecyclePartition(t, cfg)
			got, err := p.EventState(req.Reference)
			if err != nil || got.CurrentOffset != state.CurrentOffset || p.ActiveEvent(e) {
				t.Fatalf("restart state: %v %v", got, err)
			}
			if canceledState, err := p.EventState(cancelReq.Reference); err != nil || !canceledState.GetCanceled() || p.ActiveEvent(canceled) {
				t.Fatalf("cancellation did not survive restart: %v %v", canceledState, err)
			}
			if err = pm.StartPartition(0); err != nil {
				t.Fatal(err)
			}
			if p.Scheduler.GetColdStoreCount() != 1 {
				t.Fatalf("restart restored obsolete timers: %d", p.Scheduler.GetColdStoreCount())
			}
			if err = pm.Close(); err != nil {
				t.Fatal(err)
			}
			pm = nil
			cfg.EventLifecycle = false
			blocked := NewPartitionManager("disabled", cfg)
			if err = blocked.CreatePartition(0, "a"); err == nil {
				blocked.Close()
				t.Fatal("opened lifecycle data without feature")
			}
			blocked.Close()
			cfg.EventLifecycle = true
			cfg.DataDir = t.TempDir()
			restore := filepath.Join(cfg.DataDir, "partitions", "0")
			if err = storage.RestoreWAL(filepath.Join(backup, "partitions", "0"), restore); err != nil {
				t.Fatal(err)
			}
			pm, p = lifecyclePartition(t, cfg)
			got, err = p.EventState(req.Reference)
			if err != nil || got.CurrentOffset != state.CurrentOffset || p.ActiveEvent(e) {
				t.Fatalf("restored state: %v %v", got, err)
			}
			if canceledState, err := p.EventState(cancelReq.Reference); err != nil || !canceledState.GetCanceled() || p.ActiveEvent(canceled) {
				t.Fatalf("cancellation did not survive backup: %v %v", canceledState, err)
			}
		})
	}
}

func TestLifecycleRelocationFreesPrefixAndKeepsStableIdentity(t *testing.T) {
	cfg := lifecycleConfig(t)
	cfg.WALRelocationBytes = 4096
	pm, p := lifecyclePartition(t, cfg)
	defer pm.Close()
	if err := p.ConsumerGroup.CreateGroup("g", "a", []int32{0}); err != nil {
		t.Fatal(err)
	}
	future := appendMaintenanceEvent(t, p, "future", "a", time.Now().Add(24*time.Hour), 2048)
	done := appendMaintenanceEvent(t, p, "done", "a", time.Now().Add(-time.Hour), 2048)
	appendMaintenanceEvent(t, p, "tail", "a", time.Now().Add(-time.Hour), 32)
	if err := p.ConsumerGroup.CommitDelivery("g", 0, []*types.Event{done}); err != nil {
		t.Fatal(err)
	}
	n, err := p.PruneWAL(context.Background(), storage.PruneOptions{AllCompleted: true})
	if err != nil || n < 2 {
		t.Fatalf("prefix still pinned: removed=%d error=%v", n, err)
	}
	state, err := p.EventState(lifecycle.Reference(future))
	if err != nil || state.CurrentOffset <= done.Offset || state.RevisionOffset != future.Offset {
		t.Fatalf("lost stable identity: %v %v", state, err)
	}
	copy, err := p.Wal.ReadEvent(state.CurrentOffset)
	if err != nil || !p.ActiveEvent(copy) || string(copy.Payload) != string(future.Payload) {
		t.Fatalf("lost replacement: %v", err)
	}
	if p.ConsumerGroup.IsCompleted("g", 0, copy.Offset) {
		t.Fatal("moving a pending event inherited completion")
	}
	if p.Wal.GetFirstOffset() <= future.Offset {
		t.Fatal("old copy was retained")
	}
	req := mutation(future, "cancel-after-move", 0)
	if _, err = p.MutateEvent(context.Background(), req, lifecycle.Cancel); err != nil {
		t.Fatalf("stable reference after pruning: %v", err)
	}
}

func TestLifecycleRelocationBudgetAndConsumerBlocker(t *testing.T) {
	cfg := lifecycleConfig(t)
	cfg.SegmentSizeBytes = 6000
	cfg.WALRelocationBytes = 3000
	pm, p := lifecyclePartition(t, cfg)
	defer pm.Close()
	if err := p.ConsumerGroup.CreateGroup("g", "a", []int32{0}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		appendMaintenanceEvent(t, p, fmt.Sprint(i), "a", time.Now().Add(time.Hour), 2048)
	}
	due := appendMaintenanceEvent(t, p, "due", "a", time.Now().Add(-time.Hour), 2048)
	before := p.Wal.GetLastOffset()
	if n, err := p.PruneWAL(context.Background(), storage.PruneOptions{AllCompleted: true}); err != nil || n != 0 || p.Wal.GetLastOffset() != before+1 {
		t.Fatalf("unbounded first pass: removed=%d end=%d error=%v", n, p.Wal.GetLastOffset(), err)
	}
	// Only one of the three live records fits per pass. The due event remains
	// pending in the next segment even after the oldest segment is evacuated.
	for i := 0; i < 2; i++ {
		if _, err := p.PruneWAL(context.Background(), storage.PruneOptions{AllCompleted: true}); err != nil {
			t.Fatal(err)
		}
	}
	if p.Wal.GetFirstOffset() != due.Offset {
		t.Fatalf("consumer obligation was bypassed: first=%d due=%d", p.Wal.GetFirstOffset(), due.Offset)
	}
}

func TestLifecycleDeliveryGateExcludesConcurrentCancellation(t *testing.T) {
	pm, p := lifecyclePartition(t, lifecycleConfig(t))
	defer pm.Close()
	e := appendMaintenanceEvent(t, p, "future", "a", time.Now().Add(time.Hour), 32)
	entered, release, sent := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		sent <- p.WithActiveEvent(e, func(*types.Event) error { close(entered); <-release; return nil })
	}()
	<-entered
	canceled := make(chan error, 1)
	go func() {
		_, err := p.MutateEvent(context.Background(), mutation(e, "cancel", 0), lifecycle.Cancel)
		canceled <- err
	}()
	select {
	case err := <-canceled:
		t.Fatalf("mutation crossed an active send: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	if err := <-canceled; err != nil {
		t.Fatal(err)
	}
	if err := p.WithActiveEvent(e, func(*types.Event) error { t.Error("canceled event replayed"); return nil }); err != nil {
		t.Fatal(err)
	}
	stream := &collectingStream{}
	sub := &delivery.Subscription{ID: "g:0:one", ConsumerGroup: "g", Topic: "a", Partition: &types.Partition{ID: 0}, MaxCredits: 10, Stream: stream}
	if err := p.Dispatcher.Subscribe(sub); err != nil {
		t.Fatal(err)
	}
	defer p.Dispatcher.Unsubscribe(sub.ID)
	// A worker may already own an obsolete ready reference when cancellation
	// commits. Even that stale ready event must not reach the stream.
	e.ScheduleTs = 1
	if err := p.Dispatcher.DispatchBatch([]*types.Event{e}); err != nil {
		t.Fatal(err)
	}
	if len(stream.got()) != 0 {
		t.Fatal("stale queued event was delivered")
	}
}

// A durable local append can still have an unresolved acceptance outcome.
// HoldUnaccepted supplies that boundary without enabling the cluster protocol.
func TestLifecycleStagedMutationCannotReleasePrefixOrReportSuccess(t *testing.T) {
	for _, kind := range []string{lifecycle.Cancel, lifecycle.Reschedule, lifecycle.Relocate} {
		t.Run(kind, func(t *testing.T) {
			pm, p := lifecyclePartition(t, lifecycleConfig(t))
			defer pm.Close()
			original := appendMaintenanceEvent(t, p, "pending", "a", time.Now().Add(time.Hour), 2048)
			at := original.ScheduleTs
			request := "staged"
			if kind == lifecycle.Cancel {
				at = 0
			} else if kind == lifecycle.Relocate {
				request = ""
			}
			next, err := lifecycle.Replacement(original, kind, request, at)
			if err != nil {
				t.Fatal(err)
			}
			if err := p.Wal.AppendEvent(next); err != nil {
				t.Fatal(err)
			}
			if err := p.Wal.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := p.lifecycleIndex.Apply(next); err != nil {
				t.Fatal(err)
			}
			p.HoldUnaccepted([]*types.Event{next}, false)
			appendMaintenanceEvent(t, p, "tail", "a", time.Now().Add(time.Hour), 2048)
			if through := p.AcceptedThrough(); through >= next.Offset {
				t.Fatalf("fixture accepted staged offset %d through %d", next.Offset, through)
			}
			if n, err := p.PruneWAL(context.Background(), storage.PruneOptions{AllCompleted: true}); err != nil || n != 0 || p.Wal.GetFirstOffset() != original.Offset {
				t.Fatalf("unaccepted successor released original: removed=%d first=%d error=%v", n, p.Wal.GetFirstOffset(), err)
			}
			for _, e := range []*types.Event{original, next} {
				if err := p.WithActiveEvent(e, func(*types.Event) error {
					t.Error("replayed unresolved mutation or its frozen predecessor")
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			if state, err := p.EventState(lifecycle.Reference(original)); status.Code(err) != codes.Unavailable || state != nil {
				t.Fatalf("staged mutation reported as accepted state: %v %v", state, err)
			}
			if kind != lifecycle.Relocate {
				if state, err := p.MutateEvent(context.Background(), mutation(original, request, at), kind); status.Code(err) != codes.Unavailable || state != nil {
					t.Fatalf("retry reported success before acceptance: %v %v", state, err)
				}
			}
			if err := p.AcceptThrough(next.Offset); err != nil {
				t.Fatal(err)
			}
			if state, err := p.EventState(lifecycle.Reference(original)); err != nil || state.GetCurrentOffset() != next.Offset || state.GetCanceled() != (kind == lifecycle.Cancel) {
				t.Fatalf("accepted state not visible: %v %v", state, err)
			}
			sends := 0
			if err := p.WithActiveEvent(next, func(e *types.Event) error {
				sends++
				if lifecycle.HasReservedMeta(e) {
					t.Error("internal envelope reached replay")
				}
				return nil
			}); err != nil || (sends == 1) != (kind != lifecycle.Cancel) {
				t.Fatalf("accepted replay: sends=%d error=%v", sends, err)
			}
			if n, err := p.PruneWAL(context.Background(), storage.PruneOptions{AllCompleted: true}); err != nil || n == 0 || p.Wal.GetFirstOffset() <= original.Offset {
				t.Fatalf("accepted mutation still pins original: removed=%d first=%d error=%v", n, p.Wal.GetFirstOffset(), err)
			}
		})
	}
}

func TestLifecycleCutLogRebuildsBeforeOffsetReuse(t *testing.T) {
	pm, p := lifecyclePartition(t, lifecycleConfig(t))
	defer pm.Close()
	original := appendMaintenanceEvent(t, p, "pending", "a", time.Now().Add(time.Hour), 2048)
	state, err := p.MutateEvent(context.Background(), mutation(original, "old-reschedule", original.ScheduleTs+1000), lifecycle.Reschedule)
	if err != nil {
		t.Fatal(err)
	}
	old, err := p.Wal.ReadEvent(state.CurrentOffset)
	if err != nil {
		t.Fatal(err)
	}
	p.ReplicateMu.Lock()
	removed, err := p.CutLog(old.Offset)
	p.ReplicateMu.Unlock()
	if err != nil || removed != 1 {
		t.Fatalf("suffix cut: removed=%d error=%v", removed, err)
	}
	state, err = p.EventState(lifecycle.Reference(original))
	if err != nil || state.GetCurrentOffset() != original.Offset || !p.ActiveEvent(original) {
		t.Fatalf("cut left old mutation state: %v %v", state, err)
	}
	if p.ActiveEvent(old) || p.Wal.Failure() != nil {
		t.Fatalf("discarded worker reference broke the recovered WAL: %v", p.Wal.Failure())
	}
	p.Wal.SetCurrentTerm(old.Term + 1)
	state, err = p.MutateEvent(context.Background(), mutation(original, "new-reschedule", original.ScheduleTs+2000), lifecycle.Reschedule)
	if err != nil || state.GetCurrentOffset() != old.Offset || state.GetRevisionTerm() <= old.Term {
		t.Fatalf("replacement generation: %v %v", state, err)
	}
	if p.ActiveEvent(old) {
		t.Fatal("cached old term became active at a reused mutation offset")
	}
}

func TestLifecycleFollowLogStartDoesNotResurrectDiscardedReferences(t *testing.T) {
	pm, p := lifecyclePartition(t, lifecycleConfig(t))
	defer pm.Close()
	if err := p.ConsumerGroup.CreateGroup("g", "a", []int32{p.ID}); err != nil {
		t.Fatal(err)
	}
	original := appendMaintenanceEvent(t, p, "pending", "a", time.Now().Add(time.Hour), 2048)
	state, err := p.MutateEvent(context.Background(), mutation(original, "reschedule", original.ScheduleTs+1000), lifecycle.Reschedule)
	if err != nil {
		t.Fatal(err)
	}
	current, err := p.Wal.ReadEvent(state.CurrentOffset)
	if err != nil {
		t.Fatal(err)
	}
	// The accepted leader is ahead of this replica's whole retained log.
	p.leader.Store(false)
	start := p.Wal.GetNextOffset() + 10
	p.ReplicateMu.Lock()
	err = p.FollowLogStart(start)
	p.ReplicateMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range []*types.Event{original, current} {
		if p.ActiveEvent(e) {
			t.Fatalf("discarded reference resurrected after reset: offset=%d", e.Offset)
		}
	}
	if state, err := p.EventState(lifecycle.Reference(original)); status.Code(err) != codes.NotFound || state != nil {
		t.Fatalf("discarded mutation remained resolvable: %v %v", state, err)
	}
	pending := appendMaintenanceEvent(t, p, "retained", "a", time.Now().Add(time.Hour), 32)
	if pending.Offset != start || p.ConsumerGroup.IsCompleted("g", p.ID, pending.Offset) || !p.ActiveEvent(pending) {
		t.Fatal("reset completed or suppressed new work at the retained start")
	}
}

func TestLifecycleRetentionFailsClosedOnIncompleteIndex(t *testing.T) {
	pm, p := lifecyclePartition(t, lifecycleConfig(t))
	defer pm.Close()
	if err := p.ConsumerGroup.CreateGroup("g", "a", []int32{p.ID}); err != nil {
		t.Fatal(err)
	}
	done := appendMaintenanceEvent(t, p, "done", "a", time.Now().Add(-time.Hour), 2048)
	appendMaintenanceEvent(t, p, "tail", "a", time.Now().Add(time.Hour), 32)
	if err := p.ConsumerGroup.CommitDelivery("g", p.ID, []*types.Event{done}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.lifecycleIndex.Rebuild(ctx, p.Wal); err == nil {
		t.Fatal("canceled rebuild succeeded")
	}
	n, err := p.PruneWAL(context.Background(), storage.PruneOptions{AllCompleted: true})
	if err == nil || n != 0 || p.Wal.GetFirstOffset() != done.Offset || p.Wal.Failure() == nil {
		t.Fatalf("retention used incomplete index: removed=%d first=%d error=%v failure=%v", n, p.Wal.GetFirstOffset(), err, p.Wal.Failure())
	}
}

func TestLifecycleEvacuationWaitsForStagedMutationInOldestSegment(t *testing.T) {
	cfg := lifecycleConfig(t)
	cfg.SegmentSizeBytes, cfg.WALRelocationBytes = 6000, 4096
	pm, p := lifecyclePartition(t, cfg)
	defer pm.Close()
	original := appendMaintenanceEvent(t, p, "pending", "a", time.Now().Add(time.Hour), 2048)
	appendMaintenanceEvent(t, p, "other", "a", time.Now().Add(time.Hour), 2048)
	next, err := lifecycle.Replacement(original, lifecycle.Reschedule, "staged", original.ScheduleTs+1000)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Wal.AppendEvent(next); err != nil {
		t.Fatal(err)
	}
	if err := p.Wal.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := p.lifecycleIndex.Apply(next); err != nil {
		t.Fatal(err)
	}
	p.HoldUnaccepted([]*types.Event{next}, false)
	before := p.Wal.GetLastOffset()
	if n, err := p.PruneWAL(context.Background(), storage.PruneOptions{AllCompleted: true}); err != nil || n != 0 || p.Wal.GetLastOffset() != before {
		t.Fatalf("evacuated a segment still pinned by a staged mutation: removed=%d end=%d error=%v", n, p.Wal.GetLastOffset(), err)
	}
}

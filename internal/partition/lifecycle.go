package partition

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/lifecycle"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (p *Partition) LifecycleEnabled() bool { return p.lifecycleIndex != nil }

// WithActiveEvent checks and sends a replay record under the same gate as live
// delivery. An event canceled between the cursor read and send is suppressed.
func (p *Partition) WithActiveEvent(e *types.Event, send func(*types.Event) error) error {
	p.lifecycleMu.RLock()
	defer p.lifecycleMu.RUnlock()
	through := int64(math.MaxInt64)
	if p.lifecycleIndex != nil {
		through = p.DeliverableThrough()
	}
	if p.activeEventThrough(e, through) {
		return send(lifecycle.Public(e))
	}
	return p.Wal.Failure()
}

func (p *Partition) lifecycleError() error {
	if !p.LifecycleEnabled() {
		return status.Error(codes.FailedPrecondition, "event-lifecycle is disabled")
	}
	if p.replicated || p.followed {
		return status.Error(codes.FailedPrecondition, "event-lifecycle requires a standalone partition")
	}
	if err := p.Wal.Failure(); err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	return nil
}

// ActiveEvent is also used by replay and scheduler recovery. The delivery gate
// holds lifecycleMu across this test and stream.Send, excluding durable changes.
func (p *Partition) ActiveEvent(e *types.Event) bool {
	// Timer reconstruction includes staged successors. Sending them also
	// requires an accepted boundary, checked separately by delivery/replay.
	return p.activeEventThrough(e, math.MaxInt64)
}

func (p *Partition) activeEventThrough(e *types.Event, through int64) bool {
	if p.Wal.Failure() != nil {
		return false
	}
	if e == nil || e.Offset > through {
		return false
	}
	if p.lifecycleIndex == nil {
		if lifecycle.HasReservedMeta(e) {
			err := fmt.Errorf("lifecycle WAL requires --event-lifecycle")
			p.Wal.Fail(err)
			p.setReplayError(err)
			return false
		}
		return true
	}
	active, err := p.lifecycleIndex.DeliverableThrough(e, through)
	if err != nil {
		p.Wal.Fail(fmt.Errorf("event lifecycle lookup: %w", err))
		p.setReplayError(err)
		return false
	}
	return active
}

func (p *Partition) lifecycleState(ref *types.EventReference) (*types.Event, *lifecycle.Record, error) {
	if !lifecycle.ValidReference(ref) || ref.PartitionId != p.ID {
		return nil, nil, status.Error(codes.InvalidArgument, "valid event reference for this partition is required")
	}
	// Periodic publishes can acknowledge bytes still in the WAL's write
	// buffer. Disk readers must see that tail before resolving a fresh handle.
	if err := p.Wal.Flush(); err != nil {
		p.Wal.Fail(err)
		return nil, nil, status.Error(codes.Unavailable, err.Error())
	}
	offset := ref.Offset
	if head, found, err := p.lifecycleIndex.Latest(ref); err != nil {
		p.Wal.Fail(err)
		return nil, nil, status.Error(codes.Unavailable, err.Error())
	} else if found {
		offset = head
	}
	if offset < p.Wal.GetFirstOffset() || offset > p.Wal.GetLastOffset() {
		return nil, nil, status.Error(codes.NotFound, "event state is no longer retained")
	}
	if offset > p.AcceptedThrough() {
		return nil, nil, status.Error(codes.Unavailable, "event state has not been accepted; retry after its outcome is resolved")
	}
	e, err := p.Wal.ReadEvent(offset)
	if err != nil {
		return nil, nil, status.Error(codes.Unavailable, err.Error())
	}
	root, err := lifecycle.Root(e)
	if err != nil {
		p.Wal.Fail(err)
		return nil, nil, status.Error(codes.Unavailable, err.Error())
	}
	if !lifecycle.SameReference(root, ref) {
		return nil, nil, status.Error(codes.NotFound, "event reference does not match the retained event")
	}
	r, err := lifecycle.Decode(e)
	if err != nil {
		return nil, nil, err
	}
	return e, r, nil
}

func stateResponse(e *types.Event, r *lifecycle.Record) *types.EventStateResponse {
	root := lifecycle.Reference(e)
	if r != nil {
		root = r.Root
	}
	offset, term := lifecycle.Revision(e, r)
	return &types.EventStateResponse{Reference: root, CurrentOffset: e.Offset, RevisionOffset: offset, RevisionTerm: term, ScheduleTs: e.ScheduleTs, Canceled: r != nil && r.Kind == lifecycle.Cancel}
}

func (p *Partition) EventState(ref *types.EventReference) (*types.EventStateResponse, error) {
	p.lifecycleMu.RLock()
	defer p.lifecycleMu.RUnlock()
	if err := p.lifecycleError(); err != nil {
		return nil, err
	}
	e, r, err := p.lifecycleState(ref)
	if err != nil {
		return nil, err
	}
	return stateResponse(e, r), nil
}

func (p *Partition) MutateEvent(ctx context.Context, req *types.EventMutationRequest, kind string) (*types.EventStateResponse, error) {
	if req == nil || req.RequestId == "" || len(req.RequestId) > 128 || (kind != lifecycle.Cancel && kind != lifecycle.Reschedule) || req.ExpectedRevisionOffset < 0 || req.ExpectedRevisionTerm < 0 {
		return nil, status.Error(codes.InvalidArgument, "request ID and expected event revision are required")
	}
	if kind == lifecycle.Cancel && req.ScheduleTs != 0 {
		return nil, status.Error(codes.InvalidArgument, "cancel schedule_ts must be zero")
	}
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := p.lifecycleError(); err != nil {
		return nil, err
	}
	e, r, err := p.lifecycleState(req.Reference)
	if err != nil {
		return nil, err
	}
	if r != nil && r.RequestID == req.RequestId {
		// Relocation preserves the last user request and its revision. The
		// revision's predecessor is encoded there, independent of physical moves.
		if kind != r.RequestKind || (kind == lifecycle.Reschedule && req.ScheduleTs != e.ScheduleTs) || req.ExpectedRevisionOffset != r.ExpectedOffset || req.ExpectedRevisionTerm != r.ExpectedTerm {
			return nil, status.Error(codes.InvalidArgument, "request ID was already used with different mutation arguments")
		}
		return stateResponse(e, r), nil
	}
	rev, term := lifecycle.Revision(e, r)
	if req.ExpectedRevisionOffset != rev || req.ExpectedRevisionTerm != term {
		return nil, status.Error(codes.Aborted, "event revision changed; read its current state")
	}
	if r != nil && r.Kind == lifecycle.Cancel {
		return nil, status.Error(codes.FailedPrecondition, "event is canceled")
	}
	now := time.Now().UnixMilli()
	if e.ScheduleTs <= now {
		return nil, status.Error(codes.FailedPrecondition, "only future events can be changed")
	}
	if kind == lifecycle.Reschedule && req.ScheduleTs <= now {
		return nil, status.Error(codes.InvalidArgument, "new schedule must be in the future")
	}
	replacement, err := lifecycle.Replacement(e, kind, req.RequestId, req.ScheduleTs)
	if err != nil {
		return nil, err
	}
	if err = p.appendLifecycle(replacement, e); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	if kind == lifecycle.Cancel && p.Dispatcher != nil && p.Dispatcher.OnDeliveryComplete != nil && e.Meta["tenant_id"] != "" {
		// Cancellation is a final disposition, once per new durable command.
		// Exact retries above must not release the tenant's credit again.
		p.Dispatcher.OnDeliveryComplete(e.Meta["tenant_id"])
	}
	r, err = lifecycle.Decode(replacement)
	if err != nil {
		return nil, err
	}
	return stateResponse(replacement, r), nil
}

// appendLifecycle runs under lifecycleMu exclusively. Never make an old copy
// inactive until its self-contained successor and format marker are durable.
func (p *Partition) appendLifecycle(next, previous *types.Event) error {
	marker := filepath.Join(p.DataDir, lifecycle.Marker)
	if _, err := os.Stat(marker); errors.Is(err, os.ErrNotExist) {
		if err = utils.AtomicWriteFile(marker, []byte("1\n"), 0600); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if err := p.Wal.AppendEvent(next); err != nil {
		return err
	}
	if err := p.Wal.Flush(); err != nil {
		p.Wal.Fail(err)
		return err
	}
	if err := p.lifecycleIndex.Apply(next); err != nil {
		p.Wal.Fail(err)
		return err
	}
	if p.Scheduler != nil {
		if err := p.Scheduler.Remove(previous.Offset, previous.ScheduleTs); err != nil {
			p.Wal.Fail(err)
			return err
		}
		r, err := lifecycle.Decode(next)
		if err != nil {
			p.Wal.Fail(err)
			return err
		}
		if r.Kind != lifecycle.Cancel {
			if err := p.Scheduler.Schedule(next); err != nil {
				p.Wal.Fail(err)
				return err
			}
		}
	}
	return nil
}

package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client/internal/errs"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// EventReference identifies one published event, for GetEventState,
// CancelEvent and RescheduleEvent. Take it from SendResult.Reference when the
// event is published and store it with your own record of the event: the
// server cannot look an event up by message ID alone.
//
// The reference stays valid when the event is rescheduled; it stops resolving
// (ErrorKindNotFound) once the event is no longer retained.
type EventReference struct {
	// Topic is the topic the event was published to.
	Topic string
	// PartitionID is the partition that accepted the event.
	PartitionID int32
	// Offset is the log offset the event was first written at.
	Offset int64
	// Term is the leadership term of that write. With Offset it tells the
	// event apart from a different event written at the same offset after a
	// leader change.
	Term int64
	// MessageID is the event's message ID.
	MessageID string
}

// Revision names one version of an event. Every successful cancel or
// reschedule creates a new revision. A change must name the revision it
// replaces, so that two writers cannot overwrite each other's changes
// unknowingly.
type Revision struct {
	Offset int64
	Term   int64
}

// InitialRevision is the revision of an event that has not been changed since
// it was published: its own offset and term.
func (r EventReference) InitialRevision() Revision {
	return Revision{Offset: r.Offset, Term: r.Term}
}

// EventState is the current state of an event, as GetEventState and the
// mutation calls return it.
type EventState struct {
	// Reference is the event's original reference; it does not change when
	// the event is rescheduled.
	Reference EventReference
	// CurrentOffset is where the event's current version is in the log.
	CurrentOffset int64
	// Revision is the current revision; pass it as EventChange.ExpectedRevision
	// to change the event again.
	Revision Revision
	// ScheduleTS is when the event is due (Unix milliseconds).
	ScheduleTS int64
	// Canceled reports whether the event was canceled. A canceled event is
	// never delivered and cannot be changed again.
	Canceled bool
}

// ScheduleAt returns ScheduleTS as a time.Time.
func (s EventState) ScheduleAt() time.Time { return time.UnixMilli(s.ScheduleTS) }

// EventChange describes one cancel or reschedule request.
type EventChange struct {
	// RequestID identifies this change (required, at most 128 bytes). If a
	// call fails with an unknown outcome, retry it with the same RequestID
	// and the same arguments: the server applies the change at most once and
	// answers the retry with the result. Use a new RequestID for every new
	// change.
	RequestID string
	// ExpectedRevision is the revision the change replaces: EventState.Revision
	// from GetEventState or from the previous change, or
	// EventReference.InitialRevision for an event never changed. If the event
	// has changed since, the call fails with ErrorKindConflict.
	ExpectedRevision Revision
}

// GetEventState returns the current state of a published event. The server
// must run with --event-lifecycle, and the caller needs publish permission
// on the event's topic when authentication is enabled.
func (c *Client) GetEventState(ctx context.Context, ref EventReference) (*EventState, error) {
	return c.lifecycleCall(ctx, "event.get_state", ref, func(ctx context.Context, cl types.EventServiceClient) (*types.EventStateResponse, error) {
		return cl.GetEventState(ctx, &types.EventStateRequest{Reference: ref.toProto()})
	})
}

// CancelEvent cancels an event that is not yet due, so that it is never
// delivered. Requirements are as for GetEventState.
//
// Errors carry a Kind: ErrorKindConflict when the event changed after
// ExpectedRevision (read its state and decide again), ErrorKindPrecondition
// when it is already canceled, already due, or the server does not have
// lifecycle operations enabled, ErrorKindNotFound when the reference does not
// resolve, and ErrorKindUnavailable when the outcome is unknown: retry the
// same EventChange. A timeout or canceled context also leaves the outcome
// uncertain; retry the same change with a fresh context.
func (c *Client) CancelEvent(ctx context.Context, ref EventReference, change EventChange) (*EventState, error) {
	return c.mutateEvent(ctx, "event.cancel", ref, change, 0)
}

// RescheduleEvent moves an event that is not yet due to a new time in the
// future. Errors are as for CancelEvent.
func (c *Client) RescheduleEvent(ctx context.Context, ref EventReference, change EventChange, scheduleAt time.Time) (*EventState, error) {
	// Let the server check the time after looking up the request ID. An
	// unchanged retry must still resolve after its target time has passed.
	return c.mutateEvent(ctx, "event.reschedule", ref, change, scheduleAt.UnixMilli())
}

func (c *Client) mutateEvent(ctx context.Context, op string, ref EventReference, change EventChange, scheduleTS int64) (*EventState, error) {
	if change.RequestID == "" || len(change.RequestID) > 128 {
		return nil, wrapError(op, ErrorKindValidation, errors.New("a request ID of 1 to 128 bytes is required"))
	}
	req := &types.EventMutationRequest{
		Reference:              ref.toProto(),
		RequestId:              change.RequestID,
		ExpectedRevisionOffset: change.ExpectedRevision.Offset,
		ExpectedRevisionTerm:   change.ExpectedRevision.Term,
		ScheduleTs:             scheduleTS,
	}
	return c.lifecycleCall(ctx, op, ref, func(ctx context.Context, cl types.EventServiceClient) (*types.EventStateResponse, error) {
		if op == "event.cancel" {
			return cl.CancelEvent(ctx, req)
		}
		return cl.RescheduleEvent(ctx, req)
	})
}

// lifecycleRounds bounds how often a lifecycle call goes through the
// partition's candidate nodes when none of them settles it. The caller's
// deadline bounds it too.
const lifecycleRounds = 4

// lifecycleCall sends a lifecycle request to the node that leads the event's
// partition. Every node that does not lead it says so, and the next one is
// asked. An unknown outcome is retried as it is: the request is the same, so
// the server applies it at most once.
func (c *Client) lifecycleCall(ctx context.Context, op string, ref EventReference, call func(context.Context, types.EventServiceClient) (*types.EventStateResponse, error)) (*EventState, error) {
	if ref.MessageID == "" || ref.PartitionID < 0 || ref.Offset < 0 || ref.Term < 0 {
		return nil, wrapError(op, ErrorKindValidation, errors.New("a complete event reference (topic, partition, offset, term and message ID) is required"))
	}
	var lastErr error
	for round := 0; round < lifecycleRounds; round++ {
		if round > 0 {
			select {
			case <-ctx.Done():
				return nil, wrapError(op, ErrorKindTimeout, fmt.Errorf("%w (last error: %v)", ctx.Err(), lastErr))
			case <-time.After(time.Duration(round) * 100 * time.Millisecond):
			}
		}
		route, err := c.RouteForPartition(ref.PartitionID)
		if err != nil {
			lastErr = err
			continue
		}
		for _, addr := range route.CandidateAddresses {
			resp, err := c.lifecycleCallAt(ctx, op, addr, call)
			if err == nil {
				c.noteServed(ref.PartitionID, addr)
				return stateFromProto(resp), nil
			}
			lastErr = err
			if ctx.Err() != nil {
				return nil, wrapError(op, ErrorKindTimeout, err)
			}
			if errs.IsLeaderRelated(err) {
				// Another node leads the partition.
				c.noteRefused(ref.PartitionID, addr)
				c.MarkMetadataStale()
				continue
			}
			if status.Code(err) == codes.Unavailable {
				// The outcome is unknown, or the node is unreachable or not
				// ready; the same request may be retried anywhere.
				continue
			}
			kind := lifecycleErrorKind(err)
			c.observeError(op, kind, err)
			return nil, wrapError(op, kind, err)
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no node known for partition %d", ref.PartitionID)
	}
	c.observeError(op, ErrorKindUnavailable, lastErr)
	return nil, wrapError(op, ErrorKindUnavailable, lastErr)
}

func (c *Client) lifecycleCallAt(ctx context.Context, op, addr string, call func(context.Context, types.EventServiceClient) (*types.EventStateResponse, error)) (*types.EventStateResponse, error) {
	cl, err := c.eventClientForAddress(addr)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	reqCtx, cancel := c.requestContext(ctx)
	defer cancel()
	start := time.Now()
	resp, err := call(reqCtx, cl)
	c.observeRequest(op, addr, start, err)
	return resp, err
}

// lifecycleErrorKind classifies a final answer to a lifecycle call.
func lifecycleErrorKind(err error) ErrorKind {
	switch status.Code(err) {
	case codes.InvalidArgument:
		return ErrorKindValidation
	case codes.NotFound:
		return ErrorKindNotFound
	case codes.Aborted:
		return ErrorKindConflict
	case codes.FailedPrecondition:
		return ErrorKindPrecondition
	case codes.ResourceExhausted:
		return ErrorKindOverloaded
	case codes.Unauthenticated, codes.PermissionDenied:
		return ErrorKindAuth
	case codes.DeadlineExceeded, codes.Canceled:
		return ErrorKindTimeout
	default:
		return ErrorKindInternal
	}
}

func (r EventReference) toProto() *types.EventReference {
	return &types.EventReference{PartitionId: r.PartitionID, Offset: r.Offset, Term: r.Term, MessageId: r.MessageID, Topic: r.Topic}
}

func referenceFromProto(r *types.EventReference) EventReference {
	return EventReference{
		Topic:       r.GetTopic(),
		PartitionID: r.GetPartitionId(),
		Offset:      r.GetOffset(),
		Term:        r.GetTerm(),
		MessageID:   r.GetMessageId(),
	}
}

func stateFromProto(s *types.EventStateResponse) *EventState {
	return &EventState{
		Reference:     referenceFromProto(s.GetReference()),
		CurrentOffset: s.GetCurrentOffset(),
		Revision:      Revision{Offset: s.GetRevisionOffset(), Term: s.GetRevisionTerm()},
		ScheduleTS:    s.GetScheduleTs(),
		Canceled:      s.GetCanceled(),
	}
}

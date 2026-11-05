package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client/internal/errs"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// ReplayRequest describes historical replay query options for Client.Replay.
type ReplayRequest struct {
	// Topic is the topic to replay (required). Only its events are returned.
	Topic string
	// PartitionID is the partition to replay (required; must be >= 0, and 0 is
	// partition 0). A replay reads one partition.
	PartitionID int32

	// StartOffset is the first offset to include when replaying by offset.
	StartOffset int64
	// EndOffset is the last offset to include when replaying by offset. When
	// StartOffset >= 0 and EndOffset >= StartOffset, the request asks for
	// EndOffset-StartOffset+1 events and Count is ignored. A zero EndOffset is
	// treated as unset when Count is positive, so Count alone works; with both
	// offsets and Count at zero the request asks for one event (offset 0).
	// ReplayByOffsetRange sets both offsets.
	EndOffset int64
	// Count is the most events to return when replaying by offset and the
	// range above does not set it; the server uses 1000 when it is not
	// positive. A time-based replay ignores it.
	Count int64

	// StartTime selects events whose schedule time is at or after it. A replay
	// with StartTime or EndTime set is time-based; it reads the whole partition
	// log as of the request and returns the events in offset order.
	StartTime time.Time
	// EndTime selects events whose schedule time is at or before it; when only
	// StartTime is set the server uses the current time.
	EndTime time.Time

	// ConsumerGroup is an optional group ID associated with the replay session.
	// The server does not act on it: a replay neither reads nor changes a
	// group's progress.
	ConsumerGroup string
	// SubscriptionID is an optional client-visible subscription label for the stream
	// (not acted on by the server).
	SubscriptionID string
	// Speed paces the replay. 0 and 1 send events without a pause. Any other
	// value pauses 10 ms divided by Speed after each event (2 gives 5 ms, 0.5
	// gives 20 ms). The server refuses a negative Speed or one below 0.001.
	Speed float64
}

// ReplayEvent is one event emitted on a Replay stream.
type ReplayEvent struct {
	// Event is the historical event payload and metadata.
	Event *types.Event
	// ReplayOffset is the event's position in this replay's range, counting
	// from 0 (including events of other topics that the replay skipped). It is
	// not a log offset; use Event.Offset for that.
	ReplayOffset int64
}

// ReplayHandler is invoked for each replayed event. Return ErrStopReplay to stop cleanly.
type ReplayHandler func(context.Context, ReplayEvent) error

// ErrStopReplay can be returned by ReplayHandler to stop replay without error.
var ErrStopReplay = errors.New("stop replay")

// Replay streams historical events for a partition.
func (c *Client) Replay(ctx context.Context, req ReplayRequest, handler ReplayHandler) error {
	if req.Topic == "" {
		return wrapError("client.replay", ErrorKindValidation, fmt.Errorf("topic is required"))
	}
	if req.PartitionID < 0 {
		return wrapError("client.replay", ErrorKindValidation, fmt.Errorf("partition_id is required"))
	}

	protoReq := req.toProto()
	route, err := c.RouteForPartition(req.PartitionID)
	if err != nil {
		return wrapError("client.replay", ErrorKindMetadataStale, err)
	}

	var lastErr error
	for _, addr := range route.CandidateAddresses {
		eventClient, clientErr := c.eventClientForAddress(addr)
		if clientErr != nil {
			lastErr = clientErr
			continue
		}

		streamCtx, cancel := context.WithCancel(ctx)
		start := time.Now()
		stream, streamErr := eventClient.Replay(streamCtx, protoReq)
		c.observeRequest("event.replay.open", addr, start, streamErr)
		if streamErr != nil {
			cancel()
			lastErr = streamErr
			if errs.IsLeaderRelated(streamErr) {
				c.MarkMetadataStale()
			}
			if errs.IsRetryable(streamErr) {
				continue
			}
			return wrapError("client.replay", ErrorKindTransport, streamErr)
		}

		for {
			start = time.Now()
			replayEvent, recvErr := stream.Recv()
			c.observeRequest("event.replay.recv", addr, start, recvErr)
			if recvErr == io.EOF {
				cancel()
				return nil
			}
			if recvErr != nil {
				cancel()
				lastErr = recvErr
				if errs.IsLeaderRelated(recvErr) {
					c.MarkMetadataStale()
				}
				break
			}
			if handler != nil {
				if handlerErr := handler(ctx, ReplayEvent{
					Event:        replayEvent.GetEvent(),
					ReplayOffset: replayEvent.GetReplayOffset(),
				}); handlerErr != nil {
					cancel()
					if errors.Is(handlerErr, ErrStopReplay) {
						return nil
					}
					return wrapError("client.replay_handler", ErrorKindValidation, handlerErr)
				}
			}
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no replay route candidates available")
	}
	return wrapError("client.replay", ErrorKindUnavailable, lastErr)
}

// ReplayByOffsetRange replays events from [startOffset, endOffset].
func (c *Client) ReplayByOffsetRange(ctx context.Context, topic string, partitionID int32, startOffset int64, endOffset int64, handler ReplayHandler) error {
	req := ReplayRequest{
		Topic:       topic,
		PartitionID: partitionID,
		StartOffset: startOffset,
		EndOffset:   endOffset,
	}
	return c.Replay(ctx, req, handler)
}

// ReplayByTimeRange replays events between [startTime, endTime].
func (c *Client) ReplayByTimeRange(ctx context.Context, topic string, partitionID int32, startTime time.Time, endTime time.Time, handler ReplayHandler) error {
	req := ReplayRequest{
		Topic:       topic,
		PartitionID: partitionID,
		StartTime:   startTime,
		EndTime:     endTime,
	}
	return c.Replay(ctx, req, handler)
}

// SeekEarliest returns the earliest replayable offset for a partition.
func (c *Client) SeekEarliest(ctx context.Context, partitionID int32) (int64, error) {
	status, err := c.getWALStatus(ctx, partitionID)
	if err != nil {
		return 0, wrapError("client.seek_earliest", ErrorKindMetadataStale, err)
	}
	return status.GetFirstOffset(), nil
}

// SeekLatest returns the offset that points to new events after current tail.
func (c *Client) SeekLatest(ctx context.Context, partitionID int32) (int64, error) {
	info, err := c.getPartitionInfo(ctx, partitionID)
	if err != nil {
		return 0, wrapError("client.seek_latest", ErrorKindMetadataStale, err)
	}
	return info.GetLastOffset() + 1, nil
}

// SeekTimestamp resolves the first offset at/after timestamp.
func (c *Client) SeekTimestamp(ctx context.Context, topic string, partitionID int32, ts time.Time) (int64, error) {
	if ts.IsZero() {
		return c.SeekEarliest(ctx, partitionID)
	}

	resolved := int64(-1)
	err := c.Replay(ctx, ReplayRequest{
		Topic:       topic,
		PartitionID: partitionID,
		StartTime:   ts,
		Count:       1,
	}, func(_ context.Context, ev ReplayEvent) error {
		if resolved < 0 && ev.Event != nil {
			resolved = ev.Event.GetOffset()
			return ErrStopReplay
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if resolved >= 0 {
		return resolved, nil
	}
	return c.SeekLatest(ctx, partitionID)
}

// ReplayToLive replays history and then transitions to live subscription. The
// handler gets replayed events as Deliveries with no acknowledgement to send
// (Delivery.Ack returns an error), and live ones as for Subscribe. When
// liveCfg.PartitionID is negative the live subscription uses
// replayReq.PartitionID, and when liveCfg.StartOffset is negative it starts
// after the last replayed offset.
func (c *Client) ReplayToLive(ctx context.Context, replayReq ReplayRequest, liveCfg ConsumerConfig, handler MessageHandler) error {
	var resumeOffset int64 = replayReq.StartOffset
	if resumeOffset < 0 {
		resumeOffset = 0
	}

	err := c.Replay(ctx, replayReq, func(ctx context.Context, ev ReplayEvent) error {
		if ev.Event != nil {
			if ev.Event.GetOffset()+1 > resumeOffset {
				resumeOffset = ev.Event.GetOffset() + 1
			}
			return handler(ctx, Delivery{
				Event:      ev.Event,
				DeliveryID: fmt.Sprintf("replay-%d", ev.ReplayOffset),
				Attempt:    1,
			})
		}
		return nil
	})
	if err != nil {
		return err
	}

	nextCfg := liveCfg
	if nextCfg.Topic == "" {
		nextCfg.Topic = replayReq.Topic
	}
	if nextCfg.PartitionID < 0 {
		nextCfg.PartitionID = replayReq.PartitionID
	}
	if nextCfg.StartOffset < 0 {
		nextCfg.StartOffset = resumeOffset
	}
	return c.Subscribe(ctx, nextCfg, handler)
}

func (c *Client) getPartitionInfo(ctx context.Context, partitionID int32) (*types.PartitionInfo, error) {
	route, err := c.RouteForPartition(partitionID)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for _, addr := range route.CandidateAddresses {
		partitionClient, clientErr := c.partitionClientForAddress(addr)
		if clientErr != nil {
			lastErr = clientErr
			continue
		}
		reqCtx, cancel := c.requestContext(ctx)
		start := time.Now()
		info, reqErr := partitionClient.GetPartition(reqCtx, &types.GetPartitionRequest{PartitionId: partitionID})
		cancel()
		c.observeRequest("partition.get", addr, start, reqErr)
		if reqErr != nil {
			lastErr = reqErr
			continue
		}
		if info != nil {
			return info, nil
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("partition %d not found", partitionID)
	}
	return nil, lastErr
}

// getWALStatus fetches WAL status for a partition from one of the candidate nodes.
func (c *Client) getWALStatus(ctx context.Context, partitionID int32) (*types.WALStatus, error) {
	route, err := c.RouteForPartition(partitionID)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for _, addr := range route.CandidateAddresses {
		partitionClient, clientErr := c.partitionClientForAddress(addr)
		if clientErr != nil {
			lastErr = clientErr
			continue
		}
		reqCtx, cancel := c.requestContext(ctx)
		start := time.Now()
		status, reqErr := partitionClient.GetWALStatus(reqCtx, &types.GetWALStatusRequest{PartitionId: partitionID})
		cancel()
		c.observeRequest("partition.wal_status", addr, start, reqErr)
		if reqErr != nil {
			lastErr = reqErr
			continue
		}
		if status != nil {
			return status, nil
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("partition %d wal status not found", partitionID)
	}
	return nil, lastErr
}

func (r ReplayRequest) toProto() *types.ReplayRequest {
	protoReq := &types.ReplayRequest{
		Topic:          r.Topic,
		PartitionId:    r.PartitionID,
		StartOffset:    r.StartOffset,
		Count:          r.Count,
		ConsumerGroup:  r.ConsumerGroup,
		SubscriptionId: r.SubscriptionID,
		Speed:          r.Speed,
	}
	if !r.StartTime.IsZero() {
		protoReq.StartTs = r.StartTime.UnixMilli()
	}
	if !r.EndTime.IsZero() {
		protoReq.EndTs = r.EndTime.UnixMilli()
	}
	// A zero EndOffset next to a zero StartOffset is more likely an unset range
	// than a request for offset 0 alone, so a positive Count wins there.
	if r.StartOffset >= 0 && r.EndOffset >= r.StartOffset && (r.EndOffset > 0 || r.Count <= 0) {
		protoReq.Count = (r.EndOffset - r.StartOffset) + 1
	}
	return protoReq
}

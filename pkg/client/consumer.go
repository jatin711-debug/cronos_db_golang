package client

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client/internal/errs"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"

	"google.golang.org/grpc"
)

var subscriptionCounter atomic.Uint64

// AckMode controls whether acknowledgements are sent automatically or manually.
type AckMode string

const (
	// AckModeAuto acks successful handler returns without an explicit Ack call.
	AckModeAuto AckMode = "auto"
	// AckModeManual requires the handler to call Delivery.Ack* methods.
	AckModeManual AckMode = "manual"
)

// CommitStrategy controls whether ack RPCs are flushed asynchronously or synchronously.
type CommitStrategy string

const (
	// CommitStrategyAsync batches acks and flushes in the background.
	CommitStrategyAsync CommitStrategy = "async"
	// CommitStrategySync waits for ack RPC completion before returning from Ack.
	CommitStrategySync CommitStrategy = "sync"
)

// Assignment carries consumer assignment details for callback hooks.
type Assignment struct {
	// Topic is the subscribed topic.
	Topic string
	// ConsumerGroup is the consumer group ID.
	ConsumerGroup string
	// PartitionID is the partition this subscription is consuming.
	PartitionID int32
	// SubscriptionID is the client-generated or server-visible subscription identifier.
	SubscriptionID string
	// NodeAddress is the gRPC address of the node serving the stream.
	NodeAddress string
}

// OffsetCheckpointStore persists committed offsets for recovery/resume across process restarts.
type OffsetCheckpointStore interface {
	// LoadOffset returns the last saved next-offset, or a store-defined sentinel when missing.
	LoadOffset(ctx context.Context, consumerGroup string, topic string, partitionID int32) (int64, error)
	// SaveOffset records nextOffset after a successful commit progress.
	SaveOffset(ctx context.Context, consumerGroup string, topic string, partitionID int32, nextOffset int64) error
}

// Delivery wraps a server delivery payload for handler processing and ack control.
type Delivery struct {
	// Event is the single event when the server did not batch deliveries.
	Event *types.Event
	// Batch is a multi-event delivery payload when the server batches.
	Batch []*types.Event
	// DeliveryID is the server-assigned ID required for Ack RPCs.
	DeliveryID string
	// Attempt is the 1-based delivery attempt count from the server.
	Attempt int32

	acker *deliveryAcker // internal ack sender bound to this subscription stream
}

// LastOffset returns the highest event offset in this delivery payload.
func (d Delivery) LastOffset() int64 {
	if len(d.Batch) > 0 {
		return d.Batch[len(d.Batch)-1].GetOffset()
	}
	if d.Event != nil {
		return d.Event.GetOffset()
	}
	return -1
}

// AckSuccess commits successful processing for this delivery.
func (d Delivery) AckSuccess(ctx context.Context) error {
	return d.Ack(ctx, true, d.LastOffset()+1, nil)
}

// AckFailure marks this delivery failed for retry/dead-letter handling.
func (d Delivery) AckFailure(ctx context.Context, processingErr error) error {
	return d.Ack(ctx, false, d.LastOffset()+1, processingErr)
}

// Ack sends an acknowledgement for this delivery.
func (d Delivery) Ack(ctx context.Context, success bool, nextOffset int64, processingErr error) error {
	if d.acker == nil {
		return fmt.Errorf("delivery ack is unavailable")
	}
	ack := &types.AckRequest{
		DeliveryId: d.DeliveryID,
		Success:    success,
		NextOffset: nextOffset,
	}
	if processingErr != nil {
		ack.Error = processingErr.Error()
	}
	return d.acker.send(ctx, ack)
}

// Decode decodes a single-event delivery payload using the provided codec.
func (d Delivery) Decode(codec Codec, out any) error {
	if d.Event == nil {
		return fmt.Errorf("decode requires a single event delivery")
	}
	return DecodePayload(codec, d.Event.GetPayload(), out)
}

// MessageHandler is invoked by the consumer for each delivery.
type MessageHandler func(context.Context, Delivery) error

// ConsumerConfig controls consumer stream lifecycle, ack behavior, and reconnect policy.
type ConsumerConfig struct {
	// Topic is the topic to subscribe to (required).
	Topic string
	// ConsumerGroup is the consumer group ID used for offset tracking (required).
	ConsumerGroup string
	// SubscriptionID optionally labels this subscription; empty lets the client generate one.
	SubscriptionID string

	// PartitionID selects a partition. -1 uses server auto-assignment.
	PartitionID int32
	// StartOffset is the initial read offset. -1 means latest (server semantics).
	StartOffset int64

	// MaxBufferSize is the max events the server may buffer for this subscription.
	MaxBufferSize int32
	// WorkerConcurrency is how many handler goroutines process deliveries concurrently.
	WorkerConcurrency int

	// AutoAck is kept for backward compatibility. Prefer AckMode.
	AutoAck bool
	// AckMode selects automatic vs manual acknowledgements.
	AckMode AckMode

	// AckQueueSize is the capacity of the outbound ack queue.
	AckQueueSize int
	// AckBatchSize is the max acks coalesced per flush when using async commits.
	AckBatchSize int
	// AckFlushInterval is the max time between async ack flushes.
	AckFlushInterval time.Duration
	// CommitStrategy selects async batching vs sync ack RPCs.
	CommitStrategy CommitStrategy
	// CheckpointStore optionally persists local resume offsets (nil disables local checkpoints).
	CheckpointStore OffsetCheckpointStore
	// ResumeCheckpoint loads StartOffset from CheckpointStore when true and a checkpoint exists.
	ResumeCheckpoint bool
	// MaxPayloadBytes is reserved for future payload guards (handler-side limits).
	MaxPayloadBytes int
	// PreferredCodec is the default codec for Delivery.Decode helpers.
	PreferredCodec Codec

	// NodeAddress optionally pins the initial subscribe attempt to a specific node.
	NodeAddress string

	// ReconnectBackoff is the initial delay between subscribe reconnect attempts.
	ReconnectBackoff time.Duration
	// MaxReconnectBackoff caps exponential reconnect backoff.
	MaxReconnectBackoff time.Duration
	// ReconnectJitter is the fractional jitter applied to reconnect delays (e.g. 0.2 = ±20%).
	ReconnectJitter float64
	// MaxReconnectAttempts limits reconnects (0 = unlimited until context cancel).
	MaxReconnectAttempts int

	// HeartbeatInterval is how often optional OnHeartbeat callbacks fire while connected.
	HeartbeatInterval time.Duration
	// OnJoin is called when a subscription successfully connects.
	OnJoin func(context.Context, Assignment)
	// OnLeave is called when a subscription disconnects cleanly or on shutdown.
	OnLeave func(context.Context, Assignment)
	// OnHeartbeat is called periodically while the stream is alive.
	OnHeartbeat func(context.Context, Assignment)
	// OnAssigned is called when a partition assignment is established.
	OnAssigned func(context.Context, Assignment)
	// OnRevoked is called when an assignment is lost (e.g. before reconnect).
	OnRevoked func(context.Context, Assignment)
	// OnReconnect is called before each reconnect attempt with attempt number and last error.
	OnReconnect func(context.Context, int, error)
}

// DefaultConsumerConfig returns safe consumer defaults.
func DefaultConsumerConfig(topic, consumerGroup string) ConsumerConfig {
	return ConsumerConfig{
		Topic:                topic,
		ConsumerGroup:        consumerGroup,
		PartitionID:          -1,
		StartOffset:          -1,
		MaxBufferSize:        1024,
		WorkerConcurrency:    1,
		AutoAck:              true,
		AckMode:              AckModeAuto,
		AckQueueSize:         4096,
		AckBatchSize:         64,
		AckFlushInterval:     50 * time.Millisecond,
		CommitStrategy:       CommitStrategyAsync,
		ResumeCheckpoint:     true,
		PreferredCodec:       JSONCodec{},
		ReconnectBackoff:     1 * time.Second,
		MaxReconnectBackoff:  15 * time.Second,
		ReconnectJitter:      0.2,
		MaxReconnectAttempts: 0,
		HeartbeatInterval:    3 * time.Second,
	}
}

func (c ConsumerConfig) withDefaults() ConsumerConfig {
	out := c
	if out.PartitionID == 0 && c.PartitionID == 0 {
		// Keep explicit partition 0 if user passed it.
	}
	if out.StartOffset == 0 && c.StartOffset == 0 {
		// Keep explicit offset 0 if user passed it.
	}
	if out.MaxBufferSize <= 0 {
		out.MaxBufferSize = 1024
	}
	if out.WorkerConcurrency <= 0 {
		out.WorkerConcurrency = 1
	}
	if out.AckQueueSize <= 0 {
		out.AckQueueSize = 4096
	}
	if out.AckBatchSize <= 0 {
		out.AckBatchSize = 64
	}
	if out.AckFlushInterval <= 0 {
		out.AckFlushInterval = 50 * time.Millisecond
	}
	if out.ReconnectBackoff <= 0 {
		out.ReconnectBackoff = 1 * time.Second
	}
	if out.MaxReconnectBackoff <= 0 {
		out.MaxReconnectBackoff = 15 * time.Second
	}
	if out.ReconnectJitter < 0 {
		out.ReconnectJitter = 0
	}
	if out.HeartbeatInterval <= 0 {
		out.HeartbeatInterval = 3 * time.Second
	}
	if out.SubscriptionID == "" {
		out.SubscriptionID = fmt.Sprintf("client-sub-%d", subscriptionCounter.Add(1))
	}
	if out.AckMode == "" {
		if c.AutoAck {
			out.AckMode = AckModeAuto
		} else {
			out.AckMode = AckModeManual
		}
	}
	if out.CommitStrategy == "" {
		out.CommitStrategy = CommitStrategyAsync
	}
	if out.PreferredCodec == nil {
		out.PreferredCodec = JSONCodec{}
	}
	return out
}

// Validate checks that required consumer settings are present and positive where required.
func (c ConsumerConfig) Validate() error {
	if c.Topic == "" {
		return fmt.Errorf("topic is required")
	}
	if c.ConsumerGroup == "" {
		return fmt.Errorf("consumer_group is required")
	}
	if c.WorkerConcurrency <= 0 {
		return fmt.Errorf("worker_concurrency must be > 0")
	}
	if c.MaxBufferSize <= 0 {
		return fmt.Errorf("max_buffer_size must be > 0")
	}
	switch c.AckMode {
	case AckModeAuto, AckModeManual:
	default:
		return fmt.Errorf("ack_mode must be one of: %s, %s", AckModeAuto, AckModeManual)
	}
	switch c.CommitStrategy {
	case CommitStrategyAsync, CommitStrategySync:
	default:
		return fmt.Errorf("commit_strategy must be one of: %s, %s", CommitStrategyAsync, CommitStrategySync)
	}
	if c.AckBatchSize <= 0 {
		return fmt.Errorf("ack_batch_size must be > 0")
	}
	if c.AckFlushInterval <= 0 {
		return fmt.Errorf("ack_flush_interval must be > 0")
	}
	if c.AckQueueSize <= 0 {
		return fmt.Errorf("ack_queue_size must be > 0")
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("heartbeat_interval must be > 0")
	}
	if c.MaxReconnectBackoff <= 0 {
		return fmt.Errorf("max_reconnect_backoff must be > 0")
	}
	if c.ReconnectBackoff > c.MaxReconnectBackoff {
		return fmt.Errorf("reconnect_backoff must be <= max_reconnect_backoff")
	}
	if c.ReconnectJitter < 0 || c.ReconnectJitter > 1 {
		return fmt.Errorf("reconnect_jitter must be between 0 and 1")
	}
	return nil
}

type assignmentTracker struct {
	mu       sync.Mutex
	current  Assignment
	assigned bool

	onAssign func(context.Context, Assignment)
}

func newAssignmentTracker(initial Assignment, onAssign func(context.Context, Assignment)) *assignmentTracker {
	return &assignmentTracker{
		current:  initial,
		onAssign: onAssign,
	}
}

func (a *assignmentTracker) assignment() Assignment {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.current
}

func (a *assignmentTracker) notifyAssigned(ctx context.Context) {
	a.mu.Lock()
	assignment := a.current
	changed := !a.assigned
	a.assigned = true
	a.mu.Unlock()

	if changed && a.onAssign != nil {
		a.onAssign(ctx, assignment)
	}
}

func (a *assignmentTracker) updateFromDelivery(ctx context.Context, delivery *types.Delivery) {
	if delivery == nil {
		return
	}
	partitionID, topic := deliveryPartitionTopic(delivery)
	if partitionID < 0 {
		return
	}

	a.mu.Lock()
	changed := false
	if a.current.PartitionID != partitionID {
		a.current.PartitionID = partitionID
		changed = true
	}
	if topic != "" && a.current.Topic != topic {
		a.current.Topic = topic
		changed = true
	}
	assignment := a.current
	alreadyAssigned := a.assigned
	if changed {
		a.assigned = true
	}
	a.mu.Unlock()

	if changed && a.onAssign != nil {
		a.onAssign(ctx, assignment)
		return
	}
	if !alreadyAssigned && partitionID >= 0 {
		a.notifyAssigned(ctx)
	}
}

type ackEnvelope struct {
	req         *types.AckRequest
	partitionID int32
	topic       string
}

type deliveryAcker struct {
	mu          sync.Mutex
	sent        bool
	sendErr     error
	template    ackEnvelope
	sendRequest func(context.Context, ackEnvelope) error
}

func (a *deliveryAcker) wasSent() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.sent
}

func (a *deliveryAcker) send(ctx context.Context, req *types.AckRequest) error {
	if req == nil {
		return fmt.Errorf("ack request is required")
	}

	a.mu.Lock()
	if a.sent {
		err := a.sendErr
		a.mu.Unlock()
		return err
	}
	a.sent = true
	a.mu.Unlock()

	env := a.template
	env.req = req
	err := a.sendRequest(ctx, env)

	a.mu.Lock()
	a.sendErr = err
	a.mu.Unlock()
	return err
}

// Consumer is a high-level subscribe runtime.
// Consumer runs a subscribe stream, dispatches deliveries to a handler, and manages acks/reconnects.
type Consumer struct {
	client  *Client        // shared metadata-aware client
	cfg     ConsumerConfig // resolved consumer settings
	handler MessageHandler // user callback invoked per delivery
}

// NewConsumer creates a consumer runtime.
func NewConsumer(client *Client, cfg ConsumerConfig, handler MessageHandler) (*Consumer, error) {
	if client == nil {
		return nil, wrapError("consumer.new", ErrorKindValidation, fmt.Errorf("client is required"))
	}
	if handler == nil {
		return nil, wrapError("consumer.new", ErrorKindValidation, fmt.Errorf("handler is required"))
	}
	cfg = cfg.withDefaults()
	if cfg.MaxPayloadBytes <= 0 {
		cfg.MaxPayloadBytes = client.cfg.MaxRecvMsgSize
	}
	if err := cfg.Validate(); err != nil {
		client.observeError("consumer.new", ErrorKindValidation, err)
		return nil, wrapError("consumer.new", ErrorKindValidation, err)
	}

	return &Consumer{
		client:  client,
		cfg:     cfg,
		handler: handler,
	}, nil
}

// Subscribe creates and runs a consumer until ctx cancellation.
func (c *Client) Subscribe(ctx context.Context, cfg ConsumerConfig, handler MessageHandler) error {
	consumer, err := NewConsumer(c, cfg, handler)
	if err != nil {
		return err
	}
	return consumer.Run(ctx)
}

// Run executes the consumer stream lifecycle with reconnect behavior.
func (c *Consumer) Run(ctx context.Context) error {
	attempt := 0
	var lastErr error
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if c.cfg.MaxReconnectAttempts > 0 && attempt >= c.cfg.MaxReconnectAttempts {
			if lastErr == nil {
				lastErr = fmt.Errorf("max reconnect attempts reached")
			}
			return c.wrapErr("consumer.run", ErrorKindUnavailable, lastErr)
		}
		if attempt > 0 && c.cfg.PartitionID >= 0 {
			refreshCtx, cancel := context.WithTimeout(ctx, c.client.cfg.RequestTimeout)
			_ = c.client.ForceMetadataRefresh(refreshCtx)
			cancel()
		}

		candidates := c.candidateAddresses()
		if len(candidates) == 0 {
			return c.wrapErr("consumer.run", ErrorKindValidation, fmt.Errorf("no candidate node addresses"))
		}

		for _, addr := range candidates {
			err := c.consumeFromNode(ctx, addr)
			if err == nil || ctx.Err() != nil {
				return err
			}
			lastErr = err
			if c.cfg.OnReconnect != nil {
				c.cfg.OnReconnect(ctx, attempt+1, err)
			}
			if errs.IsLeaderRelated(err) {
				c.client.MarkMetadataStale()
			}
		}

		attempt++
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.reconnectBackoff(attempt)):
		}
	}
}

func (c *Consumer) candidateAddresses() []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(c.client.pool.Addresses())+3)

	if c.cfg.NodeAddress != "" {
		out = append(out, c.cfg.NodeAddress)
		seen[c.cfg.NodeAddress] = struct{}{}
	}
	if c.cfg.PartitionID >= 0 {
		route, err := c.client.RouteForPartition(c.cfg.PartitionID)
		if err == nil {
			for _, addr := range route.CandidateAddresses {
				if _, exists := seen[addr]; exists || addr == "" {
					continue
				}
				seen[addr] = struct{}{}
				out = append(out, addr)
			}
		}
	}
	for _, addr := range c.client.pool.Addresses() {
		if _, exists := seen[addr]; exists {
			continue
		}
		seen[addr] = struct{}{}
		out = append(out, addr)
	}
	return out
}

func (c *Consumer) consumeFromNode(ctx context.Context, addr string) error {
	eventClient, err := c.client.eventClientForAddress(addr)
	if err != nil {
		return c.wrapErr("consumer.node_client", ErrorKindTransport, err)
	}

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	start := time.Now()
	subscribeStream, err := eventClient.Subscribe(subCtx)
	c.client.observeRequest("event.subscribe.open", addr, start, err)
	if err != nil {
		return c.wrapErr("consumer.subscribe_stream", ErrorKindTransport, err)
	}

	start = time.Now()
	ackStream, err := eventClient.Ack(subCtx)
	c.client.observeRequest("event.ack.open", addr, start, err)
	if err != nil {
		return c.wrapErr("consumer.ack_stream", ErrorKindTransport, err)
	}

	startOffset := c.resolveStartOffset(subCtx)
	subReq := &types.SubscribeRequest{
		ConsumerGroup:  c.cfg.ConsumerGroup,
		Topic:          c.cfg.Topic,
		PartitionId:    c.cfg.PartitionID,
		StartOffset:    startOffset,
		MaxBufferSize:  c.cfg.MaxBufferSize,
		SubscriptionId: c.cfg.SubscriptionID,
	}
	start = time.Now()
	err = subscribeStream.Send(subReq)
	c.client.observeRequest("event.subscribe.send", addr, start, err)
	if err != nil {
		return c.wrapErr("consumer.send_subscribe_request", ErrorKindTransport, err)
	}

	assignment := Assignment{
		Topic:          c.cfg.Topic,
		ConsumerGroup:  c.cfg.ConsumerGroup,
		PartitionID:    c.cfg.PartitionID,
		SubscriptionID: c.cfg.SubscriptionID,
		NodeAddress:    addr,
	}
	assignmentState := newAssignmentTracker(assignment, c.cfg.OnAssigned)
	if c.cfg.OnJoin != nil {
		c.cfg.OnJoin(subCtx, assignmentState.assignment())
	}
	if c.cfg.PartitionID >= 0 {
		assignmentState.notifyAssigned(subCtx)
	}

	ackQueue := make(chan ackEnvelope, c.cfg.AckQueueSize)
	deliveries := make(chan *types.Delivery, c.cfg.MaxBufferSize)

	var infraWG sync.WaitGroup
	infraWG.Add(1)
	utils.GoSafe("consumer-heartbeat", func() {
		defer infraWG.Done()
		c.heartbeatLoop(subCtx, assignmentState)
	})
	infraWG.Add(1)
	utils.GoSafe("consumer-ack-sender", func() {
		defer infraWG.Done()
		c.ackSender(subCtx, addr, ackStream, ackQueue)
	})
	if c.cfg.CommitStrategy == CommitStrategyAsync {
		infraWG.Add(1)
		utils.GoSafe("consumer-ack-drain", func() {
			defer infraWG.Done()
			c.drainAckResponses(subCtx, addr, ackStream)
		})
	}

	var workerWG sync.WaitGroup
	workerWG.Add(c.cfg.WorkerConcurrency)
	for i := 0; i < c.cfg.WorkerConcurrency; i++ {
		utils.GoSafe("consumer-worker", func() {
			defer workerWG.Done()
			c.worker(subCtx, deliveries, ackQueue)
		})
	}

	recvErr := c.recvLoop(subCtx, addr, subscribeStream, assignmentState, deliveries)

	close(deliveries)
	workerWG.Wait()
	close(ackQueue)
	infraWG.Wait()

	finalAssignment := assignmentState.assignment()
	if c.cfg.OnRevoked != nil {
		c.cfg.OnRevoked(context.Background(), finalAssignment)
	}
	if c.cfg.OnLeave != nil {
		c.cfg.OnLeave(context.Background(), finalAssignment)
	}

	if recvErr == nil || recvErr == io.EOF {
		return nil
	}
	return recvErr
}

func (c *Consumer) recvLoop(
	ctx context.Context,
	addr string,
	stream grpc.BidiStreamingClient[types.SubscribeRequest, types.Delivery],
	assignmentState *assignmentTracker,
	out chan<- *types.Delivery,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		start := time.Now()
		delivery, err := stream.Recv()
		c.client.observeRequest("event.subscribe.recv", addr, start, err)
		if err != nil {
			return err
		}
		if assignmentState != nil {
			assignmentState.updateFromDelivery(ctx, delivery)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- delivery:
			c.observeState(len(out), 0)
		}
	}
}

func (c *Consumer) worker(ctx context.Context, deliveries <-chan *types.Delivery, ackQueue chan<- ackEnvelope) {
	for delivery := range deliveries {
		d := Delivery{
			Event:      delivery.GetEvent(),
			Batch:      delivery.GetBatch(),
			DeliveryID: delivery.GetDeliveryId(),
			Attempt:    delivery.GetAttempt(),
		}

		defaultAck := c.buildDefaultAck(delivery, nil)
		d.acker = &deliveryAcker{
			template: defaultAck,
			sendRequest: func(ctx context.Context, env ackEnvelope) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case ackQueue <- env:
					c.observeState(len(deliveries), len(ackQueue))
					return nil
				}
			},
		}
		if c.cfg.MaxPayloadBytes > 0 && deliveryPayloadBytes(delivery) > c.cfg.MaxPayloadBytes {
			if c.cfg.AckMode == AckModeAuto {
				guardErr := fmt.Errorf("delivery payload exceeds max_payload_bytes")
				ack := c.buildDefaultAck(delivery, guardErr)
				_ = d.acker.send(ctx, ack.req)
			}
			continue
		}

		err := c.handler(ctx, d)
		if c.cfg.AckMode != AckModeAuto {
			continue
		}
		if d.acker.wasSent() {
			continue
		}
		ack := c.buildDefaultAck(delivery, err)
		_ = d.acker.send(ctx, ack.req)
	}
}

func (c *Consumer) buildDefaultAck(delivery *types.Delivery, handlerErr error) ackEnvelope {
	if delivery == nil {
		return ackEnvelope{}
	}

	nextOffset := int64(0)
	if len(delivery.GetBatch()) > 0 {
		nextOffset = delivery.GetBatch()[len(delivery.GetBatch())-1].GetOffset() + 1
	} else if delivery.GetEvent() != nil {
		nextOffset = delivery.GetEvent().GetOffset() + 1
	}

	partitionID, topic := deliveryPartitionTopic(delivery)
	ack := &types.AckRequest{
		DeliveryId: delivery.GetDeliveryId(),
		Success:    handlerErr == nil,
		NextOffset: nextOffset,
	}
	if handlerErr != nil {
		ack.Error = handlerErr.Error()
	}
	return ackEnvelope{
		req:         ack,
		partitionID: partitionID,
		topic:       topic,
	}
}

func (c *Consumer) ackSender(
	ctx context.Context,
	addr string,
	stream grpc.BidiStreamingClient[types.AckRequest, types.AckResponse],
	acks <-chan ackEnvelope,
) {
	batch := make([]ackEnvelope, 0, c.cfg.AckBatchSize)
	timer := time.NewTimer(c.cfg.AckFlushInterval)
	defer timer.Stop()

	flush := func() bool {
		for _, ack := range batch {
			if ack.req == nil {
				continue
			}
			start := time.Now()
			err := stream.Send(ack.req)
			c.client.observeRequest("event.ack.send", addr, start, err)
			if err != nil {
				return false
			}

			if c.cfg.CommitStrategy == CommitStrategySync {
				start = time.Now()
				resp, recvErr := stream.Recv()
				c.client.observeRequest("event.ack.recv", addr, start, recvErr)
				if recvErr != nil {
					return false
				}
				if resp != nil && !resp.GetSuccess() {
					continue
				}
				committed := ack.req.GetNextOffset()
				if resp != nil && resp.GetCommittedOffset() > 0 {
					committed = resp.GetCommittedOffset()
				}
				c.persistCheckpoint(ctx, ack, committed)
				continue
			}

			c.persistCheckpoint(ctx, ack, ack.req.GetNextOffset())
		}
		batch = batch[:0]
		c.observeState(0, len(acks))
		return true
	}

	for {
		select {
		case <-ctx.Done():
			_ = flush()
			return
		case ack, ok := <-acks:
			if !ok {
				_ = flush()
				return
			}
			if ack.req == nil {
				continue
			}
			batch = append(batch, ack)
			c.observeState(0, len(acks))
			if len(batch) >= c.cfg.AckBatchSize {
				if !flush() {
					return
				}
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(c.cfg.AckFlushInterval)
			}
		case <-timer.C:
			if len(batch) > 0 && !flush() {
				return
			}
			timer.Reset(c.cfg.AckFlushInterval)
		}
	}
}

func (c *Consumer) drainAckResponses(
	ctx context.Context,
	addr string,
	stream grpc.BidiStreamingClient[types.AckRequest, types.AckResponse],
) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		start := time.Now()
		_, err := stream.Recv()
		c.client.observeRequest("event.ack.recv", addr, start, err)
		if err != nil {
			return
		}
	}
}

func (c *Consumer) wrapErr(op string, kind ErrorKind, err error) error {
	if c != nil && c.client != nil {
		c.client.observeError(op, kind, err)
	}
	return wrapError(op, kind, err)
}

func (c *Consumer) observeState(deliveryQueueDepth int, ackQueueDepth int) {
	if c == nil || c.client == nil || c.client.cfg.Hooks == nil {
		return
	}
	if hook, ok := c.client.cfg.Hooks.(ConsumerStateHook); ok {
		hook.OnConsumerState(deliveryQueueDepth, ackQueueDepth)
	}
}

func (c *Consumer) heartbeatLoop(ctx context.Context, assignmentState *assignmentTracker) {
	if c.cfg.OnHeartbeat == nil {
		<-ctx.Done()
		return
	}
	ticker := time.NewTicker(c.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.cfg.OnHeartbeat(ctx, assignmentState.assignment())
		}
	}
}

func (c *Consumer) reconnectBackoff(attempt int) time.Duration {
	base := float64(c.cfg.ReconnectBackoff) * math.Pow(2, float64(attempt-1))
	max := float64(c.cfg.MaxReconnectBackoff)
	if base > max {
		base = max
	}
	if c.cfg.ReconnectJitter > 0 {
		delta := base * c.cfg.ReconnectJitter
		base = base - delta + rand.Float64()*(2*delta)
	}
	if base < float64(time.Millisecond) {
		base = float64(time.Millisecond)
	}
	return time.Duration(base)
}

func (c *Consumer) resolveStartOffset(ctx context.Context) int64 {
	if c.cfg.StartOffset >= 0 {
		return c.cfg.StartOffset
	}
	if !c.cfg.ResumeCheckpoint || c.cfg.CheckpointStore == nil || c.cfg.PartitionID < 0 {
		return c.cfg.StartOffset
	}
	offset, err := c.cfg.CheckpointStore.LoadOffset(ctx, c.cfg.ConsumerGroup, c.cfg.Topic, c.cfg.PartitionID)
	if err != nil || offset < 0 {
		return c.cfg.StartOffset
	}
	return offset
}

func (c *Consumer) persistCheckpoint(ctx context.Context, ack ackEnvelope, committedOffset int64) {
	if c.cfg.CheckpointStore == nil || ack.partitionID < 0 || ack.topic == "" {
		return
	}
	if ack.req == nil || !ack.req.GetSuccess() {
		return
	}
	_ = c.cfg.CheckpointStore.SaveOffset(ctx, c.cfg.ConsumerGroup, ack.topic, ack.partitionID, committedOffset)
}

func deliveryPayloadBytes(d *types.Delivery) int {
	if d == nil {
		return 0
	}
	if d.GetEvent() != nil {
		return len(d.GetEvent().GetPayload())
	}
	total := 0
	for _, event := range d.GetBatch() {
		total += len(event.GetPayload())
	}
	return total
}

func deliveryPartitionTopic(delivery *types.Delivery) (int32, string) {
	if delivery == nil {
		return -1, ""
	}
	if event := delivery.GetEvent(); event != nil {
		return event.GetPartitionId(), event.GetTopic()
	}
	if batch := delivery.GetBatch(); len(batch) > 0 {
		last := batch[len(batch)-1]
		return last.GetPartitionId(), last.GetTopic()
	}
	return parsePartitionID(delivery.GetDeliveryId()), ""
}

func parsePartitionID(deliveryID string) int32 {
	parts := strings.Split(deliveryID, ":")
	if len(parts) < 2 {
		return -1
	}
	pid, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return -1
	}
	return int32(pid)
}

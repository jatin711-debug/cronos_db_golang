package client

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cronos_db/pkg/client/internal/errs"
	"cronos_db/pkg/types"

	"google.golang.org/grpc"
)

var subscriptionCounter atomic.Uint64

// AckMode controls whether acknowledgements are sent automatically or manually.
type AckMode string

const (
	AckModeAuto   AckMode = "auto"
	AckModeManual AckMode = "manual"
)

// CommitStrategy controls ack commit behavior.
type CommitStrategy string

const (
	CommitStrategyAsync CommitStrategy = "async"
	CommitStrategySync  CommitStrategy = "sync"
)

// Assignment carries consumer assignment details for callback hooks.
type Assignment struct {
	Topic          string
	ConsumerGroup  string
	PartitionID    int32
	SubscriptionID string
	NodeAddress    string
}

// OffsetCheckpointStore persists committed offsets for recovery/resume.
type OffsetCheckpointStore interface {
	LoadOffset(ctx context.Context, consumerGroup string, topic string, partitionID int32) (int64, error)
	SaveOffset(ctx context.Context, consumerGroup string, topic string, partitionID int32, nextOffset int64) error
}

// Delivery wraps server delivery payload for handler processing.
type Delivery struct {
	Event      *types.Event
	Batch      []*types.Event
	DeliveryID string
	Attempt    int32

	acker *deliveryAcker
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

// ConsumerConfig controls consumer stream lifecycle and concurrency.
type ConsumerConfig struct {
	Topic          string
	ConsumerGroup  string
	SubscriptionID string

	// -1 uses server auto-assignment.
	PartitionID int32
	// -1 starts at latest (server semantics).
	StartOffset int64

	MaxBufferSize     int32
	WorkerConcurrency int

	// AutoAck is kept for backward compatibility. Prefer AckMode.
	AutoAck bool
	AckMode AckMode

	AckQueueSize     int
	AckBatchSize     int
	AckFlushInterval time.Duration
	CommitStrategy   CommitStrategy
	CheckpointStore  OffsetCheckpointStore
	ResumeCheckpoint bool
	MaxPayloadBytes  int
	PreferredCodec   Codec

	// Optional preferred node for initial subscribe attempt.
	NodeAddress string

	ReconnectBackoff     time.Duration
	MaxReconnectAttempts int

	OnAssigned  func(context.Context, Assignment)
	OnRevoked   func(context.Context, Assignment)
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
		MaxReconnectAttempts: 0,
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
	return nil
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
type Consumer struct {
	client  *Client
	cfg     ConsumerConfig
	handler MessageHandler
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
			return wrapError("consumer.run", ErrorKindUnavailable, lastErr)
		}

		candidates := c.candidateAddresses()
		if len(candidates) == 0 {
			return wrapError("consumer.run", ErrorKindValidation, fmt.Errorf("no candidate node addresses"))
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
		case <-time.After(c.cfg.ReconnectBackoff):
		}
	}
}

func (c *Consumer) candidateAddresses() []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(c.client.pool.Addresses())+1)

	if c.cfg.NodeAddress != "" {
		out = append(out, c.cfg.NodeAddress)
		seen[c.cfg.NodeAddress] = struct{}{}
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
		return wrapError("consumer.node_client", ErrorKindTransport, err)
	}

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	start := time.Now()
	subscribeStream, err := eventClient.Subscribe(subCtx)
	c.client.observeRequest("event.subscribe.open", addr, start, err)
	if err != nil {
		return wrapError("consumer.subscribe_stream", ErrorKindTransport, err)
	}

	start = time.Now()
	ackStream, err := eventClient.Ack(subCtx)
	c.client.observeRequest("event.ack.open", addr, start, err)
	if err != nil {
		return wrapError("consumer.ack_stream", ErrorKindTransport, err)
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
		return wrapError("consumer.send_subscribe_request", ErrorKindTransport, err)
	}

	assignment := Assignment{
		Topic:          c.cfg.Topic,
		ConsumerGroup:  c.cfg.ConsumerGroup,
		PartitionID:    c.cfg.PartitionID,
		SubscriptionID: c.cfg.SubscriptionID,
		NodeAddress:    addr,
	}
	if c.cfg.OnAssigned != nil {
		c.cfg.OnAssigned(subCtx, assignment)
	}
	defer func() {
		if c.cfg.OnRevoked != nil {
			c.cfg.OnRevoked(context.Background(), assignment)
		}
	}()

	ackQueue := make(chan ackEnvelope, c.cfg.AckQueueSize)
	deliveries := make(chan *types.Delivery, c.cfg.MaxBufferSize)

	var infraWG sync.WaitGroup
	infraWG.Add(1)
	go func() {
		defer infraWG.Done()
		c.ackSender(subCtx, addr, ackStream, ackQueue)
	}()
	if c.cfg.CommitStrategy == CommitStrategyAsync {
		infraWG.Add(1)
		go func() {
			defer infraWG.Done()
			c.drainAckResponses(subCtx, addr, ackStream)
		}()
	}

	var workerWG sync.WaitGroup
	workerWG.Add(c.cfg.WorkerConcurrency)
	for i := 0; i < c.cfg.WorkerConcurrency; i++ {
		go func() {
			defer workerWG.Done()
			c.worker(subCtx, deliveries, ackQueue)
		}()
	}

	recvErr := c.recvLoop(subCtx, addr, subscribeStream, deliveries)

	close(deliveries)
	workerWG.Wait()
	close(ackQueue)
	infraWG.Wait()

	if recvErr == nil || recvErr == io.EOF {
		return nil
	}
	return recvErr
}

func (c *Consumer) recvLoop(
	ctx context.Context,
	addr string,
	stream grpc.BidiStreamingClient[types.SubscribeRequest, types.Delivery],
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

		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- delivery:
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

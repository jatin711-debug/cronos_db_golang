package client

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"cronos_db/pkg/client/internal/errs"
	"cronos_db/pkg/types"

	"google.golang.org/grpc"
)

var subscriptionCounter atomic.Uint64

// Delivery wraps server delivery payload for handler processing.
type Delivery struct {
	Event      *types.Event
	Batch      []*types.Event
	DeliveryID string
	Attempt    int32
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
	AutoAck           bool
	AckQueueSize      int

	// Optional preferred node for initial subscribe attempt.
	NodeAddress string

	ReconnectBackoff     time.Duration
	MaxReconnectAttempts int
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
		AckQueueSize:         4096,
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
	if out.ReconnectBackoff <= 0 {
		out.ReconnectBackoff = 1 * time.Second
	}
	if out.SubscriptionID == "" {
		out.SubscriptionID = fmt.Sprintf("client-sub-%d", subscriptionCounter.Add(1))
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
	return nil
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
	candidates := c.candidateAddresses()
	if len(candidates) == 0 {
		return wrapError("consumer.run", ErrorKindValidation, fmt.Errorf("no candidate node addresses"))
	}

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

		for _, addr := range candidates {
			err := c.consumeFromNode(ctx, addr)
			if err == nil || ctx.Err() != nil {
				return err
			}
			lastErr = err
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

	subscribeStream, err := eventClient.Subscribe(subCtx)
	if err != nil {
		return wrapError("consumer.subscribe_stream", ErrorKindTransport, err)
	}
	ackStream, err := eventClient.Ack(subCtx)
	if err != nil {
		return wrapError("consumer.ack_stream", ErrorKindTransport, err)
	}

	subReq := &types.SubscribeRequest{
		ConsumerGroup:  c.cfg.ConsumerGroup,
		Topic:          c.cfg.Topic,
		PartitionId:    c.cfg.PartitionID,
		StartOffset:    c.cfg.StartOffset,
		MaxBufferSize:  c.cfg.MaxBufferSize,
		SubscriptionId: c.cfg.SubscriptionID,
	}
	if err := subscribeStream.Send(subReq); err != nil {
		return wrapError("consumer.send_subscribe_request", ErrorKindTransport, err)
	}

	ackQueue := make(chan *types.AckRequest, c.cfg.AckQueueSize)
	deliveries := make(chan *types.Delivery, c.cfg.MaxBufferSize)

	var infraWG sync.WaitGroup
	infraWG.Add(2)
	go func() {
		defer infraWG.Done()
		c.ackSender(subCtx, ackStream, ackQueue)
	}()
	go func() {
		defer infraWG.Done()
		c.drainAckResponses(subCtx, ackStream)
	}()

	var workerWG sync.WaitGroup
	workerWG.Add(c.cfg.WorkerConcurrency)
	for i := 0; i < c.cfg.WorkerConcurrency; i++ {
		go func() {
			defer workerWG.Done()
			c.worker(subCtx, deliveries, ackQueue)
		}()
	}

	recvErr := c.recvLoop(subCtx, subscribeStream, deliveries)

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
	stream grpc.BidiStreamingClient[types.SubscribeRequest, types.Delivery],
	out chan<- *types.Delivery,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		delivery, err := stream.Recv()
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

func (c *Consumer) worker(ctx context.Context, deliveries <-chan *types.Delivery, ackQueue chan<- *types.AckRequest) {
	for delivery := range deliveries {
		d := Delivery{
			Event:      delivery.GetEvent(),
			Batch:      delivery.GetBatch(),
			DeliveryID: delivery.GetDeliveryId(),
			Attempt:    delivery.GetAttempt(),
		}

		err := c.handler(ctx, d)
		if !c.cfg.AutoAck {
			continue
		}

		ack := c.buildAck(delivery, err)
		if ack == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case ackQueue <- ack:
		}
	}
}

func (c *Consumer) buildAck(delivery *types.Delivery, handlerErr error) *types.AckRequest {
	if delivery == nil {
		return nil
	}

	nextOffset := int64(0)
	if len(delivery.GetBatch()) > 0 {
		nextOffset = delivery.GetBatch()[len(delivery.GetBatch())-1].GetOffset() + 1
	} else if delivery.GetEvent() != nil {
		nextOffset = delivery.GetEvent().GetOffset() + 1
	}

	ack := &types.AckRequest{
		DeliveryId: delivery.GetDeliveryId(),
		Success:    handlerErr == nil,
		NextOffset: nextOffset,
	}
	if handlerErr != nil {
		ack.Error = handlerErr.Error()
	}
	return ack
}

func (c *Consumer) ackSender(
	ctx context.Context,
	stream grpc.BidiStreamingClient[types.AckRequest, types.AckResponse],
	acks <-chan *types.AckRequest,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case ack, ok := <-acks:
			if !ok {
				return
			}
			if ack == nil {
				continue
			}
			if err := stream.Send(ack); err != nil {
				return
			}
		}
	}
}

func (c *Consumer) drainAckResponses(ctx context.Context, stream grpc.BidiStreamingClient[types.AckRequest, types.AckResponse]) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if _, err := stream.Recv(); err != nil {
			return
		}
	}
}

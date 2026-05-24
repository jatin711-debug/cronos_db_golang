package client

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client/internal/errs"
	"github.com/jatin711-debug/cronos_db_golang/pkg/client/internal/retry"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
)

// Message represents a producer payload.
type Message struct {
	MessageID string
	Topic     string
	Payload   []byte
	Value     any
	Codec     Codec

	// Use ScheduleTS directly when already computed. If zero, ScheduleAt is used.
	ScheduleTS int64
	ScheduleAt time.Time

	Meta         map[string]string
	PartitionKey string
	PartitionID  *int32

	AllowDuplicate bool
}

// SendResult contains publish result metadata.
type SendResult struct {
	MessageID   string
	PartitionID int32
	Offset      int64
	ScheduleTS  int64
	NodeAddress string
	LeaderID    string
}

// BatchSendResult contains aggregate send batch results.
type BatchSendResult struct {
	Results        []*SendResult
	PublishedCount int32
	DuplicateCount int32
	ErrorCount     int32
}

// DeliveryCallback is invoked for async sends after completion.
type DeliveryCallback func(*SendResult, error)

// Partitioner provides custom partitioning logic.
type Partitioner interface {
	Partition(key string, partitionCount int) (int32, error)
}

type defaultPartitioner struct{}

func (defaultPartitioner) Partition(key string, partitionCount int) (int32, error) {
	if partitionCount <= 0 {
		return 0, fmt.Errorf("partition count must be > 0")
	}
	return utils.HashToPartitionID(key, partitionCount), nil
}

// ProducerConfig controls producer behavior.
type ProducerConfig struct {
	Partitioner           Partitioner
	DefaultAllowDuplicate bool
	RetryPolicy           retry.Policy
	AutoMessageID         bool
	Codec                 Codec
	MaxPayloadBytes       int

	AsyncQueueSize   int
	AsyncWorkers     int
	BlockOnQueueFull bool
	MaxInFlight      int
	MaxQueuedBytes   int64
	CircuitBreaker   CircuitBreakerConfig
}

// CircuitBreakerConfig controls producer circuit breaker behavior.
type CircuitBreakerConfig struct {
	FailureThreshold int
	Cooldown         time.Duration
}

// DefaultProducerConfig returns safe producer defaults.
func DefaultProducerConfig() ProducerConfig {
	return ProducerConfig{
		DefaultAllowDuplicate: false,
		RetryPolicy:           retry.DefaultPolicy(),
		AutoMessageID:         true,
		Codec:                 JSONCodec{},
		AsyncQueueSize:        4096,
		AsyncWorkers:          2,
		BlockOnQueueFull:      true,
		MaxInFlight:           1024,
		MaxQueuedBytes:        256 * 1024 * 1024, // 256MB async queue cap
		CircuitBreaker: CircuitBreakerConfig{
			FailureThreshold: 0,
			Cooldown:         5 * time.Second,
		},
	}
}

type asyncSendRequest struct {
	msg      Message
	callback DeliveryCallback
	future   *DeliveryFuture
	sizeHint int64
}

// Producer implements sync and async publish APIs.
type Producer struct {
	client      *Client
	cfg         ProducerConfig
	partitioner Partitioner

	queue       chan asyncSendRequest
	inFlight    chan struct{}
	closed      atomic.Bool
	queuedBytes atomic.Int64
	wg          sync.WaitGroup
	breaker     circuitBreaker
}

// NewProducer creates a producer bound to a client.
func NewProducer(client *Client, cfg ProducerConfig) (*Producer, error) {
	if client == nil {
		return nil, wrapError("producer.new", ErrorKindValidation, fmt.Errorf("client is required"))
	}
	if cfg.AsyncQueueSize <= 0 {
		cfg.AsyncQueueSize = DefaultProducerConfig().AsyncQueueSize
	}
	if cfg.AsyncWorkers <= 0 {
		cfg.AsyncWorkers = DefaultProducerConfig().AsyncWorkers
	}
	if cfg.Partitioner == nil {
		cfg.Partitioner = defaultPartitioner{}
	}
	if cfg.RetryPolicy.MaxAttempts <= 0 {
		cfg.RetryPolicy = retry.DefaultPolicy()
	}
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = DefaultProducerConfig().MaxInFlight
	}
	if cfg.Codec == nil {
		cfg.Codec = DefaultProducerConfig().Codec
	}
	if cfg.MaxPayloadBytes <= 0 {
		cfg.MaxPayloadBytes = client.cfg.MaxSendMsgSize
	}
	if cfg.CircuitBreaker.Cooldown <= 0 {
		cfg.CircuitBreaker.Cooldown = 5 * time.Second
	}

	p := &Producer{
		client:      client,
		cfg:         cfg,
		partitioner: cfg.Partitioner,
		queue:       make(chan asyncSendRequest, cfg.AsyncQueueSize),
		inFlight:    make(chan struct{}, cfg.MaxInFlight),
		breaker: circuitBreaker{
			threshold: cfg.CircuitBreaker.FailureThreshold,
			cooldown:  cfg.CircuitBreaker.Cooldown,
		},
	}
	for i := 0; i < cfg.AsyncWorkers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p, nil
}

// NewProducer creates a producer from the client.
func (c *Client) NewProducer(cfg ProducerConfig) (*Producer, error) {
	return NewProducer(c, cfg)
}

// Send publishes one message synchronously.
func (p *Producer) Send(ctx context.Context, msg Message) (*SendResult, error) {
	if !p.breaker.Allow() {
		return nil, p.wrapErr("producer.send", ErrorKindUnavailable, ErrCircuitOpen)
	}
	if err := p.acquireInFlight(ctx); err != nil {
		return nil, p.wrapErr("producer.send", ErrorKindTimeout, err)
	}
	defer p.releaseInFlight()

	msg, err := normalizeMessage(msg, p.cfg)
	if err != nil {
		return nil, p.wrapErr("producer.send", ErrorKindValidation, err)
	}
	if err := validateMessage(msg); err != nil {
		return nil, p.wrapErr("producer.send", ErrorKindValidation, err)
	}

	var lastErr error
	for attempt := 0; attempt < p.cfg.RetryPolicy.MaxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(p.cfg.RetryPolicy.Backoff(attempt)):
			}
		}

		partitionID, route, err := p.resolveRouteForMessage(msg)
		if err != nil {
			lastErr = err
			continue
		}

		event := toProtoEvent(msg, partitionID)
		allowDuplicate := p.cfg.DefaultAllowDuplicate || msg.AllowDuplicate
		shouldRetry := false

		for _, addr := range route.CandidateAddresses {
			client, err := p.client.eventClientForAddress(addr)
			if err != nil {
				lastErr = err
				continue
			}

			reqCtx, cancel := p.client.requestContext(ctx)
			start := time.Now()
			resp, err := client.Publish(reqCtx, &types.PublishRequest{
				Event:          event,
				AllowDuplicate: allowDuplicate,
			})
			cancel()
			p.client.observeRequest("event.publish", addr, start, err)

			if err != nil {
				lastErr = err
				if errs.IsLeaderRelated(err) {
					p.client.MarkMetadataStale()
					shouldRetry = true
				}
				if errs.IsRetryable(err) {
					shouldRetry = true
					continue
				}
				p.breaker.RecordFailure()
				return nil, p.wrapErr("producer.send", ErrorKindTransport, err)
			}
			if resp == nil {
				lastErr = fmt.Errorf("nil publish response from %s", addr)
				shouldRetry = true
				continue
			}
			if resp.GetSuccess() {
				p.breaker.RecordSuccess()
				return &SendResult{
					MessageID:   msg.MessageID,
					PartitionID: resp.GetPartitionId(),
					Offset:      resp.GetOffset(),
					ScheduleTS:  resp.GetScheduleTs(),
					NodeAddress: addr,
					LeaderID:    route.LeaderID,
				}, nil
			}

			lastErr = errors.New(resp.GetError())
			if isLeaderRelatedMessage(resp.GetError()) {
				p.client.MarkMetadataStale()
				shouldRetry = true
				continue
			}
			p.breaker.RecordFailure()
			return nil, p.wrapErr("producer.send", ErrorKindValidation, lastErr)
		}

		if !shouldRetry {
			continue
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("publish failed after %d attempts", p.cfg.RetryPolicy.MaxAttempts)
	}
	p.breaker.RecordFailure()
	return nil, p.wrapErr("producer.send", ErrorKindUnavailable, lastErr)
}

// SendBatch publishes a set of messages.
func (p *Producer) SendBatch(ctx context.Context, msgs []Message) (*BatchSendResult, error) {
	if len(msgs) == 0 {
		return &BatchSendResult{}, nil
	}

	type batchGroup struct {
		addr           string
		allowDuplicate bool
		msgs           []Message
		events         []*types.Event
	}

	groupKey := func(addr string, allowDuplicate bool) string {
		if allowDuplicate {
			return addr + "|dup:1"
		}
		return addr + "|dup:0"
	}

	groups := make(map[string]*batchGroup)
	result := &BatchSendResult{
		Results: make([]*SendResult, 0, len(msgs)),
	}

	for _, msg := range msgs {
		msg, err := normalizeMessage(msg, p.cfg)
		if err != nil {
			result.ErrorCount++
			continue
		}
		if err := validateMessage(msg); err != nil {
			result.ErrorCount++
			continue
		}

		partitionID, route, err := p.resolveRouteForMessage(msg)
		if err != nil || len(route.CandidateAddresses) == 0 {
			result.ErrorCount++
			continue
		}

		addr := route.CandidateAddresses[0]
		allowDuplicate := p.cfg.DefaultAllowDuplicate || msg.AllowDuplicate
		key := groupKey(addr, allowDuplicate)
		group, exists := groups[key]
		if !exists {
			group = &batchGroup{
				addr:           addr,
				allowDuplicate: allowDuplicate,
				msgs:           make([]Message, 0, 64),
				events:         make([]*types.Event, 0, 64),
			}
			groups[key] = group
		}
		group.msgs = append(group.msgs, msg)
		group.events = append(group.events, toProtoEvent(msg, partitionID))
	}

	for _, group := range groups {
		client, err := p.client.eventClientForAddress(group.addr)
		if err != nil {
			for _, msg := range group.msgs {
				sendRes, sendErr := p.Send(ctx, msg)
				if sendErr == nil {
					result.PublishedCount++
					result.Results = append(result.Results, sendRes)
				} else {
					result.ErrorCount++
				}
			}
			continue
		}

		reqCtx, cancel := p.client.requestContext(ctx)
		start := time.Now()
		resp, err := client.PublishBatch(reqCtx, &types.PublishBatchRequest{
			Events:         group.events,
			AllowDuplicate: group.allowDuplicate,
		})
		cancel()
		p.client.observeRequest("event.publish_batch", group.addr, start, err)

		if err != nil || resp == nil || !resp.GetSuccess() {
			if err != nil && errs.IsLeaderRelated(err) {
				p.client.MarkMetadataStale()
			}
			for _, msg := range group.msgs {
				sendRes, sendErr := p.Send(ctx, msg)
				if sendErr != nil {
					result.ErrorCount++
					continue
				}
				result.PublishedCount++
				result.Results = append(result.Results, sendRes)
			}
			continue
		}

		result.PublishedCount += resp.GetPublishedCount()
		result.DuplicateCount += resp.GetDuplicateCount()
		result.ErrorCount += resp.GetErrorCount()
		for _, msg := range group.msgs {
			result.Results = append(result.Results, &SendResult{
				MessageID:   msg.MessageID,
				NodeAddress: group.addr,
				ScheduleTS:  resolveScheduleTS(msg),
			})
		}
	}

	return result, nil
}

// SendAsync publishes asynchronously and returns a future.
func (p *Producer) SendAsync(msg Message, callback DeliveryCallback) (*DeliveryFuture, error) {
	return p.SendAsyncContext(context.Background(), msg, callback)
}

// SendAsyncContext publishes asynchronously and returns a future.
func (p *Producer) SendAsyncContext(ctx context.Context, msg Message, callback DeliveryCallback) (*DeliveryFuture, error) {
	if p.closed.Load() {
		return nil, p.wrapErr("producer.send_async", ErrorKindValidation, fmt.Errorf("producer is closed"))
	}

	msg, err := normalizeMessage(msg, p.cfg)
	if err != nil {
		return nil, p.wrapErr("producer.send_async", ErrorKindValidation, err)
	}
	sizeHint := estimateMessageBytes(msg)
	if err := p.reserveQueuedBytes(ctx, sizeHint); err != nil {
		return nil, err
	}

	future := newDeliveryFuture()
	req := asyncSendRequest{
		msg:      msg,
		callback: callback,
		future:   future,
		sizeHint: sizeHint,
	}

	if p.cfg.BlockOnQueueFull {
		select {
		case <-ctx.Done():
			p.releaseQueuedBytes(sizeHint)
			return nil, p.wrapErr("producer.send_async", ErrorKindTimeout, ctx.Err())
		case p.queue <- req:
			p.observeState()
		}
		return future, nil
	}

	select {
	case p.queue <- req:
		p.observeState()
		return future, nil
	default:
		p.releaseQueuedBytes(sizeHint)
		return nil, p.wrapErr("producer.send_async", ErrorKindUnavailable, fmt.Errorf("async queue is full"))
	}
}

// Close flushes queued async work and stops workers.
func (p *Producer) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(p.queue)
	p.wg.Wait()
	p.observeState()
	return nil
}

func (p *Producer) worker() {
	defer p.wg.Done()
	for req := range p.queue {
		ctx, cancel := context.WithTimeout(context.Background(), p.client.cfg.RequestTimeout)
		res, err := p.Send(ctx, req.msg)
		cancel()
		p.releaseQueuedBytes(req.sizeHint)
		p.observeState()
		req.future.complete(res, err)
		if req.callback != nil {
			req.callback(res, err)
		}
	}
}

func (p *Producer) resolveRouteForMessage(msg Message) (int32, *Route, error) {
	if msg.PartitionID != nil {
		route, err := p.client.RouteForPartition(*msg.PartitionID)
		return *msg.PartitionID, route, err
	}

	key := msg.PartitionKey
	if key == "" {
		key = msg.MessageID
	}

	partitions := p.client.metadata.PartitionCount()
	if partitions <= 0 {
		partitions = p.client.cfg.PartitionCount
	}
	if partitions <= 0 {
		return 0, nil, fmt.Errorf("partition count unavailable")
	}

	partitionID, err := p.partitioner.Partition(key, partitions)
	if err != nil {
		return 0, nil, err
	}
	route, err := p.client.RouteForPartition(partitionID)
	return partitionID, route, err
}

func validateMessage(msg Message) error {
	if msg.MessageID == "" {
		return fmt.Errorf("message_id is required")
	}
	if msg.Topic == "" {
		return fmt.Errorf("topic is required")
	}
	if len(msg.Payload) == 0 {
		return fmt.Errorf("payload is required")
	}
	if resolveScheduleTS(msg) <= 0 {
		return fmt.Errorf("schedule_ts or schedule_at is required")
	}
	return nil
}

func normalizeMessage(msg Message, cfg ProducerConfig) (Message, error) {
	if msg.Meta == nil {
		msg.Meta = map[string]string{}
	}

	codec := msg.Codec
	if codec == nil {
		codec = cfg.Codec
	}
	if len(msg.Payload) == 0 && msg.Value != nil {
		payload, codecName, err := encodeWithCodec(codec, msg.Value)
		if err != nil {
			return msg, err
		}
		msg.Payload = payload
		if codecName != "" {
			msg.Meta[MetaCodecNameKey] = codecName
		}
	}
	if cfg.MaxPayloadBytes > 0 && len(msg.Payload) > cfg.MaxPayloadBytes {
		return msg, fmt.Errorf("payload exceeds max_payload_bytes (%d > %d)", len(msg.Payload), cfg.MaxPayloadBytes)
	}
	if msg.MessageID == "" && cfg.AutoMessageID {
		msg.MessageID = deterministicMessageID(msg)
	}
	return msg, nil
}

func deterministicMessageID(msg Message) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(msg.Topic))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(msg.PartitionKey))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strconv.FormatInt(resolveScheduleTS(msg), 10)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(msg.Payload)
	return "auto-" + hex.EncodeToString(h.Sum(nil))
}

func toProtoEvent(msg Message, partitionID int32) *types.Event {
	meta := make(map[string]string, len(msg.Meta)+1)
	for k, v := range msg.Meta {
		meta[k] = v
	}
	if msg.PartitionKey != "" {
		meta["partition_key"] = msg.PartitionKey
	}

	return &types.Event{
		MessageId:   msg.MessageID,
		ScheduleTs:  resolveScheduleTS(msg),
		Payload:     msg.Payload,
		Topic:       msg.Topic,
		Meta:        meta,
		PartitionId: partitionID,
	}
}

func resolveScheduleTS(msg Message) int64 {
	if msg.ScheduleTS > 0 {
		return msg.ScheduleTS
	}
	return msg.ScheduleAt.UnixMilli()
}

func isLeaderRelatedMessage(message string) bool {
	m := strings.ToLower(message)
	return strings.Contains(m, "leader") || strings.Contains(m, "partition")
}

func (p *Producer) wrapErr(op string, kind ErrorKind, err error) error {
	if p != nil && p.client != nil {
		p.client.observeError(op, kind, err)
	}
	return wrapError(op, kind, err)
}

func (p *Producer) observeState() {
	if p == nil || p.client == nil || p.client.cfg.Hooks == nil {
		return
	}
	if hook, ok := p.client.cfg.Hooks.(ProducerStateHook); ok {
		hook.OnProducerState(len(p.queue), p.queuedBytes.Load(), len(p.inFlight))
	}
}

func (p *Producer) acquireInFlight(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.inFlight <- struct{}{}:
		p.observeState()
		return nil
	}
}

func (p *Producer) releaseInFlight() {
	select {
	case <-p.inFlight:
		p.observeState()
	default:
	}
}

func (p *Producer) reserveQueuedBytes(ctx context.Context, n int64) error {
	if n <= 0 || p.cfg.MaxQueuedBytes <= 0 {
		return nil
	}

	tryReserve := func() bool {
		for {
			current := p.queuedBytes.Load()
			next := current + n
			if next > p.cfg.MaxQueuedBytes {
				return false
			}
			if p.queuedBytes.CompareAndSwap(current, next) {
				return true
			}
		}
	}

	if tryReserve() {
		p.observeState()
		return nil
	}
	if !p.cfg.BlockOnQueueFull {
		return p.wrapErr("producer.send_async", ErrorKindUnavailable, fmt.Errorf("async queued bytes limit exceeded"))
	}

	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return p.wrapErr("producer.send_async", ErrorKindTimeout, ctx.Err())
		case <-ticker.C:
			if p.closed.Load() {
				return p.wrapErr("producer.send_async", ErrorKindValidation, fmt.Errorf("producer is closed"))
			}
			if tryReserve() {
				p.observeState()
				return nil
			}
		}
	}
}

func (p *Producer) releaseQueuedBytes(n int64) {
	if n <= 0 || p.cfg.MaxQueuedBytes <= 0 {
		return
	}
	for {
		current := p.queuedBytes.Load()
		next := current - n
		if next < 0 {
			next = 0
		}
		if p.queuedBytes.CompareAndSwap(current, next) {
			p.observeState()
			return
		}
	}
}

func estimateMessageBytes(msg Message) int64 {
	size := int64(len(msg.MessageID) + len(msg.Topic) + len(msg.Payload) + len(msg.PartitionKey) + 16)
	for k, v := range msg.Meta {
		size += int64(len(k) + len(v))
	}
	return size
}

// ErrCircuitOpen indicates producer circuit breaker is open.
var ErrCircuitOpen = errors.New("producer circuit breaker is open")

type circuitBreaker struct {
	mu sync.Mutex

	threshold int
	cooldown  time.Duration

	failures  int
	openUntil time.Time
}

func (c *circuitBreaker) Allow() bool {
	if c.threshold <= 0 {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.openUntil.IsZero() || time.Now().After(c.openUntil) {
		return true
	}
	return false
}

func (c *circuitBreaker) RecordSuccess() {
	if c.threshold <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failures = 0
	c.openUntil = time.Time{}
}

func (c *circuitBreaker) RecordFailure() {
	if c.threshold <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failures++
	if c.failures < c.threshold {
		return
	}
	c.failures = 0
	c.openUntil = time.Now().Add(c.cooldown)
}

type deliveryOutcome struct {
	result *SendResult
	err    error
}

// DeliveryFuture is returned by async sends for later awaiting.
type DeliveryFuture struct {
	once sync.Once
	ch   chan deliveryOutcome
}

func newDeliveryFuture() *DeliveryFuture {
	return &DeliveryFuture{
		ch: make(chan deliveryOutcome, 1),
	}
}

func (f *DeliveryFuture) complete(result *SendResult, err error) {
	f.once.Do(func() {
		f.ch <- deliveryOutcome{result: result, err: err}
		close(f.ch)
	})
}

// Wait blocks until async send completion or context cancellation.
func (f *DeliveryFuture) Wait(ctx context.Context) (*SendResult, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case out, ok := <-f.ch:
		if !ok {
			return nil, fmt.Errorf("delivery future closed")
		}
		return out.result, out.err
	}
}

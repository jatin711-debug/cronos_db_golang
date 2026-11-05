package client

import (
	"context"
	"errors"
	"time"
)

func ExampleDefaultConfig() {
	cfg := DefaultConfig("127.0.0.1:9000", "127.0.0.1:9001")
	cfg.Security.Insecure = true
	cfg.Metadata.TTL = 30 * time.Second
	cfg.Metadata.RefreshInterval = 5 * time.Second

	_ = cfg
}

func ExampleMessage() {
	msg := Message{
		Topic:        "orders",
		PartitionKey: "customer-42",
		Value: map[string]any{
			"order_id": "ord-1001",
			"status":   "created",
		},
		Codec:      JSONCodec{},
		ScheduleTS: time.Now().Add(10 * time.Second).UnixMilli(),
	}

	_ = msg
}

func ExampleDefaultConsumerConfig() {
	cfg := DefaultConsumerConfig("orders", "order-processors")
	cfg.AckMode = AckModeAuto
	cfg.WorkerConcurrency = 4
	cfg.MaxBufferSize = 4096
	cfg.ReconnectBackoff = 1 * time.Second
	cfg.MaxReconnectBackoff = 15 * time.Second

	_ = cfg
}

func ExampleReplayRequest() {
	// A replay reads one partition (0 is partition 0). With StartTime or
	// EndTime set it is time-based: it returns the events whose schedule time
	// falls in the window, and Count does not apply.
	req := ReplayRequest{
		Topic:       "orders",
		PartitionID: 0,
		StartTime:   time.Now().Add(-1 * time.Hour),
		EndTime:     time.Now(),
	}

	_ = req
}

func ExampleProducer_Send() {
	ctx := context.Background()
	c, err := Dial(ctx, DefaultConfig("127.0.0.1:9000"))
	if err != nil {
		return
	}
	defer c.Close()
	producer, err := c.NewProducer(DefaultProducerConfig())
	if err != nil {
		return
	}
	defer producer.Close()

	_, err = producer.Send(ctx, Message{Topic: "orders", MessageID: "order-1003", Payload: []byte(`{"order_id":"ord-1003"}`), ScheduleAt: time.Now().Add(time.Hour)})
	var ce *Error
	if !errors.As(err, &ce) {
		return // nil, or an error that is not typed
	}
	switch ce.Kind {
	case ErrorKindOverloaded:
		// The node is at capacity, or a quota or publish-rate limit is used up.
		// The event was not stored, and the SDK did not retry: send less, or
		// try again later.
	case ErrorKindValidation:
		// A refusal that retrying unchanged does not cure, for example a
		// schema validation failure or a duplicate message ID.
	case ErrorKindUnavailable:
		// The retries ran out. Send again with the same MessageID.
	}
}

func ExampleClient_RescheduleEvent() {
	ctx := context.Background()
	c, err := Dial(ctx, DefaultConfig("127.0.0.1:9000"))
	if err != nil {
		return
	}
	defer c.Close()
	producer, err := c.NewProducer(DefaultProducerConfig())
	if err != nil {
		return
	}
	defer producer.Close()

	sent, err := producer.Send(ctx, Message{Topic: "orders", MessageID: "order-1002-reminder", Payload: []byte(`{}`), ScheduleAt: time.Now().Add(24 * time.Hour)})
	if err != nil {
		return
	}
	ref := sent.Reference() // keep it with your record of the event

	// Move the event, naming the revision it replaces and a request ID that
	// makes the call safe to retry.
	state, err := c.RescheduleEvent(ctx, ref, EventChange{RequestID: "order-1002-move-1", ExpectedRevision: ref.InitialRevision()}, time.Now().Add(48*time.Hour))
	var ce *Error
	if errors.As(err, &ce) && ce.Kind == ErrorKindUnavailable {
		// Unknown outcome: retry with the same EventChange and time.
		return
	}
	if err != nil {
		return
	}
	_, _ = c.CancelEvent(ctx, ref, EventChange{RequestID: "order-1002-cancel", ExpectedRevision: state.Revision})
}

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
	req := ReplayRequest{
		Topic:       "orders",
		PartitionID: 0,
		StartTime:   time.Now().Add(-1 * time.Hour),
		EndTime:     time.Now(),
		Count:       1000,
	}

	_ = req
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

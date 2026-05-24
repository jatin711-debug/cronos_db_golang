package client

import "time"

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

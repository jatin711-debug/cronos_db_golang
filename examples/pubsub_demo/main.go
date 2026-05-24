// Package main is a runnable end-to-end demo for CronosDB publish + subscribe.
//
// Usage:
//
//	go run ./examples/pubsub_demo              # connects to 127.0.0.1:9000
//	go run ./examples/pubsub_demo -addr :9001  # alternate node
//
// What it does:
//  1. Publishes one JSON event scheduled 10 seconds in the future.
//  2. Subscribes to the same topic and logs every received message to stdout.
//  3. Exits cleanly after 60 seconds (or Ctrl-C).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	client "github.com/jatin711-debug/cronos_db_golang/pkg/client"
)

const (
	topic         = "demo-topic"
	consumerGroup = "demo-group"
	demoTimeout   = 60 * time.Second
	scheduleDelay = 10 * time.Second
)

// DemoPayload is the JSON event body published in the demo.
type DemoPayload struct {
	Hello  string `json:"hello"`
	Source string `json:"source"`
	SentAt string `json:"sent_at"`
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "CronosDB node address")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	logger := log.New(os.Stdout, "[pubsub-demo] ", log.LstdFlags|log.Lmicroseconds)

	// Root context – cancelled on SIGINT/SIGTERM or after demoTimeout.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithTimeout(ctx, demoTimeout)
	defer cancel()

	// ── Dial ─────────────────────────────────────────────────────────────────
	cfg := client.DefaultConfig(*addr)
	cfg.Security.Insecure = true // plaintext for local dev

	logger.Printf("connecting to %s …", *addr)
	c, err := client.Dial(ctx, cfg)
	if err != nil {
		logger.Fatalf("dial failed: %v", err)
	}
	defer func() {
		if err := c.Close(); err != nil {
			logger.Printf("close error: %v", err)
		}
	}()
	logger.Println("connected ✓")

	codec := client.JSONCodec{}

	// ── Consumer (start first so it's ready before the message arrives) ──────
	consCfg := client.DefaultConsumerConfig(topic, consumerGroup)
	consCfg.AckMode = client.AckModeAuto

	consumerDone := make(chan error, 1)
	go func() {
		logger.Printf("subscribing to topic=%s group=%s …", topic, consumerGroup)
		consumerDone <- c.Subscribe(ctx, consCfg, func(ctx context.Context, d client.Delivery) error {
			return handleDelivery(logger, codec, d)
		})
	}()

	// Give the stream a moment to establish before publishing.
	select {
	case <-ctx.Done():
		logger.Println("context cancelled before publish")
		return
	case <-time.After(500 * time.Millisecond):
	}

	// ── Producer ─────────────────────────────────────────────────────────────
	producer, err := client.NewProducer(c, client.ProducerConfig{})
	if err != nil {
		logger.Fatalf("producer init failed: %v", err)
	}
	defer producer.Close()

	payload := DemoPayload{
		Hello:  "cronos",
		Source: "pubsub_demo",
		SentAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	raw, _ := json.Marshal(payload)
	logger.Printf("publishing event (scheduled +%s): %s", scheduleDelay, string(raw))

	result, err := producer.Send(ctx, client.Message{
		Topic:      topic,
		Value:      payload,
		Codec:      codec,
		ScheduleTS: time.Now().Add(scheduleDelay).UnixMilli(),
	})
	if err != nil {
		logger.Printf("⚠  publish failed: %v", err)
	} else {
		logger.Printf("published ✓  messageID=%s partition=%d offset=%d scheduleTS=%d",
			result.MessageID, result.PartitionID, result.Offset, result.ScheduleTS)
	}

	// ── Wait ─────────────────────────────────────────────────────────────────
	logger.Printf("waiting for delivery (up to %s) …", demoTimeout)
	select {
	case <-ctx.Done():
		logger.Println("demo complete – shutting down")
	case err := <-consumerDone:
		if err != nil && ctx.Err() == nil {
			logger.Printf("consumer exited with error: %v", err)
		}
	}
}

// handleDelivery decodes and logs every received message.
func handleDelivery(logger *log.Logger, codec client.Codec, d client.Delivery) error {
	event := d.Event
	if event == nil && len(d.Batch) > 0 {
		event = d.Batch[0]
	}
	if event == nil {
		logger.Println("⚠  received empty delivery")
		return nil
	}

	// Try to pretty-print as JSON; fall back to raw bytes.
	var pretty any
	if err := codec.Decode(event.GetPayload(), &pretty); err == nil {
		formatted, _ := json.MarshalIndent(pretty, "", "  ")
		fmt.Printf("\n╔══ MESSAGE RECEIVED ══════════════════════════════════╗\n")
		fmt.Printf("  topic      : %s\n", event.GetTopic())
		fmt.Printf("  messageID  : %s\n", event.GetMessageId())
		fmt.Printf("  deliveryID : %s\n", d.DeliveryID)
		fmt.Printf("  offset     : %d\n", event.GetOffset())
		fmt.Printf("  scheduleTS : %d (%s)\n",
			event.GetScheduleTs(),
			time.UnixMilli(event.GetScheduleTs()).UTC().Format(time.RFC3339))
		fmt.Printf("  payload    :\n%s\n", formatted)
		fmt.Printf("╚══════════════════════════════════════════════════════╝\n\n")
	} else {
		logger.Printf("received raw payload (%d bytes): %q", len(event.GetPayload()), event.GetPayload())
	}
	return nil
}

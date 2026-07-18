// Package main is a minimal end-to-end demo for CronosDB publish + subscribe.
//
// Usage:
//
//	# 1. Start a single local node
//	make build-api
//	./bin/cronos-api --dev --node-id=node1 --data-dir=./data
//
//	# 2. In another terminal, run the demo
//	go run ./examples/pubsub_demo
//
// What it does:
//
//  1. Connects to CronosDB on localhost:9000 (plaintext, dev mode).
//  2. Subscribes to "demo-topic" so it is ready before the event fires.
//  3. Publishes one JSON event scheduled 10 seconds in the future.
//  4. Prints the event when it is delivered at its scheduled time.
//  5. Exits cleanly after delivery or after a 60-second timeout.
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
	scheduleDelay = 10 * time.Second
	demoTimeout   = 60 * time.Second
)

// DemoPayload is the JSON event body published in the demo.
type DemoPayload struct {
	Hello  string `json:"hello"`
	Source string `json:"source"`
	SentAt string `json:"sent_at"`
}

func main() {
	addr := flag.String("addr", "localhost:9000", "CronosDB gRPC address")
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

	logger.Printf("connecting to %s ...", *addr)
	c, err := client.Dial(ctx, cfg)
	if err != nil {
		logger.Fatalf("dial failed: %v", err)
	}
	defer c.Close()
	logger.Println("connected")

	// ── Consumer (start first so it's ready before the message arrives) ──────
	consCfg := client.DefaultConsumerConfig(topic, consumerGroup)
	consCfg.AckMode = client.AckModeAuto

	delivered := make(chan struct{}, 1)
	consumerDone := make(chan error, 1)

	go func() {
		logger.Printf("subscribing to topic=%s group=%s ...", topic, consumerGroup)
		consumerDone <- c.Subscribe(ctx, consCfg, func(_ context.Context, d client.Delivery) error {
			handleDelivery(d)
			select {
			case delivered <- struct{}{}:
			default:
			}
			return nil
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
	producer, err := client.NewProducer(c, client.DefaultProducerConfig())
	if err != nil {
		logger.Fatalf("producer init failed: %v", err)
	}
	defer producer.Close()

	payload := DemoPayload{
		Hello:  "cronos",
		Source: "pubsub_demo",
		SentAt: time.Now().UTC().Format(time.RFC3339Nano),
	}

	scheduleAt := time.Now().Add(scheduleDelay)
	logger.Printf("publishing event scheduled for %s (+%s)", scheduleAt.UTC().Format(time.RFC3339), scheduleDelay)

	result, err := producer.Send(ctx, client.Message{
		Topic:        topic,
		PartitionKey: topic,
		Value:        payload,
		Codec:        client.JSONCodec{},
		ScheduleAt:   scheduleAt,
	})
	if err != nil {
		logger.Fatalf("publish failed: %v", err)
	}
	logger.Printf("published: messageID=%s partition=%d offset=%d scheduleTS=%d",
		result.MessageID, result.PartitionID, result.Offset, result.ScheduleTS)

	// ── Wait for delivery ────────────────────────────────────────────────────
	logger.Printf("waiting for delivery (up to %s) ...", demoTimeout)
	select {
	case <-delivered:
		logger.Println("event delivered – demo complete")
	case err := <-consumerDone:
		if err != nil && ctx.Err() == nil {
			logger.Fatalf("consumer exited with error: %v", err)
		}
		logger.Println("consumer exited")
	case <-ctx.Done():
		logger.Println("timed out waiting for delivery – demo complete")
	}
}

// handleDelivery decodes and logs every received message.
func handleDelivery(d client.Delivery) {
	// Prefer the single-event form; fall back to the first item in a batch.
	event := d.Event
	if event == nil && len(d.Batch) > 0 {
		event = d.Batch[0]
	}
	if event == nil {
		fmt.Println("received empty delivery")
		return
	}

	var payload DemoPayload
	codec := client.JSONCodec{}
	if err := codec.Decode(event.GetPayload(), &payload); err == nil {
		formatted, _ := json.MarshalIndent(payload, "", "  ")
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
		fmt.Printf("received raw payload (%d bytes): %q\n", len(event.GetPayload()), event.GetPayload())
	}
}

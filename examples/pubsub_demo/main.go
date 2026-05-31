// Package main is a runnable end-to-end demo for CronosDB publish + subscribe.
//
// Usage:
//
//	go run ./examples/pubsub_demo
//	go run ./examples/pubsub_demo -addrs 127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002
//	go run ./examples/pubsub_demo -addr 127.0.0.1:9001 # single-node shortcut
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
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
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
	addrsFlag := flag.String("addrs",
		"127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002",
		"Comma-separated CronosDB bootstrap node addresses")
	addr := flag.String("addr", "", "Single CronosDB node address (legacy shortcut; overrides -addrs)")
	flag.Parse()

	bootstrapAddrs := parseBootstrapAddrs(*addrsFlag)
	if strings.TrimSpace(*addr) != "" {
		bootstrapAddrs = parseBootstrapAddrs(*addr)
	}
	if len(bootstrapAddrs) == 0 {
		log.Fatalf("no valid node addresses provided")
	}

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	logger := log.New(os.Stdout, "[pubsub-demo] ", log.LstdFlags|log.Lmicroseconds)

	// Root context – cancelled on SIGINT/SIGTERM or after demoTimeout.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithTimeout(ctx, demoTimeout)
	defer cancel()

	// ── Dial ─────────────────────────────────────────────────────────────────
	cfg := client.DefaultConfig(bootstrapAddrs...)
	cfg.Security.Insecure = true // plaintext for local dev
	cfg.ConnectionsPerNode = 1   // demo favors fast startup over max throughput
	cfg.DialTimeout = 1 * time.Second
	cfg.NodeIDToAddress = inferNodeIDAddressMap(bootstrapAddrs)

	logger.Printf("connecting to bootstrap addresses=%v …", bootstrapAddrs)
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

	topicPartitionID, err := c.PartitionForKey(topic)
	if err != nil {
		logger.Fatalf("partition resolution failed for topic %q: %v", topic, err)
	}
	logger.Printf("resolved topic partition=%d", topicPartitionID)

	codec := client.JSONCodec{}
	runConsumerGroup := fmt.Sprintf("%s-%d", consumerGroup, time.Now().UnixNano())

	// ── Consumer (start first so it's ready before the message arrives) ──────
	consCfg := client.DefaultConsumerConfig(topic, runConsumerGroup)
	consCfg.AckMode = client.AckModeAuto
	consCfg.PartitionID = topicPartitionID
	consCfg.SubscriptionID = fmt.Sprintf("client-sub-%d", time.Now().UnixNano())
	consCfg.OnReconnect = func(_ context.Context, attempt int, err error) {
		logger.Printf("consumer reconnect attempt=%d err=%v", attempt, err)
	}

	consumerDone := make(chan error, 1)
	var received atomic.Bool
	go func() {
		logger.Printf("subscribing to topic=%s group=%s subscription=%s …", topic, runConsumerGroup, consCfg.SubscriptionID)
		consumerDone <- c.Subscribe(ctx, consCfg, func(ctx context.Context, d client.Delivery) error {
			received.Store(true)
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
	raw, _ := json.Marshal(payload)
	logger.Printf("publishing event (scheduled +%s): %s", scheduleDelay, string(raw))

	msg := client.Message{
		Topic:        topic,
		PartitionKey: topic, // must match server's topic-based subscribe routing
		Value:        payload,
		Codec:        codec,
		ScheduleTS:   time.Now().Add(scheduleDelay).UnixMilli(),
	}

	result, err := sendWithMetadataRefresh(ctx, c, producer, msg)
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
		if !received.Load() {
			logger.Printf("⚠  no delivery observed before timeout; check scheduler/dispatcher logs for partition=%d", topicPartitionID)
		}
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

func sendWithMetadataRefresh(
	ctx context.Context,
	c *client.Client,
	producer *client.Producer,
	msg client.Message,
) (*client.SendResult, error) {
	res, err := producer.Send(ctx, msg)
	if err == nil || !isLeaderRedirect(err) {
		return res, err
	}

	// In cluster mode a publish may race leadership movement; refresh metadata and retry once.
	refreshCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = c.ForceMetadataRefresh(refreshCtx)

	return producer.Send(ctx, msg)
}

func isLeaderRedirect(err error) bool {
	if err == nil {
		return false
	}

	text := strings.ToLower(err.Error())
	return strings.Contains(text, "partition") || strings.Contains(text, "leader")
}

func parseBootstrapAddrs(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))

	for _, part := range parts {
		addr := strings.TrimSpace(part)
		if addr == "" {
			continue
		}
		if _, exists := seen[addr]; exists {
			continue
		}
		seen[addr] = struct{}{}
		out = append(out, addr)
	}

	return out
}

func inferNodeIDAddressMap(addrs []string) map[string]string {
	mapping := make(map[string]string, len(addrs))

	// Stable fallback: node1 -> first address, node2 -> second, etc.
	for i, addr := range addrs {
		mapping[fmt.Sprintf("node%d", i+1)] = addr
	}

	// Local default stack heuristic: map 9000/9001/9002 to node1/node2/node3.
	for _, addr := range addrs {
		_, port, err := net.SplitHostPort(addr)
		if err != nil {
			continue
		}

		portNum, err := strconv.Atoi(port)
		if err != nil || portNum < 9000 {
			continue
		}

		nodeNum := portNum - 9000 + 1
		if nodeNum <= 0 {
			continue
		}
		mapping[fmt.Sprintf("node%d", nodeNum)] = addr
	}

	return mapping
}

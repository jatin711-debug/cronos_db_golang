//go:build loadtest
// +build loadtest

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"cronos_db/pkg/types"
)

// LoadTestConfig holds load test configuration
type LoadTestConfig struct {
	ServerAddr     string
	NumPublishers  int
	NumSubscribers int
	EventsPerPub   int
	PayloadSize    int
	ScheduleDelay  time.Duration // How far in future to schedule
	Topic          string
	ConsumerGroup  string
	Duration       time.Duration // Test duration (0 = until all events published)
	WarmupEvents   int
	ReportInterval time.Duration
}

// LoadTestMetrics holds collected metrics
type LoadTestMetrics struct {
	// Publish metrics
	TotalPublished   int64
	PublishErrors    int64
	PublishLatencies []time.Duration
	PublishStartTime time.Time
	PublishEndTime   time.Time

	// Subscribe metrics
	TotalReceived    int64
	ReceiveErrors    int64
	ReceiveLatencies []time.Duration // Time from schedule_ts to receive
	FirstReceiveTime time.Time
	LastReceiveTime  time.Time

	// E2E metrics
	E2ELatencies []time.Duration // Time from publish to receive

	mu sync.Mutex
}

func main() {
	config := parseFlags()

	fmt.Println("╔════════════════════════════════════════════════════════════╗")
	fmt.Println("║           CronosDB Load Test                              ║")
	fmt.Println("╚════════════════════════════════════════════════════════════╝")
	fmt.Println()

	printConfig(config)

	// Run load test
	metrics := runLoadTest(config)

	// Print results
	printResults(config, metrics)
}

func parseFlags() *LoadTestConfig {
	config := &LoadTestConfig{}

	flag.StringVar(&config.ServerAddr, "server", "localhost:9000", "Server address")
	flag.IntVar(&config.NumPublishers, "publishers", 10, "Number of concurrent publishers")
	flag.IntVar(&config.NumSubscribers, "subscribers", 4, "Number of concurrent subscribers")
	flag.IntVar(&config.EventsPerPub, "events", 2000, "Events per publisher")
	flag.IntVar(&config.PayloadSize, "payload-size", 256, "Payload size in bytes")
	flag.DurationVar(&config.ScheduleDelay, "schedule-delay", 500*time.Millisecond, "Schedule delay from now")
	flag.StringVar(&config.Topic, "topic", "load-test-topic", "Topic name")
	flag.StringVar(&config.ConsumerGroup, "consumer-group", "load-test-group", "Consumer group name")
	flag.DurationVar(&config.Duration, "duration", 0, "Test duration (0 = until complete)")
	flag.IntVar(&config.WarmupEvents, "warmup", 0, "Warmup events (not counted)")
	flag.DurationVar(&config.ReportInterval, "report-interval", 5*time.Second, "Progress report interval")

	flag.Parse()

	return config
}

func printConfig(config *LoadTestConfig) {
	fmt.Println("Configuration:")
	fmt.Println("─────────────────────────────────────────")
	fmt.Printf("  Server:          %s\n", config.ServerAddr)
	fmt.Printf("  Publishers:      %d\n", config.NumPublishers)
	fmt.Printf("  Subscribers:     %d\n", config.NumSubscribers)
	fmt.Printf("  Events/Publisher:%d\n", config.EventsPerPub)
	fmt.Printf("  Total Events:    %d\n", config.NumPublishers*config.EventsPerPub)
	fmt.Printf("  Payload Size:    %d bytes\n", config.PayloadSize)
	fmt.Printf("  Schedule Delay:  %v\n", config.ScheduleDelay)
	fmt.Printf("  Topic:           %s\n", config.Topic)
	fmt.Printf("  Warmup Events:   %d\n", config.WarmupEvents)
	fmt.Println()
}

func runLoadTest(config *LoadTestConfig) *LoadTestMetrics {
	metrics := &LoadTestMetrics{
		PublishLatencies: make([]time.Duration, 0, config.NumPublishers*config.EventsPerPub),
		ReceiveLatencies: make([]time.Duration, 0, config.NumPublishers*config.EventsPerPub),
		E2ELatencies:     make([]time.Duration, 0, config.NumPublishers*config.EventsPerPub),
	}

	// Track published events for E2E latency calculation
	publishTimes := &sync.Map{} // messageID -> publishTime

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	// Progress reporter
	go progressReporter(ctx, config, metrics)

	// Start subscribers first
	fmt.Println("Starting subscribers...")
	for i := 0; i < config.NumSubscribers; i++ {
		wg.Add(1)
		go func(subID int) {
			defer wg.Done()
			runSubscriber(ctx, config, metrics, publishTimes, subID)
		}(i)
	}

	// Give subscribers time to connect
	time.Sleep(500 * time.Millisecond)

	// Run warmup
	if config.WarmupEvents > 0 {
		fmt.Printf("Running warmup (%d events)...\n", config.WarmupEvents)
		runWarmup(config)
	}

	// Start publishers
	fmt.Println("Starting publishers...")
	metrics.PublishStartTime = time.Now()

	var pubWg sync.WaitGroup
	for i := 0; i < config.NumPublishers; i++ {
		pubWg.Add(1)
		go func(pubID int) {
			defer pubWg.Done()
			runPublisher(config, metrics, publishTimes, pubID)
		}(i)
	}

	// Wait for publishers to complete
	pubWg.Wait()
	metrics.PublishEndTime = time.Now()

	fmt.Println("\nPublishing complete. Waiting for all events to be received...")

	// Wait for all events to be received (with timeout)
	expectedEvents := int64(config.NumPublishers * config.EventsPerPub)
	timeout := time.After(120 * time.Second) // 2 min timeout for larger tests
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

waitLoop:
	for {
		select {
		case <-timeout:
			fmt.Println("Timeout waiting for events")
			break waitLoop
		case <-ticker.C:
			if atomic.LoadInt64(&metrics.TotalReceived) >= expectedEvents {
				break waitLoop
			}
		}
	}

	// Cancel context to stop subscribers
	cancel()

	// Give subscribers time to clean up
	time.Sleep(500 * time.Millisecond)

	return metrics
}

func runWarmup(config *LoadTestConfig) {
	conn, err := grpc.Dial(config.ServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Warmup connection failed: %v", err)
		return
	}
	defer conn.Close()

	client := types.NewEventServiceClient(conn)
	ctx := context.Background()

	for i := 0; i < config.WarmupEvents; i++ {
		msgID := fmt.Sprintf("warmup-%d-%d", time.Now().UnixNano(), i)
		req := &types.PublishRequest{
			Event: &types.Event{
				MessageId:  msgID,
				ScheduleTs: time.Now().UnixMilli(),
				Payload:    []byte("warmup"),
				Topic:      config.Topic + "-warmup",
			},
		}
		client.Publish(ctx, req)
	}
}

func runPublisher(config *LoadTestConfig, metrics *LoadTestMetrics, publishTimes *sync.Map, pubID int) {
	conn, err := grpc.Dial(config.ServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Publisher %d connection failed: %v", pubID, err)
		return
	}
	defer conn.Close()

	client := types.NewEventServiceClient(conn)
	ctx := context.Background()

	payload := make([]byte, config.PayloadSize)
	rand.Read(payload)

	for i := 0; i < config.EventsPerPub; i++ {
		msgID := fmt.Sprintf("pub%d-msg%d-%d", pubID, i, time.Now().UnixNano())
		scheduleTs := time.Now().Add(config.ScheduleDelay).UnixMilli()

		req := &types.PublishRequest{
			Event: &types.Event{
				MessageId:  msgID,
				ScheduleTs: scheduleTs,
				Payload:    payload,
				Topic:      config.Topic,
				Meta: map[string]string{
					"publisher": fmt.Sprintf("%d", pubID),
					"sequence":  fmt.Sprintf("%d", i),
				},
			},
		}

		publishStart := time.Now()
		_, err := client.Publish(ctx, req)
		publishLatency := time.Since(publishStart)

		if err != nil {
			atomic.AddInt64(&metrics.PublishErrors, 1)
		} else {
			atomic.AddInt64(&metrics.TotalPublished, 1)
			publishTimes.Store(msgID, publishStart)

			metrics.mu.Lock()
			metrics.PublishLatencies = append(metrics.PublishLatencies, publishLatency)
			metrics.mu.Unlock()
		}
	}
}

func runSubscriber(ctx context.Context, config *LoadTestConfig, metrics *LoadTestMetrics, publishTimes *sync.Map, subID int) {
	conn, err := grpc.Dial(config.ServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Subscriber %d connection failed: %v", subID, err)
		return
	}
	defer conn.Close()

	client := types.NewEventServiceClient(conn)

	// Create bidirectional stream for subscribe
	stream, err := client.Subscribe(ctx)
	if err != nil {
		log.Printf("Subscriber %d subscribe failed: %v", subID, err)
		return
	}

	// Create separate ack stream
	ackStream, err := client.Ack(ctx)
	if err != nil {
		log.Printf("Subscriber %d ack stream failed: %v", subID, err)
		return
	}

	// Send subscription request
	subReq := &types.SubscribeRequest{
		ConsumerGroup:  config.ConsumerGroup,
		Topic:          config.Topic,
		PartitionId:    -1, // Auto-assign based on topic
		StartOffset:    -1, // Latest
		MaxBufferSize:  1000,
		SubscriptionId: fmt.Sprintf("loadtest-sub-%d-%d", subID, time.Now().UnixNano()),
	}

	if err := stream.Send(subReq); err != nil {
		log.Printf("Subscriber %d send request failed: %v", subID, err)
		return
	}

	// Start ack response reader (to drain responses)
	go func() {
		for {
			_, err := ackStream.Recv()
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		delivery, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return // Context cancelled
			}
			atomic.AddInt64(&metrics.ReceiveErrors, 1)
			continue
		}

		receiveTime := time.Now()
		event := delivery.GetEvent()
		if event == nil {
			continue
		}

		// Record first receive time
		metrics.mu.Lock()
		if metrics.FirstReceiveTime.IsZero() {
			metrics.FirstReceiveTime = receiveTime
		}
		metrics.LastReceiveTime = receiveTime
		metrics.mu.Unlock()

		atomic.AddInt64(&metrics.TotalReceived, 1)

		// Calculate latencies
		scheduleTs := time.UnixMilli(event.GetScheduleTs())
		receiveLatency := receiveTime.Sub(scheduleTs)

		metrics.mu.Lock()
		metrics.ReceiveLatencies = append(metrics.ReceiveLatencies, receiveLatency)
		metrics.mu.Unlock()

		// E2E latency (if we have publish time)
		if publishTime, ok := publishTimes.Load(event.GetMessageId()); ok {
			e2eLatency := receiveTime.Sub(publishTime.(time.Time))
			metrics.mu.Lock()
			metrics.E2ELatencies = append(metrics.E2ELatencies, e2eLatency)
			metrics.mu.Unlock()
		}

		// Send ack via ack stream
		ackReq := &types.AckRequest{
			DeliveryId: delivery.GetDeliveryId(),
			Success:    true,
			NextOffset: event.GetOffset() + 1,
		}
		if err := ackStream.Send(ackReq); err != nil {
			// Log but don't fail - ack stream may be closed
			if ctx.Err() == nil {
				log.Printf("Subscriber %d ack send failed: %v", subID, err)
			}
		}
	}
}

func progressReporter(ctx context.Context, config *LoadTestConfig, metrics *LoadTestMetrics) {
	ticker := time.NewTicker(config.ReportInterval)
	defer ticker.Stop()

	expectedEvents := int64(config.NumPublishers * config.EventsPerPub)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			published := atomic.LoadInt64(&metrics.TotalPublished)
			received := atomic.LoadInt64(&metrics.TotalReceived)
			pubErrors := atomic.LoadInt64(&metrics.PublishErrors)
			recvErrors := atomic.LoadInt64(&metrics.ReceiveErrors)

			pubPercent := float64(published) / float64(expectedEvents) * 100
			recvPercent := float64(received) / float64(expectedEvents) * 100

			fmt.Printf("  Progress: Published %d/%d (%.1f%%) | Received %d/%d (%.1f%%) | Errors: pub=%d recv=%d\n",
				published, expectedEvents, pubPercent,
				received, expectedEvents, recvPercent,
				pubErrors, recvErrors)
		}
	}
}

func printResults(config *LoadTestConfig, metrics *LoadTestMetrics) {
	fmt.Println()
	fmt.Println("╔════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    LOAD TEST RESULTS                       ║")
	fmt.Println("╚════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// Publish Results
	fmt.Println("📤 PUBLISH METRICS")
	fmt.Println("─────────────────────────────────────────")

	totalPublished := atomic.LoadInt64(&metrics.TotalPublished)
	publishDuration := metrics.PublishEndTime.Sub(metrics.PublishStartTime)
	publishThroughput := float64(totalPublished) / publishDuration.Seconds()

	fmt.Printf("  Total Published:    %d events\n", totalPublished)
	fmt.Printf("  Publish Errors:     %d\n", atomic.LoadInt64(&metrics.PublishErrors))
	fmt.Printf("  Duration:           %v\n", publishDuration.Round(time.Millisecond))
	fmt.Printf("  Throughput:         %.2f events/sec\n", publishThroughput)

	if len(metrics.PublishLatencies) > 0 {
		stats := calculateStats(metrics.PublishLatencies)
		fmt.Printf("  Latency (min):      %v\n", stats.Min)
		fmt.Printf("  Latency (max):      %v\n", stats.Max)
		fmt.Printf("  Latency (avg):      %v\n", stats.Avg)
		fmt.Printf("  Latency (p50):      %v\n", stats.P50)
		fmt.Printf("  Latency (p95):      %v\n", stats.P95)
		fmt.Printf("  Latency (p99):      %v\n", stats.P99)
	}

	fmt.Println()

	// Receive Results
	fmt.Println("📥 RECEIVE METRICS")
	fmt.Println("─────────────────────────────────────────")

	totalReceived := atomic.LoadInt64(&metrics.TotalReceived)
	var receiveDuration time.Duration
	var receiveThroughput float64
	if !metrics.FirstReceiveTime.IsZero() && !metrics.LastReceiveTime.IsZero() {
		receiveDuration = metrics.LastReceiveTime.Sub(metrics.FirstReceiveTime)
		if receiveDuration > 0 {
			receiveThroughput = float64(totalReceived) / receiveDuration.Seconds()
		}
	}

	fmt.Printf("  Total Received:     %d events\n", totalReceived)
	fmt.Printf("  Receive Errors:     %d\n", atomic.LoadInt64(&metrics.ReceiveErrors))
	fmt.Printf("  Duration:           %v\n", receiveDuration.Round(time.Millisecond))
	fmt.Printf("  Throughput:         %.2f events/sec\n", receiveThroughput)

	if len(metrics.ReceiveLatencies) > 0 {
		stats := calculateStats(metrics.ReceiveLatencies)
		fmt.Println()
		fmt.Println("  Schedule→Receive Latency (from schedule_ts):")
		fmt.Printf("    Min:              %v\n", stats.Min)
		fmt.Printf("    Max:              %v\n", stats.Max)
		fmt.Printf("    Avg:              %v\n", stats.Avg)
		fmt.Printf("    P50:              %v\n", stats.P50)
		fmt.Printf("    P95:              %v\n", stats.P95)
		fmt.Printf("    P99:              %v\n", stats.P99)
	}

	fmt.Println()

	// E2E Results
	fmt.Println("🔄 END-TO-END METRICS")
	fmt.Println("─────────────────────────────────────────")

	if len(metrics.E2ELatencies) > 0 {
		stats := calculateStats(metrics.E2ELatencies)
		fmt.Printf("  Events Tracked:     %d\n", len(metrics.E2ELatencies))
		fmt.Printf("  E2E Latency (min):  %v\n", stats.Min)
		fmt.Printf("  E2E Latency (max):  %v\n", stats.Max)
		fmt.Printf("  E2E Latency (avg):  %v\n", stats.Avg)
		fmt.Printf("  E2E Latency (p50):  %v\n", stats.P50)
		fmt.Printf("  E2E Latency (p95):  %v\n", stats.P95)
		fmt.Printf("  E2E Latency (p99):  %v\n", stats.P99)
	} else {
		fmt.Println("  No E2E metrics collected")
	}

	fmt.Println()

	// Summary
	fmt.Println("📊 SUMMARY")
	fmt.Println("─────────────────────────────────────────")

	expectedEvents := int64(config.NumPublishers * config.EventsPerPub)
	successRate := float64(totalReceived) / float64(expectedEvents) * 100

	fmt.Printf("  Expected Events:    %d\n", expectedEvents)
	fmt.Printf("  Published:          %d (%.1f%%)\n", totalPublished, float64(totalPublished)/float64(expectedEvents)*100)
	fmt.Printf("  Received:           %d (%.1f%%)\n", totalReceived, successRate)
	fmt.Printf("  Success Rate:       %.2f%%\n", successRate)
	fmt.Printf("  Publish Throughput: %.2f events/sec\n", publishThroughput)

	if receiveThroughput > 0 {
		fmt.Printf("  Receive Throughput: %.2f events/sec\n", receiveThroughput)
	}

	fmt.Println()
	fmt.Println("════════════════════════════════════════════════════════════")
}

type LatencyStats struct {
	Min time.Duration
	Max time.Duration
	Avg time.Duration
	P50 time.Duration
	P95 time.Duration
	P99 time.Duration
}

func calculateStats(latencies []time.Duration) LatencyStats {
	if len(latencies) == 0 {
		return LatencyStats{}
	}

	// Sort for percentiles
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	var sum time.Duration
	for _, l := range sorted {
		sum += l
	}

	return LatencyStats{
		Min: sorted[0],
		Max: sorted[len(sorted)-1],
		Avg: sum / time.Duration(len(sorted)),
		P50: percentile(sorted, 50),
		P95: percentile(sorted, 95),
		P99: percentile(sorted, 99),
	}
}

func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * float64(p) / 100.0)
	return sorted[idx]
}

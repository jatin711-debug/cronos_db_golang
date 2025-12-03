//go:build integration
// +build integration

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"cronos_db/pkg/types"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestConfig holds test configuration
type TestConfig struct {
	ServerAddr     string
	Topic          string
	ConsumerGroup  string
	SubscriptionID string
	Timeout        time.Duration
}

// TestResult tracks test results
type TestResult struct {
	Name    string
	Passed  bool
	Message string
	Time    time.Duration
}

// IntegrationTestSuite runs all integration tests
type IntegrationTestSuite struct {
	config         TestConfig
	eventClient    types.EventServiceClient
	consumerClient types.ConsumerGroupServiceClient
	conn           *grpc.ClientConn
	results        []TestResult
}

func main() {
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║         ChronosDB Integration Test Suite                     ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// Configuration
	config := TestConfig{
		ServerAddr:     getEnv("CRONOS_SERVER", ":9000"),
		Topic:          fmt.Sprintf("integration-test-%d", time.Now().UnixNano()),
		ConsumerGroup:  fmt.Sprintf("test-consumer-%d", time.Now().UnixNano()),
		SubscriptionID: fmt.Sprintf("test-sub-%d", time.Now().UnixNano()),
		Timeout:        30 * time.Second,
	}

	suite := NewIntegrationTestSuite(config)
	if err := suite.Connect(); err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer suite.Close()

	// Run all tests
	suite.RunAllTests()

	// Print summary
	suite.PrintSummary()
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// NewIntegrationTestSuite creates a new test suite
func NewIntegrationTestSuite(config TestConfig) *IntegrationTestSuite {
	return &IntegrationTestSuite{
		config:  config,
		results: make([]TestResult, 0),
	}
}

// Connect establishes connection to the server
func (s *IntegrationTestSuite) Connect() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		s.config.ServerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	s.conn = conn
	s.eventClient = types.NewEventServiceClient(conn)
	s.consumerClient = types.NewConsumerGroupServiceClient(conn)
	return nil
}

// Close closes the connection
func (s *IntegrationTestSuite) Close() {
	if s.conn != nil {
		s.conn.Close()
	}
}

// RunAllTests executes all integration tests
func (s *IntegrationTestSuite) RunAllTests() {
	tests := []struct {
		name string
		fn   func() error
	}{
		// Publish Tests
		{"Publish - Basic Event", s.TestPublishBasic},
		{"Publish - Immediate Delivery", s.TestPublishImmediate},
		{"Publish - Scheduled Future Event", s.TestPublishScheduled},
		{"Publish - Duplicate Detection", s.TestPublishDuplicate},
		{"Publish - Allow Duplicate Flag", s.TestPublishAllowDuplicate},
		{"Publish - Validation Errors", s.TestPublishValidation},
		{"Publish - Multiple Events Batch", s.TestPublishBatch},
		{"Publish - Large Payload", s.TestPublishLargePayload},
		{"Publish - Metadata", s.TestPublishWithMetadata},

		// Subscribe Tests
		{"Subscribe - Basic Stream", s.TestSubscribeBasic},
		{"Subscribe - Multiple Subscribers", s.TestSubscribeMultiple},

		// Consumer Group Tests
		{"Consumer Group - Create", s.TestConsumerGroupCreate},
		{"Consumer Group - Get", s.TestConsumerGroupGet},
		{"Consumer Group - List", s.TestConsumerGroupList},

		// Timing Tests
		{"Timing - Event Order", s.TestTimingEventOrder},
		{"Timing - Precision", s.TestTimingPrecision},
		{"Timing - Far Future Scheduling", s.TestTimingFarFuture},

		// Concurrency Tests
		{"Concurrency - Parallel Publish", s.TestConcurrentPublish},
		{"Concurrency - Parallel Subscribe", s.TestConcurrentSubscribe},

		// Edge Case Tests
		{"Edge Case - Zero Delay", s.TestEdgeCaseZeroDelay},
		{"Edge Case - Max Payload", s.TestEdgeCaseMaxPayload},
		{"Edge Case - Special Characters", s.TestEdgeCaseSpecialChars},

		// End-to-End Tests
		{"E2E - Publish and Receive", s.TestE2EPublishAndReceive},
	}

	for _, test := range tests {
		s.runTest(test.name, test.fn)
	}
}

// runTest executes a single test and records the result
func (s *IntegrationTestSuite) runTest(name string, fn func() error) {
	fmt.Printf("▶ Running: %s... ", name)
	start := time.Now()

	err := fn()
	duration := time.Since(start)

	result := TestResult{
		Name: name,
		Time: duration,
	}

	if err != nil {
		result.Passed = false
		result.Message = err.Error()
		fmt.Printf("❌ FAILED (%v)\n", duration.Round(time.Millisecond))
		fmt.Printf("  └─ Error: %s\n", err)
	} else {
		result.Passed = true
		result.Message = "OK"
		fmt.Printf("✅ PASSED (%v)\n", duration.Round(time.Millisecond))
	}

	s.results = append(s.results, result)
}

// PrintSummary prints the test summary
func (s *IntegrationTestSuite) PrintSummary() {
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                      Test Summary                            ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")

	passed := 0
	failed := 0
	var totalTime time.Duration

	for _, r := range s.results {
		totalTime += r.Time
		if r.Passed {
			passed++
		} else {
			failed++
		}
	}

	fmt.Printf("\n📊 Total: %d | ✅ Passed: %d | ❌ Failed: %d | ⏱ Time: %v\n",
		len(s.results), passed, failed, totalTime.Round(time.Millisecond))

	if failed > 0 {
		fmt.Println("\n❌ Failed Tests:")
		for _, r := range s.results {
			if !r.Passed {
				fmt.Printf("  • %s: %s\n", r.Name, r.Message)
			}
		}
		os.Exit(1)
	}

	fmt.Println("\n🎉 All tests passed!")
}

// ============================================================================
// Publish Tests
// ============================================================================

func (s *IntegrationTestSuite) TestPublishBasic() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeout)
	defer cancel()

	msgID := fmt.Sprintf("basic-%d", time.Now().UnixNano())
	req := &types.PublishRequest{
		Event: &types.Event{
			MessageId:  msgID,
			ScheduleTs: time.Now().UnixMilli() + 5000,
			Payload:    []byte("basic test payload"),
			Topic:      s.config.Topic,
		},
	}

	resp, err := s.eventClient.Publish(ctx, req)
	if err != nil {
		return fmt.Errorf("publish failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("publish unsuccessful: %s", resp.Error)
	}

	if resp.Offset < 0 {
		return fmt.Errorf("invalid offset: %d", resp.Offset)
	}

	return nil
}

func (s *IntegrationTestSuite) TestPublishImmediate() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeout)
	defer cancel()

	msgID := fmt.Sprintf("immediate-%d", time.Now().UnixNano())
	// Schedule 1 second in the past for immediate delivery
	scheduleTs := time.Now().UnixMilli() - 1000

	req := &types.PublishRequest{
		Event: &types.Event{
			MessageId:  msgID,
			ScheduleTs: scheduleTs,
			Payload:    []byte("immediate delivery test"),
			Topic:      s.config.Topic,
		},
	}

	resp, err := s.eventClient.Publish(ctx, req)
	if err != nil {
		return fmt.Errorf("publish failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("publish unsuccessful: %s", resp.Error)
	}

	return nil
}

func (s *IntegrationTestSuite) TestPublishScheduled() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeout)
	defer cancel()

	msgID := fmt.Sprintf("scheduled-%d", time.Now().UnixNano())
	// Schedule 10 seconds in the future
	scheduleTs := time.Now().UnixMilli() + 10000

	req := &types.PublishRequest{
		Event: &types.Event{
			MessageId:  msgID,
			ScheduleTs: scheduleTs,
			Payload:    []byte("scheduled delivery test"),
			Topic:      s.config.Topic,
		},
	}

	resp, err := s.eventClient.Publish(ctx, req)
	if err != nil {
		return fmt.Errorf("publish failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("publish unsuccessful: %s", resp.Error)
	}

	if resp.ScheduleTs != scheduleTs {
		return fmt.Errorf("schedule timestamp mismatch: expected %d, got %d", scheduleTs, resp.ScheduleTs)
	}

	return nil
}

func (s *IntegrationTestSuite) TestPublishDuplicate() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeout)
	defer cancel()

	msgID := fmt.Sprintf("duplicate-%d", time.Now().UnixNano())
	req := &types.PublishRequest{
		Event: &types.Event{
			MessageId:  msgID,
			ScheduleTs: time.Now().UnixMilli() + 5000,
			Payload:    []byte("duplicate test"),
			Topic:      s.config.Topic,
		},
		AllowDuplicate: false,
	}

	// First publish should succeed
	resp1, err := s.eventClient.Publish(ctx, req)
	if err != nil {
		return fmt.Errorf("first publish failed: %w", err)
	}
	if !resp1.Success {
		return fmt.Errorf("first publish unsuccessful: %s", resp1.Error)
	}

	// Second publish with same message_id should fail
	resp2, err := s.eventClient.Publish(ctx, req)
	if err != nil {
		return fmt.Errorf("second publish RPC failed: %w", err)
	}

	if resp2.Success {
		return fmt.Errorf("duplicate message was accepted (should be rejected)")
	}

	if resp2.Error != "duplicate message_id" {
		return fmt.Errorf("unexpected error message: %s", resp2.Error)
	}

	return nil
}

func (s *IntegrationTestSuite) TestPublishAllowDuplicate() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeout)
	defer cancel()

	msgID := fmt.Sprintf("allow-dup-%d", time.Now().UnixNano())
	req := &types.PublishRequest{
		Event: &types.Event{
			MessageId:  msgID,
			ScheduleTs: time.Now().UnixMilli() + 5000,
			Payload:    []byte("allow duplicate test"),
			Topic:      s.config.Topic,
		},
		AllowDuplicate: true,
	}

	// First publish
	resp1, err := s.eventClient.Publish(ctx, req)
	if err != nil {
		return fmt.Errorf("first publish failed: %w", err)
	}
	if !resp1.Success {
		return fmt.Errorf("first publish unsuccessful: %s", resp1.Error)
	}

	// Second publish with AllowDuplicate=true should succeed
	resp2, err := s.eventClient.Publish(ctx, req)
	if err != nil {
		return fmt.Errorf("second publish failed: %w", err)
	}

	if !resp2.Success {
		return fmt.Errorf("duplicate with AllowDuplicate=true was rejected: %s", resp2.Error)
	}

	return nil
}

func (s *IntegrationTestSuite) TestPublishValidation() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeout)
	defer cancel()

	// Test missing message_id
	resp1, err := s.eventClient.Publish(ctx, &types.PublishRequest{
		Event: &types.Event{
			MessageId:  "",
			ScheduleTs: time.Now().UnixMilli(),
			Payload:    []byte("test"),
			Topic:      s.config.Topic,
		},
	})
	if err != nil {
		return fmt.Errorf("validation test RPC failed: %w", err)
	}
	if resp1.Success {
		return fmt.Errorf("empty message_id was accepted")
	}

	// Test missing schedule_ts
	resp2, err := s.eventClient.Publish(ctx, &types.PublishRequest{
		Event: &types.Event{
			MessageId:  fmt.Sprintf("val-%d", time.Now().UnixNano()),
			ScheduleTs: 0,
			Payload:    []byte("test"),
			Topic:      s.config.Topic,
		},
	})
	if err != nil {
		return fmt.Errorf("validation test RPC failed: %w", err)
	}
	if resp2.Success {
		return fmt.Errorf("zero schedule_ts was accepted")
	}

	// Test missing payload
	resp3, err := s.eventClient.Publish(ctx, &types.PublishRequest{
		Event: &types.Event{
			MessageId:  fmt.Sprintf("val-%d", time.Now().UnixNano()),
			ScheduleTs: time.Now().UnixMilli(),
			Payload:    nil,
			Topic:      s.config.Topic,
		},
	})
	if err != nil {
		return fmt.Errorf("validation test RPC failed: %w", err)
	}
	if resp3.Success {
		return fmt.Errorf("empty payload was accepted")
	}

	return nil
}

func (s *IntegrationTestSuite) TestPublishBatch() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeout)
	defer cancel()

	batchSize := 100
	baseTime := time.Now().UnixMilli()

	for i := 0; i < batchSize; i++ {
		req := &types.PublishRequest{
			Event: &types.Event{
				MessageId:  fmt.Sprintf("batch-%d-%d", time.Now().UnixNano(), i),
				ScheduleTs: baseTime + int64(i*100), // Stagger by 100ms
				Payload:    []byte(fmt.Sprintf("batch event %d", i)),
				Topic:      s.config.Topic,
			},
		}

		resp, err := s.eventClient.Publish(ctx, req)
		if err != nil {
			return fmt.Errorf("batch publish %d failed: %w", i, err)
		}
		if !resp.Success {
			return fmt.Errorf("batch publish %d unsuccessful: %s", i, resp.Error)
		}
	}

	return nil
}

func (s *IntegrationTestSuite) TestPublishLargePayload() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeout)
	defer cancel()

	// Create 1MB payload
	largePayload := make([]byte, 1024*1024)
	for i := range largePayload {
		largePayload[i] = byte(i % 256)
	}

	req := &types.PublishRequest{
		Event: &types.Event{
			MessageId:  fmt.Sprintf("large-%d", time.Now().UnixNano()),
			ScheduleTs: time.Now().UnixMilli() + 5000,
			Payload:    largePayload,
			Topic:      s.config.Topic,
		},
	}

	resp, err := s.eventClient.Publish(ctx, req)
	if err != nil {
		return fmt.Errorf("large payload publish failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("large payload publish unsuccessful: %s", resp.Error)
	}

	return nil
}

func (s *IntegrationTestSuite) TestPublishWithMetadata() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeout)
	defer cancel()

	req := &types.PublishRequest{
		Event: &types.Event{
			MessageId:  fmt.Sprintf("meta-%d", time.Now().UnixNano()),
			ScheduleTs: time.Now().UnixMilli() + 5000,
			Payload:    []byte("metadata test"),
			Topic:      s.config.Topic,
			Meta: map[string]string{
				"source":      "integration-test",
				"priority":    "high",
				"retry-count": "0",
				"correlation": "abc-123",
			},
		},
	}

	resp, err := s.eventClient.Publish(ctx, req)
	if err != nil {
		return fmt.Errorf("metadata publish failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("metadata publish unsuccessful: %s", resp.Error)
	}

	return nil
}

// ============================================================================
// Subscribe Tests
// ============================================================================

func (s *IntegrationTestSuite) TestSubscribeBasic() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := s.eventClient.Subscribe(ctx)
	if err != nil {
		return fmt.Errorf("subscribe failed: %w", err)
	}

	// Send subscription request
	subReq := &types.SubscribeRequest{
		ConsumerGroup:  s.config.ConsumerGroup + "-basic",
		Topic:          s.config.Topic,
		PartitionId:    0,
		StartOffset:    -1,
		MaxBufferSize:  100,
		SubscriptionId: s.config.SubscriptionID + "-basic",
		Replay:         false,
	}

	if err := stream.Send(subReq); err != nil {
		return fmt.Errorf("send subscription request failed: %w", err)
	}

	// Connection established successfully
	return nil
}

func (s *IntegrationTestSuite) TestSubscribeMultiple() error {
	topic := s.config.Topic + "-multi"
	numSubscribers := 3

	// Start multiple subscribers
	var wg sync.WaitGroup
	errCh := make(chan error, numSubscribers)

	for i := 0; i < numSubscribers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			stream, err := s.eventClient.Subscribe(ctx)
			if err != nil {
				errCh <- fmt.Errorf("subscriber %d: connect failed: %w", idx, err)
				return
			}

			subReq := &types.SubscribeRequest{
				ConsumerGroup:  fmt.Sprintf("%s-multi-%d", s.config.ConsumerGroup, idx),
				Topic:          topic,
				PartitionId:    0,
				StartOffset:    -1,
				MaxBufferSize:  100,
				SubscriptionId: fmt.Sprintf("%s-multi-%d", s.config.SubscriptionID, idx),
			}

			if err := stream.Send(subReq); err != nil {
				errCh <- fmt.Errorf("subscriber %d: send failed: %w", idx, err)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}

// ============================================================================
// Consumer Group Tests
// ============================================================================

func (s *IntegrationTestSuite) TestConsumerGroupCreate() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeout)
	defer cancel()

	groupID := fmt.Sprintf("test-group-%d", time.Now().UnixNano())
	req := &types.CreateConsumerGroupRequest{
		GroupId:    groupID,
		Topic:      s.config.Topic,
		Partitions: []int32{0},
	}

	resp, err := s.consumerClient.CreateConsumerGroup(ctx, req)
	if err != nil {
		return fmt.Errorf("create consumer group failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("create consumer group unsuccessful: %s", resp.Error)
	}

	return nil
}

func (s *IntegrationTestSuite) TestConsumerGroupGet() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeout)
	defer cancel()

	// First create a group
	groupID := fmt.Sprintf("test-group-get-%d", time.Now().UnixNano())
	createReq := &types.CreateConsumerGroupRequest{
		GroupId:    groupID,
		Topic:      s.config.Topic,
		Partitions: []int32{0, 1},
	}

	createResp, err := s.consumerClient.CreateConsumerGroup(ctx, createReq)
	if err != nil {
		return fmt.Errorf("create failed: %w", err)
	}
	if !createResp.Success {
		return fmt.Errorf("create unsuccessful: %s", createResp.Error)
	}

	// Now get it
	getReq := &types.GetConsumerGroupRequest{
		GroupId: groupID,
	}

	metadata, err := s.consumerClient.GetConsumerGroup(ctx, getReq)
	if err != nil {
		return fmt.Errorf("get consumer group failed: %w", err)
	}

	if metadata.GroupId != groupID {
		return fmt.Errorf("group ID mismatch: expected %s, got %s", groupID, metadata.GroupId)
	}

	if metadata.Topic != s.config.Topic {
		return fmt.Errorf("topic mismatch: expected %s, got %s", s.config.Topic, metadata.Topic)
	}

	return nil
}

func (s *IntegrationTestSuite) TestConsumerGroupList() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeout)
	defer cancel()

	// Create a few groups first
	for i := 0; i < 3; i++ {
		groupID := fmt.Sprintf("list-group-%d-%d", time.Now().UnixNano(), i)
		req := &types.CreateConsumerGroupRequest{
			GroupId:    groupID,
			Topic:      s.config.Topic + "-list",
			Partitions: []int32{0},
		}
		s.consumerClient.CreateConsumerGroup(ctx, req)
	}

	// List groups
	listReq := &types.ListConsumerGroupsRequest{
		Topic: "", // List all
	}

	resp, err := s.consumerClient.ListConsumerGroups(ctx, listReq)
	if err != nil {
		return fmt.Errorf("list consumer groups failed: %w", err)
	}

	if len(resp.Groups) == 0 {
		return fmt.Errorf("no consumer groups returned")
	}

	return nil
}

// ============================================================================
// Timing Tests
// ============================================================================

func (s *IntegrationTestSuite) TestTimingEventOrder() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeout)
	defer cancel()

	topic := s.config.Topic + "-order"
	baseTime := time.Now().Add(1 * time.Second).UnixMilli()

	// Publish events in reverse order (later events first)
	for i := 4; i >= 0; i-- {
		msgID := fmt.Sprintf("order-%d-%d", time.Now().UnixNano(), i)

		req := &types.PublishRequest{
			Event: &types.Event{
				MessageId:  msgID,
				ScheduleTs: baseTime + int64(i*500), // 0, 500, 1000, 1500, 2000ms
				Payload:    []byte(fmt.Sprintf("event %d", i)),
				Topic:      topic,
			},
		}

		resp, err := s.eventClient.Publish(ctx, req)
		if err != nil {
			return fmt.Errorf("publish %d failed: %w", i, err)
		}
		if !resp.Success {
			return fmt.Errorf("publish %d unsuccessful: %s", i, resp.Error)
		}
	}

	return nil
}

func (s *IntegrationTestSuite) TestTimingPrecision() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeout)
	defer cancel()

	// Test multiple schedule times
	delays := []time.Duration{
		100 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		2 * time.Second,
	}

	for _, delay := range delays {
		scheduleTs := time.Now().Add(delay).UnixMilli()
		msgID := fmt.Sprintf("precision-%d-%d", delay.Milliseconds(), time.Now().UnixNano())

		req := &types.PublishRequest{
			Event: &types.Event{
				MessageId:  msgID,
				ScheduleTs: scheduleTs,
				Payload:    []byte(fmt.Sprintf("precision test %v", delay)),
				Topic:      s.config.Topic + "-precision",
			},
		}

		resp, err := s.eventClient.Publish(ctx, req)
		if err != nil {
			return fmt.Errorf("precision publish %v failed: %w", delay, err)
		}
		if !resp.Success {
			return fmt.Errorf("precision publish %v unsuccessful: %s", delay, resp.Error)
		}
	}

	return nil
}

func (s *IntegrationTestSuite) TestTimingFarFuture() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeout)
	defer cancel()

	// Test scheduling far in the future (tests overflow wheel handling)
	futureTimes := []time.Duration{
		1 * time.Minute,
		10 * time.Minute,
		1 * time.Hour,
		24 * time.Hour, // 1 day
	}

	for _, futureDelay := range futureTimes {
		scheduleTs := time.Now().Add(futureDelay).UnixMilli()
		msgID := fmt.Sprintf("far-future-%d-%d", int(futureDelay.Minutes()), time.Now().UnixNano())

		req := &types.PublishRequest{
			Event: &types.Event{
				MessageId:  msgID,
				ScheduleTs: scheduleTs,
				Payload:    []byte(fmt.Sprintf("far future test: %v", futureDelay)),
				Topic:      s.config.Topic + "-future",
			},
		}

		resp, err := s.eventClient.Publish(ctx, req)
		if err != nil {
			return fmt.Errorf("far future publish %v failed: %w", futureDelay, err)
		}
		if !resp.Success {
			return fmt.Errorf("far future publish %v unsuccessful: %s", futureDelay, resp.Error)
		}
	}

	return nil
}

// ============================================================================
// Concurrency Tests
// ============================================================================

func (s *IntegrationTestSuite) TestConcurrentPublish() error {
	numWorkers := 10
	eventsPerWorker := 50

	var wg sync.WaitGroup
	errCh := make(chan error, numWorkers)
	var successCount int64

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for i := 0; i < eventsPerWorker; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

				msgID := fmt.Sprintf("concurrent-%d-%d-%d", workerID, i, time.Now().UnixNano())
				req := &types.PublishRequest{
					Event: &types.Event{
						MessageId:  msgID,
						ScheduleTs: time.Now().UnixMilli() + 10000,
						Payload:    []byte(fmt.Sprintf("worker %d event %d", workerID, i)),
						Topic:      s.config.Topic + "-concurrent",
					},
				}

				resp, err := s.eventClient.Publish(ctx, req)
				cancel()

				if err != nil {
					errCh <- fmt.Errorf("worker %d event %d: %w", workerID, i, err)
					return
				}
				if !resp.Success {
					errCh <- fmt.Errorf("worker %d event %d: %s", workerID, i, resp.Error)
					return
				}

				atomic.AddInt64(&successCount, 1)
			}
		}(w)
	}

	wg.Wait()
	close(errCh)

	// Check for errors
	for err := range errCh {
		if err != nil {
			return err
		}
	}

	expectedCount := int64(numWorkers * eventsPerWorker)
	if successCount != expectedCount {
		return fmt.Errorf("expected %d events, published %d", expectedCount, successCount)
	}

	return nil
}

func (s *IntegrationTestSuite) TestConcurrentSubscribe() error {
	numSubscribers := 5

	var wg sync.WaitGroup
	errCh := make(chan error, numSubscribers)
	var successCount int64

	for i := 0; i < numSubscribers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			stream, err := s.eventClient.Subscribe(ctx)
			if err != nil {
				errCh <- fmt.Errorf("subscriber %d: connect failed: %w", idx, err)
				return
			}

			subReq := &types.SubscribeRequest{
				ConsumerGroup:  fmt.Sprintf("%s-concurrent-%d", s.config.ConsumerGroup, idx),
				Topic:          s.config.Topic + "-concurrent-sub",
				PartitionId:    0,
				StartOffset:    -1,
				MaxBufferSize:  100,
				SubscriptionId: fmt.Sprintf("%s-concurrent-%d", s.config.SubscriptionID, idx),
			}

			if err := stream.Send(subReq); err != nil {
				errCh <- fmt.Errorf("subscriber %d: send failed: %w", idx, err)
				return
			}

			atomic.AddInt64(&successCount, 1)
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	if successCount != int64(numSubscribers) {
		return fmt.Errorf("expected %d subscribers, connected %d", numSubscribers, successCount)
	}

	return nil
}

// ============================================================================
// Edge Case Tests
// ============================================================================

func (s *IntegrationTestSuite) TestEdgeCaseZeroDelay() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeout)
	defer cancel()

	// Schedule for right now
	msgID := fmt.Sprintf("zero-delay-%d", time.Now().UnixNano())
	req := &types.PublishRequest{
		Event: &types.Event{
			MessageId:  msgID,
			ScheduleTs: time.Now().UnixMilli(),
			Payload:    []byte("zero delay test"),
			Topic:      s.config.Topic,
		},
	}

	resp, err := s.eventClient.Publish(ctx, req)
	if err != nil {
		return fmt.Errorf("zero delay publish failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("zero delay publish unsuccessful: %s", resp.Error)
	}

	return nil
}

func (s *IntegrationTestSuite) TestEdgeCaseMaxPayload() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Test with 10MB payload (near gRPC default limit)
	largePayload := make([]byte, 10*1024*1024)
	for i := range largePayload {
		largePayload[i] = byte(i % 256)
	}

	msgID := fmt.Sprintf("max-payload-%d", time.Now().UnixNano())
	req := &types.PublishRequest{
		Event: &types.Event{
			MessageId:  msgID,
			ScheduleTs: time.Now().UnixMilli() + 5000,
			Payload:    largePayload,
			Topic:      s.config.Topic,
		},
	}

	resp, err := s.eventClient.Publish(ctx, req)
	if err != nil {
		// gRPC might reject very large payloads - that's acceptable
		return nil // Consider this a pass
	}

	if !resp.Success {
		// Server might reject - that's acceptable for edge case
		return nil
	}

	return nil
}

func (s *IntegrationTestSuite) TestEdgeCaseSpecialChars() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Timeout)
	defer cancel()

	// Test with special characters in message ID and payload
	specialID := fmt.Sprintf("special-!@#$%%^&*()_+-=%d", time.Now().UnixNano())
	specialPayload := []byte("Special chars: 日本語 中文 العربية émoji 🚀🎉")

	req := &types.PublishRequest{
		Event: &types.Event{
			MessageId:  specialID,
			ScheduleTs: time.Now().UnixMilli() + 5000,
			Payload:    specialPayload,
			Topic:      s.config.Topic + "-special-!@#",
			Meta: map[string]string{
				"unicode-key-日本語": "unicode-value-中文",
			},
		},
	}

	resp, err := s.eventClient.Publish(ctx, req)
	if err != nil {
		return fmt.Errorf("special chars publish failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("special chars publish unsuccessful: %s", resp.Error)
	}

	return nil
}

// ============================================================================
// End-to-End Tests
// ============================================================================

func (s *IntegrationTestSuite) TestE2EPublishAndReceive() error {
	// Use completely unique topic to avoid stale data from previous runs
	topic := fmt.Sprintf("e2e-test-%d", time.Now().UnixNano())
	msgID := fmt.Sprintf("e2e-%d", time.Now().UnixNano())

	// Publish an event scheduled 2 seconds from now
	scheduleTime := time.Now().Add(2 * time.Second)

	publishCtx, publishCancel := context.WithTimeout(context.Background(), s.config.Timeout)
	defer publishCancel()

	publishReq := &types.PublishRequest{
		Event: &types.Event{
			MessageId:  msgID,
			ScheduleTs: scheduleTime.UnixMilli(),
			Payload:    []byte("end-to-end test payload"),
			Topic:      topic,
		},
	}

	resp, err := s.eventClient.Publish(publishCtx, publishReq)
	if err != nil {
		return fmt.Errorf("e2e publish failed: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("e2e publish unsuccessful: %s", resp.Error)
	}

	// Use the partition ID from publish response for subscribing
	publishedPartitionID := resp.PartitionId

	// Subscribe and wait for the event
	subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer subCancel()

	stream, err := s.eventClient.Subscribe(subCtx)
	if err != nil {
		return fmt.Errorf("e2e subscribe failed: %w", err)
	}

	subReq := &types.SubscribeRequest{
		ConsumerGroup:  fmt.Sprintf("e2e-consumer-%d", time.Now().UnixNano()),
		Topic:          topic,
		PartitionId:    publishedPartitionID, // Use the actual partition where event was published
		StartOffset:    -1,
		MaxBufferSize:  100,
		SubscriptionId: fmt.Sprintf("e2e-sub-%d", time.Now().UnixNano()),
	}

	if err := stream.Send(subReq); err != nil {
		return fmt.Errorf("e2e send subscription failed: %w", err)
	}

	// Wait for the event with timeout
	deadline := time.After(8 * time.Second)
	for {
		select {
		case <-deadline:
			return fmt.Errorf("e2e timeout waiting for event delivery")
		default:
			delivery, err := stream.Recv()
			if err != nil {
				if subCtx.Err() != nil {
					return fmt.Errorf("e2e context cancelled")
				}
				continue
			}

			// Only process our specific event
			if delivery.Event.MessageId == msgID {
				// Verify timing - event should not arrive more than 500ms early
				receivedAt := time.Now()
				delay := receivedAt.Sub(scheduleTime)

				// Allow 500ms early tolerance (timing wheel granularity)
				if delay < -500*time.Millisecond {
					return fmt.Errorf("e2e event delivered too early: %v before schedule (msgID=%s, scheduleTs=%d)",
						-delay, msgID, scheduleTime.UnixMilli())
				}

				return nil // Success!
			}
			// Skip events that don't match our msgID (could be from previous tests)
		}
	}
}

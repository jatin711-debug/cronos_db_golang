package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"cronos_db/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	fmt.Println("=== ChronosDB Pub/Sub Integration Test ===\n")

	// Connect to server
	conn, err := grpc.Dial(":9000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	eventClient := types.NewEventServiceClient(conn)
	consumerClient := types.NewConsumerGroupServiceClient(conn)

	// Test parameters
	topic := "integration-test-topic"
	consumerGroup := "integration-test-consumer"
	subscriptionID := "integration-test-subscription"

	// Generate unique message IDs
	msgID1 := fmt.Sprintf("test-msg-%d-1", time.Now().UnixNano())
	msgID2 := fmt.Sprintf("test-msg-%d-2", time.Now().UnixNano())
	msgID3 := fmt.Sprintf("test-msg-%d-3", time.Now().UnixNano())

	// Test 1: Immediate delivery (schedule in the past)
	fmt.Println("=== Test 1: Immediate Delivery ===")
	testImmediateDelivery(eventClient, topic, msgID1)

	// Test 2: Scheduled delivery (2 seconds in the future)
	fmt.Println("\n=== Test 2: Scheduled Delivery (2s) ===")
	testScheduledDelivery(eventClient, topic, msgID2, 2*time.Second)

	// Test 3: Multiple events
	fmt.Println("\n=== Test 3: Multiple Events ===")
	testMultipleEvents(eventClient, topic, msgID3)

	// Test 4: Subscriber receives events
	fmt.Println("\n=== Test 4: Subscriber Test ===")
	testSubscriber(eventClient, consumerClient, topic, consumerGroup, subscriptionID, []string{msgID1, msgID2})

	fmt.Println("\n=== All Tests Completed Successfully! ===")
}

func testImmediateDelivery(client types.EventServiceClient, topic, msgID string) {
	scheduleTs := time.Now().UnixMilli() - 1000 // 1 second in the past

	req := &types.PublishRequest{
		Event: &types.Event{
			MessageId:  msgID,
			ScheduleTs: scheduleTs,
			Payload:    []byte("Immediate delivery test"),
			Topic:      topic,
		},
		AllowDuplicate: false,
	}

	resp, err := client.Publish(context.Background(), req)
	if err != nil {
		log.Fatalf("Test 1 - Publish failed: %v", err)
	}

	if !resp.Success {
		log.Fatalf("Test 1 - Publish failed: %s", resp.Error)
	}

	fmt.Printf("✅ Test 1 PASSED: Published immediately deliverable event (offset=%d)\n", resp.Offset)
}

func testScheduledDelivery(client types.EventServiceClient, topic, msgID string, delay time.Duration) {
	scheduleTs := time.Now().UnixMilli() + delay.Milliseconds()

	req := &types.PublishRequest{
		Event: &types.Event{
			MessageId:  msgID,
			ScheduleTs: scheduleTs,
			Payload:    []byte("Scheduled delivery test"),
			Topic:      topic,
		},
		AllowDuplicate: false,
	}

	resp, err := client.Publish(context.Background(), req)
	if err != nil {
		log.Fatalf("Test 2 - Publish failed: %v", err)
	}

	if !resp.Success {
		log.Fatalf("Test 2 - Publish failed: %s", resp.Error)
	}

	fmt.Printf("✅ Test 2 PASSED: Published scheduled event (offset=%d, delay=%v)\n", resp.Offset, delay)
}

func testMultipleEvents(client types.EventServiceClient, topic, msgID string) {
	// Publish multiple events with different delays
	delays := []time.Duration{
		500 * time.Millisecond,
		1500 * time.Millisecond,
		3000 * time.Millisecond,
	}

	for i, delay := range delays {
		id := fmt.Sprintf("%s-%d", msgID, i)
		scheduleTs := time.Now().UnixMilli() + delay.Milliseconds()

		req := &types.PublishRequest{
			Event: &types.Event{
				MessageId:  id,
				ScheduleTs: scheduleTs,
				Payload:    []byte(fmt.Sprintf("Event %d", i)),
				Topic:      topic,
			},
			AllowDuplicate: false,
		}

		resp, err := client.Publish(context.Background(), req)
		if err != nil {
			log.Fatalf("Test 3 - Publish %d failed: %v", i, err)
		}

		if !resp.Success {
			log.Fatalf("Test 3 - Publish %d failed: %s", i, resp.Error)
		}
	}

	fmt.Printf("✅ Test 3 PASSED: Published %d scheduled events\n", len(delays))
}

func testSubscriber(
	client types.EventServiceClient,
	consumerClient types.ConsumerGroupServiceClient,
	topic, consumerGroup, subscriptionID string,
	expectedMsgIDs []string,
) {
	// Create subscription request
	subscribeReq := &types.SubscribeRequest{
		ConsumerGroup:  consumerGroup,
		Topic:          topic,
		PartitionId:    0,
		StartOffset:    -1, // Start from latest
		MaxBufferSize:  100,
		SubscriptionId: subscriptionID,
		Replay:         false,
	}

	// Create stream
	stream, err := client.Subscribe(context.Background())
	if err != nil {
		log.Fatalf("Test 4 - Subscribe failed: %v", err)
	}

	// Send subscription request
	err = stream.Send(subscribeReq)
	if err != nil {
		log.Fatalf("Test 4 - Send subscription failed: %v", err)
	}

	fmt.Println("✅ Test 4 - Subscriber connected")

	// Receive events
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	receivedCount := 0
	timeout := time.After(10 * time.Second)

	for receivedCount < len(expectedMsgIDs) {
		select {
		case <-timeout:
			log.Fatalf("Test 4 - Timeout waiting for events (received %d/%d)", receivedCount, len(expectedMsgIDs))
		case <-ctx.Done():
			log.Fatalf("Test 4 - Context cancelled (received %d/%d)", receivedCount, len(expectedMsgIDs))
		default:
			delivery, err := stream.Recv()
			if err != nil {
				log.Fatalf("Test 4 - Error receiving delivery: %v", err)
			}

			event := delivery.Event

			// Check if this is one of our expected messages
			found := false
			for _, expectedID := range expectedMsgIDs {
				if event.MessageId == expectedID {
					found = true
					receivedCount++
					fmt.Printf("✅ Test 4 - Received event %d/%d: %s (offset=%d)\n",
						receivedCount, len(expectedMsgIDs), event.MessageId, event.Offset)
					break
				}
			}

			if !found {
				// Ignore other events
				continue
			}

			if receivedCount == len(expectedMsgIDs) {
				fmt.Printf("✅ Test 4 PASSED: Received all %d expected events\n", len(expectedMsgIDs))
				return
			}
		}
	}
}

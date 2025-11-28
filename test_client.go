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
	// Connect to server
	conn, err := grpc.Dial(":9000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	eventClient := types.NewEventServiceClient(conn)

	// Generate unique message ID
	msgID := fmt.Sprintf("test-msg-%d", time.Now().UnixNano())
	topic := "test-topic"

	fmt.Printf("=== Testing Pub/Sub Flow ===\n")
	fmt.Printf("Message ID: %s\n", msgID)
	fmt.Printf("Topic: %s\n\n", topic)

	// Step 1: Publish event
	fmt.Println("Step 1: Publishing event...")
	scheduleTs := time.Now().UnixMilli() + 2000 // Schedule 2 seconds in the future

	publishReq := &types.PublishRequest{
		Event: &types.Event{
			MessageId:  msgID,
			ScheduleTs: scheduleTs,
			Payload:    []byte("Test payload"),
			Topic:      topic,
		},
		AllowDuplicate: false,
	}

	publishResp, err := eventClient.Publish(context.Background(), publishReq)
	if err != nil {
		log.Fatalf("Publish failed: %v", err)
	}

	if !publishResp.Success {
		log.Fatalf("Publish failed: %s", publishResp.Error)
	}

	fmt.Printf("✅ Published successfully\n")
	fmt.Printf("   Offset: %d\n", publishResp.Offset)
	fmt.Printf("   Partition: %d\n", publishResp.PartitionId)
	fmt.Printf("   Schedule TS: %d\n\n", publishResp.ScheduleTs)

	// Step 2: Subscribe to topic
	fmt.Println("Step 2: Subscribing to topic...")
	subscribeReq := &types.SubscribeRequest{
		ConsumerGroup:  "test-consumer-group",
		Topic:          topic,
		PartitionId:    -1, // Auto-assign partition
		StartOffset:    -1, // Start from latest
		MaxBufferSize:  100,
		SubscriptionId: "test-subscription",
		Replay:         false,
	}

	stream, err := eventClient.Subscribe(context.Background())
	if err != nil {
		log.Fatalf("Subscribe failed: %v", err)
	}

	// Send subscription request
	err = stream.Send(subscribeReq)
	if err != nil {
		log.Fatalf("Send subscription failed: %v", err)
	}

	fmt.Println("✅ Subscription created")

	// Step 3: Wait for event delivery
	fmt.Println("Step 3: Waiting for event delivery (max 10 seconds)...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var receivedEvent *types.Event
	var deliveryID string

	done := make(chan bool)
	go func() {
		for {
			delivery, err := stream.Recv()
			if err != nil {
				fmt.Printf("❌ Error receiving delivery: %v\n", err)
				done <- false
				return
			}

			if delivery.Event.MessageId == msgID {
				receivedEvent = delivery.Event
				deliveryID = delivery.DeliveryId
				fmt.Printf("✅ Event received!\n")
				fmt.Printf("   Message ID: %s\n", receivedEvent.MessageId)
				fmt.Printf("   Topic: %s\n", receivedEvent.Topic)
				fmt.Printf("   Payload: %s\n", string(receivedEvent.Payload))
				fmt.Printf("   Delivery ID: %s\n\n", deliveryID)
				done <- true
				return
			}
		}
	}()

	// Wait for event with timeout
	select {
	case success := <-done:
		if !success {
			log.Fatal("Failed to receive event")
		}
	case <-ctx.Done():
		log.Fatalf("❌ Timeout waiting for event delivery")
	}

	// Step 4: Acknowledge event
	fmt.Println("Step 4: Acknowledging event...")

	// Create a new ack stream
	ackStream, err := eventClient.Ack(context.Background())
	if err != nil {
		log.Fatalf("Ack stream failed: %v", err)
	}

	// Send ack request
	ackReq := &types.AckRequest{
		DeliveryId: deliveryID,
		Success:    true,
	}

	err = ackStream.Send(ackReq)
	if err != nil {
		log.Fatalf("Send ack failed: %v", err)
	}

	// Receive ack response
	ackResp, err := ackStream.Recv()
	if err != nil {
		log.Fatalf("Ack response failed: %v", err)
	}

	if ackResp.Success {
		fmt.Println("✅ Event acknowledged successfully")
		fmt.Printf("   Committed Offset: %d\n\n", ackResp.CommittedOffset)
	} else {
		log.Fatalf("Ack failed: %s", ackResp.Error)
	}

	fmt.Println("=== Test Complete! ===")
}

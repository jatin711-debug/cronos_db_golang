package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"cronos_db/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	// Connect to server
	conn, err := grpc.Dial("localhost:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := types.NewEventServiceClient(conn)
	groupClient := types.NewConsumerGroupServiceClient(conn)

	// Step 1: Create Consumer Group
	fmt.Println("=== Creating Consumer Group ===")
	createGroupResp, err := groupClient.CreateConsumerGroup(context.Background(), &types.CreateConsumerGroupRequest{
		GroupId:    "test-group",
		Topic:      "test-topic",
		Partitions: []int32{0},
	})
	if err != nil {
		// Check if group already exists
		if st, ok := status.FromError(err); ok && st.Code() == codes.AlreadyExists {
			fmt.Println("Group already exists, continuing...")
		} else {
			log.Printf("Warning: Failed to create group: %v", err)
		}
	} else {
		fmt.Printf("Group created: %+v\n", createGroupResp)
	}

	// Step 2: Publish multiple events
	fmt.Println("\n=== Publishing Events ===")
	for i := 1; i <= 5; i++ {
		resp, err := client.Publish(context.Background(), &types.PublishRequest{
			Event: &types.Event{
				MessageId:  fmt.Sprintf("test-msg-%d", i),
				ScheduleTs: time.Now().UnixMilli(),
				Payload:    []byte(fmt.Sprintf("Test message %d", i)),
				Topic:      "test-topic",
				Meta:       map[string]string{"index": fmt.Sprintf("%d", i)},
			},
			AllowDuplicate: true,
		})
		if err != nil {
			log.Printf("Publish %d failed: %v", i, err)
		} else {
			fmt.Printf("Published msg-%d: offset=%d, partition=%d\n", i, resp.Offset, resp.PartitionId)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Step 3: Subscribe to events
	fmt.Println("\n=== Subscribing to Events (press Ctrl+C to stop) ===")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create stream
	stream, err := client.Subscribe(ctx)
	if err != nil {
		log.Fatalf("Subscribe failed: %v", err)
	}

	// Send subscription request
	if err := stream.Send(&types.SubscribeRequest{
		ConsumerGroup:   "test-group",
		Topic:          "test-topic",
		PartitionId:    0,
		MaxBufferSize:  10,
	}); err != nil {
		log.Fatalf("Send subscription request failed: %v", err)
	}

	// Receive events
	for i := 0; i < 5; i++ {
		delivery, err := stream.Recv()
		if err != nil {
			log.Printf("Stream error: %v", err)
			break
		}
		fmt.Printf("\n✓ Received Event %d:\n", i+1)
		fmt.Printf("  Delivery ID: %s\n", delivery.GetDeliveryId())
		fmt.Printf("  Message ID: %s\n", delivery.Event.GetMessageId())
		fmt.Printf("  Topic: %s\n", delivery.Event.Topic)
		fmt.Printf("  Payload: %s\n", string(delivery.Event.Payload))
		fmt.Printf("  Offset: %d\n", delivery.Event.Offset)
		fmt.Printf("  Partition: %d\n", delivery.Event.GetPartitionId())

	}
		// Note: For production, send ACKs on a separate Ack stream
		fmt.Printf("  (ACK would be sent on separate stream)");
	fmt.Println("\n=== Test Complete ===")
}

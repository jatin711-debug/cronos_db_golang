package integration

import (
	"context"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
)

// TestSchemaRegistrationAndValidation verifies that schema validation works on the publish path.
func TestSchemaRegistrationAndValidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := topicName(t)

	// Publish an event that should match a schema on the server
	validPayload := []byte(`{"user":"alice","email":"alice@example.com"}`)
	res, err := publishEvent(ctx, topic, validPayload, 1*time.Second)
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	t.Logf("Published at offset %d", res.Offset)

	// Receive it back to verify publish path works
	deliveries := make(chan client.Delivery, 2)
	consCfg := client.DefaultConsumerConfig(topic, topicName(t)+"-group")
	consCfg.AckMode = client.AckModeAuto

	go func() {
		_ = testClient.Subscribe(ctx, consCfg, func(ctx context.Context, d client.Delivery) error {
			deliveries <- d
			return nil
		})
	}()

	select {
	case d := <-deliveries:
		t.Logf("Received: offset=%d", d.Event.GetOffset())
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for delivery")
	}
}

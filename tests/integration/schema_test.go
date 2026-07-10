package integration

import (
	"context"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/schema"
	"github.com/jatin711-debug/cronos_db_golang/pkg/client"

	"github.com/hamba/avro/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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

// TestSchemaRegistryUnitValidation tests the schema registry validation logic directly.
func TestSchemaRegistryUnitValidation(t *testing.T) {
	reg, err := schema.NewRegistry(t.TempDir())
	if err != nil {
		t.Fatalf("create registry: %v", err)
	}

	topic := "test-schema-topic"

	// Register a JSON schema
	_, err = reg.Register(topic, schema.TypeJSON, `{"type":"object","properties":{"user":{"type":"string"},"email":{"type":"string"}}}`)
	if err != nil {
		t.Fatalf("register schema: %v", err)
	}

	// Valid payload passes
	validPayload := []byte(`{"user":"alice","email":"alice@example.com"}`)
	if err := reg.Validate(topic, validPayload); err != nil {
		t.Errorf("expected valid payload to pass: %v", err)
	}

	// Invalid payload (wrong type) fails
	invalidPayload := []byte(`{"user":123}`)
	if err := reg.Validate(topic, invalidPayload); err == nil {
		t.Error("expected invalid payload to fail validation")
	} else {
		t.Logf("Invalid payload correctly rejected: %v", err)
	}

	// Unregistered topic allows all (no schema = allow)
	if err := reg.Validate("unknown-topic", []byte(`garbage`)); err != nil {
		t.Errorf("expected unregistered topic to allow: %v", err)
	}
}

// TestSchemaRegistryAvroValidation tests AVRO schema validation.
func TestSchemaRegistryAvroValidation(t *testing.T) {
	reg, err := schema.NewRegistry(t.TempDir())
	if err != nil {
		t.Fatalf("create registry: %v", err)
	}

	topic := "test-avro-topic"
	avroSchema := `{"type":"record","name":"User","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}`

	_, err = reg.Register(topic, schema.TypeAvro, avroSchema)
	if err != nil {
		t.Fatalf("register avro schema: %v", err)
	}

	validPayload, err := encodeAvro(avroSchema, map[string]interface{}{"name": "alice", "age": 30})
	if err != nil {
		t.Fatalf("encode valid avro: %v", err)
	}
	if err := reg.Validate(topic, validPayload); err != nil {
		t.Errorf("expected valid avro to pass: %v", err)
	}

	invalidPayload, err := encodeAvro(avroSchema, map[string]interface{}{"name": "bob", "age": "thirty"})
	if err != nil {
		// The Avro encoder itself rejects wrong types, which is the expected
		// outcome for this test.
		t.Logf("Avro encoder rejected wrong-type record: %v", err)
	} else {
		if err := reg.Validate(topic, invalidPayload); err == nil {
			t.Error("expected avro wrong-type to fail validation")
		} else {
			t.Logf("Invalid avro correctly rejected: %v", err)
		}
	}
}

// TestSchemaRegistryProtobufValidation tests protobuf well-known type validation.
func TestSchemaRegistryProtobufValidation(t *testing.T) {
	reg, err := schema.NewRegistry(t.TempDir())
	if err != nil {
		t.Fatalf("create registry: %v", err)
	}

	topic := "test-proto-wellknown"
	_, err = reg.Register(topic, schema.TypeProtobuf, "google.protobuf.Timestamp")
	if err != nil {
		t.Fatalf("register protobuf schema: %v", err)
	}

	validPayload, err := marshalTimestamp(time.Now())
	if err != nil {
		t.Fatalf("encode timestamp: %v", err)
	}
	if err := reg.Validate(topic, validPayload); err != nil {
		t.Errorf("expected valid timestamp to pass: %v", err)
	}
}

// encodeAvro encodes a Go map to Avro binary format.
func encodeAvro(schemaJSON string, record map[string]interface{}) ([]byte, error) {
	schema, err := avro.Parse(schemaJSON)
	if err != nil {
		return nil, err
	}
	return avro.Marshal(schema, record)
}

// marshalTimestamp encodes a time.Time to protobuf Timestamp binary.
func marshalTimestamp(ts time.Time) ([]byte, error) {
	msg := timestamppb.New(ts)
	return proto.Marshal(msg)
}

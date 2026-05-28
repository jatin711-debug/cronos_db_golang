package cdc

import (
	"context"
	"encoding/json"
	"fmt"
)

// KafkaSink is a CDC sink that writes to Kafka (placeholder implementation).
// In production, use segmentio/kafka-go or confluent-kafka-go.
type KafkaSink struct {
	brokers []string
	topic   string
}

// NewKafkaSink creates a Kafka CDC sink.
func NewKafkaSink(brokers []string, topic string) *KafkaSink {
	return &KafkaSink{brokers: brokers, topic: topic}
}

func (k *KafkaSink) Name() string { return "kafka" }

func (k *KafkaSink) Write(ctx context.Context, event *ChangeEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_ = data
	// Placeholder: real implementation would produce to Kafka
	return fmt.Errorf("kafka sink not implemented; would produce %d bytes to %s", len(data), k.topic)
}

func (k *KafkaSink) Close() error { return nil }

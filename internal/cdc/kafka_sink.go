package cdc

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaSink is a CDC sink that writes to Kafka using segmentio/kafka-go.
type KafkaSink struct {
	brokers []string
	topic   string
	writer  *kafka.Writer
	mu      sync.Mutex
}

// NewKafkaSink creates a Kafka CDC sink.
func NewKafkaSink(brokers []string, topic string) *KafkaSink {
	return &KafkaSink{
		brokers: brokers,
		topic:   topic,
	}
}

func (k *KafkaSink) Name() string { return "kafka" }

func (k *KafkaSink) Write(ctx context.Context, event *ChangeEvent) error {
	// Lazy-init writer on first use
	if err := k.ensureWriter(); err != nil {
		return err
	}

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal change event: %w", err)
	}

	key := fmt.Sprintf("%s/%d", event.Topic, event.PartitionID)
	msg := kafka.Message{
		Key:   []byte(key),
		Value: data,
		Time:  event.Timestamp,
	}

	return k.writer.WriteMessages(ctx, msg)
}

func (k *KafkaSink) Close() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.writer != nil {
		return k.writer.Close()
	}
	return nil
}

func (k *KafkaSink) ensureWriter() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.writer != nil {
		return nil
	}

	if len(k.brokers) == 0 {
		return fmt.Errorf("no Kafka brokers configured")
	}

	k.writer = &kafka.Writer{
		Addr:         kafka.TCP(k.brokers...),
		Topic:        k.topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    100,
	}
	return nil
}

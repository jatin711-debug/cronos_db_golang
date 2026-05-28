package client

import (
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client/internal/circuitbreaker"
)

func TestNormalizeMessageEncodesValueAndSetsCodecMeta(t *testing.T) {
	cfg := ProducerConfig{
		AutoMessageID:   true,
		Codec:           JSONCodec{},
		MaxPayloadBytes: 1024,
	}
	msg := Message{
		Topic:      "orders",
		ScheduleTS: 123,
		Value: map[string]any{
			"id": "o-1",
		},
	}

	out, err := normalizeMessage(msg, cfg)
	if err != nil {
		t.Fatalf("normalize message: %v", err)
	}
	if len(out.Payload) == 0 {
		t.Fatal("expected encoded payload")
	}
	if out.MessageID == "" {
		t.Fatal("expected auto message id")
	}
	if got := out.Meta[MetaCodecNameKey]; got != "json" {
		t.Fatalf("expected codec meta json, got %q", got)
	}
}

func TestNormalizeMessagePayloadLimit(t *testing.T) {
	cfg := ProducerConfig{
		AutoMessageID:   false,
		Codec:           JSONCodec{},
		MaxPayloadBytes: 5,
	}
	msg := Message{
		MessageID:  "m-1",
		Topic:      "orders",
		ScheduleTS: 123,
		Payload:    []byte("payload-too-large"),
	}

	if _, err := normalizeMessage(msg, cfg); err == nil {
		t.Fatal("expected payload size error")
	}
}

func TestCircuitBreakerOpensAndRecovers(t *testing.T) {
	cb := circuitbreaker.New(circuitbreaker.Config{
		FailureThreshold: 2,
		SuccessThreshold: 1,
		Timeout:          30 * time.Millisecond,
	})
	if !cb.Allow() {
		t.Fatal("expected breaker to allow initially")
	}

	cb.RecordFailure()
	if !cb.Allow() {
		t.Fatal("expected breaker to still allow before threshold")
	}

	cb.RecordFailure()
	if cb.Allow() {
		t.Fatal("expected breaker to be open after threshold")
	}

	time.Sleep(40 * time.Millisecond)
	if !cb.Allow() {
		t.Fatal("expected breaker to close after cooldown")
	}
}

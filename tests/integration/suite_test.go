package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

var (
	serverAddr string
	testClient *client.Client
)

func TestMain(m *testing.M) {
	serverAddr = os.Getenv("CRONOS_TEST_ADDR")
	if serverAddr == "" {
		serverAddr = "localhost:9000"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := client.DefaultConfig(serverAddr)
	cfg.Security.Insecure = true
	cfg.DialTimeout = 10 * time.Second
	cfg.RequestTimeout = 10 * time.Second

	c, err := client.Dial(ctx, cfg)
	if err != nil {
		// When CRONOS_TEST_INTEGRATION=1 (CI), a missing server is a hard
		// failure so the run can't silently report green. Otherwise skip the
		// suite gracefully for local dev where no server is running.
		if os.Getenv("CRONOS_TEST_INTEGRATION") == "1" {
			fmt.Fprintf(os.Stderr, "INTEGRATION FAIL: cannot dial %s: %v\n", serverAddr, err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "INTEGRATION SKIP: cannot dial %s: %v\n", serverAddr, err)
		os.Exit(0)
	}
	testClient = c
	defer testClient.Close()

	code := m.Run()
	os.Exit(code)
}

func topicName(t *testing.T) string {
	return fmt.Sprintf("test-%s-%d", t.Name(), time.Now().UnixNano())
}

func waitForDelivery(t *testing.T, ctx context.Context, topic string, expectedCount int, timeout time.Duration) ([]client.Delivery, error) {
	var deliveries []client.Delivery
	done := make(chan struct{})

	consCfg := client.DefaultConsumerConfig(topic, fmt.Sprintf("test-group-%d", time.Now().UnixNano()))
	consCfg.AckMode = client.AckModeAuto

	go func() {
		_ = testClient.Subscribe(ctx, consCfg, func(ctx context.Context, d client.Delivery) error {
			deliveries = append(deliveries, d)
			if len(deliveries) >= expectedCount {
				close(done)
			}
			return nil
		})
	}()

	select {
	case <-done:
		return deliveries, nil
	case <-time.After(timeout):
		return deliveries, fmt.Errorf("timeout waiting for %d deliveries, got %d", expectedCount, len(deliveries))
	}
}

func publishEvent(ctx context.Context, topic string, payload []byte, scheduleDelay time.Duration) (*client.SendResult, error) {
	producer, err := client.NewProducer(testClient, client.DefaultProducerConfig())
	if err != nil {
		return nil, err
	}
	defer producer.Close()

	return producer.Send(ctx, client.Message{
		Topic:        topic,
		PartitionKey: topic,
		Payload:      payload,
		ScheduleTS:   time.Now().Add(scheduleDelay).UnixMilli(),
	})
}

func partitionClient() types.PartitionServiceClient {
	// Access internal pool to get partition client
	// This is a test helper; in real tests we use the public API
	return nil
}

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
		// Default to the IPv4 loopback to avoid IPv6 resolution differences in CI.
		serverAddr = "127.0.0.1:9000"
	}

	// Integration tests need an external server and can leave persistent state
	// behind. Only run them when explicitly requested so that `go test ./...`
	// does not fail against a leftover dev server.
	if os.Getenv("CRONOS_TEST_INTEGRATION") != "1" {
		fmt.Fprintf(os.Stderr, "INTEGRATION SKIP: set CRONOS_TEST_INTEGRATION=1 to run integration tests against %s\n", serverAddr)
		os.Exit(0)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := client.DefaultConfig(serverAddr)
	cfg.Security.Insecure = true
	cfg.DialTimeout = 10 * time.Second
	cfg.RequestTimeout = 10 * time.Second

	c, err := client.Dial(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "INTEGRATION FAIL: cannot dial %s: %v\n", serverAddr, err)
		os.Exit(1)
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

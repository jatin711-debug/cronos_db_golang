package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func getHTTPAddr() string {
	addr := os.Getenv("CRONOS_TEST_HTTP_ADDR")
	if addr == "" {
		// Derive from gRPC address
		grpc := serverAddr
		addr = strings.Replace(grpc, ":9000", ":8080", 1)
		if addr == grpc {
			addr = addr + ":8080"
		}
	}
	return addr
}

func TestHealthEndpoint(t *testing.T) {
	addr := getHTTPAddr()
	url := fmt.Sprintf("http://%s/health", addr)

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health returned %d", resp.StatusCode)
	}
	t.Logf("Health OK from %s", url)
}

func TestDeepHealthEndpoint(t *testing.T) {
	addr := getHTTPAddr()
	url := fmt.Sprintf("http://%s/health/deep", addr)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("deep health check failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("deep health returned unexpected %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode deep health: %v", err)
	}

	status, _ := result["status"].(string)
	t.Logf("Deep health status: %s", status)
}

func TestMetricsEndpoint(t *testing.T) {
	addr := getHTTPAddr()
	url := fmt.Sprintf("http://%s/metrics", addr)

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("metrics check failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics returned %d", resp.StatusCode)
	}

	// Verify some expected metrics are present
	// In a full test, we'd parse the Prometheus exposition format
	t.Logf("Metrics endpoint OK from %s", url)
}

package cdc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// WebhookSink sends CDC events as JSON POSTs to an HTTP endpoint.
type WebhookSink struct {
	url        string
	httpClient *http.Client
}

// NewWebhookSink creates a webhook CDC sink with a 10s HTTP client timeout.
func NewWebhookSink(url string) *WebhookSink {
	return &WebhookSink{
		url: url,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Name implements Sink.
func (w *WebhookSink) Name() string { return "webhook" }

// Write POSTs the change event as application/json to the configured URL.
// Non-2xx responses are returned as errors.
func (w *WebhookSink) Write(ctx context.Context, event *ChangeEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", w.url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned %d", resp.StatusCode)
	}
	return nil
}

// Close implements Sink; webhook sinks hold no long-lived resources.
func (w *WebhookSink) Close() error { return nil }

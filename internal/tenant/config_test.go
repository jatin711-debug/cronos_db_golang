package tenant

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadRateLimits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "limits.json")
	for _, body := range []string{`null`, `{}`, `{"a":null}`, `{"a":{"max_events_per_second":-1}}`, `{"a":{"max_events_per_second":0.5}}`, `{"a":{"max_storage_bytes":10}}`, `{"a":{"max_events_per_second":2}} {}`} {
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		a := NewAccountant()
		if err := a.LoadRateLimits(path); err == nil || a.HasConfiguredLimits() {
			t.Fatalf("accepted invalid configuration: %s", body)
		}
	}
	if err := os.WriteFile(path, []byte(`{"alice":{"max_events_per_second":2},"bob":{"max_events_per_second":3}}`), 0600); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		a := NewAccountant() // restart loads the same limits with fresh buckets
		if err := a.LoadRateLimits(path); err != nil {
			t.Fatal(err)
		}
		if !a.ReservePublishBatch("alice", 2, 0) || a.ReservePublishBatch("alice", 2, 0) {
			t.Fatal("alice's rate limit was not enforced")
		}
		if !a.ReservePublishBatch("bob", 3, 0) {
			t.Fatal("alice exhausted bob's bucket")
		}
		if !a.ReservePublishBatch("unlisted", 100, 0) {
			t.Fatal("unlisted subject should remain unlimited")
		}
	}
}

package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWebPartitionIDs(t *testing.T) {
	for _, tc := range []struct {
		query string
		valid bool
	}{{"?partition_id=0", true}, {"?partition_id=1", true}, {"", false}, {"?partition_id=-1", false}, {"?partition_id=2147483648", false}, {"?partition_id=no", false}} {
		t.Run(tc.query, func(t *testing.T) {
			_, err := parsePartitionID(httptest.NewRequest("GET", "/"+tc.query, nil))
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v, error=%v", tc.valid, err)
			}
		})
	}
}

// On the HTTP API partition_id=0 names partition 0; only a missing partition_id
// means every partition. The RPCs make the same distinction with an optional
// field.
func TestWebReplicationLagPartitionZeroIsNotAll(t *testing.T) {
	h := newTestWebHandler(t, nil)
	mux := http.NewServeMux()
	h.Register(mux)

	// The test manager holds no partitions: "all" answers with an empty
	// aggregate, while asking for partition 0 itself finds nothing.
	all := httptest.NewRecorder()
	mux.ServeHTTP(all, httptest.NewRequest("GET", "/api/admin/replication-lag", nil))
	if all.Code != http.StatusOK {
		t.Fatalf("no partition_id: want 200, got %d (%s)", all.Code, all.Body.String())
	}
	zero := httptest.NewRecorder()
	mux.ServeHTTP(zero, httptest.NewRequest("GET", "/api/admin/replication-lag?partition_id=0", nil))
	if zero.Code == http.StatusOK {
		t.Fatalf("partition_id=0 was answered as all partitions: %s", zero.Body.String())
	}
}

// The maintenance endpoints pass partition_id=0 on as partition 0. Up to
// v0.13.1 they ran retention or compaction on every partition of the node.
func TestWebMaintenancePartitionZeroIsNotAll(t *testing.T) {
	h := newTestWebHandler(t, nil)
	mux := http.NewServeMux()
	h.Register(mux)
	post := func(target string) string {
		t.Helper()
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, httptest.NewRequest("POST", target, nil))
		if rr.Code != http.StatusOK {
			t.Fatalf("%s: want 200, got %d (%s)", target, rr.Code, rr.Body.String())
		}
		return rr.Body.String()
	}

	// The test manager holds no partitions.
	for _, target := range []string{"/api/admin/compaction/run?partition_id=0", "/api/admin/retention/run?partition_id=0&max_size_bytes=1"} {
		if body := post(target); !strings.Contains(body, "partition 0 not found") {
			t.Fatalf("%s was not answered for partition 0 alone: %s", target, body)
		}
	}
	if body := post("/api/admin/compaction/run"); !strings.Contains(body, "this node has no partitions") {
		t.Fatalf("compaction without partition_id did not cover every partition: %s", body)
	}
}

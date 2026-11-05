package api

import (
	"net/http"
	"net/http/httptest"
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
// means every partition. The RPC cannot tell them apart and keeps 0 as "all".
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

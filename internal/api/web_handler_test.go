package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// newTestWebHandler constructs a WebHandler backed by an
// AdminServiceHandler with no PM/cluster (matches the standalone-mode
// path in the admin handler). authCfg is nil in tests that don't
// need it.
func newTestWebHandler(t *testing.T, authCfg *auth.Config) *WebHandler {
	t.Helper()
	// Build the minimum partition manager the admin handler needs.
	// In standalone mode, only the cluster=nil and pm=nil code paths
	// are exercised; the constructor still runs.
	cfg := &types.Config{
		NodeID:           "test-node",
		DataDir:          t.TempDir(),
		PartitionCount:   1,
		TickMS:           10,
		WheelSize:        100,
		SegmentSizeBytes: 1024 * 1024,
		IndexInterval:    1,
		FsyncMode:        "periodic",
		FlushIntervalMS:  100,
		DedupTTLHours:    1,
		BloomCapacity:    1000,
	}
	pm := partition.NewPartitionManager("test-node", cfg)
	admin := NewAdminServiceHandler(pm, nil, authCfg, "test-node", t.TempDir(), nil, nil)
	return NewWebHandler(admin, authCfg)
}

func TestWebHandler_RedirectsRoot(t *testing.T) {
	h := newTestWebHandler(t, nil)
	mux := http.NewServeMux()
	h.Register(mux)

	// Root redirects to /ui/.
	req := httptest.NewRequest("GET", "/", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusFound {
		t.Errorf("expected 302 Found, got %d", rr.Code)
	}
	if loc := rr.Header().Get("Location"); loc != "/ui/" {
		t.Errorf("expected Location=/ui/, got %q", loc)
	}

	// /ui (no slash) also redirects to /ui/.
	req = httptest.NewRequest("GET", "/ui", nil)
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusFound {
		t.Errorf("expected 302 Found for /ui, got %d", rr.Code)
	}
	if loc := rr.Header().Get("Location"); loc != "/ui/" {
		t.Errorf("expected /ui -> /ui/ redirect, got %q", loc)
	}
}

func TestWebHandler_ServesDashboardIndex(t *testing.T) {
	h := newTestWebHandler(t, nil)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("GET", "/ui/", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d (body=%q)", rr.Code, rr.Body.String())
	}
	ct := rr.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "text/html") {
		t.Errorf("expected text/html Content-Type, got %q", ct)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "<!doctype html") && !strings.Contains(body, "<!DOCTYPE html") {
		t.Errorf("expected HTML doctype in body, got: %.200s", body)
	}
}

func TestWebHandler_SPAFallbackServesIndexHTML(t *testing.T) {
	// Hard refresh on a client-side route like /ui/partitions should
	// return the SPA's index.html, not a 404.
	h := newTestWebHandler(t, nil)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("GET", "/ui/partitions", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 OK for SPA fallback, got %d (body=%q)", rr.Code, rr.Body.String())
	}
	ct := rr.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "text/html") {
		t.Errorf("expected text/html Content-Type for fallback, got %q", ct)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "<!doctype html") && !strings.Contains(body, "<!DOCTYPE html") {
		t.Errorf("expected HTML doctype in fallback body, got: %.200s", body)
	}
}

func TestWebHandler_JSONProxy_DevModeAllowsAnonymous(t *testing.T) {
	// authCfg == nil means dev mode: no auth required.
	h := newTestWebHandler(t, nil)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("GET", "/api/admin/topology", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 OK in dev mode, got %d (body=%q)", rr.Code, rr.Body.String())
	}
	ct := rr.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "application/json") {
		t.Errorf("expected application/json, got %q", ct)
	}

	// Body must be the ClusterTopology JSON. Field naming matches
	// the proto json tags: snake_case. Note: proto-generated bool
	// fields use `omitempty`, so is_cluster_mode=false is absent
	// from the wire (not a literal false). We check the field is
	// either absent or explicitly false.
	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	if v, ok := resp["is_cluster_mode"]; ok && v != false {
		t.Errorf("expected is_cluster_mode absent or false, got %v", v)
	}
	if resp["local_node_id"] != "test-node" {
		t.Errorf("expected local_node_id=test-node, got %v", resp["local_node_id"])
	}
	if _, ok := resp["nodes"]; !ok {
		t.Error("expected nodes key in response")
	}
}

func TestWebHandler_JSONProxy_AuthRequired_WhenAuthEnabled(t *testing.T) {
	// authCfg != nil but no token sent -> 401.
	authCfg := &auth.Config{
		Enabled:   true,
		JWTSecret: []byte("test-secret"),
		Policy:    auth.AllowAllPolicy(),
	}
	h := newTestWebHandler(t, authCfg)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("GET", "/api/admin/topology", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized without bearer, got %d", rr.Code)
	}
}

func TestWebHandler_JSONProxy_AdminAllowed(t *testing.T) {
	authCfg := &auth.Config{
		Enabled:   true,
		JWTSecret: []byte("test-secret"),
		Policy: &auth.Policy{
			Subjects: map[string]*auth.Subject{
				"ops": {Admin: true},
			},
		},
	}
	h := newTestWebHandler(t, authCfg)
	mux := http.NewServeMux()
	h.Register(mux)

	token, err := auth.GenerateToken("ops", []byte("test-secret"), 1*time.Hour)
	if err != nil {
		t.Fatalf("generate token: %v", err)
	}

	req := httptest.NewRequest("GET", "/api/admin/topology", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 OK with admin token, got %d (body=%q)", rr.Code, rr.Body.String())
	}
}

func TestWebHandler_JSONProxy_AdminDenied(t *testing.T) {
	authCfg := &auth.Config{
		Enabled:   true,
		JWTSecret: []byte("test-secret"),
		Policy: &auth.Policy{
			Subjects: map[string]*auth.Subject{
				"user": {Admin: false},
			},
		},
	}
	h := newTestWebHandler(t, authCfg)
	mux := http.NewServeMux()
	h.Register(mux)

	token, err := auth.GenerateToken("user", []byte("test-secret"), 1*time.Hour)
	if err != nil {
		t.Fatalf("generate token: %v", err)
	}

	req := httptest.NewRequest("GET", "/api/admin/topology", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized for non-admin, got %d", rr.Code)
	}
}

func TestWebHandler_JSONProxy_RunRetention_MethodNotAllowed(t *testing.T) {
	// RunRetention is a mutating RPC; the handler enforces POST.
	h := newTestWebHandler(t, nil)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("GET", "/api/admin/retention/run", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for GET on /retention/run, got %d", rr.Code)
	}
}

func TestWebHandler_JSONProxy_RunRetention_AuthGated(t *testing.T) {
	// Auth is enabled and admin is set; mutating RPC must still
	// require the bearer token. Without a token -> 401.
	authCfg := &auth.Config{
		Enabled:   true,
		JWTSecret: []byte("test-secret"),
		Policy: &auth.Policy{
			Subjects: map[string]*auth.Subject{
				"ops": {Admin: true},
			},
		},
	}
	h := newTestWebHandler(t, authCfg)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("POST", "/api/admin/retention/run?partition_id=0", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized on mutating RPC without token, got %d", rr.Code)
	}
}

func TestWebHandler_JSONProxy_GRPCErrorMapping(t *testing.T) {
	// GetPartitionHealth with partition_id=0 returns a gRPC
	// InvalidArgument error. The proxy should map that to HTTP 400.
	h := newTestWebHandler(t, nil)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("GET", "/api/admin/partition-health?partition_id=0", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for partition_id=0, got %d (body=%q)", rr.Code, rr.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	if _, ok := resp["error"]; !ok {
		t.Errorf("expected error key in body, got %v", resp)
	}
}

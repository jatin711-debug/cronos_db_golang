package api

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

//go:embed web/dist/*
var dashboardFS embed.FS

// WebHandler serves the admin dashboard SPA at /ui/ and exposes a thin
// JSON proxy at /api/admin/* that delegates to the existing
// AdminServiceHandler. The JSON proxy reuses the same gRPC code paths
// (term fencing, RBAC, per-partition iteration) as the gRPC
// AdminService — no business logic is duplicated here.
//
// Auth: every /api/admin/* path is gated by authMiddleware. When
// auth is disabled (--dev mode, authCfg == nil) the middleware is a
// no-op pass-through. When auth is enabled, requests must carry an
// `Authorization: Bearer <jwt>` header signed with the same secret/public
// key as the gRPC service, and the JWT subject must have
// `Subject.Admin = true` in the RBAC policy.
//
// Embed: dashboardFS is populated at compile time from
// internal/api/web/dist/, which is staged by `make build` from
// web/dashboard/dist/. If the dist is empty (e.g. `go build` invoked
// directly without first running `make dashboard`), the SPA still
// builds but /ui/ returns a 404 with a clear remediation message.
type WebHandler struct {
	adminHandler *AdminServiceHandler
	authCfg      *auth.Config
	logger       *slog.Logger
}

// NewWebHandler constructs the handler. adminHandler must be non-nil
// (every JSON endpoint is a thin wrapper around one of its methods).
// authCfg may be nil (dev mode).
func NewWebHandler(admin *AdminServiceHandler, authCfg *auth.Config) *WebHandler {
	return &WebHandler{
		adminHandler: admin,
		authCfg:      authCfg,
		logger:       slog.Default(),
	}
}

// Register mounts the JSON proxy at /api/admin/* and the SPA at
// /ui/. The same *http.ServeMux that already serves /health and
// /metrics gets these new endpoints; no new listener is started.
func (h *WebHandler) Register(mux *http.ServeMux) {
	// JSON proxy. We mount the auth middleware as a wrapper so the
	// sub-mux's own HandleFunc registrations don't need to repeat
	// the auth check.
	apiMux := http.NewServeMux()
	apiMux.HandleFunc("/topology", h.handleTopology)
	apiMux.HandleFunc("/partition-health", h.handlePartitionHealth)
	apiMux.HandleFunc("/replication-lag", h.handleReplicationLag)
	apiMux.HandleFunc("/consumer-groups", h.handleListConsumerGroups)
	apiMux.HandleFunc("/consumer-group-lag", h.handleConsumerGroupLag)
	apiMux.HandleFunc("/schemas", h.handleListSchemas)
	apiMux.HandleFunc("/schema", h.handleGetSchema)
	apiMux.HandleFunc("/tenant-usage", h.handleTenantUsage)
	apiMux.HandleFunc("/retention/run", h.handleRunRetention)
	apiMux.HandleFunc("/compaction/run", h.handleRunCompaction)
	apiMux.HandleFunc("/cluster/rebalance", h.handleRebalance)

	// Wrap the inner apiMux with both authMiddleware (so every JSON
	// path is RBAC-gated) and http.StripPrefix (so the inner mux's
	// "/topology" patterns match "/api/admin/topology" requests).
	mux.Handle("/api/admin/", h.authMiddleware(http.StripPrefix("/api/admin", apiMux)))

	// SPA. We try to load the embedded dist; if it's empty (e.g. the
	// developer ran `go build` without `make dashboard` first), we
	// register a 404 handler that explains the situation rather than
	// silently serving an empty page.
	//
	// The embed pattern is `web/dist/*`, so the FS key prefix is
	// "web/dist/". We strip that prefix via fs.Sub to make the file
	// server see the SPA's natural layout (index.html at the root).
	sub, err := fs.Sub(dashboardFS, "web/dist")
	if err != nil {
		h.logger.Warn("dashboard dist not embedded; serving 404 from /ui/. Run `make dashboard` to build the SPA.", "err", err)
		mux.HandleFunc("/ui/", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "dashboard not built; run `make dashboard`", http.StatusNotFound)
		})
	} else {
		// SPA file server: serve static assets when they exist, otherwise
		// fall back to index.html so React Router can handle client-side
		// routes like /ui/partitions on hard refresh.
		spa := spaFileServer(sub)
		mux.Handle("/ui/", http.StripPrefix("/ui/", spa))
	}
	// Redirect /ui -> /ui/ and / -> /ui/ so a bare host root lands on
	// the dashboard.
	mux.HandleFunc("/ui", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/", http.StatusFound)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, "/ui/", http.StatusFound)
	})
}

// ---------------------------------------------------------------------------
// Auth middleware
// ---------------------------------------------------------------------------

func (h *WebHandler) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r, err := h.authorize(r)
		if err != nil {
			h.logger.Debug("admin API auth denied", "path", r.URL.Path, "err", err)
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (h *WebHandler) authorize(r *http.Request) (*http.Request, error) {
	// Dev mode: auth is disabled (--dev). Permit all.
	if h.authCfg == nil {
		return r, nil
	}

	token := bearerToken(r)
	if token == "" {
		return r, errors.New("missing bearer token in Authorization header")
	}

	claims, err := auth.ParseToken(token, h.authCfg)
	if err != nil {
		return r, err
	}

	ctx := auth.WithClaims(r.Context(), claims)
	if err := auth.CheckAdminPermission(ctx, h.authCfg.Policy); err != nil {
		return r, err
	}
	// Stash the claims on the request context so downstream handlers
	// can read them if needed.
	return r.WithContext(ctx), nil
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if h == "" {
		return ""
	}
	// Accept both "Bearer <token>" and "<token>" (some clients omit
	// the scheme). We require either a known scheme or a non-empty
	// raw token to avoid false positives.
	if strings.HasPrefix(h, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
	}
	if strings.HasPrefix(h, "bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(h, "bearer "))
	}
	return strings.TrimSpace(h)
}

// ---------------------------------------------------------------------------
// JSON proxy handlers
// ---------------------------------------------------------------------------

// All handlers follow the same pattern:
//   1. Decode optional request from query string or form body.
//   2. Call the corresponding AdminServiceHandler method.
//   3. Write the proto response as JSON (proto-generated types have
//      json:"snake_case,omitempty" tags that match the SPA's
//      lib/api-mock.ts shapes exactly).
//   4. On gRPC error, map the code to HTTP and write the message.

func (h *WebHandler) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	// Proto-generated types are safe to json.Marshal — they implement
	// json.Marshaler or have json tags.
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

// writeGRPCError maps a gRPC status error to an HTTP response. The
// status code mapping follows grpc-gateway conventions: NotFound ->
// 404, PermissionDenied -> 403, Unauthenticated -> 401, etc. Falls
// back to 500 for unknown codes.
func (h *WebHandler) writeGRPCError(w http.ResponseWriter, err error) {
	st, ok := status.FromError(err)
	if !ok {
		h.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	httpCode := grpcCodeToHTTP(st.Code())
	h.writeJSON(w, httpCode, map[string]string{"error": st.Message()})
}

func grpcCodeToHTTP(c codes.Code) int {
	switch c {
	case codes.OK:
		return http.StatusOK
	case codes.Canceled:
		return 499 // nginx-style
	case codes.Unknown:
		return http.StatusInternalServerError
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists:
		return http.StatusConflict
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.FailedPrecondition:
		return http.StatusPreconditionFailed
	case codes.Aborted:
		return http.StatusConflict
	case codes.OutOfRange:
		return http.StatusBadRequest
	case codes.Unimplemented:
		return http.StatusNotImplemented
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	default:
		return http.StatusInternalServerError
	}
}

// ---------------------------------------------------------------------------
// Per-RPC handlers
// ---------------------------------------------------------------------------

func (h *WebHandler) handleTopology(w http.ResponseWriter, r *http.Request) {
	resp, err := h.adminHandler.GetClusterTopology(r.Context(), &types.GetClusterTopologyRequest{})
	if err != nil {
		h.writeGRPCError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, resp)
}

func (h *WebHandler) handlePartitionHealth(w http.ResponseWriter, r *http.Request) {
	pid, err := parsePartitionID(r)
	if err != nil {
		h.writeGRPCError(w, status.Error(codes.InvalidArgument, err.Error()))
		return
	}
	resp, err := h.adminHandler.GetPartitionHealth(r.Context(), &types.PartitionHealth{PartitionId: pid})
	if err != nil {
		h.writeGRPCError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, resp)
}

func (h *WebHandler) handleReplicationLag(w http.ResponseWriter, r *http.Request) {
	pid, err := parsePartitionIDOrZero(r)
	if err != nil {
		h.writeGRPCError(w, status.Error(codes.InvalidArgument, err.Error()))
		return
	}
	resp, err := h.adminHandler.GetReplicationLag(r.Context(), &types.ReplicationLag{PartitionId: pid})
	if err != nil {
		h.writeGRPCError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, resp)
}

func (h *WebHandler) handleListConsumerGroups(w http.ResponseWriter, r *http.Request) {
	resp, err := h.adminHandler.ListConsumerGroups(r.Context(), &types.AdminListConsumerGroupsRequest{})
	if err != nil {
		h.writeGRPCError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, resp)
}

func (h *WebHandler) handleConsumerGroupLag(w http.ResponseWriter, r *http.Request) {
	groupID := r.URL.Query().Get("group_id")
	if groupID == "" {
		h.writeGRPCError(w, status.Error(codes.InvalidArgument, "group_id query parameter is required"))
		return
	}
	pid, err := parsePartitionID(r)
	if err != nil {
		h.writeGRPCError(w, status.Error(codes.InvalidArgument, err.Error()))
		return
	}
	resp, err := h.adminHandler.GetConsumerGroupLag(r.Context(), &types.AdminConsumerGroupLag{
		GroupId: groupID, PartitionId: pid,
	})
	if err != nil {
		h.writeGRPCError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, resp)
}

func (h *WebHandler) handleListSchemas(w http.ResponseWriter, r *http.Request) {
	resp, err := h.adminHandler.ListSchemas(r.Context(), &types.ListSchemasRequest{})
	if err != nil {
		h.writeGRPCError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, resp)
}

func (h *WebHandler) handleGetSchema(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		h.writeGRPCError(w, status.Error(codes.InvalidArgument, "topic query parameter is required"))
		return
	}
	version := int32(0)
	if v := r.URL.Query().Get("version"); v != "" {
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			h.writeGRPCError(w, status.Error(codes.InvalidArgument, "version must be an integer"))
			return
		}
		version = int32(n)
	}
	resp, err := h.adminHandler.GetSchema(r.Context(), &types.GetSchemaRequest{Topic: topic, Version: version})
	if err != nil {
		h.writeGRPCError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, resp)
}

func (h *WebHandler) handleTenantUsage(w http.ResponseWriter, r *http.Request) {
	tenantID := r.URL.Query().Get("tenant_id")
	resp, err := h.adminHandler.GetTenantUsage(r.Context(), &types.TenantUsage{TenantId: tenantID})
	if err != nil {
		h.writeGRPCError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, resp)
}

func (h *WebHandler) handleRunRetention(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeGRPCError(w, status.Error(codes.InvalidArgument, "POST required"))
		return
	}
	pid, err := parsePartitionIDOrZero(r)
	if err != nil {
		h.writeGRPCError(w, status.Error(codes.InvalidArgument, err.Error()))
		return
	}
	maxAge, _ := strconv.ParseInt(r.URL.Query().Get("max_age_hours"), 10, 64)
	maxSize, _ := strconv.ParseInt(r.URL.Query().Get("max_size_bytes"), 10, 64)
	resp, err := h.adminHandler.RunRetention(r.Context(), &types.RunRetentionRequest{
		PartitionId:  pid,
		MaxAgeHours:  maxAge,
		MaxSizeBytes: maxSize,
	})
	if err != nil {
		h.writeGRPCError(w, err)
		return
	}
	// RunRetention returns success=true/false; map false to 200 with
	// the error in the body (per the proto contract) and true to 200
	// with the body.
	h.writeJSON(w, http.StatusOK, resp)
}

func (h *WebHandler) handleRunCompaction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeGRPCError(w, status.Error(codes.InvalidArgument, "POST required"))
		return
	}
	pid, err := parsePartitionID(r)
	if err != nil {
		h.writeGRPCError(w, status.Error(codes.InvalidArgument, err.Error()))
		return
	}
	resp, err := h.adminHandler.RunCompaction(r.Context(), &types.RunCompactionRequest{PartitionId: pid})
	if err != nil {
		h.writeGRPCError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, resp)
}

func (h *WebHandler) handleRebalance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeGRPCError(w, status.Error(codes.InvalidArgument, "POST required"))
		return
	}
	resp, err := h.adminHandler.TriggerRebalance(r.Context(), &types.RebalanceRequest{})
	if err != nil {
		h.writeGRPCError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, resp)
}

// spaFileServer serves files from the embedded SPA dist. If the requested
// path does not match an existing file, it serves index.html so the React
// Router SPA can render the route client-side.
func spaFileServer(root fs.FS) http.Handler {
	fileServer := http.FileServer(http.FS(root))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		// Clean the path to prevent directory traversal.
		if path == "" || path == "/" {
			path = "index.html"
		}
		// Try to open the requested file. If it exists and is not a
		// directory, serve it directly. This preserves hashed asset files
		// like /ui/assets/index-abc.js.
		f, err := root.Open(path)
		if err == nil {
			defer f.Close()
			stat, err := f.Stat()
			if err == nil && !stat.IsDir() {
				fileServer.ServeHTTP(w, r)
				return
			}
		}
		// Fall back to index.html for any non-file path.
		r.URL.Path = "/"
		fileServer.ServeHTTP(w, r)
	})
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// parsePartitionID requires ?partition_id=N with N > 0.
func parsePartitionID(r *http.Request) (int32, error) {
	pid, err := parsePartitionIDOrZero(r)
	if err != nil {
		return 0, err
	}
	if pid == 0 {
		return 0, errors.New("partition_id is required (got 0)")
	}
	return pid, nil
}

// parsePartitionIDOrZero returns 0 when ?partition_id is missing.
func parsePartitionIDOrZero(r *http.Request) (int32, error) {
	v := r.URL.Query().Get("partition_id")
	if v == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		return 0, errors.New("partition_id must be a 32-bit integer")
	}
	return int32(n), nil
}

// ensure context import is used even when no other call site needs it.
var _ = context.Background

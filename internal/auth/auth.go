// Package auth provides JWT authentication and RBAC authorization for CronosDB
// gRPC and HTTP APIs.
//
// When enabled, Interceptor and StreamInterceptor require a Bearer token,
// attach Claims to the request context, and handlers call CheckTopicPermission
// or CheckAdminPermission against a JSON policy file.
package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Config holds authentication and authorization settings for the server.
type Config struct {
	// Enabled turns on JWT verification in gRPC/HTTP interceptors.
	Enabled bool
	// JWTSecret is the HMAC key for HS256/384/512 verification.
	JWTSecret []byte
	// JWTPublicKey is an ed25519.PublicKey, *rsa.PublicKey, or *ecdsa.PublicKey
	// used for asymmetric JWT verification.
	JWTPublicKey interface{}
	// Policy is the in-memory RBAC policy; nil means policy is not configured.
	Policy *Policy
}

// Policy is a simple in-memory RBAC policy keyed by JWT subject.
type Policy struct {
	// Subjects maps principal names (JWT sub) to their permissions.
	Subjects map[string]*Subject `json:"subjects"`
}

// Subject holds permissions for a principal.
type Subject struct {
	// Topics maps topic names to publish/subscribe/admin permissions.
	Topics map[string]TopicPerms `json:"topics"`

	// Admin grants the principal access to the AdminService RPCs
	// (operator-facing cluster, partition, replication, retention, schema,
	// tenant, and rebalance operations). It is a global flag, not
	// per-topic. JSON tag uses omitempty so existing policy files without
	// this field decode unchanged (Admin == false).
	Admin bool `json:"admin,omitempty"`
}

// TopicPerms holds allowed operations on a topic.
type TopicPerms struct {
	// Publish allows producing events to the topic.
	Publish bool `json:"publish"`
	// Subscribe allows consuming or replaying events from the topic.
	Subscribe bool `json:"subscribe"`
	// Admin allows topic-scoped administrative operations.
	Admin bool `json:"admin"`
}

// Claims extends jwt.RegisteredClaims with CronosDB-specific fields.
type Claims struct {
	jwt.RegisteredClaims
	// SubjectID is a CronosDB-specific subject identifier (JSON "sub_id").
	SubjectID string `json:"sub_id"`
}

// NewPolicyFromFile loads an RBAC policy from a JSON file.
func NewPolicyFromFile(path string) (*Policy, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read policy file: %w", err)
	}
	p := &Policy{Subjects: make(map[string]*Subject)}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &p.Subjects); err != nil {
			return nil, fmt.Errorf("parse policy file: %w", err)
		}
	}
	return p, nil
}

// AllowAllPolicy returns a policy that permits everything.
func AllowAllPolicy() *Policy {
	return &Policy{Subjects: make(map[string]*Subject)}
}

// LoadPublicKey loads a PEM-encoded public key from file.
func LoadPublicKey(path string) (any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("no PEM block found")
	}
	switch block.Type {
	case "PUBLIC KEY":
		pub, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, err
		}
		switch k := pub.(type) {
		case ed25519.PublicKey, *rsa.PublicKey, *ecdsa.PublicKey:
			return k, nil
		default:
			return nil, fmt.Errorf("unsupported public key type: %T", pub)
		}
	case "RSA PUBLIC KEY":
		return x509.ParsePKCS1PublicKey(block.Bytes)
	default:
		return nil, fmt.Errorf("unsupported PEM type: %s", block.Type)
	}
}

// Interceptor returns a gRPC unary interceptor that enforces JWT authentication.
// When cfg is nil or disabled, it is a pass-through. On success, claims are
// stored on the context via WithClaims.
func Interceptor(cfg *Config) grpc.UnaryServerInterceptor {
	if cfg == nil || !cfg.Enabled {
		return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			return handler(ctx, req)
		}
	}

	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}

		tokenStr := extractBearer(md)
		if tokenStr == "" {
			return nil, status.Error(codes.Unauthenticated, "missing bearer token")
		}

		claims, err := ParseToken(tokenStr, cfg)
		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
		}

		ctx = WithClaims(ctx, claims)
		return handler(ctx, req)
	}
}

// StreamInterceptor returns a gRPC stream interceptor that enforces JWT auth
// and attaches claims to the stream context. Pass-through when auth is disabled.
func StreamInterceptor(cfg *Config) grpc.StreamServerInterceptor {
	if cfg == nil || !cfg.Enabled {
		return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			return handler(srv, stream)
		}
	}

	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, ok := metadata.FromIncomingContext(stream.Context())
		if !ok {
			return status.Error(codes.Unauthenticated, "missing metadata")
		}

		tokenStr := extractBearer(md)
		if tokenStr == "" {
			return status.Error(codes.Unauthenticated, "missing bearer token")
		}

		claims, err := ParseToken(tokenStr, cfg)
		if err != nil {
			return status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
		}

		ctx := WithClaims(stream.Context(), claims)
		return handler(srv, &contextServerStream{ServerStream: stream, ctx: ctx})
	}
}

type contextServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *contextServerStream) Context() context.Context { return s.ctx }

func extractBearer(md metadata.MD) string {
	if auth := md.Get("authorization"); len(auth) > 0 {
		parts := strings.SplitN(auth[0], " ", 2)
		if len(parts) == 2 && strings.EqualFold(parts[0], "bearer") {
			return parts[1]
		}
	}
	return ""
}

// ParseToken verifies a signed JWT and returns the embedded Claims.
// Supports HMAC (HS256/384/512), Ed25519, RSA, and ECDSA per the
// signing method. Used by both the gRPC auth.Interceptor and the
// HTTP auth middleware in internal/api/web_handler.go.
func ParseToken(tokenStr string, cfg *Config) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenStr, &Claims{}, func(t *jwt.Token) (interface{}, error) {
		switch t.Method.(type) {
		case *jwt.SigningMethodHMAC:
			if len(cfg.JWTSecret) == 0 {
				return nil, fmt.Errorf("HMAC secret not configured")
			}
			return cfg.JWTSecret, nil
		case *jwt.SigningMethodEd25519:
			if cfg.JWTPublicKey == nil {
				return nil, fmt.Errorf("public key not configured")
			}
			return cfg.JWTPublicKey, nil
		case *jwt.SigningMethodRSA, *jwt.SigningMethodRSAPSS:
			if cfg.JWTPublicKey == nil {
				return nil, fmt.Errorf("public key not configured")
			}
			return cfg.JWTPublicKey, nil
		case *jwt.SigningMethodECDSA:
			if cfg.JWTPublicKey == nil {
				return nil, fmt.Errorf("public key not configured")
			}
			return cfg.JWTPublicKey, nil
		default:
			return nil, fmt.Errorf("unsupported signing method: %v", t.Header["alg"])
		}
	}, jwt.WithValidMethods([]string{"HS256", "HS384", "HS512", "EdDSA", "RS256", "RS384", "RS512", "ES256", "ES384", "ES512"}))
	if err != nil {
		return nil, err
	}
	if !token.Valid {
		return nil, fmt.Errorf("token invalid")
	}
	claims, ok := token.Claims.(*Claims)
	if !ok {
		return nil, fmt.Errorf("invalid claims type")
	}
	return claims, nil
}

type contextKey struct{}

var claimsKey = &contextKey{}

// WithClaims attaches claims to a context.
func WithClaims(ctx context.Context, claims *Claims) context.Context {
	return context.WithValue(ctx, claimsKey, claims)
}

// ClaimsFromContext extracts claims from a context.
func ClaimsFromContext(ctx context.Context) (*Claims, bool) {
	c, ok := ctx.Value(claimsKey).(*Claims)
	return c, ok
}

// CheckTopicPermission verifies if the authenticated subject has permission
// for the given topic and operation ("publish", "subscribe", or "admin").
// If no policy is configured (nil or empty), all requests are allowed so that
// auth-disabled deployments remain backward compatible.
func CheckTopicPermission(ctx context.Context, topic string, op string, policy *Policy) error {
	if policy == nil {
		// No policy configured at all. When the RBAC interceptor is active
		// (auth enabled) this is a misconfiguration — fail closed so topic-level
		// authorization is not silently bypassed. When auth is disabled the
		// handler never calls this function (policy is nil and auth is off),
		// so this path is only reached when auth is on but no policy file was
		// loaded.
		return status.Error(codes.FailedPrecondition, "authorization policy not configured")
	}

	if len(policy.Subjects) == 0 {
		// Explicit allow-all policy (empty subjects map). This is only set
		// deliberately via AllowAllPolicy(), so we honor it.
		return nil
	}

	claims, ok := ClaimsFromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing auth context")
	}

	subject, ok := policy.Subjects[claims.Subject]
	if !ok {
		return status.Errorf(codes.PermissionDenied, "subject %q not found in policy", claims.Subject)
	}

	topicPerms, ok := subject.Topics[topic]
	if !ok {
		return status.Errorf(codes.PermissionDenied, "topic %q not authorized for subject %q", topic, claims.Subject)
	}

	switch op {
	case "publish":
		if !topicPerms.Publish {
			return status.Errorf(codes.PermissionDenied, "subject %q not allowed to publish to %q", claims.Subject, topic)
		}
	case "subscribe":
		if !topicPerms.Subscribe {
			return status.Errorf(codes.PermissionDenied, "subject %q not allowed to subscribe to %q", claims.Subject, topic)
		}
	case "admin":
		if !topicPerms.Admin {
			return status.Errorf(codes.PermissionDenied, "subject %q not allowed admin access to %q", claims.Subject, topic)
		}
	default:
		return status.Errorf(codes.InvalidArgument, "unknown operation %q", op)
	}

	return nil
}

// CheckAdminPermission verifies if the authenticated subject has admin
// privileges (the global Subject.Admin flag, not the per-topic admin flag
// inside TopicPerms). It is the RBAC gate for AdminService RPCs.
//
// Semantics mirror CheckTopicPermission:
//
//   - If policy is nil and auth is enabled, fail closed (the policy file
//     was not loaded; without a file we cannot prove the subject is
//     authorized, so refuse).
//   - If policy is the empty AllowAll policy (Subjects map is empty), allow
//     every authenticated principal.
//   - Otherwise look up the subject; missing subject -> PermissionDenied,
//     subject present but Admin==false -> PermissionDenied,
//     subject present and Admin==true -> allow.
func CheckAdminPermission(ctx context.Context, policy *Policy) error {
	if policy == nil {
		return status.Error(codes.FailedPrecondition, "authorization policy not configured")
	}

	if len(policy.Subjects) == 0 {
		// Explicit AllowAllPolicy().
		return nil
	}

	claims, ok := ClaimsFromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing auth context")
	}

	subject, ok := policy.Subjects[claims.Subject]
	if !ok {
		return status.Errorf(codes.PermissionDenied, "subject %q not found in policy", claims.Subject)
	}

	if !subject.Admin {
		return status.Errorf(codes.PermissionDenied, "subject %q is not an admin", claims.Subject)
	}

	return nil
}

// GenerateToken creates a signed JWT for testing / CLI use.
func GenerateToken(subject string, secret []byte, ttl time.Duration) (string, error) {
	now := time.Now()
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   subject,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		},
		SubjectID: subject,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(secret)
}

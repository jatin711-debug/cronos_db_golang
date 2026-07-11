package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc/metadata"
)

func TestAllowAllPolicy(t *testing.T) {
	p := AllowAllPolicy()
	if p == nil {
		t.Fatal("policy should not be nil")
	}
	if p.Subjects == nil {
		t.Fatal("subjects map should be initialized")
	}
}

func TestNewPolicyFromFile_NotFound(t *testing.T) {
	_, err := NewPolicyFromFile("/nonexistent/path/policy.json")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestNewPolicyFromFile_Valid(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "policy.json")
	os.WriteFile(path, []byte(`{}`), 0644)

	p, err := NewPolicyFromFile(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Subjects == nil {
		t.Fatal("subjects should be initialized")
	}
}

func TestGenerateToken(t *testing.T) {
	secret := []byte("test-secret-key-min-32-bytes-long")
	token, err := GenerateToken("user-1", secret, time.Hour)
	if err != nil {
		t.Fatalf("GenerateToken failed: %v", err)
	}
	if token == "" {
		t.Fatal("token should not be empty")
	}

	// Parse it back
	claims, err := parseToken(token, &Config{JWTSecret: secret})
	if err != nil {
		t.Fatalf("parseToken failed: %v", err)
	}
	if claims.Subject != "user-1" {
		t.Errorf("expected subject user-1, got %s", claims.Subject)
	}
	if claims.SubjectID != "user-1" {
		t.Errorf("expected subjectID user-1, got %s", claims.SubjectID)
	}
}

func TestGenerateToken_Expired(t *testing.T) {
	secret := []byte("test-secret-key-min-32-bytes-long")
	token, _ := GenerateToken("user-1", secret, -time.Hour)

	_, err := parseToken(token, &Config{JWTSecret: secret})
	if err == nil {
		t.Error("expected error for expired token")
	}
}

func TestParseToken_InvalidSecret(t *testing.T) {
	secret1 := []byte("test-secret-key-min-32-bytes-long")
	secret2 := []byte("wrong-secret-key-min-32-bytes-long")
	token, _ := GenerateToken("user-1", secret1, time.Hour)

	_, err := parseToken(token, &Config{JWTSecret: secret2})
	if err == nil {
		t.Error("expected error for wrong secret")
	}
}

func TestParseToken_InvalidFormat(t *testing.T) {
	_, err := parseToken("not-a-token", &Config{JWTSecret: []byte("secret")})
	if err == nil {
		t.Error("expected error for invalid token")
	}
}

func TestParseToken_Ed25519(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	now := time.Now()
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-ed",
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
		},
		SubjectID: "user-ed",
	}
	token, err := jwtNewWithClaims(jwtSigningMethodEdDSA, claims).SignedString(priv)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}

	parsed, err := parseToken(token, &Config{JWTPublicKey: priv.Public()})
	if err != nil {
		t.Fatalf("parse ed25519 token: %v", err)
	}
	if parsed.SubjectID != "user-ed" {
		t.Errorf("expected user-ed, got %s", parsed.SubjectID)
	}
}

func TestParseToken_RSA(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}

	now := time.Now()
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-rsa",
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
		},
		SubjectID: "user-rsa",
	}
	token, err := jwtNewWithClaims(jwtSigningMethodRS256, claims).SignedString(priv)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}

	parsed, err := parseToken(token, &Config{JWTPublicKey: &priv.PublicKey})
	if err != nil {
		t.Fatalf("parse rsa token: %v", err)
	}
	if parsed.SubjectID != "user-rsa" {
		t.Errorf("expected user-rsa, got %s", parsed.SubjectID)
	}
}

func TestParseToken_ECDSA(t *testing.T) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate ecdsa key: %v", err)
	}

	now := time.Now()
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-ec",
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
		},
		SubjectID: "user-ec",
	}
	token, err := jwtNewWithClaims(jwtSigningMethodES256, claims).SignedString(priv)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}

	parsed, err := parseToken(token, &Config{JWTPublicKey: &priv.PublicKey})
	if err != nil {
		t.Fatalf("parse ecdsa token: %v", err)
	}
	if parsed.SubjectID != "user-ec" {
		t.Errorf("expected user-ec, got %s", parsed.SubjectID)
	}
}

func TestLoadPublicKey_InvalidPath(t *testing.T) {
	_, err := LoadPublicKey("/nonexistent/key.pem")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestLoadPublicKey_InvalidPEM(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "bad.pem")
	os.WriteFile(path, []byte("not a pem"), 0644)

	_, err := LoadPublicKey(path)
	if err == nil {
		t.Error("expected error for invalid PEM")
	}
}

func TestLoadPublicKey_RSA(t *testing.T) {
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "rsa.pub")

	pubDER, _ := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})
	os.WriteFile(path, pemBytes, 0644)

	key, err := LoadPublicKey(path)
	if err != nil {
		t.Fatalf("load public key: %v", err)
	}
	if _, ok := key.(*rsa.PublicKey); !ok {
		t.Errorf("expected *rsa.PublicKey, got %T", key)
	}
}

func TestLoadPublicKey_PKCS1RSA(t *testing.T) {
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "rsa.pub")

	pubDER := x509.MarshalPKCS1PublicKey(&priv.PublicKey)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "RSA PUBLIC KEY", Bytes: pubDER})
	os.WriteFile(path, pemBytes, 0644)

	key, err := LoadPublicKey(path)
	if err != nil {
		t.Fatalf("load pkcs1 public key: %v", err)
	}
	if _, ok := key.(*rsa.PublicKey); !ok {
		t.Errorf("expected *rsa.PublicKey, got %T", key)
	}
}

func TestInterceptor_Disabled(t *testing.T) {
	interceptor := Interceptor(nil)
	if interceptor == nil {
		t.Fatal("interceptor should not be nil")
	}

	// Should pass through without auth
	ctx := context.Background()
	resp, err := interceptor(ctx, "request", nil, func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != "ok" {
		t.Errorf("expected ok, got %v", resp)
	}
}

func TestInterceptor_Enabled_MissingMetadata(t *testing.T) {
	secret := []byte("test-secret-key-min-32-bytes-long")
	cfg := &Config{Enabled: true, JWTSecret: secret}
	interceptor := Interceptor(cfg)

	ctx := context.Background()
	_, err := interceptor(ctx, "request", nil, func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	})
	if err == nil {
		t.Fatal("expected error for missing metadata")
	}
}

func TestInterceptor_Enabled_MissingToken(t *testing.T) {
	secret := []byte("test-secret-key-min-32-bytes-long")
	cfg := &Config{Enabled: true, JWTSecret: secret}
	interceptor := Interceptor(cfg)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(nil))
	_, err := interceptor(ctx, "request", nil, func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	})
	if err == nil {
		t.Fatal("expected error for missing token")
	}
}

func TestInterceptor_Enabled_InvalidToken(t *testing.T) {
	secret := []byte("test-secret-key-min-32-bytes-long")
	cfg := &Config{Enabled: true, JWTSecret: secret}
	interceptor := Interceptor(cfg)

	md := metadata.New(map[string]string{"authorization": "Bearer invalid-token"})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	_, err := interceptor(ctx, "request", nil, func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	})
	if err == nil {
		t.Fatal("expected error for invalid token")
	}
}

func TestInterceptor_Enabled_ValidToken(t *testing.T) {
	secret := []byte("test-secret-key-min-32-bytes-long")
	cfg := &Config{Enabled: true, JWTSecret: secret}
	interceptor := Interceptor(cfg)

	token, _ := GenerateToken("user-1", secret, time.Hour)
	md := metadata.New(map[string]string{"authorization": "Bearer " + token})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	var capturedCtx context.Context
	resp, err := interceptor(ctx, "request", nil, func(ctx context.Context, req interface{}) (interface{}, error) {
		capturedCtx = ctx
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != "ok" {
		t.Errorf("expected ok, got %v", resp)
	}

	claims, ok := ClaimsFromContext(capturedCtx)
	if !ok {
		t.Fatal("claims should be in context")
	}
	if claims.Subject != "user-1" {
		t.Errorf("expected user-1, got %s", claims.Subject)
	}
}

func TestStreamInterceptor_Disabled(t *testing.T) {
	interceptor := StreamInterceptor(nil)
	if interceptor == nil {
		t.Fatal("interceptor should not be nil")
	}
}

func TestClaimsContext(t *testing.T) {
	claims := ClaimsWithSubject("test-user")
	ctx := WithClaims(context.Background(), claims)

	retrieved, ok := ClaimsFromContext(ctx)
	if !ok {
		t.Fatal("should retrieve claims")
	}
	if retrieved.Subject != "test-user" {
		t.Errorf("expected test-user, got %s", retrieved.Subject)
	}
}

func TestClaimsFromContext_Missing(t *testing.T) {
	_, ok := ClaimsFromContext(context.Background())
	if ok {
		t.Error("should not find claims in empty context")
	}
}

func TestCheckTopicPermission_NoClaims_NilPolicy_Allowed(t *testing.T) {
	err := CheckTopicPermission(context.Background(), "topic", "publish", nil)
	if err != nil {
		t.Fatalf("expected nil policy to allow unauthenticated requests, got: %v", err)
	}
}

func TestCheckTopicPermission_NoClaims_WithPolicy_Denied(t *testing.T) {
	policy := &Policy{
		Subjects: map[string]*Subject{
			"user": {
				Topics: map[string]TopicPerms{
					"topic": {Publish: true},
				},
			},
		},
	}
	err := CheckTopicPermission(context.Background(), "topic", "publish", policy)
	if err == nil {
		t.Fatal("expected error when a policy exists but claims are missing")
	}
}

func TestCheckTopicPermission_WithClaims_NoPolicy(t *testing.T) {
	claims := ClaimsWithSubject("user")
	ctx := WithClaims(context.Background(), claims)
	err := CheckTopicPermission(ctx, "topic", "publish", nil)
	if err != nil {
		t.Fatalf("unexpected error with nil policy: %v", err)
	}
}

func TestCheckTopicPermission_WithClaims_Allowed(t *testing.T) {
	claims := ClaimsWithSubject("user")
	ctx := WithClaims(context.Background(), claims)
	policy := &Policy{
		Subjects: map[string]*Subject{
			"user": {
				Topics: map[string]TopicPerms{
					"topic": {Publish: true, Subscribe: true, Admin: true},
				},
			},
		},
	}
	err := CheckTopicPermission(ctx, "topic", "publish", policy)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCheckTopicPermission_WithClaims_Denied(t *testing.T) {
	claims := ClaimsWithSubject("user")
	ctx := WithClaims(context.Background(), claims)
	policy := &Policy{
		Subjects: map[string]*Subject{
			"user": {
				Topics: map[string]TopicPerms{
					"topic": {Publish: false, Subscribe: true, Admin: false},
				},
			},
		},
	}
	err := CheckTopicPermission(ctx, "topic", "publish", policy)
	if err == nil {
		t.Fatal("expected permission denied error")
	}
}

func TestCheckTopicPermission_UnknownSubject(t *testing.T) {
	claims := ClaimsWithSubject("unknown")
	ctx := WithClaims(context.Background(), claims)
	policy := &Policy{
		Subjects: map[string]*Subject{
			"user": {
				Topics: map[string]TopicPerms{
					"topic": {Publish: true},
				},
			},
		},
	}
	err := CheckTopicPermission(ctx, "topic", "publish", policy)
	if err == nil {
		t.Fatal("expected permission denied error for unknown subject")
	}
}

func TestExtractBearer(t *testing.T) {
	tests := []struct {
		name string
		md   metadata.MD
		want string
	}{
		{"valid bearer", metadata.New(map[string]string{"authorization": "Bearer token123"}), "token123"},
		{"lowercase bearer", metadata.New(map[string]string{"authorization": "bearer token123"}), "token123"},
		{"missing auth", metadata.New(nil), ""},
		{"no bearer prefix", metadata.New(map[string]string{"authorization": "token123"}), ""},
		{"empty auth", metadata.New(map[string]string{"authorization": ""}), ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := extractBearer(tc.md)
			if got != tc.want {
				t.Errorf("extractBearer() = %q, want %q", got, tc.want)
			}
		})
	}
}

// jwt helpers for tests
func jwtNewWithClaims(method jwt.SigningMethod, claims Claims) *jwt.Token {
	return jwt.NewWithClaims(method, claims)
}

var jwtSigningMethodHS256 = jwt.SigningMethodHS256
var jwtSigningMethodEdDSA = jwt.SigningMethodEdDSA
var jwtSigningMethodRS256 = jwt.SigningMethodRS256
var jwtSigningMethodES256 = jwt.SigningMethodES256

// ParseTokenForTest parses a token for use in other package tests
func ParseTokenForTest(tokenStr string, secret []byte) (*Claims, error) {
	return parseToken(tokenStr, &Config{JWTSecret: secret})
}

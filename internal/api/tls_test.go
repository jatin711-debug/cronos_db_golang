package api

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func generateTestCert(t *testing.T, dir string) (certPath, keyPath string) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"CronosDB Test"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           nil,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}

	certPath = filepath.Join(dir, "server.crt")
	certFile, err := os.Create(certPath)
	if err != nil {
		t.Fatalf("create cert file: %v", err)
	}
	if err := pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		t.Fatalf("encode cert: %v", err)
	}
	certFile.Close()

	keyPath = filepath.Join(dir, "server.key")
	keyBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal private key: %v", err)
	}
	keyFile, err := os.Create(keyPath)
	if err != nil {
		t.Fatalf("create key file: %v", err)
	}
	if err := pem.Encode(keyFile, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}); err != nil {
		t.Fatalf("encode key: %v", err)
	}
	keyFile.Close()

	return certPath, keyPath
}

// TestBuildServerTLSConfig_DisabledFailsClosed ensures that calling the TLS
// builder when TLS is disabled returns an error rather than a nil config.
func TestBuildServerTLSConfig_DisabledFailsClosed(t *testing.T) {
	cfg := &TLSConfig{Enabled: false}
	_, err := BuildServerTLSConfig(cfg)
	if err == nil {
		t.Fatal("expected error when TLS is disabled, got nil")
	}
}

// TestBuildServerTLSConfig_MissingCertFails ensures missing cert/key surfaces
// a clear error.
func TestBuildServerTLSConfig_MissingCertFails(t *testing.T) {
	cfg := &TLSConfig{Enabled: true}
	_, err := BuildServerTLSConfig(cfg)
	if err == nil {
		t.Fatal("expected error for missing cert/key, got nil")
	}
}

// TestBuildServerTLSConfig_ClientAuthRequiresCA ensures mTLS cannot be enabled
// without a CA file.
func TestBuildServerTLSConfig_ClientAuthRequiresCA(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateTestCert(t, dir)

	cfg := &TLSConfig{
		Enabled:    true,
		CertFile:   certPath,
		KeyFile:    keyPath,
		ClientAuth: true,
	}
	_, err := BuildServerTLSConfig(cfg)
	if err == nil {
		t.Fatal("expected error when client-auth is enabled without CA file, got nil")
	}
}

// TestBuildServerTLSConfig_Valid loads a valid cert/key pair successfully.
func TestBuildServerTLSConfig_Valid(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateTestCert(t, dir)

	cfg := &TLSConfig{
		Enabled:  true,
		CertFile: certPath,
		KeyFile:  keyPath,
	}
	tlsCfg, err := BuildServerTLSConfig(cfg)
	if err != nil {
		t.Fatalf("expected valid TLS config, got error: %v", err)
	}
	if tlsCfg == nil {
		t.Fatal("expected non-nil tls.Config")
	}
}

// TestNewGRPCServer_TLSMisconfigReturnsError ensures NewGRPCServer fails when
// TLS is requested but cannot be configured.
func TestNewGRPCServer_TLSMisconfigReturnsError(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TLS = &TLSConfig{Enabled: true} // missing cert/key

	_, err := NewGRPCServer(cfg)
	if err == nil {
		t.Fatal("expected NewGRPCServer to return error for bad TLS config, got nil")
	}
}

// TestNewGRPCServer_NoTLSWorks ensures plaintext still works when TLS is not requested.
func TestNewGRPCServer_NoTLSWorks(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TLS = nil

	server, err := NewGRPCServer(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if server == nil {
		t.Fatal("expected non-nil server")
	}
}

// TestBuildHTTPTLSConfig_ServesTheHTTPPortOverTLS serves an HTTP handler with
// the configuration the HTTP port gets. A client that verifies the server's
// certificate is answered; one that speaks plain HTTP is not.
func TestBuildHTTPTLSConfig_ServesTheHTTPPortOverTLS(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateTestCert(t, dir)

	if _, err := BuildHTTPTLSConfig("", keyPath); err == nil {
		t.Fatal("a configuration without a certificate was built")
	}
	if _, err := BuildHTTPTLSConfig(certPath, filepath.Join(dir, "no.key")); err == nil {
		t.Fatal("a configuration with a key that does not exist was built")
	}
	tlsCfg, err := BuildHTTPTLSConfig(certPath, keyPath)
	if err != nil {
		t.Fatalf("BuildHTTPTLSConfig failed: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{
		TLSConfig: tlsCfg,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, "ready")
		}),
	}
	go func() { _ = server.ServeTLS(listener, "", "") }()
	defer server.Close()

	// The test certificate is its own authority and is valid for localhost.
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(certPEM)
	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, ServerName: "localhost"}},
	}
	resp, err := client.Get("https://" + listener.Addr().String() + "/health")
	if err != nil {
		t.Fatalf("a client that trusts the certificate was not answered: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "ready" {
		t.Fatalf("answer over TLS: status %d, body %q", resp.StatusCode, body)
	}

	plain := &http.Client{Timeout: 5 * time.Second}
	if resp, err := plain.Get("http://" + listener.Addr().String() + "/health"); err == nil {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Fatalf("a client without TLS was answered: %q", body)
		}
	}

	strangers := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: x509.NewCertPool(), ServerName: "localhost"}},
	}
	if resp, err := strangers.Get("https://" + listener.Addr().String() + "/health"); err == nil {
		resp.Body.Close()
		t.Fatal("a client that does not trust the certificate went on with the connection")
	}
}

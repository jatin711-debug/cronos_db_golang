package api

import (
	"crypto/tls"
	"fmt"

	"github.com/jatin711-debug/cronos_db_golang/internal/tlsfiles"
)

// TLSConfig holds TLS configuration for the public gRPC server.
type TLSConfig struct {
	// Enabled turns on TLS for the public listener.
	Enabled bool
	// CAFile is the CA certificate path used to verify client certs when ClientAuth is set.
	CAFile string
	// CertFile is the server certificate path.
	CertFile string
	// KeyFile is the server private key path.
	KeyFile string
	// ClientAuth requires and verifies client certificates (mTLS) when true.
	ClientAuth bool
}

// serverCipherSuites are the cipher suites the servers offer under TLS 1.2.
var serverCipherSuites = []uint16{
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
}

// BuildHTTPTLSConfig builds the tls.Config of the HTTP port: health, metrics,
// the admin API and the dashboard. It presents the certificate of the gRPC
// server, read again when its files change, and asks callers for none: a
// browser, a probe and a metrics scraper have no certificate to show, and the
// admin API has its own authentication.
func BuildHTTPTLSConfig(certFile, keyFile string) (*tls.Config, error) {
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("http-tls-enabled requires tls-cert-file and tls-key-file")
	}
	pair, err := tlsfiles.LoadKeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load server cert/key: %w", err)
	}
	return &tls.Config{
		GetCertificate: pair.GetCertificate,
		MinVersion:     tls.VersionTLS12,
		CipherSuites:   serverCipherSuites,
	}, nil
}

// BuildServerTLSConfig builds a tls.Config for the gRPC server.
// Returns an error if TLS is disabled or if the configuration is invalid.
//
// The certificate, its key and the CA are read from their files again when the
// files change, so they can be replaced while the server runs (see tlsfiles).
func BuildServerTLSConfig(cfg *TLSConfig) (*tls.Config, error) {
	if !cfg.Enabled {
		return nil, fmt.Errorf("TLS is disabled")
	}

	if cfg.CertFile == "" || cfg.KeyFile == "" {
		return nil, fmt.Errorf("tls-enabled requires tls-cert-file and tls-key-file")
	}

	if cfg.ClientAuth && cfg.CAFile == "" {
		return nil, fmt.Errorf("tls-client-auth requires tls-ca-file")
	}

	pair, err := tlsfiles.LoadKeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server cert/key: %w", err)
	}

	tlsCfg := &tls.Config{
		GetCertificate: pair.GetCertificate,
		MinVersion:     tls.VersionTLS12,
		CipherSuites:   serverCipherSuites,
	}

	if cfg.ClientAuth {
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	if cfg.CAFile != "" {
		pool, err := tlsfiles.LoadPool(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		tlsfiles.FollowClientCAs(tlsCfg, pool)
	}

	return tlsCfg, nil
}

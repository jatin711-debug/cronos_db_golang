package replication

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// MTLSConfig holds internal mutual-TLS configuration for replication channels.
// This is separate from client-facing gRPC TLS and is intended to encrypt and
// authenticate traffic between cluster nodes (leader -> follower replication,
// bulk sync, heartbeats).
type MTLSConfig struct {
	Enabled    bool
	CAFile     string
	CertFile   string
	KeyFile    string
	ServerName string

	// InsecureSkipVerify is provided for testing only. It must be false in
	// production so that the replication channel fails closed.
	InsecureSkipVerify bool
}

// BuildClientTLSConfig returns a tls.Config for a replication client. The
// client presents its certificate and verifies the server certificate against
// the configured CA. If mTLS is disabled or misconfigured, it returns an error
// so callers can fail closed rather than silently falling back to plaintext.
func BuildClientTLSConfig(cfg *MTLSConfig) (*tls.Config, error) {
	if cfg == nil || !cfg.Enabled {
		return nil, fmt.Errorf("replication mTLS is disabled")
	}
	if cfg.CertFile == "" || cfg.KeyFile == "" {
		return nil, fmt.Errorf("replication mTLS requires cert-file and key-file")
	}
	if cfg.CAFile == "" && !cfg.InsecureSkipVerify {
		return nil, fmt.Errorf("replication mTLS requires ca-file or insecure-skip-verify")
	}

	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load replication client cert/key: %w", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		},
		ServerName:         cfg.ServerName,
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	}

	if cfg.CAFile != "" {
		caPEM, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read replication CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("parse replication CA file: no certs loaded")
		}
		tlsCfg.RootCAs = pool
	}

	return tlsCfg, nil
}

// BuildServerTLSConfig returns a tls.Config for a replication server. It
// requires and verifies a client certificate, ensuring only cluster members can
// connect to the replication socket.
func BuildServerTLSConfig(cfg *MTLSConfig) (*tls.Config, error) {
	if cfg == nil || !cfg.Enabled {
		return nil, fmt.Errorf("replication mTLS is disabled")
	}
	if cfg.CertFile == "" || cfg.KeyFile == "" {
		return nil, fmt.Errorf("replication mTLS requires cert-file and key-file")
	}
	if cfg.CAFile == "" {
		return nil, fmt.Errorf("replication mTLS server requires ca-file")
	}

	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load replication server cert/key: %w", err)
	}

	caPEM, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read replication CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("parse replication CA file: no certs loaded")
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		},
	}, nil
}

package api

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// TLSConfig holds TLS configuration for the gRPC server.
type TLSConfig struct {
	Enabled     bool
	CAFile      string
	CertFile    string
	KeyFile     string
	ClientAuth  bool // require client certs (mTLS)
}

// BuildServerTLSConfig builds a tls.Config for the gRPC server.
// Returns an error if TLS is disabled or if the configuration is invalid.
func BuildServerTLSConfig(cfg *TLSConfig) (*tls.Config, error) {
	if !cfg.Enabled {
		return nil, fmt.Errorf("TLS is disabled")
	}

	if cfg.CertFile == "" || cfg.KeyFile == "" {
		return nil, fmt.Errorf("tls-enabled requires tls-cert-file and tls-key-file")
	}

	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server cert/key: %w", err)
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
	}

	if cfg.CAFile != "" {
		caPEM, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("parse CA file: no certs loaded")
		}
		tlsCfg.ClientCAs = pool
	}

	if cfg.ClientAuth {
		if cfg.CAFile == "" {
			return nil, fmt.Errorf("tls-client-auth requires tls-ca-file")
		}
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return tlsCfg, nil
}

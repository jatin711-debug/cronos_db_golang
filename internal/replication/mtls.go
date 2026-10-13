package replication

import (
	"crypto/tls"
	"fmt"

	"github.com/jatin711-debug/cronos_db_golang/internal/tlsfiles"
)

// MTLSConfig holds internal mutual-TLS configuration for replication channels.
// This is separate from client-facing gRPC TLS and is intended to encrypt and
// authenticate traffic between cluster nodes (leader -> follower replication,
// bulk sync, heartbeats).
type MTLSConfig struct {
	// Enabled turns on mutual TLS for replication connections.
	Enabled bool
	// CAFile is the path to the CA certificate used to verify peer certificates.
	CAFile string
	// CertFile is the path to this node's certificate presented to peers.
	CertFile string
	// KeyFile is the path to this node's private key for CertFile.
	KeyFile string
	// ServerName is the expected TLS server name when dialing as a client.
	ServerName string
	// InsecureSkipVerify is provided for testing only. It must be false in
	// production so that the replication channel fails closed.
	InsecureSkipVerify bool
}

// peerCipherSuites are the cipher suites nodes use with each other.
var peerCipherSuites = []uint16{
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
}

// BuildClientTLS returns the TLS configuration of a replication client. The
// client presents its certificate and verifies the server certificate against
// the configured CA. If mTLS is disabled or misconfigured, it returns an error
// so callers can fail closed rather than silently falling back to plaintext.
//
// The certificate, its key and the CA are read from their files again when
// the files change (see tlsfiles). What is returned gives the configuration to
// make a connection with at that moment, and gRPC credentials that do so for
// every connection of a client.
func BuildClientTLS(cfg *MTLSConfig) (*tlsfiles.Client, error) {
	if cfg == nil || !cfg.Enabled {
		return nil, fmt.Errorf("replication mTLS is disabled")
	}
	if cfg.CertFile == "" || cfg.KeyFile == "" {
		return nil, fmt.Errorf("replication mTLS requires cert-file and key-file")
	}
	if cfg.CAFile == "" && !cfg.InsecureSkipVerify {
		return nil, fmt.Errorf("replication mTLS requires ca-file or insecure-skip-verify")
	}

	pair, err := tlsfiles.LoadKeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load replication client cert/key: %w", err)
	}

	tlsCfg := &tls.Config{
		GetClientCertificate: pair.GetClientCertificate,
		MinVersion:           tls.VersionTLS12,
		CipherSuites:         peerCipherSuites,
		ServerName:           cfg.ServerName,
		InsecureSkipVerify:   cfg.InsecureSkipVerify,
	}

	var pool *tlsfiles.Pool
	if cfg.CAFile != "" {
		if pool, err = tlsfiles.LoadPool(cfg.CAFile); err != nil {
			return nil, fmt.Errorf("read replication CA file: %w", err)
		}
	}
	return tlsfiles.NewClient(tlsCfg, pool), nil
}

// BuildClientTLSConfig returns the tls.Config of a replication client as its
// files are now. A caller that keeps what it gets does not see a CA that is
// replaced later; BuildClientTLS is for those.
func BuildClientTLSConfig(cfg *MTLSConfig) (*tls.Config, error) {
	client, err := BuildClientTLS(cfg)
	if err != nil {
		return nil, err
	}
	return client.Config(), nil
}

// BuildServerTLSConfig returns a tls.Config for a replication server. It
// requires and verifies a client certificate, ensuring only cluster members can
// connect to the replication socket. Its certificate, key and CA follow their
// files, as a client's do.
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

	pair, err := tlsfiles.LoadKeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load replication server cert/key: %w", err)
	}
	pool, err := tlsfiles.LoadPool(cfg.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read replication CA file: %w", err)
	}

	tlsCfg := &tls.Config{
		GetCertificate: pair.GetCertificate,
		ClientAuth:     tls.RequireAndVerifyClientCert,
		MinVersion:     tls.VersionTLS12,
		CipherSuites:   peerCipherSuites,
	}
	tlsfiles.FollowClientCAs(tlsCfg, pool)
	return tlsCfg, nil
}

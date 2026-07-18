package client

import (
	"fmt"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client/internal/circuitbreaker"
	"google.golang.org/grpc/credentials"
)

const (
	defaultConnectionsPerNode = 2
	defaultDialTimeout        = 5 * time.Second
	defaultRequestTimeout     = 10 * time.Second
	defaultKeepaliveTime      = 30 * time.Second
	defaultKeepaliveTimeout   = 10 * time.Second
	defaultMaxRecvMsgSize     = 16 * 1024 * 1024
	defaultMaxSendMsgSize     = 16 * 1024 * 1024
	defaultReadBufferSize     = 4 * 1024 * 1024
	defaultWriteBufferSize    = 4 * 1024 * 1024
	defaultMetadataTTL        = 15 * time.Second
	defaultMetadataRefresh    = 5 * time.Second
)

// MetadataConfig controls how the client caches and refreshes cluster metadata.
type MetadataConfig struct {
	// TTL is how long cached partition/leader metadata is considered fresh.
	TTL time.Duration
	// RefreshInterval is how often background refresh runs; must be <= TTL.
	RefreshInterval time.Duration
}

// SecurityConfig controls transport security and per-RPC credentials.
type SecurityConfig struct {
	// Insecure uses plaintext transport credentials.
	// This should be false in production.
	Insecure bool

	// ServerName overrides the TLS server name used for certificate verification.
	ServerName string
	// CACertFile is the path to a CA PEM used to verify the server certificate.
	CACertFile string
	// ClientCertFile is the path to a client certificate PEM for mTLS.
	ClientCertFile string
	// ClientKeyFile is the path to the client private key PEM for mTLS.
	ClientKeyFile string
	// InsecureSkipVerify disables server certificate verification (development only).
	InsecureSkipVerify bool

	// PerRPCCredentials is used when provided (e.g. custom token providers).
	PerRPCCredentials credentials.PerRPCCredentials

	// BearerToken is a static bearer token used when PerRPCCredentials is nil.
	BearerToken string
	// BearerScheme is the auth scheme prefix (default "Bearer").
	BearerScheme string
}

// Config defines client runtime settings used by Dial for pooling, metadata,
// security, and default RPC deadlines.
type Config struct {
	// BootstrapAddresses is the initial list of gRPC endpoints used to discover
	// cluster metadata. At least one address is required.
	BootstrapAddresses []string

	// NodeIDToAddress is an optional explicit mapping for leader_id → address.
	// Useful until server metadata includes leader addresses.
	NodeIDToAddress map[string]string

	// PartitionCount is an optional fallback for key hashing when metadata is
	// temporarily empty. 0 means rely only on server-advertised counts.
	PartitionCount int

	// ConnectionsPerNode is how many pooled gRPC connections to keep per node address.
	ConnectionsPerNode int
	// DialTimeout bounds how long Dial waits when opening a connection.
	DialTimeout time.Duration
	// RequestTimeout is the default per-RPC timeout when the call context has no deadline.
	RequestTimeout time.Duration

	// ResolveDNS enables DNS resolution of bootstrap addresses when true.
	ResolveDNS bool

	// KeepaliveTime is the gRPC keepalive ping interval.
	KeepaliveTime time.Duration
	// KeepaliveTimeout is how long to wait for a keepalive ack before closing.
	KeepaliveTimeout time.Duration
	// MaxRecvMsgSize is the max inbound message size in bytes.
	MaxRecvMsgSize int
	// MaxSendMsgSize is the max outbound message size in bytes.
	MaxSendMsgSize int
	// ReadBufferSize is the gRPC transport read buffer size in bytes.
	ReadBufferSize int
	// WriteBufferSize is the gRPC transport write buffer size in bytes.
	WriteBufferSize int

	// Metadata configures partition/leader metadata caching.
	Metadata MetadataConfig
	// Security configures TLS/mTLS and bearer credentials.
	Security SecurityConfig
	// Hooks receives optional instrumentation callbacks (may be nil → NopHooks).
	Hooks Hooks

	// CircuitBreaker controls per-address resilience gates for requests issued
	// through this client (e.g. by Producer). Threshold of 0 disables breakers.
	CircuitBreaker circuitbreaker.Config
}

// DefaultConfig returns a config with throughput-safe defaults.
// Security defaults to Insecure=true for local development; set Insecure=false for production.
func DefaultConfig(bootstrapAddresses ...string) Config {
	return Config{
		BootstrapAddresses: append([]string(nil), bootstrapAddresses...),
		ConnectionsPerNode: defaultConnectionsPerNode,
		DialTimeout:        defaultDialTimeout,
		RequestTimeout:     defaultRequestTimeout,
		ResolveDNS:         true,
		KeepaliveTime:      defaultKeepaliveTime,
		KeepaliveTimeout:   defaultKeepaliveTimeout,
		MaxRecvMsgSize:     defaultMaxRecvMsgSize,
		MaxSendMsgSize:     defaultMaxSendMsgSize,
		ReadBufferSize:     defaultReadBufferSize,
		WriteBufferSize:    defaultWriteBufferSize,
		Metadata: MetadataConfig{
			TTL:             defaultMetadataTTL,
			RefreshInterval: defaultMetadataRefresh,
		},
		Security: SecurityConfig{
			Insecure: true,
		},
		Hooks: NopHooks{},
		CircuitBreaker: circuitbreaker.Config{
			FailureThreshold: 5,
			SuccessThreshold: 2,
			Timeout:          10 * time.Second,
		},
	}
}

// withDefaults fills zero-valued optional fields with production-oriented defaults.
func (c Config) withDefaults() Config {
	out := c

	if out.ConnectionsPerNode <= 0 {
		out.ConnectionsPerNode = defaultConnectionsPerNode
	}
	if out.DialTimeout <= 0 {
		out.DialTimeout = defaultDialTimeout
	}
	if out.RequestTimeout <= 0 {
		out.RequestTimeout = defaultRequestTimeout
	}
	if out.KeepaliveTime <= 0 {
		out.KeepaliveTime = defaultKeepaliveTime
	}
	if out.KeepaliveTimeout <= 0 {
		out.KeepaliveTimeout = defaultKeepaliveTimeout
	}
	if out.MaxRecvMsgSize <= 0 {
		out.MaxRecvMsgSize = defaultMaxRecvMsgSize
	}
	if out.MaxSendMsgSize <= 0 {
		out.MaxSendMsgSize = defaultMaxSendMsgSize
	}
	if out.ReadBufferSize <= 0 {
		out.ReadBufferSize = defaultReadBufferSize
	}
	if out.WriteBufferSize <= 0 {
		out.WriteBufferSize = defaultWriteBufferSize
	}
	if out.Metadata.TTL <= 0 {
		out.Metadata.TTL = defaultMetadataTTL
	}
	if out.Metadata.RefreshInterval <= 0 {
		out.Metadata.RefreshInterval = defaultMetadataRefresh
	}
	if out.Security.BearerScheme == "" {
		out.Security.BearerScheme = "Bearer"
	}
	if out.NodeIDToAddress == nil {
		out.NodeIDToAddress = map[string]string{}
	}
	if out.Hooks == nil {
		out.Hooks = NopHooks{}
	}
	if out.CircuitBreaker.Timeout <= 0 {
		out.CircuitBreaker.Timeout = 10 * time.Second
	}

	return out
}

// Validate checks config for correctness and safety before Dial.
func (c Config) Validate() error {
	if len(c.BootstrapAddresses) == 0 {
		return fmt.Errorf("bootstrap addresses are required")
	}
	if c.ConnectionsPerNode <= 0 {
		return fmt.Errorf("connections_per_node must be > 0")
	}
	if c.DialTimeout <= 0 {
		return fmt.Errorf("dial_timeout must be > 0")
	}
	if c.RequestTimeout <= 0 {
		return fmt.Errorf("request_timeout must be > 0")
	}
	if c.Metadata.TTL <= 0 {
		return fmt.Errorf("metadata ttl must be > 0")
	}
	if c.Metadata.RefreshInterval <= 0 {
		return fmt.Errorf("metadata refresh interval must be > 0")
	}
	if c.Metadata.RefreshInterval > c.Metadata.TTL {
		return fmt.Errorf("metadata refresh interval must be <= metadata ttl")
	}
	if c.PartitionCount < 0 {
		return fmt.Errorf("partition_count must be >= 0")
	}
	if c.Security.Insecure {
		if c.Security.PerRPCCredentials != nil && c.Security.PerRPCCredentials.RequireTransportSecurity() {
			return fmt.Errorf("insecure transport cannot be used with transport-security-required per-rpc credentials")
		}
	}
	if (c.Security.ClientCertFile != "" && c.Security.ClientKeyFile == "") ||
		(c.Security.ClientCertFile == "" && c.Security.ClientKeyFile != "") {
		return fmt.Errorf("both client_cert_file and client_key_file must be set together")
	}
	return nil
}

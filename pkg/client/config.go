package client

import (
	"fmt"
	"time"

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

// MetadataConfig controls metadata cache behavior.
type MetadataConfig struct {
	TTL             time.Duration
	RefreshInterval time.Duration
}

// SecurityConfig controls transport and per-RPC credentials.
type SecurityConfig struct {
	// Insecure uses plaintext transport credentials.
	// This should be false in production.
	Insecure bool

	// TLS options (used when Insecure=false).
	ServerName         string
	CACertFile         string
	ClientCertFile     string
	ClientKeyFile      string
	InsecureSkipVerify bool

	// PerRPCCredentials is used when provided.
	PerRPCCredentials credentials.PerRPCCredentials

	// Static bearer token fallback when PerRPCCredentials is nil.
	BearerToken  string
	BearerScheme string
}

// Config defines client runtime settings.
type Config struct {
	BootstrapAddresses []string

	// Optional explicit mapping for leader_id -> address.
	// Useful until server metadata includes leader addresses.
	NodeIDToAddress map[string]string

	// Optional fallback for partition hashing if metadata is temporarily empty.
	PartitionCount int

	ConnectionsPerNode int
	DialTimeout        time.Duration
	RequestTimeout     time.Duration

	ResolveDNS bool

	KeepaliveTime    time.Duration
	KeepaliveTimeout time.Duration
	MaxRecvMsgSize   int
	MaxSendMsgSize   int
	ReadBufferSize   int
	WriteBufferSize  int

	Metadata MetadataConfig
	Security SecurityConfig
}

// DefaultConfig returns a config with throughput-safe defaults.
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
	}
}

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

	return out
}

// Validate validates config for correctness and safety.
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

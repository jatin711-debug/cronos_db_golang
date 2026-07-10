package config

import (
	"flag"
	"os"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func init() {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
}

func TestLoadConfig_Defaults(t *testing.T) {
	os.Args = []string{"test", "-dev", "-node-id=test-node"}
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	config, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if config.NodeID != "test-node" {
		t.Errorf("expected NodeID=test-node, got %q", config.NodeID)
	}
	if config.DataDir != DefaultDataDir {
		t.Errorf("expected DataDir=%q, got %q", DefaultDataDir, config.DataDir)
	}
	if config.GPRCAddress != DefaultGRPCAddress {
		t.Errorf("expected GPRCAddress=%q, got %q", DefaultGRPCAddress, config.GPRCAddress)
	}
	if config.HTTPAddress != DefaultHTTPAddress {
		t.Errorf("expected HTTPAddress=%q, got %q", DefaultHTTPAddress, config.HTTPAddress)
	}
	if config.PartitionCount != DefaultPartitionCount {
		t.Errorf("expected PartitionCount=%d, got %d", DefaultPartitionCount, config.PartitionCount)
	}
	if config.ReplicationFactor != DefaultReplicationFactor {
		t.Errorf("expected ReplicationFactor=%d, got %d", DefaultReplicationFactor, config.ReplicationFactor)
	}
	if config.SegmentSizeBytes != DefaultSegmentSizeBytes {
		t.Errorf("expected SegmentSizeBytes=%d, got %d", DefaultSegmentSizeBytes, config.SegmentSizeBytes)
	}
	if config.IndexInterval != DefaultIndexInterval {
		t.Errorf("expected IndexInterval=%d, got %d", DefaultIndexInterval, config.IndexInterval)
	}
	if config.FsyncMode != DefaultFsyncMode {
		t.Errorf("expected FsyncMode=%q, got %q", DefaultFsyncMode, config.FsyncMode)
	}
}

func TestLoadConfig_CLIFlagsOverride(t *testing.T) {
	os.Args = []string{
		"test",
		"-dev",
		"-node-id=test-node",
		"-data-dir=/tmp/test-data",
		"-grpc-addr=localhost:9999",
		"-http-addr=localhost:8888",
		"-partition-count=5",
		"-replication-factor=3",
		"-segment-size=1073741824", // 1GB
		"-index-interval=500",
		"-fsync-mode=every_event",
		"-tls-enabled",
		"-tls-cert-file=/tmp/cert.pem",
		"-tls-key-file=/tmp/key.pem",
		"-encryption-enabled",
		"-encryption-key-file=/tmp/enc.key",
		"-auth-enabled",
		"-auth-jwt-secret=mysecret",
		"-dev",
	}
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	config, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if config.NodeID != "test-node" {
		t.Errorf("expected NodeID=test-node, got %q", config.NodeID)
	}
	if config.DataDir != "/tmp/test-data" {
		t.Errorf("expected DataDir=/tmp/test-data, got %q", config.DataDir)
	}
	if config.GPRCAddress != "localhost:9999" {
		t.Errorf("expected GPRCAddress=localhost:9999, got %q", config.GPRCAddress)
	}
	if config.HTTPAddress != "localhost:8888" {
		t.Errorf("expected HTTPAddress=localhost:8888, got %q", config.HTTPAddress)
	}
	if config.PartitionCount != 5 {
		t.Errorf("expected PartitionCount=5, got %d", config.PartitionCount)
	}
	if config.ReplicationFactor != 3 {
		t.Errorf("expected ReplicationFactor=3, got %d", config.ReplicationFactor)
	}
	if config.SegmentSizeBytes != 1073741824 {
		t.Errorf("expected SegmentSizeBytes=1073741824, got %d", config.SegmentSizeBytes)
	}
	if config.IndexInterval != 500 {
		t.Errorf("expected IndexInterval=500, got %d", config.IndexInterval)
	}
	if config.FsyncMode != "every_event" {
		t.Errorf("expected FsyncMode=every_event, got %q", config.FsyncMode)
	}
	if !config.TLSEnabled {
		t.Error("expected TLSEnabled=true")
	}
	if config.TLSCertFile != "/tmp/cert.pem" {
		t.Errorf("expected TLSCertFile=/tmp/cert.pem, got %q", config.TLSCertFile)
	}
	if config.TLSKeyFile != "/tmp/key.pem" {
		t.Errorf("expected TLSKeyFile=/tmp/key.pem, got %q", config.TLSKeyFile)
	}
	if !config.EncryptionEnabled {
		t.Error("expected EncryptionEnabled=true")
	}
	if config.EncryptionKeyFile != "/tmp/enc.key" {
		t.Errorf("expected EncryptionKeyFile=/tmp/enc.key, got %q", config.EncryptionKeyFile)
	}
	if !config.AuthEnabled {
		t.Error("expected AuthEnabled=true")
	}
	if config.AuthJWTSecret != "mysecret" {
		t.Errorf("expected AuthJWTSecret=mysecret, got %q", config.AuthJWTSecret)
	}
}

func TestLoadConfig_EnvOverrides(t *testing.T) {
	os.Args = []string{"test", "-dev"}
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	// Set env vars before loading config
	originalNodeID := os.Getenv("CRONOS_NODE_ID")
	originalDataDir := os.Getenv("CRONOS_DATA_DIR")
	originalGRPCAddr := os.Getenv("CRONOS_GRPC_ADDR")
	originalHTTPAddr := os.Getenv("CRONOS_HTTP_ADDR")
	originalTLSEnabled := os.Getenv("CRONOS_TLS_ENABLED")
	originalTLSFiles := map[string]string{
		"CRONOS_TLS_CA_FILE":   os.Getenv("CRONOS_TLS_CA_FILE"),
		"CRONOS_TLS_CERT_FILE": os.Getenv("CRONOS_TLS_CERT_FILE"),
		"CRONOS_TLS_KEY_FILE":  os.Getenv("CRONOS_TLS_KEY_FILE"),
	}
	originalAuthEnabled := os.Getenv("CRONOS_AUTH_ENABLED")
	originalJWTSecret := os.Getenv("CRONOS_AUTH_JWT_SECRET")
	originalRack := os.Getenv("CRONOS_NODE_RACK")
	originalZone := os.Getenv("CRONOS_NODE_ZONE")
	originalRegion := os.Getenv("CRONOS_NODE_REGION")
	originalTracing := os.Getenv("CRONOS_TRACING_ENABLED")

	defer func() {
		os.Setenv("CRONOS_NODE_ID", originalNodeID)
		os.Setenv("CRONOS_DATA_DIR", originalDataDir)
		os.Setenv("CRONOS_GRPC_ADDR", originalGRPCAddr)
		os.Setenv("CRONOS_HTTP_ADDR", originalHTTPAddr)
		os.Setenv("CRONOS_TLS_ENABLED", originalTLSEnabled)
		for k, v := range originalTLSFiles {
			os.Setenv(k, v)
		}
		os.Setenv("CRONOS_AUTH_ENABLED", originalAuthEnabled)
		os.Setenv("CRONOS_AUTH_JWT_SECRET", originalJWTSecret)
		os.Setenv("CRONOS_NODE_RACK", originalRack)
		os.Setenv("CRONOS_NODE_ZONE", originalZone)
		os.Setenv("CRONOS_NODE_REGION", originalRegion)
		os.Setenv("CRONOS_TRACING_ENABLED", originalTracing)
	}()

	os.Setenv("CRONOS_NODE_ID", "env-node")
	os.Setenv("CRONOS_DATA_DIR", "/env/data")
	os.Setenv("CRONOS_GRPC_ADDR", "env:9999")
	os.Setenv("CRONOS_HTTP_ADDR", "env:8888")
	os.Setenv("CRONOS_TLS_ENABLED", "true")
	os.Setenv("CRONOS_TLS_CA_FILE", "/env/ca.pem")
	os.Setenv("CRONOS_TLS_CERT_FILE", "/env/cert.pem")
	os.Setenv("CRONOS_TLS_KEY_FILE", "/env/key.pem")
	os.Setenv("CRONOS_AUTH_ENABLED", "true")
	os.Setenv("CRONOS_AUTH_JWT_SECRET", "envsecret")
	os.Setenv("CRONOS_NODE_RACK", "rack-1")
	os.Setenv("CRONOS_NODE_ZONE", "zone-a")
	os.Setenv("CRONOS_NODE_REGION", "us-east-1")
	os.Setenv("CRONOS_TRACING_ENABLED", "true")

	config, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if config.NodeID != "env-node" {
		t.Errorf("expected NodeID=env-node, got %q", config.NodeID)
	}
	if config.DataDir != "/env/data" {
		t.Errorf("expected DataDir=/env/data, got %q", config.DataDir)
	}
	if config.GPRCAddress != "env:9999" {
		t.Errorf("expected GPRCAddress=env:9999, got %q", config.GPRCAddress)
	}
	if config.HTTPAddress != "env:8888" {
		t.Errorf("expected HTTPAddress=env:8888, got %q", config.HTTPAddress)
	}
	if !config.TLSEnabled {
		t.Error("expected TLSEnabled=true from env")
	}
	if config.TLSCAFile != "/env/ca.pem" {
		t.Errorf("expected TLSCAFile=/env/ca.pem, got %q", config.TLSCAFile)
	}
	if !config.AuthEnabled {
		t.Error("expected AuthEnabled=true from env")
	}
	if config.AuthJWTSecret != "envsecret" {
		t.Errorf("expected AuthJWTSecret=envsecret, got %q", config.AuthJWTSecret)
	}
	if config.NodeRack != "rack-1" {
		t.Errorf("expected NodeRack=rack-1, got %q", config.NodeRack)
	}
	if config.NodeZone != "zone-a" {
		t.Errorf("expected NodeZone=zone-a, got %q", config.NodeZone)
	}
	if config.NodeRegion != "us-east-1" {
		t.Errorf("expected NodeRegion=us-east-1, got %q", config.NodeRegion)
	}
	if !config.TracingEnabled {
		t.Error("expected TracingEnabled=true from env")
	}
}

func TestLoadConfig_TLSValidation(t *testing.T) {
	tests := []struct {
		name        string
		flags       []string
		expectError bool
	}{
		{
			name:        "TLS enabled without cert file",
			flags:       []string{"-tls-enabled", "-tls-key-file=/tmp/key.pem"},
			expectError: true,
		},
		{
			name:        "TLS enabled without key file",
			flags:       []string{"-tls-enabled", "-tls-cert-file=/tmp/cert.pem"},
			expectError: true,
		},
		{
			name:        "TLS enabled with both cert and key",
			flags:       []string{"-tls-enabled", "-tls-cert-file=/tmp/cert.pem", "-tls-key-file=/tmp/key.pem"},
			expectError: false,
		},
		{
			name:        "TLS disabled (no validation)",
			flags:       []string{"-tls-enabled=false"},
			expectError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"test", "-node-id=test-node", "-dev"}
			args = append(args, tc.flags...)
			os.Args = args
			flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

			_, err := LoadConfig()
			if tc.expectError && err == nil {
				t.Error("expected error but got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestLoadConfig_AuthValidation(t *testing.T) {
	tests := []struct {
		name        string
		flags       []string
		expectError bool
	}{
		{
			name:        "Auth enabled without secret or public key",
			flags:       []string{"-auth-enabled"},
			expectError: true,
		},
		{
			name:        "Auth enabled with JWT secret",
			flags:       []string{"-auth-enabled", "-auth-jwt-secret=secret"},
			expectError: false,
		},
		{
			name:        "Auth disabled (no validation)",
			flags:       []string{"-auth-enabled=false"},
			expectError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"test", "-node-id=test-node", "-dev"}
			args = append(args, tc.flags...)
			os.Args = args
			flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

			_, err := LoadConfig()
			if tc.expectError && err == nil {
				t.Error("expected error but got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      *configTestHelper
		expectError bool
		errorMsg    string
	}{
		{
			name:        "empty node ID",
			config:      &configTestHelper{NodeID: ""},
			expectError: true,
			errorMsg:    "node-id is required",
		},
		{
			name:        "zero partition count",
			config:      &configTestHelper{NodeID: "node", PartitionCount: 0},
			expectError: true,
			errorMsg:    "partition-count must be > 0",
		},
		{
			name:        "negative replication factor",
			config:      &configTestHelper{NodeID: "node", PartitionCount: 1, ReplicationFactor: -1},
			expectError: true,
			errorMsg:    "replication-factor must be > 0",
		},
		{
			name:        "empty data dir",
			config:      &configTestHelper{NodeID: "node", PartitionCount: 1, ReplicationFactor: 1, DataDir: ""},
			expectError: true,
			errorMsg:    "data-dir is required",
		},
		{
			name:        "empty grpc addr",
			config:      &configTestHelper{NodeID: "node", PartitionCount: 1, ReplicationFactor: 1, DataDir: "/data", GPRCAddress: ""},
			expectError: true,
			errorMsg:    "grpc-addr is required",
		},
		{
			name:        "tracing sample ratio below 0",
			config:      &configTestHelper{NodeID: "node", PartitionCount: 1, ReplicationFactor: 1, DataDir: "/data", GPRCAddress: ":9000", TracingSampleRatio: -0.1},
			expectError: true,
			errorMsg:    "tracing-sample-ratio must be between 0.0 and 1.0",
		},
		{
			name:        "tracing sample ratio above 1",
			config:      &configTestHelper{NodeID: "node", PartitionCount: 1, ReplicationFactor: 1, DataDir: "/data", GPRCAddress: ":9000", TracingSampleRatio: 1.5},
			expectError: true,
			errorMsg:    "tracing-sample-ratio must be between 0.0 and 1.0",
		},
		{
			name: "valid config",
			config: &configTestHelper{
				NodeID: "node", PartitionCount: 1, ReplicationFactor: 1,
				DataDir: "/data", GPRCAddress: ":9000",
			},
			expectError: false,
		},
		{
			name:        "negative min-insync-replicas",
			config:      &configTestHelper{NodeID: "node", PartitionCount: 1, ReplicationFactor: 1, DataDir: "/data", GPRCAddress: ":9000", MinInSyncReplicas: -1},
			expectError: true,
			errorMsg:    "min-insync-replicas must be >= 0",
		},
		{
			name:        "min-insync-replicas exceeds replication factor",
			config:      &configTestHelper{NodeID: "node", PartitionCount: 1, ReplicationFactor: 3, DataDir: "/data", GPRCAddress: ":9000", MinInSyncReplicas: 4},
			expectError: true,
			errorMsg:    "min-insync-replicas (4) cannot exceed replication-factor (3)",
		},
		{
			name:        "zero flush interval",
			config:      &configTestHelper{NodeID: "node", PartitionCount: 1, ReplicationFactor: 1, DataDir: "/data", GPRCAddress: ":9000"},
			expectError: true,
			errorMsg:    "flush-interval must be > 0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &types.Config{
				NodeID:             tc.config.NodeID,
				PartitionCount:     tc.config.PartitionCount,
				ReplicationFactor:  tc.config.ReplicationFactor,
				MinInSyncReplicas:  tc.config.MinInSyncReplicas,
				DataDir:            tc.config.DataDir,
				GPRCAddress:        tc.config.GPRCAddress,
				TracingSampleRatio: tc.config.TracingSampleRatio,
				FlushIntervalMS:    100,
				DevMode:            true,
			}
			if tc.errorMsg == "flush-interval must be > 0" {
				cfg.FlushIntervalMS = 0
			}
			err := ValidateConfig(cfg)
			if tc.expectError {
				if err == nil {
					t.Error("expected error but got none")
				} else if tc.errorMsg != "" && err.Error() != tc.errorMsg {
					t.Errorf("expected error %q, got %q", tc.errorMsg, err.Error())
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

type configTestHelper struct {
	NodeID             string
	PartitionCount     int
	ReplicationFactor  int
	MinInSyncReplicas  int
	DataDir            string
	GPRCAddress        string
	TracingSampleRatio float64
}

func TestLoadConfig_ClusterSeeds(t *testing.T) {
	os.Args = []string{"test", "-dev", "-node-id=test-node", "-cluster-seeds=node1:7946,node2:7946,node3:7946"}
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	config, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if len(config.ClusterSeeds) != 3 {
		t.Errorf("expected 3 cluster seeds, got %d", len(config.ClusterSeeds))
	}
	if config.ClusterSeeds[0] != "node1:7946" {
		t.Errorf("expected first seed node1:7946, got %q", config.ClusterSeeds[0])
	}
}

func TestLoadConfig_TracingConfig(t *testing.T) {
	os.Args = []string{
		"test",
		"-dev",
		"-node-id=test-node",
		"-tracing-enabled",
		"-tracing-exporter=otlp",
		"-tracing-otlp-endpoint=otel:4317",
		"-tracing-sample-ratio=0.5",
		"-tracing-insecure=false",
	}
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	config, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if !config.TracingEnabled {
		t.Error("expected TracingEnabled=true")
	}
	if config.TracingExporter != "otlp" {
		t.Errorf("expected TracingExporter=otlp, got %q", config.TracingExporter)
	}
	if config.TracingOTLPEndpoint != "otel:4317" {
		t.Errorf("expected TracingOTLPEndpoint=otel:4317, got %q", config.TracingOTLPEndpoint)
	}
	if config.TracingSampleRatio != 0.5 {
		t.Errorf("expected TracingSampleRatio=0.5, got %f", config.TracingSampleRatio)
	}
	if config.TracingInsecure != false {
		t.Error("expected TracingInsecure=false")
	}
}

func TestLoadConfig_DeliveryConfig(t *testing.T) {
	os.Args = []string{
		"test",
		"-dev",
		"-node-id=test-node",
		"-ack-timeout=60s",
		"-max-retries=10",
		"-retry-backoff=2s",
		"-max-credits=5000",
	}
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	config, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if config.DefaultAckTimeout != 60*time.Second {
		t.Errorf("expected DefaultAckTimeout=60s, got %v", config.DefaultAckTimeout)
	}
	if config.MaxRetries != 10 {
		t.Errorf("expected MaxRetries=10, got %d", config.MaxRetries)
	}
	if config.RetryBackoff != 2*time.Second {
		t.Errorf("expected RetryBackoff=2s, got %v", config.RetryBackoff)
	}
	if config.MaxDeliveryCredits != 5000 {
		t.Errorf("expected MaxDeliveryCredits=5000, got %d", config.MaxDeliveryCredits)
	}
}

func TestLoadConfig_SchedulerConfig(t *testing.T) {
	os.Args = []string{
		"test",
		"-dev",
		"-node-id=test-node",
		"-tick-ms=50",
		"-wheel-size=120",
		"-hot-window-minutes=30",
		"-hydrator-min-interval=10000",
		"-hydrator-max-interval=600000",
	}
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	config, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if config.TickMS != 50 {
		t.Errorf("expected TickMS=50, got %d", config.TickMS)
	}
	if config.WheelSize != 120 {
		t.Errorf("expected WheelSize=120, got %d", config.WheelSize)
	}
	if config.HotWindowMinutes != 30 {
		t.Errorf("expected HotWindowMinutes=30, got %d", config.HotWindowMinutes)
	}
	if config.HydratorMinIntervalMs != 10000 {
		t.Errorf("expected HydratorMinIntervalMs=10000, got %d", config.HydratorMinIntervalMs)
	}
	if config.HydratorMaxIntervalMs != 600000 {
		t.Errorf("expected HydratorMaxIntervalMs=600000, got %d", config.HydratorMaxIntervalMs)
	}
}

func TestLoadConfig_ClusterConfig(t *testing.T) {
	os.Args = []string{
		"test",
		"-dev",
		"-node-id=test-node",
		"-cluster",
		"-cluster-gossip-addr=127.0.0.1:7946",
		"-cluster-grpc-addr=127.0.0.1:7947",
		"-cluster-raft-addr=127.0.0.1:7948",
		"-virtual-nodes=300",
		"-heartbeat-interval=2s",
		"-failure-timeout=10s",
		"-suspect-timeout=5s",
		"-use-memberlist",
	}
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	config, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if !config.ClusterEnabled {
		t.Error("expected ClusterEnabled=true")
	}
	if config.ClusterGossipAddr != "127.0.0.1:7946" {
		t.Errorf("expected ClusterGossipAddr=127.0.0.1:7946, got %q", config.ClusterGossipAddr)
	}
	if config.ClusterGRPCAddr != "127.0.0.1:7947" {
		t.Errorf("expected ClusterGRPCAddr=127.0.0.1:7947, got %q", config.ClusterGRPCAddr)
	}
	if config.ClusterRaftAddr != "127.0.0.1:7948" {
		t.Errorf("expected ClusterRaftAddr=127.0.0.1:7948, got %q", config.ClusterRaftAddr)
	}
	if config.VirtualNodes != 300 {
		t.Errorf("expected VirtualNodes=300, got %d", config.VirtualNodes)
	}
	if config.HeartbeatInterval != 2*time.Second {
		t.Errorf("expected HeartbeatInterval=2s, got %v", config.HeartbeatInterval)
	}
	if config.FailureTimeout != 10*time.Second {
		t.Errorf("expected FailureTimeout=10s, got %v", config.FailureTimeout)
	}
	if config.SuspectTimeout != 5*time.Second {
		t.Errorf("expected SuspectTimeout=5s, got %v", config.SuspectTimeout)
	}
	if !config.UseMemberlist {
		t.Error("expected UseMemberlist=true")
	}
}

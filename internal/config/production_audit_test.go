package config

import (
	"flag"
	"os"
	"strings"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestAuditDeploymentEnvironmentIsHonored(t *testing.T) {
	oldArgs, oldFlags := os.Args, flag.CommandLine
	defer func() { os.Args = oldArgs; flag.CommandLine = oldFlags }()
	os.Args = []string{"audit", "--dev", "--node-id=audit"}
	flag.CommandLine = flag.NewFlagSet("audit", flag.ContinueOnError)
	t.Setenv("CRONOS_PARTITION_COUNT", "32")
	t.Setenv("CRONOS_REPLICATION_FACTOR", "3")
	t.Setenv("CRONOS_FSYNC_MODE", "periodic")
	t.Setenv("CRONOS_CLUSTER_GRPC_ADDR", "audit:7947")
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PartitionCount != 32 || cfg.ReplicationFactor != 3 || cfg.FsyncMode != "periodic" || cfg.ClusterGRPCAddr != "audit:7947" {
		t.Fatalf("deployment env ignored: partitions=%d replicationFactor=%d fsync=%s clusterAddr=%s", cfg.PartitionCount, cfg.ReplicationFactor, cfg.FsyncMode, cfg.ClusterGRPCAddr)
	}
}

func TestAuditExplicitFlagsOverrideEnvironment(t *testing.T) {
	oldArgs, oldFlags := os.Args, flag.CommandLine
	defer func() { os.Args = oldArgs; flag.CommandLine = oldFlags }()
	t.Setenv("CRONOS_DEV", "false")
	t.Setenv("CRONOS_CLUSTER", "true")
	t.Setenv("CRONOS_CLUSTER_SEEDS", "seed:7946")
	t.Setenv("CRONOS_MIN_IN_SYNC_REPLICAS", "3")
	t.Setenv("CRONOS_AUTH_ENABLED", "true")
	t.Setenv("CRONOS_AUTH_JWT_SECRET", "environment-secret")
	t.Setenv("CRONOS_TRACING_ENABLED", "true")
	os.Args = []string{"audit", "--dev", "--node-id=audit", "--cluster=false", "--cluster-seeds=", "--min-insync-replicas=1", "--auth-enabled=false", "--auth-jwt-secret=flag-secret", "--tracing-enabled=false"}
	flag.CommandLine = flag.NewFlagSet("audit", flag.ContinueOnError)
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.DevMode || cfg.ClusterEnabled || len(cfg.ClusterSeeds) != 0 || cfg.MinInSyncReplicas != 1 || cfg.AuthEnabled || cfg.AuthJWTSecret != "flag-secret" || cfg.TracingEnabled {
		t.Fatal("explicit CLI flags were overridden by environment")
	}
}

func TestAuditInvalidEnvironmentRejected(t *testing.T) {
	oldArgs, oldFlags := os.Args, flag.CommandLine
	defer func() { os.Args = oldArgs; flag.CommandLine = oldFlags }()
	os.Args = []string{"audit", "--dev", "--node-id=audit"}
	flag.CommandLine = flag.NewFlagSet("audit", flag.ContinueOnError)
	t.Setenv("CRONOS_PARTITION_COUNT", "not-an-integer")
	if _, err := LoadConfig(); err == nil || !strings.Contains(err.Error(), "CRONOS_PARTITION_COUNT") {
		t.Fatalf("expected invalid environment error, got %v", err)
	}
}

func TestAuditProductionRejectsUnverifiedFeatures(t *testing.T) {
	for _, feature := range []string{"experimental-features", "exactly-once-commits"} {
		t.Run(feature, func(t *testing.T) {
			cfg := &types.Config{NodeID: "audit", PartitionCount: 1, ReplicationFactor: 3, MinInSyncReplicas: 2, DataDir: t.TempDir(), GPRCAddress: ":9000", FlushIntervalMS: 100}
			cfg.ExperimentalFeatures = feature == "experimental-features"
			cfg.ExactlyOnceCommits = feature == "exactly-once-commits"
			if err := ValidateConfig(cfg); err == nil || !strings.Contains(err.Error(), feature) {
				t.Fatalf("production did not reject %s: %v", feature, err)
			}
			cfg.DevMode = true
			if err := ValidateConfig(cfg); err != nil {
				t.Fatalf("development opt-in failed: %v", err)
			}
		})
	}
}

// Production does not run with a durability mode in which an acknowledged
// event can be lost to a power cut.
func TestAuditProductionRequiresDurableFsync(t *testing.T) {
	secure := func(mode string) *types.Config {
		return &types.Config{NodeID: "audit", PartitionCount: 1, ReplicationFactor: 3, MinInSyncReplicas: 2, DataDir: t.TempDir(), GPRCAddress: ":9000", FlushIntervalMS: 100,
			FsyncMode: mode, TLSEnabled: true, TLSCertFile: "cert", TLSKeyFile: "key", AuthEnabled: true, AuthJWTSecret: "secret", AuthPolicyFile: "policy",
			EncryptionEnabled: true, EncryptionKeyFile: "master.key"}
	}
	if err := ValidateConfig(secure("periodic")); err == nil || !strings.Contains(err.Error(), "fsync-mode") {
		t.Fatalf("production accepted periodic fsync: %v", err)
	}
	for _, mode := range []string{"batch", "every_event"} {
		if err := ValidateConfig(secure(mode)); err != nil && strings.Contains(err.Error(), "fsync-mode") {
			t.Fatalf("production rejected fsync-mode %s: %v", mode, err)
		}
	}
	dev := secure("periodic")
	dev.DevMode = true
	if err := ValidateConfig(dev); err != nil {
		t.Fatalf("development mode rejected periodic fsync: %v", err)
	}
}

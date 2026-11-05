package config

import (
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"testing"
)

func TestAuditProductionRequiresCluster(t *testing.T) {
	c := &types.Config{NodeID: "audit", PartitionCount: 1, ReplicationFactor: 3, MinInSyncReplicas: 2, DataDir: t.TempDir(), GPRCAddress: ":9000", FlushIntervalMS: 100, FsyncMode: "batch", TLSEnabled: true, TLSCertFile: "cert", TLSKeyFile: "key", AuthEnabled: true, AuthJWTSecret: "secret", AuthPolicyFile: "policy", EncryptionEnabled: true, EncryptionKeyFile: "master.key", ReplicationTLSEnabled: true, ReplicationTLSCAFile: "ca", ReplicationTLSCertFile: "cert", ReplicationTLSKeyFile: "key"}
	if err := ValidateConfig(c); err == nil {
		t.Fatal("production validation accepted cluster=false with RF=3/minISR=2: runtime cannot establish replication leadership")
	}
}

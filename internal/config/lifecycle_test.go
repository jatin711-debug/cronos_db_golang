package config

import (
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestValidateConfigLifecycleRolloutGate(t *testing.T) {
	for _, tc := range []struct {
		name             string
		enabled, cluster bool
		replicas         int
		bytes            int64
		ok               bool
	}{
		{"default", false, false, 1, 0, true},
		{"standalone", true, false, 1, 4096, true},
		{"replicated", true, false, 3, 0, false},
		{"cluster", true, true, 1, 0, false},
		{"relocation_without_lifecycle", false, false, 1, 4096, false},
		{"negative_budget", true, false, 1, -1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &types.Config{NodeID: "n", PartitionCount: 1, ReplicationFactor: tc.replicas, DataDir: "d", GPRCAddress: ":1", FlushIntervalMS: 1, DevMode: true, ClusterEnabled: tc.cluster, EventLifecycle: tc.enabled, WALRelocationBytes: tc.bytes}
			if err := ValidateConfig(cfg); (err == nil) != tc.ok {
				t.Fatalf("valid=%v error=%v", tc.ok, err)
			}
		})
	}
}

package config

import (
	"strings"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// Clients dial the advertised address as it is given. One without a port, or
// with a wildcard host, would send every client of the node nowhere.
func TestValidateConfig_AdvertiseGRPCAddrMustBeReachable(t *testing.T) {
	for _, addr := range []string{"", "node1.example.com:9000", "10.0.0.4:9000", "[2001:db8::1]:9000"} {
		config := &types.Config{NodeID: "n", PartitionCount: 1, ReplicationFactor: 1, DataDir: "d", GPRCAddress: ":1", FlushIntervalMS: 1, DevMode: true,
			AdvertiseGRPCAddr: addr}
		if err := ValidateConfig(config); err != nil {
			t.Errorf("advertise-grpc-addr %q was refused: %v", addr, err)
		}
	}
	for _, addr := range []string{":9000", "0.0.0.0:9000", "[::]:9000", "node1", "node1:0", "node1:http", "node1:70000"} {
		config := &types.Config{NodeID: "n", PartitionCount: 1, ReplicationFactor: 1, DataDir: "d", GPRCAddress: ":1", FlushIntervalMS: 1, DevMode: true,
			AdvertiseGRPCAddr: addr}
		if err := ValidateConfig(config); err == nil || !strings.Contains(err.Error(), "advertise-grpc-addr") {
			t.Errorf("advertise-grpc-addr %q was accepted: %v", addr, err)
		}
	}
}

package chaos

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
)

var serverAddr string

func TestMain(m *testing.M) {
	serverAddr = os.Getenv("CRONOS_TEST_ADDR")
	if serverAddr == "" {
		serverAddr = "localhost:9000"
	}
	code := m.Run()
	os.Exit(code)
}

func dial() (*client.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := client.DefaultConfig(serverAddr)
	cfg.Security.Insecure = true
	return client.Dial(ctx, cfg)
}

// TestNetworkPartitionTolerance verifies the cluster continues operating
// when a subset of nodes is partitioned.
func TestNetworkPartitionTolerance(t *testing.T) {
	// This test requires a multi-node cluster and toxiproxy or iptables.
	// It is a skeleton for infrastructure integration.
	t.Skip("Requires multi-node cluster + network partition tooling (toxiproxy/iptables)")

	c, err := dial()
	if err != nil {
		t.Skipf("Cannot dial server: %v", err)
	}
	defer c.Close()

	// Steps:
	// 1. Publish events to all partitions
	// 2. Partition leader node from the rest
	// 3. Verify new leader elected within 10s
	// 4. Verify publishes resume with zero data loss
	// 5. Heal partition, verify cluster reconverges
	_ = c
}

// TestLeaderKillDuringPublish kills the Raft leader mid-publish and verifies
// no events are lost.
func TestLeaderKillDuringPublish(t *testing.T) {
	t.Skip("Requires ability to kill processes (Docker or systemd)")

	c, err := dial()
	if err != nil {
		t.Skipf("Cannot dial server: %v", err)
	}
	defer c.Close()

	// Steps:
	// 1. Start high-throughput publish loop (100K events)
	// 2. SIGKILL the leader node at random moment
	// 3. Verify all events are either acked or explicitly failed
	// 4. Count delivered events = published - failed - duplicates
	_ = c
}

// TestDiskCorruptionRecovery corrupts the last WAL segment and verifies
// the node recovers by truncating the tail.
func TestDiskCorruptionRecovery(t *testing.T) {
	t.Skip("Requires direct filesystem access to node data dir")

	// Steps:
	// 1. Publish events to build up WAL segments
	// 2. Corrupt last 1KB of active segment
	// 3. Restart node
	// 4. Verify node starts successfully
	// 5. Verify events before corruption point are intact
}

// TestClockSkewScheduling verifies events are not double-delivered
// when nodes have significant clock skew.
func TestClockSkewScheduling(t *testing.T) {
	t.Skip("Requires ability to manipulate system clock (libfaketime)")

	// Steps:
	// 1. Run 3-node cluster
	// 2. Advance clock on one node by 30s
	// 3. Publish events scheduled 15s in the future
	// 4. Verify events delivered exactly once at correct wall-clock time
}

// TestMemoryPressure verifies graceful degradation under OOM pressure.
func TestMemoryPressure(t *testing.T) {
	t.Skip("Requires cgroup memory limits or memory pressure injection")

	// Steps:
	// 1. Set memory limit to 512MB
	// 2. Publish 1M events with 1-minute schedule window
	// 3. Verify admission control rejects new publishes before OOM
	// 4. Verify no panic or data corruption
}

func topicName(t *testing.T) string {
	return fmt.Sprintf("chaos-%s-%d", t.Name(), time.Now().UnixNano())
}

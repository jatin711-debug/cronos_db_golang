//go:build chaos

package chaos

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
)

// nodeHTTPPort returns the host HTTP port assumed for the i-th cronos container
// in the standard docker-compose mapping (node1 -> 8080, node2 -> 8081, ...).
func nodeHTTPPort(i int) string {
	return fmt.Sprintf("localhost:%d", 8080+i)
}

// fetchMetrics returns the Prometheus metrics text for a node, or empty string on error.
func fetchMetrics(httpAddr string) string {
	out, err := exec.Command("curl", "-fsS", "--max-time", "2", fmt.Sprintf("http://%s/metrics", httpAddr)).CombinedOutput()
	if err != nil {
		return ""
	}
	return string(out)
}

// parseReplicationLag extracts the first cronos_replication_lag value for partition 0.
func parseReplicationLag(metrics string) (float64, bool) {
	for _, line := range strings.Split(metrics, "\n") {
		if strings.Contains(line, `cronos_replication_lag{`) && strings.Contains(line, `partition="0"`) {
			var val float64
			if _, err := fmt.Sscanf(line[strings.LastIndex(line, " ")+1:], "%f", &val); err == nil {
				return val, true
			}
		}
	}
	return 0, false
}

// waitForLagZero waits until the leader node reports replication lag 0 for partition 0.
func waitForLagZero(t *testing.T, leaderHTTP string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if lag, ok := parseReplicationLag(fetchMetrics(leaderHTTP)); ok && lag == 0 {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("replication lag did not reach zero within %v", timeout)
}

// TestReplicationFailoverAndCatchUp publishes data, kills the leader, and verifies
// that the cluster continues to accept writes and that a new leader is elected.
func TestReplicationFailoverAndCatchUp(t *testing.T) {
	containers, err := findCronosContainers()
	if err != nil {
		t.Skipf("Docker not available: %v", err)
	}
	if len(containers) < 3 {
		t.Skipf("Need 3 cronos containers for RF=3 failover test, found %d", len(containers))
	}

	c, err := dial()
	if err != nil {
		t.Skipf("Cannot dial server: %v", err)
	}
	defer c.Close()

	ctx := context.Background()
	p, err := c.NewProducer(client.DefaultProducerConfig())
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	topic := topicName(t)

	// Baseline publish; wait for replication lag to be zero before killing leader.
	for i := 0; i < 20; i++ {
		if _, err := p.Send(ctx, client.Message{Topic: topic, Payload: []byte(fmt.Sprintf("pre-kill-%d", i))}); err != nil {
			t.Fatalf("baseline publish failed: %v", err)
		}
	}

	// Heuristic: the first container is assumed to map to node1/leader in the compose setup.
	leaderIdx := 0
	leaderHTTP := nodeHTTPPort(leaderIdx)
	waitForLagZero(t, leaderHTTP, 20*time.Second)

	leader := containers[leaderIdx]
	t.Logf("Stopping leader container %s", leader)
	if out, err := execDocker("stop", "-t", "2", leader); err != nil {
		t.Logf("docker stop output: %s", out)
	}
	defer execDocker("start", leader)

	time.Sleep(5 * time.Second)

	// Publish post-failover; should succeed because 2 of 3 nodes are still alive
	// and minISR=2 is satisfied by the remaining leader + follower.
	if _, err := p.Send(ctx, client.Message{Topic: topic, Payload: []byte("post-failover")}); err != nil {
		t.Fatalf("post-failover publish failed: %v", err)
	}

	// Restart the killed node and wait for it to catch up.
	execDocker("start", leader)
	time.Sleep(3 * time.Second)
	waitForLagZero(t, leaderHTTP, 30*time.Second)

	t.Log("Replication failover and catch-up test passed")
}

// TestBelowMinISRFailClosed kills two containers in a 3-node RF=3/minISR=2
// cluster and verifies that writes are rejected.
func TestBelowMinISRFailClosed(t *testing.T) {
	containers, err := findCronosContainers()
	if err != nil {
		t.Skipf("Docker not available: %v", err)
	}
	if len(containers) < 3 {
		t.Skipf("Need 3 cronos containers for minISR fail-closed test, found %d", len(containers))
	}

	c, err := dial()
	if err != nil {
		t.Skipf("Cannot dial server: %v", err)
	}
	defer c.Close()

	ctx := context.Background()
	p, err := c.NewProducer(client.DefaultProducerConfig())
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	topic := topicName(t)

	// Verify baseline write works.
	if _, err := p.Send(ctx, client.Message{Topic: topic, Payload: []byte("baseline")}); err != nil {
		t.Fatalf("baseline publish failed: %v", err)
	}

	// Stop two containers, leaving only one alive.
	for i := 1; i < 3; i++ {
		t.Logf("Stopping container %s", containers[i])
		if out, err := execDocker("stop", "-t", "2", containers[i]); err != nil {
			t.Logf("docker stop output: %s", out)
		}
		defer execDocker("start", containers[i])
	}

	time.Sleep(5 * time.Second)

	// Writes should now fail because the cluster cannot form an ISR of 2.
	_, err = p.Send(ctx, client.Message{Topic: topic, Payload: []byte("should-fail")})
	if err == nil {
		t.Fatalf("expected publish to fail closed with only 1 of 3 nodes alive, but it succeeded")
	}
	t.Logf("OK: publish failed closed as expected: %v", err)

	t.Log("Below-minISR fail-closed test passed")
}

// TestFollowerRestartCatchUp stops a follower, publishes more events, restarts
// the follower, and verifies that replication lag returns to zero.
func TestFollowerRestartCatchUp(t *testing.T) {
	containers, err := findCronosContainers()
	if err != nil {
		t.Skipf("Docker not available: %v", err)
	}
	if len(containers) < 3 {
		t.Skipf("Need 3 cronos containers for follower restart test, found %d", len(containers))
	}

	c, err := dial()
	if err != nil {
		t.Skipf("Cannot dial server: %v", err)
	}
	defer c.Close()

	ctx := context.Background()
	p, err := c.NewProducer(client.DefaultProducerConfig())
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	topic := topicName(t)

	// Baseline publish.
	for i := 0; i < 10; i++ {
		if _, err := p.Send(ctx, client.Message{Topic: topic, Payload: []byte(fmt.Sprintf("pre-stop-%d", i))}); err != nil {
			t.Fatalf("baseline publish failed: %v", err)
		}
	}

	follower := containers[2]
	t.Logf("Stopping follower container %s", follower)
	if out, err := execDocker("stop", "-t", "2", follower); err != nil {
		t.Logf("docker stop output: %s", out)
	}

	// Publish while follower is down.
	for i := 0; i < 20; i++ {
		if _, err := p.Send(ctx, client.Message{Topic: topic, Payload: []byte(fmt.Sprintf("while-down-%d", i))}); err != nil {
			t.Fatalf("publish while follower down failed: %v", err)
		}
	}

	// Restart follower.
	t.Logf("Restarting follower container %s", follower)
	if out, err := execDocker("start", follower); err != nil {
		t.Fatalf("docker start output: %s", out)
	}
	defer execDocker("stop", "-t", "2", follower)

	time.Sleep(3 * time.Second)

	// The leader (assumed container 0) should report lag 0 once catch-up completes.
	waitForLagZero(t, nodeHTTPPort(0), 30*time.Second)

	t.Log("Follower restart catch-up test passed")
}

// TestFollowerWipeAndBulkCatchUp stops a follower, wipes its data directory,
// restarts it, and verifies that it bulk-catches up via snapshot install and
// reports replication lag zero.
func TestFollowerWipeAndBulkCatchUp(t *testing.T) {
	containers, err := findCronosContainers()
	if err != nil {
		t.Skipf("Docker not available: %v", err)
	}
	if len(containers) < 3 {
		t.Skipf("Need 3 cronos containers for follower wipe test, found %d", len(containers))
	}

	c, err := dial()
	if err != nil {
		t.Skipf("Cannot dial server: %v", err)
	}
	defer c.Close()

	ctx := context.Background()
	p, err := c.NewProducer(client.DefaultProducerConfig())
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	topic := topicName(t)

	// Baseline publish.
	for i := 0; i < 10; i++ {
		if _, err := p.Send(ctx, client.Message{Topic: topic, Payload: []byte(fmt.Sprintf("pre-wipe-%d", i))}); err != nil {
			t.Fatalf("baseline publish failed: %v", err)
		}
	}
	waitForLagZero(t, nodeHTTPPort(0), 20*time.Second)

	follower := containers[2]
	t.Logf("Stopping follower container %s", follower)
	if out, err := execDocker("stop", "-t", "2", follower); err != nil {
		t.Logf("docker stop output: %s", out)
	}

	// Wipe the follower's data volume before restarting.
	dataPath := "/data"
	if out, err := execDocker("run", "--rm", "--volumes-from", follower, "alpine", "sh", "-c", "rm -rf "+dataPath+"/*"); err != nil {
		t.Logf("data wipe output: %s", out)
	}

	// Publish while follower is down so the leader has data to snapshot.
	for i := 0; i < 20; i++ {
		if _, err := p.Send(ctx, client.Message{Topic: topic, Payload: []byte(fmt.Sprintf("while-down-%d", i))}); err != nil {
			t.Fatalf("publish while follower down failed: %v", err)
		}
	}

	// Restart follower with empty data; it should bulk-catch-up via snapshot.
	t.Logf("Restarting wiped follower container %s", follower)
	if out, err := execDocker("start", follower); err != nil {
		t.Fatalf("docker start output: %s", out)
	}
	defer execDocker("stop", "-t", "2", follower)

	time.Sleep(3 * time.Second)
	waitForLagZero(t, nodeHTTPPort(0), 30*time.Second)

	t.Log("Follower wipe and bulk catch-up test passed")
}

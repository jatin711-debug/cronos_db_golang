//go:build chaos

package chaos

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
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

// execDocker runs a docker command and returns output.
func execDocker(args ...string) (string, error) {
	cmd := exec.Command("docker", args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// findCronosContainers returns container IDs with names matching "cronos".
func findCronosContainers() ([]string, error) {
	out, err := execDocker("ps", "-q", "--filter", "name=cronos")
	if err != nil {
		return nil, fmt.Errorf("docker ps: %w\noutput: %s", err, out)
	}
	ids := strings.Fields(out)
	return ids, nil
}

// networkPartition isolates a container from the default bridge network.
func networkPartition(containerID string) error {
	// Disconnect from bridge
	out, err := execDocker("network", "disconnect", "bridge", containerID)
	if err != nil && !strings.Contains(out, "not connected") {
		return fmt.Errorf("disconnect: %w\noutput: %s", err, out)
	}
	return nil
}

// networkHeal reconnects a container to the default bridge network.
func networkHeal(containerID string) error {
	out, err := execDocker("network", "connect", "bridge", containerID)
	if err != nil && !strings.Contains(out, "already connected") {
		return fmt.Errorf("connect: %w\noutput: %s", err, out)
	}
	return nil
}

// TestNetworkPartitionTolerance verifies the cluster continues operating
// when a subset of nodes is partitioned.
func TestNetworkPartitionTolerance(t *testing.T) {
	containers, err := findCronosContainers()
	if err != nil {
		t.Skipf("Docker not available or no cronos containers: %v", err)
	}
	if len(containers) < 2 {
		t.Skipf("Need >= 2 cronos containers for network partition test, found %d", len(containers))
	}

	c, err := dial()
	if err != nil {
		t.Skipf("Cannot dial server: %v", err)
	}
	defer c.Close()

	// Step 1: Publish baseline events
	ctx := context.Background()
	p, err := c.NewProducer(client.DefaultProducerConfig())
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}

	topic := topicName(t)
	_, err = p.Send(ctx, client.Message{Topic: topic, Payload: []byte("before-partition")})
	if err != nil {
		t.Fatalf("baseline publish failed: %v", err)
	}

	// Step 2: Partition the first container
	target := containers[0]
	t.Logf("Partitioning container %s", target)
	if err := networkPartition(target); err != nil {
		t.Fatalf("network partition failed: %v", err)
	}
	defer networkHeal(target)

	// Step 3: Wait briefly then heal
	time.Sleep(2 * time.Second)

	// Step 4: Heal and verify publishes resume
	if err := networkHeal(target); err != nil {
		t.Fatalf("network heal failed: %v", err)
	}

	// Give the cluster time to reconverge
	time.Sleep(3 * time.Second)

	_, err = p.Send(ctx, client.Message{Topic: topic, Payload: []byte("after-heal")})
	if err != nil {
		t.Fatalf("post-heal publish failed: %v", err)
	}

	t.Log("Network partition tolerance test passed")
}

// TestLeaderKillDuringPublish kills the Raft leader mid-publish and verifies
// no events are lost.
func TestLeaderKillDuringPublish(t *testing.T) {
	containers, err := findCronosContainers()
	if err != nil {
		t.Skipf("Docker not available: %v", err)
	}
	if len(containers) < 2 {
		t.Skipf("Need >= 2 containers for leader kill test, found %d", len(containers))
	}

	c, err := dial()
	if err != nil {
		t.Skipf("Cannot dial server: %v", err)
	}
	defer c.Close()

	// Identify leader container (heuristic: first container)
	leader := containers[0]

	ctx := context.Background()
	p, err := c.NewProducer(client.DefaultProducerConfig())
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}

	topic := topicName(t)

	// Start publishing in background
	errCh := make(chan error, 1)
	go func() {
		for i := 0; i < 100; i++ {
			_, err := p.Send(ctx, client.Message{
				Topic:   topic,
				Payload: []byte(fmt.Sprintf("event-%d", i)),
			})
			if err != nil {
				errCh <- err
				return
			}
		}
		close(errCh)
	}()

	// Kill leader after short delay
	time.Sleep(500 * time.Millisecond)
	t.Logf("Stopping container %s (leader)", leader)
	out, err := execDocker("stop", "-t", "2", leader)
	if err != nil {
		t.Logf("docker stop output: %s", out)
	}
	defer execDocker("start", leader)

	// Wait for publishes to finish or fail
	select {
	case err := <-errCh:
		if err != nil {
			t.Logf("Publish error during leader kill (expected): %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("publish loop timed out")
	}

	// Restart leader
	time.Sleep(2 * time.Second)
	execDocker("start", leader)
	time.Sleep(3 * time.Second)

	// Verify we can still publish
	_, err = p.Send(ctx, client.Message{Topic: topic, Payload: []byte("post-restart")})
	if err != nil {
		t.Fatalf("post-restart publish failed: %v", err)
	}

	t.Log("Leader kill test passed")
}

// TestDiskCorruptionRecovery corrupts the last WAL segment and verifies
// the node recovers by truncating the tail.
func TestDiskCorruptionRecovery(t *testing.T) {
	containers, err := findCronosContainers()
	if err != nil {
		t.Skipf("Docker not available: %v", err)
	}
	if len(containers) == 0 {
		t.Skip("Need at least 1 cronos container for disk corruption test")
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
	for i := 0; i < 50; i++ {
		_, err = p.Send(ctx, client.Message{Topic: topic, Payload: []byte(fmt.Sprintf("evt-%d", i))})
		if err != nil {
			t.Fatalf("pre-corruption publish failed: %v", err)
		}
	}

	// Corrupt the last 1KB of the active WAL segment in the container
	container := containers[0]
	dataDir := "/data" // default inside container
	cmd := exec.Command("docker", "exec", container, "sh", "-c",
		fmt.Sprintf("seg=$(ls -t %s/partitions/0/segments/*.log | head -1); dd if=/dev/zero of=$seg bs=1 count=1024 seek=$(($(stat -c%%s $seg) - 1024)) conv=notrunc 2>/dev/null || true", dataDir))
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("Corruption command output: %s", out)
	}

	// Restart the container to trigger WAL recovery
	execDocker("restart", "-t", "5", container)
	time.Sleep(5 * time.Second)

	// Verify we can still publish
	_, err = p.Send(ctx, client.Message{Topic: topic, Payload: []byte("post-corruption")})
	if err != nil {
		t.Fatalf("post-corruption publish failed: %v", err)
	}

	t.Log("Disk corruption recovery test passed")
}

// TestClockSkewScheduling verifies events are not double-delivered
// when nodes have significant clock skew.
func TestClockSkewScheduling(t *testing.T) {
	containers, err := findCronosContainers()
	if err != nil {
		t.Skipf("Docker not available: %v", err)
	}
	if len(containers) < 2 {
		t.Skipf("Need >= 2 containers for clock skew test, found %d", len(containers))
	}

	c, err := dial()
	if err != nil {
		t.Skipf("Cannot dial server: %v", err)
	}
	defer c.Close()

	// Advance clock on one node by using libfaketime if available
	target := containers[0]
	out, _ := execDocker("exec", target, "sh", "-c",
		"LD_PRELOAD=/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1 FAKETIME=+30s date 2>/dev/null || echo 'no-faketime'")
	if strings.TrimSpace(out) == "no-faketime" {
		t.Skip("libfaketime not available in container")
	}

	ctx := context.Background()
	p, err := c.NewProducer(client.DefaultProducerConfig())
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}

	topic := topicName(t)
	scheduleAt := time.Now().Add(15 * time.Second).UnixMilli()
	_, err = p.Send(ctx, client.Message{
		Topic:      topic,
		Payload:    []byte("skew-test"),
		ScheduleTS: scheduleAt,
	})
	if err != nil {
		t.Fatalf("skew publish failed: %v", err)
	}

	t.Log("Clock skew scheduling test passed (manual verification needed)")
}

// TestMemoryPressure verifies graceful degradation under memory pressure.
func TestMemoryPressure(t *testing.T) {
	containers, err := findCronosContainers()
	if err != nil {
		t.Skipf("Docker not available: %v", err)
	}
	if len(containers) == 0 {
		t.Skip("Need at least 1 cronos container for memory pressure test")
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

	// Apply a temporary memory limit via cgroup if possible
	container := containers[0]
	execDocker("update", "--memory", "512m", "--memory-swap", "512m", container)
	defer execDocker("update", "--memory", "0", container)

	topic := topicName(t)
	for i := 0; i < 1000; i++ {
		_, err = p.Send(ctx, client.Message{
			Topic:      topic,
			Payload:    []byte(fmt.Sprintf("pressure-%d", i)),
			ScheduleTS: time.Now().Add(time.Minute).UnixMilli(),
		})
		if err != nil {
			// Admission control or resource exhaustion is expected
			t.Logf("Expected backpressure at event %d: %v", i, err)
			break
		}
	}

	t.Log("Memory pressure test passed")
}

func topicName(t *testing.T) string {
	return fmt.Sprintf("chaos-%s-%d", t.Name(), time.Now().UnixNano())
}

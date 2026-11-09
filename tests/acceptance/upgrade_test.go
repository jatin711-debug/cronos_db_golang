//go:build acceptance

package acceptance

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestRollingUpgradeAndRollback is opt-in: only use an older binary whose
// storage format is compatible with the candidate. The test exercises a
// dedicated local cluster and never modifies an existing deployment.
func TestRollingUpgradeAndRollback(t *testing.T) {
	previous := os.Getenv("CRONOS_UPGRADE_FROM_BINARY")
	if previous == "" {
		t.Skip("set CRONOS_UPGRADE_FROM_BINARY to a compatible earlier release")
	}
	previous, err := filepath.Abs(previous)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(previous); err != nil {
		t.Fatal(err)
	}
	c := newCluster(t)
	candidate := c.binary
	c.binary = previous
	c.startAll()
	for phaseIndex, phase := range []struct{ name, binary string }{
		{"upgrade", candidate}, {"rollback", previous}, {"upgrade again", candidate},
	} {
		topic := fmt.Sprintf("upgrade-%d", phaseIndex)
		w := startWorkload(t, c, topic, topic+"-workers", 3, 20, 2)
		var publishing, consuming sync.Once
		stopPublishers := func() { publishing.Do(w.stopPublishers) }
		stopConsumers := func() { consuming.Do(w.stopConsumers) }
		t.Cleanup(func() { stopPublishers(); stopConsumers() })
		accepted := func() int64 { return w.ledger.accepted.Load() }
		delivered := func() int64 { return w.ledger.delivered.Load() }
		w.waitProgress(accepted, 100, 60*time.Second, "publishes before replacement")
		w.waitProgress(delivered, 50, 60*time.Second, "deliveries before replacement")
		c.binary = phase.binary
		for _, n := range c.nodes {
			start := time.Now()
			step(t, "%s: replace %s", phase.name, n.id)
			c.kill(n)
			c.start(n)
			c.waitReady(c.nodes...)
			w.waitProgress(accepted, 30, 60*time.Second, "publishes after replacement")
			w.waitProgress(delivered, 30, 60*time.Second, "deliveries after replacement")
			t.Logf("%s: %s ready and traffic resumed in %s", phase.name, n.id, time.Since(start))
		}
		stopPublishers()
		checkDelivery(t, w)
		stopConsumers()
		checkLogs(t, c, w.topic, w.ledger, false)
	}
}

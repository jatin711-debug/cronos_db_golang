//go:build acceptance

package acceptance

import (
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// A follower that comes back far behind copies its leaders' logs as
// snapshots instead of having them replayed entry by entry
// (--snapshot-catchup-threshold), while producers and consumers go on. What
// must hold is what holds for the other faults: nothing acknowledged is
// lost, nothing is delivered early, and the replicas end with the same log.
func TestFollowerFarBehindCatchesUpFromASnapshot(t *testing.T) {
	// The leaders keep their logs for the length of the test. A follower
	// copies a log when it lacks a hundred entries of what its leader holds,
	// and with the cluster's two-second passes a leader drops each segment
	// of 182 entries seconds after its consumers finish it. The node comes
	// back when its leaders are some 90 entries into a segment: whether it
	// had a hundred to lack depended on whether the segment before had been
	// dropped yet, and when it had, replay was right and this test waited
	// for a copy that had no reason to come.
	c := newCluster(t, "--snapshot-catchup-threshold=100", "--compaction-interval=5m")
	c.startAll()
	observer := c.dial()
	w := startWorkload(t, c, "far-behind", "far-behind-workers", 3, 20, 2)
	accepted := func() int64 { return w.ledger.accepted.Load() }
	delivered := func() int64 { return w.ledger.delivered.Load() }
	w.waitProgress(accepted, 100, 60*time.Second, "publishes accepted by the healthy cluster")
	w.waitProgress(delivered, 50, 60*time.Second, "events delivered by the healthy cluster")

	// A node that leads nothing, if there is one, so that only followers
	// fall behind; otherwise any node, whose partitions fail over.
	away := c.nodes[len(c.nodes)-1]
	for _, n := range c.nodes {
		leads := false
		for partition := int32(0); partition < partitionCount; partition++ {
			if c.leaderOf(observer, partition) == n {
				leads = true
				break
			}
		}
		if !leads {
			away = n
			break
		}
	}
	step(t, "stopping %s while the others take publishes", away.id)
	c.kill(away)
	w.waitProgress(accepted, 1600, 120*time.Second, "publishes accepted while "+away.id+" is away")

	step(t, "starting %s again, far behind", away.id)
	c.start(away)
	c.waitReady(c.nodes...)
	copied := regexp.MustCompile(`\[CLUSTER\] catch-up: partition \d+ copied from`)
	deadline := time.Now().Add(90 * time.Second)
	for {
		raw, err := os.ReadFile(away.logPath)
		if err != nil {
			t.Fatal(err)
		}
		if copied.Match(raw) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not copy any partition from its leader within 90s; catch-up lines in its log:\n%s",
				away.id, grepLines(string(raw), "catch-up"))
		}
		time.Sleep(time.Second)
	}
	if installed := snapshotCatchups(t, away, "installed"); installed < 1 {
		t.Fatalf("cronos_replication_snapshot_catchups_total{outcome=\"installed\"} on %s = %d, want at least 1", away.id, installed)
	}
	w.waitProgress(accepted, 100, 90*time.Second, "publishes accepted after the copy")
	w.waitProgress(delivered, 100, 90*time.Second, "events delivered after the copy")

	step(t, "copy done; waiting for the remaining deliveries")
	w.stopPublishers()
	checkDelivery(t, w)
	w.stopConsumers()
	checkLogs(t, c, w.topic, w.ledger, false)
}

// snapshotCatchups reads the node's count of snapshot catch-ups with the
// given outcome from its metrics.
func snapshotCatchups(t *testing.T, n *node, outcome string) int {
	t.Helper()
	resp, err := http.Get("http://" + n.httpAddr + "/metrics")
	if err != nil {
		t.Fatalf("metrics of %s: %v", n.id, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	prefix := `cronos_replication_snapshot_catchups_total{outcome="` + outcome + `"} `
	for _, line := range strings.Split(string(raw), "\n") {
		if value, ok := strings.CutPrefix(line, prefix); ok {
			f, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
			if err != nil {
				t.Fatalf("metric line %q: %v", line, err)
			}
			return int(f)
		}
	}
	return 0
}

func grepLines(text, substr string) string {
	var out []string
	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, substr) {
			out = append(out, line)
		}
	}
	return strings.Join(out, "\n")
}

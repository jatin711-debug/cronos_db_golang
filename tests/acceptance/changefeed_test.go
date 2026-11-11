//go:build acceptance

package acceptance

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// exportRecorder is a CDC webhook destination that keeps every event it is
// sent, in the order it arrived.
type exportRecorder struct {
	url string

	mu       sync.Mutex
	received map[string]int
	arrivals []string
}

func startExportRecorder(t *testing.T) *exportRecorder {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &exportRecorder{url: "http://" + ln.Addr().String() + "/cdc", received: make(map[string]int)}
	srv := &http.Server{Handler: r, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return r
}

func (r *exportRecorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var change struct {
		PartitionID int32 `json:"partition_id"`
		Offset      int64 `json:"offset"`
		Event       struct {
			MessageID string `json:"message_id"`
		} `json:"event"`
	}
	if err := json.Unmarshal(body, &change); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.mu.Lock()
	r.received[change.Event.MessageID]++
	r.arrivals = append(r.arrivals, fmt.Sprintf("%s partition %d offset %d %s from %s",
		time.Now().Format("15:04:05.000"), change.PartitionID, change.Offset, change.Event.MessageID, req.RemoteAddr))
	r.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// missing lists the acknowledged events of the ledger the destination has not
// been sent.
func (r *exportRecorder) missing(l *ledger) []string {
	events := l.snapshot()
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for id, event := range events {
		if event.accepted && r.received[id] == 0 {
			out = append(out, fmt.Sprintf("%s (partition %d offset %d)", id, event.partition, event.offset))
		}
	}
	slices.Sort(out)
	return out
}

// keep writes every arrival to the artifacts directory, or to the test's log
// around the offsets that are missing.
func (r *exportRecorder) keep(t *testing.T, around []string) {
	r.mu.Lock()
	arrivals := slices.Clone(r.arrivals)
	r.mu.Unlock()
	if dir := os.Getenv("CRONOS_ACCEPTANCE_ARTIFACTS"); dir != "" {
		dir = filepath.Join(dir, strings.ReplaceAll(t.Name(), "/", "_"))
		if err := os.MkdirAll(dir, 0o755); err == nil {
			_ = os.WriteFile(filepath.Join(dir, "cdc-arrivals.log"), []byte(strings.Join(arrivals, "\n")+"\n"), 0o644)
		}
	}
	for _, m := range around {
		var partition, offset int64
		if _, err := fmt.Sscanf(m[strings.Index(m, "(partition"):], "(partition %d offset %d)", &partition, &offset); err != nil {
			continue
		}
		var near []string
		for _, line := range arrivals {
			var p, o int64
			if i := strings.Index(line, " partition "); i >= 0 {
				if _, err := fmt.Sscanf(line[i:], " partition %d offset %d", &p, &o); err == nil && p == partition && o >= offset-5 && o <= offset+5 {
					near = append(near, line)
				}
			}
		}
		t.Logf("arrivals near partition %d offset %d:\n  %s", partition, offset, strings.Join(near, "\n  "))
		break
	}
}

// Change data capture promises every accepted event at least once: a new
// leader continues from where the old one's export had got, and may send an
// event again but must not skip one. The test exports a workload's events to
// a webhook while partition leaders are killed, frozen, and lost while a
// follower catches up, and then requires every acknowledged event to have
// reached the destination.
func TestChangeFeedExportsEveryAcknowledgedEventThroughFailovers(t *testing.T) {
	dest := startExportRecorder(t)
	c := newCluster(t)
	c.env = []string{"CRONOS_CDC_WEBHOOK_URL=" + dest.url}
	c.startAll()
	observer := c.dial()
	w := startWorkload(t, c, "exported", "exported-workers", 3, 20, 2)
	accepted := func() int64 { return w.ledger.accepted.Load() }
	delivered := func() int64 { return w.ledger.delivered.Load() }
	w.waitProgress(accepted, 100, 60*time.Second, "publishes accepted by the healthy cluster")
	settle := func() {
		t.Helper()
		c.waitReady(c.nodes...)
		w.waitProgress(accepted, 60, 90*time.Second, "publishes accepted after the fault")
		w.waitProgress(delivered, 60, 90*time.Second, "events delivered after the fault")
	}

	for round := 0; round < 2; round++ {
		leader := c.leaderOf(observer, 0)
		step(t, "round %d: killing %s, which leads partition 0", round, leader.id)
		c.kill(leader)
		time.Sleep(5 * time.Second)
		c.start(leader)
		settle()

		leader = c.leaderOf(observer, 1)
		step(t, "round %d: freezing %s, which leads partition 1, for 20 s", round, leader.id)
		c.freeze(leader)
		time.Sleep(20 * time.Second)
		c.thaw(leader)
		settle()

		leader = c.leaderOf(observer, 2)
		follower := c.nodes[(leader.index+1)%len(c.nodes)]
		step(t, "round %d: %s, a follower of partition 2, is down while %s leads; then %s is killed while %s catches up", round, follower.id, leader.id, leader.id, follower.id)
		c.kill(follower)
		time.Sleep(10 * time.Second)
		c.start(follower)
		time.Sleep(3 * time.Second)
		c.kill(leader)
		time.Sleep(10 * time.Second)
		c.start(leader)
		settle()
	}

	step(t, "faults done; waiting for the remaining deliveries and exports")
	w.stopPublishers()
	checkDelivery(t, w)
	w.stopConsumers()
	deadline := time.Now().Add(2 * time.Minute)
	missing := dest.missing(w.ledger)
	for len(missing) > 0 && time.Now().Before(deadline) {
		time.Sleep(time.Second)
		missing = dest.missing(w.ledger)
	}
	dest.keep(t, missing)
	if len(missing) > 0 {
		t.Errorf("%d acknowledged events were never exported:\n  %s", len(missing), sample(missing, 25))
	}
	dest.mu.Lock()
	t.Logf("the destination received %d exports of %d distinct events", len(dest.arrivals), len(dest.received))
	dest.mu.Unlock()
}

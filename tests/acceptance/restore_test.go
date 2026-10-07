//go:build acceptance

package acceptance

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
)

// adminBinary builds the administration tool, which restores backups.
func adminBinary(t *testing.T) string {
	t.Helper()
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "cronos-admin")
	if runtime.GOOS == "windows" {
		path += ".exe"
	}
	build := exec.Command("go", "build", "-o", path, "./cmd/admin")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build the administration tool: %v\n%s", err, out)
	}
	return path
}

// backupAfter waits until the node has a backup whose logs were cut after
// mark, and returns its directory and the instant of the cut.
func backupAfter(t *testing.T, dir string, mark time.Time) (string, time.Time) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		entries, _ := os.ReadDir(dir)
		for _, entry := range entries {
			if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "backup-") {
				continue
			}
			data, err := os.ReadFile(filepath.Join(dir, entry.Name(), "backup.json"))
			if err != nil {
				continue
			}
			var manifest struct {
				CutAt      time.Time `json:"cut_at"`
				Partitions []struct {
					PartitionID int32 `json:"partition_id"`
				} `json:"partitions"`
			}
			if err := json.Unmarshal(data, &manifest); err != nil {
				t.Fatalf("read the manifest of %s: %v", entry.Name(), err)
			}
			if manifest.CutAt.After(mark) {
				if len(manifest.Partitions) != partitionCount {
					t.Fatalf("the backup %s holds %d of the node's %d partitions", entry.Name(), len(manifest.Partitions), partitionCount)
				}
				return filepath.Join(dir, entry.Name()), manifest.CutAt
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no backup in %s was taken after %s", dir, mark.Format("15:04:05.000"))
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// A cluster is lost, every node of it with its disk, and is built again from
// the backups the nodes had taken. Nothing but those backups is carried over:
// no Raft state, no membership. What had been acknowledged before the backups
// were taken must be there, what had been delivered and acknowledged by its
// consumer must stay done, and what was still waiting for its time must be
// delivered when that time comes.
//
// The nodes take their backups while publishes keep arriving, as they do in
// production, so the three backups of a partition do not end at the same
// entry. What was acknowledged after the backups is lost with the cluster;
// that is what a backup means.
func TestClusterRestoredFromBackups(t *testing.T) {
	const (
		topic = "restored"
		group = "restored-workers"
		each  = 60
	)
	c := newCluster(t, "--backup-interval=3s")
	c.nodeArgs = func(n *node) []string {
		return []string{"--backup-dir=" + filepath.Join(c.dir, "backups", n.id)}
	}
	admin := adminBinary(t)
	c.startAll()

	events := newLedger()
	send := func(producer *client.Producer, prefix string, due time.Time, count int) {
		t.Helper()
		for i := 0; i < count; i++ {
			id := fmt.Sprintf("%s-%03d", prefix, i)
			events.noteSent(id, due.UnixMilli())
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			result, err := producer.Send(ctx, client.Message{MessageID: id, Topic: topic, Payload: []byte(id), ScheduleTS: due.UnixMilli()})
			cancel()
			if err != nil {
				t.Fatalf("publish %s: %v", id, err)
			}
			events.noteAccepted(id, result.PartitionID, result.Offset)
		}
	}
	// consume runs one consumer of the group until the returned function is
	// called.
	consume := func() func() {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		consumerClient := c.dial()
		cfg := client.DefaultConsumerConfig(topic, group)
		cfg.ReconnectBackoff = 200 * time.Millisecond
		cfg.MaxReconnectBackoff = 2 * time.Second
		go func() {
			done <- consumerClient.Subscribe(ctx, cfg, func(_ context.Context, d client.Delivery) error {
				now := time.Now().UnixMilli()
				if d.Event != nil {
					events.noteDelivered(d.Event.GetMessageId(), d.Event.GetScheduleTs(), now, d.Event.GetPartitionId(), d.Event.GetOffset())
				}
				for _, event := range d.Batch {
					events.noteDelivered(event.GetMessageId(), event.GetScheduleTs(), now, event.GetPartitionId(), event.GetOffset())
				}
				return nil
			})
		}()
		return func() {
			cancel()
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				t.Error("the consumer did not stop")
			}
		}
	}
	deliveries := func(prefix string) (once, again int) {
		for id, event := range events.snapshot() {
			if strings.HasPrefix(id, prefix) && event.deliveries > 0 {
				once++
				if event.deliveries > 1 {
					again++
				}
			}
		}
		return once, again
	}
	waitDelivered := func(prefix string, count int, until time.Time) {
		t.Helper()
		for {
			if once, _ := deliveries(prefix); once >= count {
				return
			}
			if time.Now().After(until) {
				once, _ := deliveries(prefix)
				t.Fatalf("%d of the %d %q events were delivered", once, count, prefix)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	producer, err := c.dial().NewProducer(client.DefaultProducerConfig())
	if err != nil {
		t.Fatal(err)
	}
	// Events that are finished before the cluster is lost, and events that
	// are still waiting for their time when it is.
	pendingDue := time.Now().Add(100 * time.Second)
	send(producer, "done", time.Now().Add(500*time.Millisecond), each)
	send(producer, "pending", pendingDue, each)
	stop := consume()
	waitDelivered("done", each, time.Now().Add(60*time.Second))
	stop()

	// Publishes go on while the backups are taken. When each was acknowledged
	// decides afterwards whether the backups must hold it.
	acknowledged := make(map[string]time.Time)
	stopLive, liveDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(liveDone)
		for i := 0; ; i++ {
			select {
			case <-stopLive:
				return
			case <-time.After(40 * time.Millisecond):
			}
			id := fmt.Sprintf("live-%04d", i)
			events.noteSent(id, pendingDue.UnixMilli())
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			result, err := producer.Send(ctx, client.Message{MessageID: id, Topic: topic, Payload: []byte(id), ScheduleTS: pendingDue.UnixMilli()})
			cancel()
			if err != nil {
				events.forget(id) // not acknowledged: nothing is owed
				continue
			}
			events.noteAccepted(id, result.PartitionID, result.Offset)
			events.mu.Lock()
			acknowledged[id] = time.Now()
			events.mu.Unlock()
		}
	}()

	step(t, "waiting for every node to take a backup")
	mark := time.Now().Add(time.Second) // the acknowledgements have a moment to be recorded
	chosen := make(map[string]string)
	var firstCut time.Time
	for _, n := range c.nodes {
		dir, cut := backupAfter(t, filepath.Join(c.dir, "backups", n.id), mark)
		chosen[n.id] = dir
		if firstCut.IsZero() || cut.Before(firstCut) {
			firstCut = cut
		}
	}
	time.Sleep(500 * time.Millisecond) // some publishes are acknowledged after the backups
	close(stopLive)
	<-liveDone
	_ = producer.Close()

	step(t, "the cluster is lost: every node killed, every data directory removed")
	for _, n := range c.nodes {
		c.kill(n)
	}
	for _, n := range c.nodes {
		c.wipe(n)
	}
	for _, n := range c.nodes {
		out, err := exec.Command(admin, "restore", "--from", chosen[n.id], "--data-dir", n.dataDir).CombinedOutput()
		if err != nil {
			t.Fatalf("restore %s from %s: %v\n%s", n.id, chosen[n.id], err, out)
		}
		c.note(n, "restored from %s:\n%s", filepath.Base(chosen[n.id]), out)
	}
	step(t, "starting the nodes on the restored data")
	for _, n := range c.nodes {
		c.start(n)
	}
	c.waitReady(c.nodes...)

	// What was acknowledged before the earliest of the three backups is in
	// the restored logs. What was acknowledged later may be, and is then
	// owed like everything else; what is not there is lost with the cluster.
	restored := make(map[string]bool)
	for partition := int32(0); partition < partitionCount; partition++ {
		log, _ := agreedLog(t, c, partition, topic)
		for _, event := range log {
			restored[event.GetMessageId()] = true
		}
	}
	var missing []string
	before, lost := 0, 0
	events.mu.Lock()
	times := make(map[string]time.Time, len(acknowledged))
	for id, at := range acknowledged {
		times[id] = at
	}
	events.mu.Unlock()
	for id, at := range times {
		mustBeThere := at.Before(firstCut)
		if mustBeThere {
			before++
		}
		switch {
		case restored[id]:
		case mustBeThere:
			missing = append(missing, fmt.Sprintf("%s (acknowledged %s before the first backup)", id, firstCut.Sub(at).Round(time.Millisecond)))
		default:
			lost++
			events.forget(id)
		}
	}
	t.Logf("%d events were published while the backups were taken: %d acknowledged before the first backup, %d acknowledged after it and not in the restored logs",
		len(times), before, lost)
	if len(missing) > 0 {
		t.Fatalf("%d events that were acknowledged before the backups are not in the restored logs:\n  %s", len(missing), sample(missing, 25))
	}
	if before == 0 {
		t.Fatal("no publish was acknowledged before the backups while they were being taken; the test did not test that")
	}

	// What was waiting is delivered at its time, and not before.
	stop = consume()
	waitDelivered("pending", each, pendingDue.Add(90*time.Second))
	for until := time.Now().Add(60 * time.Second); len(events.undelivered()) > 0; time.Sleep(200 * time.Millisecond) {
		if time.Now().After(until) {
			t.Fatalf("%d events of the restored logs were never delivered:\n  %s", len(events.undelivered()), sample(events.undelivered(), 25))
		}
	}
	_, doneAgain := deliveries("done")
	t.Logf("%d of the %d events that were finished before the backup were delivered again after the restore", doneAgain, each)

	// The restored cluster knows what it had accepted: the same ID is refused.
	producer, err = c.dial().NewProducer(client.DefaultProducerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	_, err = producer.Send(ctx, client.Message{MessageID: "pending-000", Topic: topic, Payload: []byte("again"), ScheduleTS: time.Now().Add(time.Second).UnixMilli()})
	cancel()
	if err == nil || !acceptedEarlier(err) {
		t.Errorf("an ID that was accepted before the backup was not refused as a duplicate after the restore: %v", err)
	}

	// And it works: new events are accepted and delivered.
	send(producer, "after", time.Now().Add(time.Second), 20)
	waitDelivered("after", 20, time.Now().Add(60*time.Second))
	stop()

	events.mu.Lock()
	early, unknown := events.early, events.unknown
	events.mu.Unlock()
	if len(early) > 0 {
		t.Errorf("%d deliveries arrived before their scheduled time:\n  %s", len(early), sample(early, 25))
	}
	if len(unknown) > 0 {
		t.Errorf("%d deliveries were of events nobody published:\n  %s", len(unknown), sample(unknown, 25))
	}
	checkLogs(t, c, topic, events, false)
}

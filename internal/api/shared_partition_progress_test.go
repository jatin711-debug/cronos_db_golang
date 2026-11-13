package api

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
)

// Two topics share a partition, their events interleaved. A group of one
// topic never takes the other's entries, so its completion floor, which moves
// over completed offsets only, used to stop at the first of them: every event
// the group completed above it was kept as a record of its own, and sent to
// followers and swept again, until the other topic's entries left the log.
// The redelivery sweep now passes them once everything of the group's own
// before them is complete.
func TestSharedPartition_GroupFloorPassesTheOtherTopic(t *testing.T) {
	ts := newTenants(t)
	producers := map[string]*client.Producer{}
	for _, who := range []string{"alice", "bob"} {
		producer, err := ts.sdk(who).NewProducer(client.DefaultProducerConfig())
		if err != nil {
			t.Fatal(err)
		}
		defer producer.Close()
		producers[who] = producer
	}
	const each = 10
	for i := 0; i < each; i++ {
		for who, topic := range map[string]string{"alice": "alpha", "bob": "beta"} {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, err := producers[who].Send(ctx, client.Message{
				MessageID:  fmt.Sprintf("%s-%d", who, i),
				Topic:      topic,
				Payload:    []byte(who),
				ScheduleTS: time.Now().UnixMilli(),
			})
			cancel()
			if err != nil {
				t.Fatalf("%s publishes to %s: %v", who, topic, err)
			}
		}
	}
	end := ts.p.Wal.GetNextOffset()
	if end != 2*each {
		t.Fatalf("the log ends before offset %d, want %d", end, 2*each)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	alice := ts.consume(ctx, "alice", "alpha", "group-a")
	deadline := time.Now().Add(20 * time.Second)
	for {
		if got, _ := alice.count(); got >= each {
			break
		}
		if time.Now().After(deadline) {
			got, _ := alice.count()
			t.Fatalf("alice received %d of %d events", got, each)
		}
		time.Sleep(20 * time.Millisecond)
	}

	deadline = time.Now().Add(10 * time.Second)
	for {
		floor := ts.p.ConsumerGroup.CompletionFloor("group-a", 0)
		_, groups := ts.p.ConsumerGroup.ExportProgress(0)
		records := -1
		for _, progress := range groups {
			if progress.GetGroupId() == "group-a" {
				records = len(progress.GetCompletedOffsets())
			}
		}
		if floor == end && records == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("group-a's floor is at offset %d with %d completion records above it; want it at the end of the log, %d, with none: it does not pass bob's entries", floor, records, end)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

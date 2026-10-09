package api

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A node that has been replaced as leader of a partition can learn it from
// the partition's other replicas before the cluster's records reach it: they
// answer for a newer epoch, and it stops leading. The cluster's records on
// that node still name it leader then, for as long as it is cut off from the
// node that keeps them.
//
// What it answers a publisher in that state decides whether the publisher
// finds the leader. It used to answer that its replication leader was not
// ready, which a client takes for a final refusal: publishers that had been
// sending to this node kept coming back to it, and published nothing for as
// long as the node stayed cut off.
func TestDeposedLeaderSendsPublishersToTheLeader(t *testing.T) {
	pm := partition.NewPartitionManager("node-a", &types.Config{
		DataDir: t.TempDir(), PartitionCount: 1, FsyncMode: "periodic", FlushIntervalMS: 10,
		TickMS: 10, WheelSize: 64, DedupTTLHours: 1, BloomCapacity: 10000,
		ClusterEnabled: true, ReplicationFactor: 1,
	})
	defer pm.Close()
	if err := pm.CreatePartition(0, "orders"); err != nil {
		t.Fatal(err)
	}
	if err := pm.PromoteToLeader(0, 5); err != nil {
		t.Fatal(err)
	}
	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatal(err)
	}
	view := &leadershipView{}
	view.leads.Store(true)
	view.epoch.Store(5)
	handler := NewEventServiceHandler(pm, p.DedupStore, p.ConsumerGroup)
	handler.SetClusterRouter(view)

	publish := func(id string) (*types.PublishResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return handler.Publish(ctx, &types.PublishRequest{Event: &types.Event{
			MessageId: id, Topic: "orders", Payload: []byte("p"), ScheduleTs: time.Now().Add(time.Hour).UnixMilli(),
		}})
	}
	if resp, err := publish("while-leading"); err != nil || !resp.GetSuccess() {
		t.Fatalf("publish while the node leads: %v, %v", resp, err)
	}

	// The partition takes the epoch of the leader that replaced this node,
	// and the node stops leading. Its records of the cluster have not moved.
	if err := p.PersistEpoch(7); err != nil {
		t.Fatal(err)
	}
	if err := pm.DemoteFromLeader(0); err != nil {
		t.Fatal(err)
	}

	resp, err := publish("after-being-replaced")
	if err == nil {
		t.Fatalf("a replaced leader answered a publish with %v, want an error that sends the publisher on", resp)
	}
	if code := status.Code(err); code != codes.FailedPrecondition && code != codes.Unavailable {
		t.Fatalf("a replaced leader answered with code %s (%v)", code, err)
	}
	// The wording a client recognizes as "ask another node".
	if !strings.Contains(err.Error(), "retry against the partition leader") {
		t.Fatalf("a replaced leader answered %q, which does not send the publisher to the leader", err)
	}
}

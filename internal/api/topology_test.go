package api

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/cluster"
	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
)

// A leader reports, over the replication service, which followers are in
// sync with each partition it leads and how far each has confirmed. Nothing
// else can: the cluster's records say who leads, not who keeps up.
func TestLeaderReport_TellsWhichFollowersKeepUp(t *testing.T) {
	leader := newPublisher(t)
	follower := newReplicaLog(t, "node-b")
	if err := leader.pm.AddFollower(0, "node-b", follower.serveReplication()); err != nil {
		t.Fatal(err)
	}
	if resp := leader.publish(orders("order", 5)); !resp.GetSuccess() {
		t.Fatalf("publish: %+v", resp)
	}
	// The leader's own offset is the end of what it has made durable.
	deadline := time.Now().Add(5 * time.Second)
	for leader.p.Wal.GetHighWatermark() != 4 {
		if time.Now().After(deadline) {
			t.Fatalf("leader log is durable to %d, want 4", leader.p.Wal.GetHighWatermark())
		}
		time.Sleep(10 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	nodeID, reports, err := follower.pm.RemoteLeaderReports(ctx, leader.serveReplication())
	if err != nil {
		t.Fatal(err)
	}
	if nodeID != "node-a" || len(reports) != 1 {
		t.Fatalf("reports from %q: %+v, want one partition from node-a", nodeID, reports)
	}
	report := reports[0]
	if report.PartitionID != 0 || report.Epoch != 1 {
		t.Fatalf("report is for partition %d at epoch %d, want partition 0 at epoch 1", report.PartitionID, report.Epoch)
	}
	if !slices.Equal(report.InSyncReplicas, []string{"node-a", "node-b"}) {
		t.Fatalf("in sync: %v, want [node-a node-b]", report.InSyncReplicas)
	}
	if report.ReplicaOffsets["node-a"] != 4 || report.ReplicaOffsets["node-b"] != 4 {
		t.Fatalf("offsets: %v, want 4 for both replicas", report.ReplicaOffsets)
	}

	if got := follower.pm.LeaderReports(); len(got) != 0 {
		t.Fatalf("a follower reported partitions it does not lead: %+v", got)
	}
}

// A follower of a partition nobody has written to holds all its leader holds.
// It used to count as out of sync until the first publish, so a partition
// that had not been written to yet was reported, and logged, as
// under-replicated.
func TestLeaderReport_FollowerOfAnEmptyPartitionIsInSync(t *testing.T) {
	leader := newPublisher(t)
	follower := newReplicaLog(t, "node-b")
	if err := leader.pm.AddFollower(0, "node-b", follower.serveReplication()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		reports := leader.pm.LeaderReports()
		if len(reports) == 1 && slices.Equal(reports[0].InSyncReplicas, []string{"node-a", "node-b"}) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the follower of an empty partition is not in sync: %+v", reports)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// The topology takes a partition's in-sync replicas from the node that leads
// it. Another node's report on the partition is from when it led it, and a
// partition whose leader did not report is marked so, with nothing in sync.
func TestTopologyPartitions_TakesEachPartitionFromItsLeader(t *testing.T) {
	replicas := []string{"node-a", "node-b", "node-c"}
	infos := map[int32]*cluster.PartitionInfo{
		2: {ID: 2, Replicas: replicas, Epoch: 7},
		1: {ID: 1, LeaderID: "node-c", Replicas: replicas, Epoch: 5, ISR: []string{"node-c"}},
		0: {ID: 0, LeaderID: "node-b", Replicas: replicas, Epoch: 4, ISR: []string{"node-b"}},
	}
	reports := map[string]map[int32]partition.LeaderReport{
		"node-a": {1: {PartitionID: 1, Epoch: 3, InSyncReplicas: []string{"node-a"}, ReplicaOffsets: map[string]int64{"node-a": 3}}},
		"node-b": {0: {PartitionID: 0, Epoch: 4, InSyncReplicas: []string{"node-b", "node-a", "node-c"},
			ReplicaOffsets: map[string]int64{"node-a": 9, "node-b": 10, "node-c": 10}}},
	}

	parts := topologyPartitions(infos, reports)
	if len(parts) != 3 || parts[0].GetPartitionId() != 0 || parts[1].GetPartitionId() != 1 || parts[2].GetPartitionId() != 2 {
		t.Fatalf("partitions are not 0, 1, 2 in order: %v", parts)
	}

	led := parts[0]
	if !led.GetLeaderReported() || led.GetInSyncCount() != 3 || led.GetLeaderHighWatermark() != 10 || led.GetEpoch() != 4 {
		t.Fatalf("partition 0: %v, want node-b's report with 3 in sync at offset 10", led)
	}
	if !slices.Equal(led.GetIsr(), []string{"node-b", "node-a", "node-c"}) || led.GetFollowerOffsets()["node-a"] != 9 {
		t.Fatalf("partition 0: in sync %v, offsets %v", led.GetIsr(), led.GetFollowerOffsets())
	}

	for _, p := range parts[1:] {
		if p.GetLeaderReported() || len(p.GetIsr()) != 0 || p.GetInSyncCount() != 0 || len(p.GetFollowerOffsets()) != 0 || p.GetLeaderHighWatermark() != 0 {
			t.Fatalf("partition %d has no report from its leader %q, and shows %v", p.GetPartitionId(), p.GetLeaderId(), p)
		}
		if !slices.Equal(p.GetReplicas(), replicas) {
			t.Fatalf("partition %d replicas %v, want %v", p.GetPartitionId(), p.GetReplicas(), replicas)
		}
	}
}

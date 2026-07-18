package cluster

import (
	"encoding/json"
	"testing"

	"github.com/hashicorp/raft"
)

// TestFSM_UpdatePartitionAppliesReplicaOffsets is a regression test for the
// unclean-election bug: applyUpdatePartition copied leader/replicas/ISR/state but
// never applied the command's ReplicaOffsets, so fresh replica offsets propagated
// through Raft were silently ignored. Failover election then ran on stale/absent
// offset data and could pick a lagging replica, losing acknowledged writes.
func TestFSM_UpdatePartitionAppliesReplicaOffsets(t *testing.T) {
	fsm := NewClusterFSM()

	apply := func(ct CommandType, info PartitionInfo) {
		payload, _ := json.Marshal(info)
		cmd := Command{Type: ct, Payload: payload}
		data, _ := json.Marshal(cmd)
		if resp := fsm.Apply(&raft.Log{Data: data}); resp != nil {
			if err, ok := resp.(error); ok && err != nil {
				t.Fatalf("Apply(%d) returned error: %v", ct, err)
			}
		}
	}

	// Assign a partition with initial replica offsets.
	apply(CommandTypeAssignPartition, PartitionInfo{
		ID:             0,
		LeaderID:       "node-1",
		Replicas:       []string{"node-1", "node-2"},
		ISR:            []string{"node-1", "node-2"},
		ReplicaOffsets: map[string]int64{"node-1": 100, "node-2": 95},
	})

	// Update the partition WITH advanced replica offsets. These must be applied so
	// election sees up-to-date lag data.
	apply(CommandTypeUpdatePartition, PartitionInfo{
		ID:             0,
		LeaderID:       "node-1",
		Replicas:       []string{"node-1", "node-2"},
		ISR:            []string{"node-1", "node-2"},
		ReplicaOffsets: map[string]int64{"node-1": 200, "node-2": 190},
	})

	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	got := fsm.state.Partitions[0]
	if got == nil {
		t.Fatal("partition 0 missing after update")
	}
	if got.ReplicaOffsets == nil {
		t.Fatal("ReplicaOffsets dropped by applyUpdatePartition")
	}
	if got.ReplicaOffsets["node-1"] != 200 || got.ReplicaOffsets["node-2"] != 190 {
		t.Fatalf("updated replica offsets not applied: %v", got.ReplicaOffsets)
	}
}

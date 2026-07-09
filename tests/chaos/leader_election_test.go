//go:build chaos

package chaos

import (
	"math/rand/v2"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/internal/cluster"
)

// TestLagAwareLeaderElection verifies that failover always promotes the alive
// replica with the highest known offset. It is run under the chaos tag because
// it injects random node failures and partition topologies.
func TestLagAwareLeaderElection(t *testing.T) {
	nodeIDs := []string{"node-a", "node-b", "node-c", "node-d", "node-e"}

	for i := 0; i < 1000; i++ {
		info := randomPartitionInfo(nodeIDs)
		alive := randomAliveSet(nodeIDs)

		leader, offset := cluster.ChooseFailoverLeader(info, alive)
		candidates := candidateSet(info, alive)

		// Invariants
		if leader == info.LeaderID {
			t.Fatalf("iteration %d: failed leader %q was re-elected", i, leader)
		}
		if leader != "" && !alive[leader] {
			t.Fatalf("iteration %d: elected leader %q is not alive", i, leader)
		}
		if leader != "" {
			if offset < 0 {
				t.Fatalf("iteration %d: negative offset %d", i, offset)
			}
			// Verify no candidate has a higher offset.
			for id := range candidates {
				if info.ReplicaOffsets[id] > offset {
					t.Fatalf("iteration %d: candidate %q has higher offset %d than leader %q (%d)",
						i, id, info.ReplicaOffsets[id], leader, offset)
				}
			}
		} else if len(candidates) > 0 {
			t.Fatalf("iteration %d: no leader elected but candidates exist: %v", i, candidates)
		}
	}
}

// TestISRFallback verifies that when all ISR members are dead, the failover
// falls back to out-of-sync replicas, but still prefers the highest offset.
func TestISRFallback(t *testing.T) {
	info := &cluster.PartitionInfo{
		ID:       1,
		LeaderID: "node-a",
		Replicas: []string{"node-a", "node-b", "node-c"},
		ISR:      []string{"node-a", "node-b"},
		ReplicaOffsets: map[string]int64{
			"node-a": 1000,
			"node-b": 900,
			"node-c": 950,
		},
	}
	alive := map[string]bool{"node-c": true}

	leader, offset := cluster.ChooseFailoverLeader(info, alive)
	if leader != "node-c" {
		t.Fatalf("expected node-c as fallback leader, got %q", leader)
	}
	if offset != 950 {
		t.Fatalf("expected offset 950, got %d", offset)
	}
}

// TestNoAvailableReplica verifies that when every other node is dead, the
// partition is left without a leader rather than promoting the failed leader.
func TestNoAvailableReplica(t *testing.T) {
	info := &cluster.PartitionInfo{
		ID:       1,
		LeaderID: "node-a",
		Replicas: []string{"node-a"},
		ISR:      []string{"node-a"},
		ReplicaOffsets: map[string]int64{
			"node-a": 100,
		},
	}
	alive := map[string]bool{"node-a": true}

	leader, offset := cluster.ChooseFailoverLeader(info, alive)
	if leader != "" {
		t.Fatalf("expected no leader when only the failed leader is alive, got %q", leader)
	}
	if offset != 0 {
		t.Fatalf("expected offset 0, got %d", offset)
	}
}

func randomPartitionInfo(nodeIDs []string) *cluster.PartitionInfo {
	r := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))

	leader := nodeIDs[r.IntN(len(nodeIDs))]

	// Random replica set containing the leader and 1-4 additional nodes.
	replicas := map[string]struct{}{leader: {}}
	for len(replicas) < 1+r.IntN(len(nodeIDs)) {
		replicas[nodeIDs[r.IntN(len(nodeIDs))]] = struct{}{}
	}
	replicaList := make([]string, 0, len(replicas))
	for id := range replicas {
		replicaList = append(replicaList, id)
	}

	// ISR is a subset of replicas containing the leader.
	isr := map[string]struct{}{leader: {}}
	for _, id := range replicaList {
		if id == leader {
			continue
		}
		if r.Float64() < 0.6 {
			isr[id] = struct{}{}
		}
	}
	isrList := make([]string, 0, len(isr))
	for id := range isr {
		isrList = append(isrList, id)
	}

	offsets := make(map[string]int64, len(replicaList))
	for _, id := range replicaList {
		offsets[id] = int64(r.IntN(1_000_000))
	}

	return &cluster.PartitionInfo{
		ID:             int32(r.IntN(1000)),
		LeaderID:       leader,
		Replicas:       replicaList,
		ISR:            isrList,
		ReplicaOffsets: offsets,
	}
}

func randomAliveSet(nodeIDs []string) map[string]bool {
	r := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	alive := make(map[string]bool, len(nodeIDs))
	for _, id := range nodeIDs {
		alive[id] = r.Float64() < 0.7
	}
	return alive
}

// candidateSet returns the same candidate set used by ChooseFailoverLeader so
// that the chaos test can verify the invariant without re-implementing the
// offset comparison.
func candidateSet(info *cluster.PartitionInfo, alive map[string]bool) map[string]struct{} {
	candidates := make(map[string]struct{})
	for _, id := range info.ISR {
		if id != info.LeaderID && alive[id] {
			candidates[id] = struct{}{}
		}
	}
	if len(candidates) == 0 {
		for _, id := range info.Replicas {
			if id != info.LeaderID && alive[id] {
				candidates[id] = struct{}{}
			}
		}
	}
	return candidates
}

// TestLeaderElectionDeterminism verifies that ties are broken deterministically
// so repeated elections with the same state converge on the same leader. This
// prevents flapping when multiple replicas have identical offsets.
func TestLeaderElectionDeterminism(t *testing.T) {
	info := &cluster.PartitionInfo{
		ID:       7,
		LeaderID: "node-a",
		Replicas: []string{"node-a", "node-b", "node-c", "node-d"},
		ISR:      []string{"node-a", "node-b", "node-c", "node-d"},
		ReplicaOffsets: map[string]int64{
			"node-b": 500,
			"node-c": 500,
			"node-d": 500,
		},
	}
	alive := map[string]bool{"node-b": true, "node-c": true, "node-d": true}

	first, _ := cluster.ChooseFailoverLeader(info, alive)
	for i := 0; i < 50; i++ {
		leader, _ := cluster.ChooseFailoverLeader(info, alive)
		if leader != first {
			t.Fatalf("leader flapped between %q and %q on iteration %d", first, leader, i)
		}
	}
	// Deterministic tie-break by sorted node ID: node-b wins.
	if first != "node-b" {
		t.Fatalf("expected node-b to win tie-break, got %q", first)
	}
}

// TestPreferISRReplica verifies that an ISR member with a lower offset is
// preferred over a non-ISR replica with a higher offset.
func TestPreferISRReplica(t *testing.T) {
	info := &cluster.PartitionInfo{
		ID:       1,
		LeaderID: "leader",
		Replicas: []string{"leader", "isr-low", "out-of-sync-high"},
		ISR:      []string{"leader", "isr-low"},
		ReplicaOffsets: map[string]int64{
			"isr-low":           100,
			"out-of-sync-high":  900,
		},
	}
	alive := map[string]bool{"isr-low": true, "out-of-sync-high": true}

	leader, offset := cluster.ChooseFailoverLeader(info, alive)
	if leader != "isr-low" {
		t.Fatalf("expected ISR member isr-low to be chosen, got %q", leader)
	}
	if offset != 100 {
		t.Fatalf("expected offset 100, got %d", offset)
	}
}

// TestLeaderElectionReport prints a summary of the election outcomes for a
// fixed random seed. This is useful for debugging when a chaos run fails.
func TestLeaderElectionReport(t *testing.T) {
	info := &cluster.PartitionInfo{
		ID:       42,
		LeaderID: "node-a",
		Replicas: []string{"node-a", "node-b", "node-c", "node-d", "node-e"},
		ISR:      []string{"node-a", "node-b", "node-c"},
		ReplicaOffsets: map[string]int64{
			"node-a": 1000,
			"node-b": 980,
			"node-c": 750,
			"node-d": 200,
			"node-e": 990,
		},
	}
	alive := map[string]bool{
		"node-b": true,
		"node-c": true,
		"node-d": true,
		"node-e": true,
	}

	leader, offset := cluster.ChooseFailoverLeader(info, alive)
	t.Logf("elected leader=%q offset=%d", leader, offset)
	if leader != "node-b" {
		t.Fatalf("expected node-b (highest ISR offset 980), got %q", leader)
	}
}

package cluster

import (
	"testing"
)

// TestRouter_ReconcileRingSelfHeals verifies that reconcileRing brings the hash
// ring back into agreement with the alive-node set after a membership event was
// (simulated as) dropped — i.e. a node became alive but no Join event reached the
// router. Without reconcile, a dropped event left the ring permanently divergent.
func TestRouter_ReconcileRingSelfHeals(t *testing.T) {
	mem := &mockMembershipService{
		local: &Node{ID: "node-1", Address: "127.0.0.1:9001"},
		nodes: map[string]*Node{
			"node-1": {ID: "node-1", Address: "127.0.0.1:9001", State: NodeStateAlive},
		},
	}
	router := NewRouter(mem, 4, 1, 150, nil)

	// Initially only node-1 is in the ring.
	if got := len(router.hashRing.Nodes()); got != 1 {
		t.Fatalf("expected 1 ring node initially, got %d", got)
	}

	// A new node joins the cluster, but its Join event is "dropped" (never
	// delivered to the router) — simulate by adding it only to membership.
	mem.nodes["node-2"] = &Node{ID: "node-2", Address: "127.0.0.1:9002", State: NodeStateAlive}

	// Event-driven path never ran, so the ring is still stale.
	if got := len(router.hashRing.Nodes()); got != 1 {
		t.Fatalf("ring should still be stale before reconcile, got %d nodes", got)
	}

	// Reconcile self-heals the ring from the authoritative alive set.
	router.reconcileRing()

	nodes := router.hashRing.Nodes()
	if len(nodes) != 2 {
		t.Fatalf("expected 2 ring nodes after reconcile, got %d (%v)", len(nodes), nodes)
	}
	seen := map[string]bool{}
	for _, id := range nodes {
		seen[id] = true
	}
	if !seen["node-1"] || !seen["node-2"] {
		t.Fatalf("ring missing expected nodes after reconcile: %v", nodes)
	}

	// A node dies (removed from membership) with a dropped Leave event; reconcile
	// must remove it from the ring too.
	delete(mem.nodes, "node-2")
	router.reconcileRing()
	if got := len(router.hashRing.Nodes()); got != 1 {
		t.Fatalf("expected 1 ring node after dead-node reconcile, got %d", got)
	}
}

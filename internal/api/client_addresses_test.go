package api

import (
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/internal/cluster"
)

// Partition metadata gives clients the address a node advertised for them,
// never its cluster address: that one serves replication and Raft, and a
// client sent there was told the event service does not exist. A node that
// advertised none is left out, so clients reach it as they did before.
func TestClientAddresses_GivesOnlyAddressesForClients(t *testing.T) {
	got := clientAddresses([]*cluster.Node{
		{ID: "node1", Address: "cronos-node1:7947", ClientAddr: "node1.example:9000"},
		{ID: "node2", Address: "cronos-node2:7947"},
		{ID: "", Address: "x:7947", ClientAddr: "x.example:9000"},
	})
	if len(got) != 1 || got["node1"] != "node1.example:9000" {
		t.Fatalf("client addresses = %v, want only node1 at node1.example:9000", got)
	}
}

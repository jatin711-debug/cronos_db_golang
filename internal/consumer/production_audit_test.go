package consumer

import (
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"testing"
)

func TestAuditNegativeUntrackedAckMustNotCommit(t *testing.T) {
	g := NewGroupManager()
	defer g.Close()
	if err := g.CreateGroup("victim", "secret", []int32{0}); err != nil {
		t.Fatal(err)
	}
	if err := g.Ack(&types.AckRequest{DeliveryId: "victim:0:never-delivered", Success: false, NextOffset: 1000000}); err != nil {
		return
	}
	n, err := g.GetCommittedOffset("victim", 0)
	if err != nil {
		t.Fatal(err)
	}
	if n == 1000000 {
		t.Fatal("unsuccessful, fabricated delivery ACK advanced victim offset to 1000000")
	}
}

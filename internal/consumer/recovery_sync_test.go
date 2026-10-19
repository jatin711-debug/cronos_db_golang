package consumer

import (
	"encoding/json"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestRecoverySyncIncludesQueuedGroupMetadata(t *testing.T) {
	store, err := NewOffsetStore(t.TempDir(), 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	g := NewGroupManagerWithStore(store)
	defer g.Close()
	// Isolate explicit recovery durability from the periodic flush tick.
	close(store.quit)
	store.wg.Wait()
	store.quit = make(chan struct{})
	if err := g.ApplyReplicatedProgress(0, 5, []*types.ConsumerGroupProgress{{GroupId: "recovered", Topic: "orders", CommittedOffset: 2, CompletedOffsets: []int64{4}}}); err != nil {
		t.Fatal(err)
	}
	if err := g.SyncProgress(); err != nil {
		t.Fatal(err)
	}
	// Reopening discovers groups from their persisted metadata. Completion
	// keys alone are insufficient if the process dies before the flush tick.
	data, closer, err := store.db.Get(store.groupMetaKey("recovered"))
	if err != nil {
		t.Fatal("recovery generation was declared durable before its group metadata:", err)
	}
	defer closer.Close()
	var saved types.ConsumerGroup
	if err := json.Unmarshal(data, &saved); err != nil || saved.CommittedOffsets[0] != 2 || saved.Topic != "orders" {
		t.Fatalf("persisted group: %v %v", saved, err)
	}
}

func TestRecoverySnapshotDoesNotInheritThePreviousLogFloor(t *testing.T) {
	g := NewGroupManager()
	if err := g.CreateGroup("workers", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	if err := g.SetLogStart(0, 8); err != nil {
		t.Fatal(err)
	}
	if err := g.ResetSnapshotProgress(0); err != nil {
		t.Fatal(err)
	}
	if err := g.ApplyReplicatedProgress(0, 6, []*types.ConsumerGroupProgress{{GroupId: "workers", Topic: "orders", CompletedOffsets: []int64{2}}}); err != nil {
		t.Fatal(err)
	}
	if g.IsCompleted("workers", 0, 5) || !g.IsCompleted("workers", 0, 2) {
		t.Fatal("completion floor crossed snapshot generations")
	}
}

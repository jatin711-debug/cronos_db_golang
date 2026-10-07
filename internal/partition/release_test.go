package partition

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func loaded(pm *PartitionManager, id int32) bool {
	_, err := pm.GetInternalPartition(id)
	return !errors.Is(err, types.ErrPartitionNotFound)
}

// A node that no longer holds a partition unloads it. Data is deleted only
// when there is none to lose; a partition with events keeps its files and
// comes back intact when it is assigned to the node again.
func TestReleasePartition(t *testing.T) {
	cfg := unacceptedTestConfig(t)
	cfg.PartitionCount = 4
	pm := NewPartitionManager("node-1", cfg)
	defer pm.Close()
	for id := int32(0); id < 4; id++ {
		if err := pm.CreatePartition(id, "orders"); err != nil {
			t.Fatal(err)
		}
		if err := pm.StartPartition(id); err != nil {
			t.Fatal(err)
		}
	}
	dirOf := func(id int32) string {
		p, err := pm.GetInternalPartition(id)
		if err != nil {
			t.Fatal(err)
		}
		return p.DataDir
	}

	t.Run("an empty partition is unloaded and its directory removed", func(t *testing.T) {
		dir := dirOf(0)
		if err := pm.ReleasePartition(0); err != nil {
			t.Fatal(err)
		}
		if loaded(pm, 0) {
			t.Fatal("partition is still loaded")
		}
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatalf("directory of an empty released partition still exists (err=%v)", err)
		}
		// Releasing what is not loaded is not an error.
		if err := pm.ReleasePartition(0); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("a partition with events keeps its files and reopens intact", func(t *testing.T) {
		p, _ := pm.GetInternalPartition(1)
		dir := p.DataDir
		events := make([]*types.Event, 5)
		for i := range events {
			events[i] = &types.Event{MessageId: fmt.Sprintf("m%d", i), Topic: "orders", Payload: []byte("p"), ScheduleTs: 1}
		}
		if err := p.Wal.AppendBatch(events); err != nil {
			t.Fatal(err)
		}
		if err := pm.ReleasePartition(1); err != nil {
			t.Fatal(err)
		}
		if loaded(pm, 1) {
			t.Fatal("partition is still loaded")
		}
		if _, err := os.Stat(dir); err != nil {
			t.Fatalf("directory of a released partition with events: %v", err)
		}

		if err := pm.CreatePartition(1, "orders"); err != nil {
			t.Fatalf("reopen the released partition: %v", err)
		}
		p, _ = pm.GetInternalPartition(1)
		logged, err := p.Wal.ReadEvents(0, 4)
		if err != nil || len(logged) != 5 || logged[4].MessageId != "m4" {
			t.Fatalf("reopened log: %d events, %v", len(logged), err)
		}
	})

	t.Run("a pinned partition stays loaded", func(t *testing.T) {
		pm.PinPartition(2)
		if err := pm.ReleasePartition(2); err != nil {
			t.Fatal(err)
		}
		if !loaded(pm, 2) {
			t.Fatal("a pinned partition was released")
		}
	})

	t.Run("a partition in use is refused", func(t *testing.T) {
		if err := pm.PromoteToLeader(3, 1); err != nil {
			t.Fatal(err)
		}
		if err := pm.ReleasePartition(3); err == nil || !loaded(pm, 3) {
			t.Fatalf("a partition this node leads was released (err=%v)", err)
		}
		if err := pm.DemoteFromLeader(3); err != nil {
			t.Fatal(err)
		}
		p, _ := pm.GetInternalPartition(3)
		p.BeginPublish()
		if err := pm.ReleasePartition(3); err == nil || !loaded(pm, 3) {
			t.Fatalf("a partition with a publish in flight was released (err=%v)", err)
		}
		p.EndPublish()
		if err := pm.ReleasePartition(3); err != nil || loaded(pm, 3) {
			t.Fatalf("an idle, demoted partition was not released (err=%v)", err)
		}
	})
}

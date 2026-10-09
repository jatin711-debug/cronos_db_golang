package partition

import (
	"fmt"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// A restart walks the log to rebuild the timers of the events not yet due. An
// event that is due already is left in the log: its subscription reads it when
// it starts, at the pace its consumers take it. Queuing it at every start as
// well put the whole undelivered backlog in memory, where nothing bounded it.
func TestRestartTimesFutureEventsAndLeavesDueOnesInTheLog(t *testing.T) {
	cfg := &types.Config{DataDir: t.TempDir(), PartitionCount: 1, FsyncMode: "periodic", FlushIntervalMS: 10, TickMS: 10, WheelSize: 64, DedupTTLHours: 1, BloomCapacity: 1000}

	pm := NewPartitionManager("node", cfg)
	if err := pm.CreatePartition(0, "topic"); err != nil {
		t.Fatal(err)
	}
	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatal(err)
	}
	const due, future = 40, 3
	now := time.Now()
	for i := 0; i < due+future; i++ {
		scheduleTs := now.Add(-time.Hour).UnixMilli()
		if i >= due {
			scheduleTs = now.Add(time.Hour).UnixMilli()
		}
		event := &types.Event{MessageId: fmt.Sprintf("m%d", i), Topic: "topic", ScheduleTs: scheduleTs, CreatedTs: now.UnixMilli(), Payload: []byte("p")}
		if err := p.Wal.AppendEvent(event); err != nil {
			t.Fatal(err)
		}
	}
	if err := pm.Close(); err != nil {
		t.Fatal(err)
	}

	pm = NewPartitionManager("node", cfg)
	defer pm.Close()
	if err := pm.CreatePartition(0, "topic"); err != nil {
		t.Fatal(err)
	}
	if err := pm.StartPartition(0); err != nil {
		t.Fatal(err)
	}
	p, err = pm.GetInternalPartition(0)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Scheduler.GetReadyQueueDepth(); got != 0 {
		t.Errorf("%d due events queued at restart, want none: a subscription reads them from the log", got)
	}
	// The future events are held by the timing wheel or, beyond its span, by
	// the cold store; between them they must hold every one of them.
	if got := p.Scheduler.GetTimingWheelDepth() + p.Scheduler.GetColdStoreCount(); got != future {
		t.Errorf("%d timers after restart, want the %d future events", got, future)
	}
}

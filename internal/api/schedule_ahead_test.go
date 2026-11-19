package api

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// With --max-schedule-ahead, an event due further ahead is refused before
// anything is claimed or written, by Publish and by PublishBatch alike, and
// one due within the limit is taken.
func TestPublish_ScheduleBeyondTheLimitIsRefused(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			pm := auditManager(t)
			p, _ := pm.GetInternalPartition(0)
			h := NewEventServiceHandler(pm, p.DedupStore, p.ConsumerGroup)
			h.SetMaxScheduleAhead(time.Hour)

			far := orders("far", 1)[0]
			far.ScheduleTs = time.Now().Add(2 * time.Hour).UnixMilli()
			if ok, desc := publishOne(h, far, batch); ok || !strings.Contains(desc, "--max-schedule-ahead") {
				t.Fatalf("event due in 2h with a 1h limit: %s", desc)
			}
			if n := p.Wal.GetNextOffset(); n != 0 {
				t.Fatalf("refused event moved the next offset to %d", n)
			}
			if !idIsFree(t, p, far.MessageId) {
				t.Fatal("refused event left its message ID claimed")
			}

			near := orders("near", 1)[0]
			near.ScheduleTs = time.Now().Add(50 * time.Minute).UnixMilli()
			if ok, desc := publishOne(h, near, batch); !ok {
				t.Fatalf("event due within the limit: %s", desc)
			}
		})
	}
}

// Without a limit, any future time is taken, as before.
func TestPublish_NoScheduleLimitByDefault(t *testing.T) {
	pm := auditManager(t)
	p, _ := pm.GetInternalPartition(0)
	h := NewEventServiceHandler(pm, p.DedupStore, p.ConsumerGroup)
	far := orders("far", 1)[0]
	far.ScheduleTs = time.Now().Add(365 * 24 * time.Hour).UnixMilli()
	if ok, desc := publishOne(h, far, false); !ok {
		t.Fatalf("event due in a year without a limit: %s", desc)
	}
}

// A reschedule is held to the same limit, before its partition is looked up.
func TestReschedule_BeyondTheLimitIsRefused(t *testing.T) {
	h := NewEventServiceHandler(nil, nil, nil)
	h.SetMaxScheduleAhead(time.Hour)
	_, err := h.RescheduleEvent(context.Background(), &types.EventMutationRequest{
		Reference:  &types.EventReference{Topic: "orders", MessageId: "m"},
		RequestId:  "r",
		ScheduleTs: time.Now().Add(3 * time.Hour).UnixMilli(),
	})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "--max-schedule-ahead") {
		t.Fatalf("reschedule to 3h ahead with a 1h limit: %v", err)
	}
}

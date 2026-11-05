package api

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
	"github.com/jatin711-debug/cronos_db_golang/internal/tenant"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestConfiguredTenantRateLimitsApplyToBatchPublish(t *testing.T) {
	pm := auditManager(t)
	p, _ := pm.GetInternalPartition(0)
	h := NewEventServiceHandler(pm, p.DedupStore, p.ConsumerGroup)
	path := filepath.Join(t.TempDir(), "limits.json")
	if err := os.WriteFile(path, []byte(`{"alice":{"max_events_per_second":1},"bob":{"max_events_per_second":2}}`), 0600); err != nil {
		t.Fatal(err)
	}
	a := tenant.NewAccountant()
	if err := a.LoadRateLimits(path); err != nil {
		t.Fatal(err)
	}
	h.SetTenantAccountant(a)
	// A two-event batch can never fit Alice's burst, but Bob can publish the
	// same IDs after her refusal: quota refusal must release dedup claims.
	request := &types.PublishBatchRequest{Events: []*types.Event{
		{MessageId: "rate-1", Topic: "audit", Payload: []byte("a"), ScheduleTs: time.Now().Add(time.Hour).UnixMilli(), Meta: map[string]string{"tenant_id": "bob"}},
		{MessageId: "rate-2", Topic: "audit", Payload: []byte("b"), ScheduleTs: time.Now().Add(time.Hour).UnixMilli()},
	}}
	for _, subject := range []string{"alice", "bob"} {
		ctx := auth.WithClaims(context.Background(), &auth.Claims{RegisteredClaims: jwt.RegisteredClaims{Subject: subject}})
		r, err := h.PublishBatch(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if r.GetSuccess() != (subject == "bob") {
			t.Fatalf("%s: %v", subject, r)
		}
	}
	if request.Events[0].Meta["tenant_id"] != "bob" {
		t.Fatal("authenticated subject was not recorded")
	}
}

package api

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
	"github.com/jatin711-debug/cronos_db_golang/internal/lifecycle"
	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/internal/tenant"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestEventLifecycleRPCAndAuthorization(t *testing.T) {
	cfg := &types.Config{DataDir: t.TempDir(), PartitionCount: 1, ReplicationFactor: 1, EventLifecycle: true, FsyncMode: "periodic", FlushIntervalMS: 10, TickMS: 10, WheelSize: 64, DedupTTLHours: 24, BloomCapacity: 1000}
	pm := partition.NewPartitionManager("lifecycle", cfg)
	defer pm.Close()
	if err := pm.CreatePartition(0, "a"); err != nil {
		t.Fatal(err)
	}
	p, _ := pm.GetInternalPartition(0)
	h := NewEventServiceHandler(pm, p.DedupStore, p.ConsumerGroup)
	scfg := DefaultConfig()
	scfg.Address = "127.0.0.1:0"
	scfg.SLORecorder = nil
	srv, err := NewGRPCServer(scfg)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()
	srv.RegisterServices(h, nil, NewPartitionServiceHandler(pm, nil, "lifecycle"), nil)
	if err = srv.Start(); err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(srv.listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := types.NewEventServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pub, err := client.Publish(ctx, &types.PublishRequest{Event: &types.Event{MessageId: "m", Topic: "a", ScheduleTs: time.Now().Add(time.Hour).UnixMilli(), Payload: []byte("payload")}})
	if err != nil || !pub.Success {
		t.Fatalf("publish: %v %v", pub, err)
	}
	if pub.Term <= 0 {
		t.Fatal("standalone lifecycle publish did not receive a generation term")
	}
	ref := &types.EventReference{PartitionId: pub.PartitionId, Offset: pub.Offset, Term: pub.Term, MessageId: "m", Topic: "a"}
	state, err := client.GetEventState(ctx, &types.EventStateRequest{Reference: ref})
	if err != nil || state.RevisionTerm != pub.Term || state.RevisionOffset != pub.Offset {
		t.Fatalf("state RPC: %v %v", state, err)
	}
	req := &types.EventMutationRequest{Reference: ref, RequestId: "move", ExpectedRevisionOffset: state.RevisionOffset, ExpectedRevisionTerm: state.RevisionTerm, ScheduleTs: time.Now().Add(2 * time.Hour).UnixMilli()}
	state, err = client.RescheduleEvent(ctx, req)
	if err != nil || state.ScheduleTs != req.ScheduleTs {
		t.Fatalf("reschedule RPC: %v %v", state, err)
	}
	state, err = client.CancelEvent(ctx, &types.EventMutationRequest{Reference: ref, RequestId: "cancel", ExpectedRevisionOffset: state.RevisionOffset, ExpectedRevisionTerm: state.RevisionTerm})
	if err != nil || !state.Canceled {
		t.Fatalf("cancel RPC: %v %v", state, err)
	}
	batch, err := client.PublishBatch(ctx, &types.PublishBatchRequest{ReturnReferences: true, Events: []*types.Event{{MessageId: "batch", Topic: "a", ScheduleTs: time.Now().Add(time.Hour).UnixMilli(), Payload: []byte("batch payload")}}})
	if err != nil || !batch.Success || len(batch.References) != 1 {
		t.Fatalf("batch receipts: %v %v", batch, err)
	}
	if _, err = client.GetEventState(ctx, &types.EventStateRequest{Reference: batch.References[0]}); err != nil {
		t.Fatalf("batch receipt does not resolve: %v", err)
	}
	refBatch := batch.References[0]
	if _, err = client.RescheduleEvent(ctx, &types.EventMutationRequest{Reference: refBatch, RequestId: "batch-move", ExpectedRevisionOffset: refBatch.Offset, ExpectedRevisionTerm: refBatch.Term, ScheduleTs: time.Now().Add(3 * time.Hour).UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	replay, err := client.Replay(ctx, &types.ReplayRequest{PartitionId: 0, Topic: "a", StartOffset: 0, Count: 100})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := replay.Recv()
	if err != nil || replayed.GetEvent().GetMessageId() != "batch" || lifecycle.HasReservedMeta(replayed.GetEvent()) {
		t.Fatalf("replay leaked obsolete/control records: %v %v", replayed, err)
	}
	if extra, err := replay.Recv(); err != io.EOF {
		t.Fatalf("replay returned an obsolete copy: %v %v", extra, err)
	}
	// The topic claimed in a handle is authorized before lookup, and must also
	// match the actual WAL event. Sharing a partition does not grant access.
	h.SetAuthPolicy(&auth.Policy{Subjects: map[string]*auth.Subject{"bob": {Topics: map[string]auth.TopicPerms{"b": {Publish: true}}}}})
	claims := &auth.Claims{}
	claims.Subject = "bob"
	bob := auth.WithClaims(context.Background(), claims)
	if _, err = h.GetEventState(bob, &types.EventStateRequest{Reference: ref}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("unauthorized topic: %v", err)
	}
	forged := proto.Clone(ref).(*types.EventReference)
	forged.Topic = "b"
	if _, err = h.GetEventState(bob, &types.EventStateRequest{Reference: forged}); status.Code(err) != codes.NotFound {
		t.Fatalf("forged topic matched WAL: %v", err)
	}
}

func TestLifecycleCancellationReleasesTenantCreditOnlyOnce(t *testing.T) {
	cfg := &types.Config{DataDir: t.TempDir(), PartitionCount: 1, ReplicationFactor: 1, EventLifecycle: true, FsyncMode: "batch", TickMS: 10, WheelSize: 64, DedupTTLHours: 24, BloomCapacity: 1000}
	pm := partition.NewPartitionManager("quota", cfg)
	defer pm.Close()
	if err := pm.CreatePartition(0, "a"); err != nil {
		t.Fatal(err)
	}
	p, _ := pm.GetInternalPartition(0)
	a := tenant.NewAccountant()
	a.SetLimits("default", tenant.Limits{MaxInFlight: 1})
	pm.SetTenantAccountant(a)
	h := NewEventServiceHandler(pm, p.DedupStore, p.ConsumerGroup)
	h.SetTenantAccountant(a)
	publish := func(id string) *types.PublishResponse {
		t.Helper()
		r, err := h.Publish(context.Background(), &types.PublishRequest{Event: &types.Event{MessageId: id, Topic: "a", Payload: []byte("p"), ScheduleTs: time.Now().Add(time.Hour).UnixMilli()}})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	first := publish("first")
	if !first.Success {
		t.Fatal(first)
	}
	req := &types.EventMutationRequest{Reference: &types.EventReference{PartitionId: 0, Offset: first.Offset, Term: first.Term, MessageId: "first", Topic: "a"}, RequestId: "cancel", ExpectedRevisionOffset: first.Offset, ExpectedRevisionTerm: first.Term}
	if _, err := h.CancelEvent(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if n, _ := a.GetUsage("default"); n != 0 {
		t.Fatalf("cancel kept tenant credit: %d", n)
	}
	second := publish("second")
	if !second.Success {
		t.Fatal(second)
	}
	if _, err := h.CancelEvent(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if n, _ := a.GetUsage("default"); n != 1 {
		t.Fatalf("cancel retry released another event's credit: %d", n)
	}
	_, err := h.Publish(context.Background(), &types.PublishRequest{Event: &types.Event{MessageId: "third", Topic: "a", Payload: []byte("p"), ScheduleTs: time.Now().Add(time.Hour).UnixMilli()}})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("quota was bypassed after retry: %v", err)
	}
}

func TestLifecycleMetadataCannotBePublishedOrReplicated(t *testing.T) {
	pm := auditManager(t)
	p, _ := pm.GetInternalPartition(0)
	h := NewEventServiceHandler(pm, p.DedupStore, p.ConsumerGroup)
	for _, batch := range []bool{false, true} {
		e := orders("forged", 1)[0]
		e.Meta = map[string]string{lifecycle.Key: `{"version":1,"kind":"cancel"}`}
		if ok, description := publishOne(h, e, batch); ok {
			t.Fatalf("internal record published: %s", description)
		}
	}
	rh := NewReplicationServiceHandler(pm)
	if _, err := rh.Append(context.Background(), &types.ReplicationAppendRequest{PartitionId: 0, Events: []*types.Event{{Meta: map[string]string{lifecycle.Key: "{}"}}}}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("replicated a lifecycle record on ordinary append: %v", err)
	}
	if p.Wal.GetNextOffset() != 0 {
		t.Fatal("forgery changed WAL")
	}
	if _, err := h.GetEventState(context.Background(), &types.EventStateRequest{Reference: &types.EventReference{PartitionId: 0, Offset: 0, MessageId: "m", Topic: "a"}}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("disabled lifecycle: %v", err)
	}
}

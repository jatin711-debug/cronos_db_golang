package api

import (
	"context"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
	"github.com/jatin711-debug/cronos_db_golang/internal/consumer"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestConsumerGroupServiceHandler(t *testing.T) {
	cm := consumer.NewGroupManager()
	h := NewConsumerGroupServiceHandler(cm)

	ctx := context.Background()

	// 1. CreateConsumerGroup
	createReq := &types.CreateConsumerGroupRequest{
		GroupId:    "test-group",
		Topic:      "test-topic",
		Partitions: []int32{0, 1, 2},
	}
	createResp, err := h.CreateConsumerGroup(ctx, createReq)
	if err != nil {
		t.Fatalf("CreateConsumerGroup failed: %v", err)
	}
	if !createResp.Success {
		t.Errorf("CreateConsumerGroup returned success false: %s", createResp.Error)
	}

	// 2. GetConsumerGroup
	getReq := &types.GetConsumerGroupRequest{
		GroupId: "test-group",
	}
	getResp, err := h.GetConsumerGroup(ctx, getReq)
	if err != nil {
		t.Fatalf("GetConsumerGroup failed: %v", err)
	}
	if getResp.GroupId != "test-group" {
		t.Errorf("Expected group ID test-group, got %s", getResp.GroupId)
	}
	if getResp.Topic != "test-topic" {
		t.Errorf("Expected topic test-topic, got %s", getResp.Topic)
	}

	// 3. ListConsumerGroups
	listResp, err := h.ListConsumerGroups(ctx, &types.ListConsumerGroupsRequest{})
	if err != nil {
		t.Fatalf("ListConsumerGroups failed: %v", err)
	}
	if len(listResp.Groups) != 1 {
		t.Errorf("Expected 1 group, got %d", len(listResp.Groups))
	}

	// 4. RebalanceConsumerGroup
	rebalanceReq := &types.RebalanceConsumerGroupRequest{
		GroupId: "test-group",
	}
	rebalanceResp, err := h.RebalanceConsumerGroup(ctx, rebalanceReq)
	if err != nil {
		t.Fatalf("RebalanceConsumerGroup failed: %v", err)
	}
	if !rebalanceResp.Success {
		t.Errorf("RebalanceConsumerGroup returned success false: %s", rebalanceResp.Error)
	}
}

// With auth enabled, the consumer group RPCs are an operator API: they need
// the admin role.
func TestConsumerGroupServiceHandler_RequiresAdmin(t *testing.T) {
	h := NewConsumerGroupServiceHandler(consumer.NewGroupManager())
	h.SetAuthPolicy(&auth.Policy{Subjects: map[string]*auth.Subject{
		"ops":  {Admin: true},
		"user": {Topics: map[string]auth.TopicPerms{"t": {Publish: true, Subscribe: true}}},
	}})
	user := auth.WithClaims(context.Background(), auth.ClaimsWithSubject("user"))
	ops := auth.WithClaims(context.Background(), auth.ClaimsWithSubject("ops"))
	create := &types.CreateConsumerGroupRequest{GroupId: "g", Topic: "t", Partitions: []int32{0}}

	if _, err := h.CreateConsumerGroup(user, create); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("create without admin: %v", err)
	}
	if _, err := h.RebalanceConsumerGroup(user, &types.RebalanceConsumerGroupRequest{GroupId: "g"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("rebalance without admin: %v", err)
	}
	if _, err := h.ListConsumerGroups(user, &types.ListConsumerGroupsRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("list without admin: %v", err)
	}
	if _, err := h.GetConsumerGroup(user, &types.GetConsumerGroupRequest{GroupId: "g"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("get without admin: %v", err)
	}
	if resp, err := h.CreateConsumerGroup(ops, create); err != nil || !resp.GetSuccess() {
		t.Fatalf("create as admin: %+v, %v", resp, err)
	}
	if _, err := h.GetConsumerGroup(ops, &types.GetConsumerGroupRequest{GroupId: "g"}); err != nil {
		t.Fatalf("get as admin: %v", err)
	}
}

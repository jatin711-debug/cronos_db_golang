package api

import (
	"context"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/internal/consumer"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
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

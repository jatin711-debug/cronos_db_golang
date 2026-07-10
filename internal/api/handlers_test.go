package api

import (
	"context"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
	"github.com/jatin711-debug/cronos_db_golang/internal/consumer"
	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

type MockDedupManager struct {
	isDuplicate      bool
	isDuplicateBatch []bool
	err              error
}

func (m *MockDedupManager) IsDuplicate(messageID string, offset int64) (bool, error) {
	return m.isDuplicate, m.err
}

func (m *MockDedupManager) IsDuplicateBatch(messageIDs []string, offsets []int64) ([]bool, error) {
	if len(m.isDuplicateBatch) != len(messageIDs) {
		return make([]bool, len(messageIDs)), m.err
	}
	return m.isDuplicateBatch, m.err
}

type MockConsumerManager struct {
	sub *types.SubscribeRequest
	ack *types.AckRequest
	err error
}

func (m *MockConsumerManager) Subscribe(request *types.SubscribeRequest) (*consumer.Subscription, error) {
	return nil, m.err
}

func (m *MockConsumerManager) Ack(request *types.AckRequest) error {
	return m.err
}

func (m *MockConsumerManager) GetCommittedOffset(groupID string, partitionID int32) (int64, error) {
	return 0, m.err
}

func (m *MockConsumerManager) LeaveGroup(groupID, memberID string) error {
	return m.err
}

func TestEventServiceHandler_Publish_Validations(t *testing.T) {
	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
	}

	pm := partition.NewPartitionManager("node-1", cfg)
	h := &EventServiceHandler{
		partitionManager: pm,
		dedupManager:     &MockDedupManager{},
	}

	ctx := context.Background()

	// 1. Missing message_id
	req := &types.PublishRequest{
		Event: &types.Event{
			Topic:      "topic-test",
			Payload:    []byte("payload"),
			ScheduleTs: time.Now().UnixMilli(),
		},
	}
	resp, err := h.Publish(ctx, req)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if resp.Success {
		t.Error("Expected Publish to fail with empty message_id")
	}
	if resp.Error != "message_id is required" {
		t.Errorf("Expected 'message_id is required', got %s", resp.Error)
	}

	// 2. Message ID too long
	req = &types.PublishRequest{
		Event: &types.Event{
			MessageId:  "this-is-a-super-long-message-id-that-exceeds-one-hundred-and-twenty-eight-characters-in-length-which-should-be-rejected-by-validation-rules-in-our-handler-code-to-prevent-overflow-and-dos",
			Topic:      "topic-test",
			Payload:    []byte("payload"),
			ScheduleTs: time.Now().UnixMilli(),
		},
	}
	resp, err = h.Publish(ctx, req)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if resp.Success {
		t.Error("Expected Publish to fail with long message_id")
	}

	// 3. Missing schedule_ts
	req = &types.PublishRequest{
		Event: &types.Event{
			MessageId: "msg-1",
			Topic:     "topic-test",
			Payload:   []byte("payload"),
		},
	}
	resp, err = h.Publish(ctx, req)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if resp.Success {
		t.Error("Expected Publish to fail with zero schedule_ts")
	}

	// 4. Missing payload
	req = &types.PublishRequest{
		Event: &types.Event{
			MessageId:  "msg-1",
			Topic:      "topic-test",
			ScheduleTs: time.Now().UnixMilli(),
		},
	}
	resp, err = h.Publish(ctx, req)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if resp.Success {
		t.Error("Expected Publish to fail with empty payload")
	}
}

func TestEventServiceHandler_Publish_Duplicate(t *testing.T) {
	skipWindows(t)

	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
		TickMS:         10,
		WheelSize:      100,
	}

	pm := partition.NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	pm.CreatePartition(0, "topic-test")
	pm.StartPartition(0)

	h := &EventServiceHandler{
		partitionManager: pm,
		dedupManager:     &MockDedupManager{isDuplicate: true},
	}

	ctx := context.Background()

	req := &types.PublishRequest{
		Event: &types.Event{
			MessageId:  "msg-1",
			Topic:      "topic-test",
			Payload:    []byte("payload"),
			ScheduleTs: time.Now().UnixMilli(),
		},
	}
	resp, err := h.Publish(ctx, req)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if resp.Success {
		t.Error("Expected duplicate message to be rejected")
	}
	if resp.Error != "duplicate message_id" {
		t.Errorf("Expected 'duplicate message_id', got %s", resp.Error)
	}
}

func TestEventServiceHandler_Publish_DurableAck(t *testing.T) {
	skipWindows(t)

	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
		TickMS:         10,
		WheelSize:      100,
		FsyncMode:      "batch", // weaker default; durable_ack forces flush
	}

	pm := partition.NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	pm.CreatePartition(0, "topic-test")
	pm.StartPartition(0)

	h := &EventServiceHandler{
		partitionManager: pm,
		dedupManager:     &MockDedupManager{},
	}

	ctx := context.Background()

	req := &types.PublishRequest{
		Event: &types.Event{
			MessageId:  "durable-msg-1",
			Topic:      "topic-test",
			Payload:    []byte("payload"),
			ScheduleTs: time.Now().UnixMilli() + 1000,
			Meta: map[string]string{
				"cronos.durable_ack": "true",
			},
		},
	}
	resp, err := h.Publish(ctx, req)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected publish to succeed with durable_ack, got error: %s", resp.Error)
	}
	if resp.Offset < 0 {
		t.Errorf("expected non-negative offset, got %d", resp.Offset)
	}
}

func TestEventServiceHandler_PublishBatch_DurableAck(t *testing.T) {
	skipWindows(t)

	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
		TickMS:         10,
		WheelSize:      100,
		FsyncMode:      "batch",
	}

	pm := partition.NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	pm.CreatePartition(0, "topic-test")
	pm.StartPartition(0)

	h := &EventServiceHandler{
		partitionManager: pm,
		dedupManager:     &MockDedupManager{},
	}

	ctx := context.Background()

	events := []*types.Event{
		{
			MessageId:  "batch-durable-1",
			Topic:      "topic-test",
			Payload:    []byte("payload1"),
			ScheduleTs: time.Now().UnixMilli() + 1000,
			Meta: map[string]string{
				"cronos.durable_ack": "true",
			},
		},
		{
			MessageId:  "batch-durable-2",
			Topic:      "topic-test",
			Payload:    []byte("payload2"),
			ScheduleTs: time.Now().UnixMilli() + 1000,
		},
	}

	resp, err := h.PublishBatch(ctx, &types.PublishBatchRequest{Events: events})
	if err != nil {
		t.Fatalf("PublishBatch failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected batch publish to succeed, got error: %s", resp.Error)
	}
	if resp.PublishedCount != 2 {
		t.Errorf("expected 2 published events, got %d", resp.PublishedCount)
	}
}

func TestEventServiceHandler_Publish_Authorization(t *testing.T) {
	skipWindows(t)

	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
		TickMS:         10,
		WheelSize:      100,
		FsyncMode:      "batch",
	}

	pm := partition.NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	pm.CreatePartition(0, "allowed-topic")
	pm.StartPartition(0)

	policy := &auth.Policy{
		Subjects: map[string]*auth.Subject{
			"user": {
				Topics: map[string]auth.TopicPerms{
					"allowed-topic": {Publish: true, Subscribe: true},
				},
			},
		},
	}

	h := &EventServiceHandler{
		partitionManager: pm,
		dedupManager:     &MockDedupManager{},
		authPolicy:       policy,
	}

	claims := &auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "user"},
		SubjectID:        "user",
	}
	ctx := auth.WithClaims(context.Background(), claims)

	// Denied topic should return a PermissionDenied gRPC error.
	deniedReq := &types.PublishRequest{
		Event: &types.Event{
			MessageId:  "denied-msg",
			Topic:      "denied-topic",
			Payload:    []byte("payload"),
			ScheduleTs: time.Now().UnixMilli() + 1000,
		},
	}
	if _, err := h.Publish(ctx, deniedReq); err == nil {
		t.Fatal("expected PermissionDenied for denied topic, got nil")
	}

	// Allowed topic should succeed.
	allowedReq := &types.PublishRequest{
		Event: &types.Event{
			MessageId:  "allowed-msg",
			Topic:      "allowed-topic",
			Payload:    []byte("payload"),
			ScheduleTs: time.Now().UnixMilli() + 1000,
		},
	}
	resp, err := h.Publish(ctx, allowedReq)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected publish to succeed on allowed topic, got: %s", resp.Error)
	}

	// Missing claims should return Unauthenticated.
	resp, err = h.Publish(context.Background(), allowedReq)
	if err == nil {
		t.Fatal("expected Unauthenticated without claims, got nil")
	}
	if resp != nil && resp.Success {
		t.Fatal("expected publish to fail without claims")
	}

	// PublishBatch should reject only the denied event and publish the allowed one.
	batchReq := &types.PublishBatchRequest{
		Events: []*types.Event{
			{
				MessageId:  "batch-allowed",
				Topic:      "allowed-topic",
				Payload:    []byte("payload"),
				ScheduleTs: time.Now().UnixMilli() + 1000,
			},
			{
				MessageId:  "batch-denied",
				Topic:      "denied-topic",
				Payload:    []byte("payload"),
				ScheduleTs: time.Now().UnixMilli() + 1000,
			},
		},
	}
	batchResp, err := h.PublishBatch(ctx, batchReq)
	if err != nil {
		t.Fatalf("PublishBatch failed: %v", err)
	}
	if batchResp.Success {
		t.Fatal("expected batch publish to fail because one event was denied")
	}
	if batchResp.PublishedCount != 1 {
		t.Errorf("expected 1 published event, got %d", batchResp.PublishedCount)
	}
	if batchResp.ErrorCount != 1 {
		t.Errorf("expected 1 error, got %d", batchResp.ErrorCount)
	}
}

package api

import (
	"context"
	"testing"
	"time"

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

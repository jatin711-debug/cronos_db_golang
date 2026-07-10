package types

import (
	"errors"
	"testing"
)

func TestErrors(t *testing.T) {
	tests := []struct {
		err  error
		name string
	}{
		{ErrNotFound, "ErrNotFound"},
		{ErrAlreadyExists, "ErrAlreadyExists"},
		{ErrInvalidArgument, "ErrInvalidArgument"},
		{ErrTimeout, "ErrTimeout"},
		{ErrPartitionNotFound, "ErrPartitionNotFound"},
		{ErrLeaderNotFound, "ErrLeaderNotFound"},
		{ErrFollowerNotFound, "ErrFollowerNotFound"},
		{ErrConsumerGroupNotFound, "ErrConsumerGroupNotFound"},
		{ErrMessageIDExists, "ErrMessageIDExists"},
		{ErrOffsetOutOfRange, "ErrOffsetOutOfRange"},
		{ErrSegmentNotFound, "ErrSegmentNotFound"},
		{ErrIndexCorrupted, "ErrIndexCorrupted"},
		{ErrWALCorrupted, "ErrWALCorrupted"},
		{ErrLeaderChange, "ErrLeaderChange"},
		{ErrReplicationLag, "ErrReplicationLag"},
		{ErrSchedulerLag, "ErrSchedulerLag"},
		{ErrDeliveryTimeout, "ErrDeliveryTimeout"},
		{ErrMaxRetriesExceeded, "ErrMaxRetriesExceeded"},
		{ErrInvalidChecksum, "ErrInvalidChecksum"},
	}

	for _, tc := range tests {
		if tc.err == nil {
			t.Errorf("%s should not be nil", tc.name)
		}
	}
}

func TestNewError(t *testing.T) {
	inner := errors.New("inner")
	e := NewError("E001", "something failed", inner)

	if e.Code != "E001" {
		t.Errorf("expected code E001, got %s", e.Code)
	}
	if e.Message != "something failed" {
		t.Errorf("expected message 'something failed', got %s", e.Message)
	}
	if e.Err != inner {
		t.Error("expected inner error")
	}
	if e.Error() != "E001: something failed" {
		t.Errorf("unexpected error string: %s", e.Error())
	}
	if !errors.Is(e.Unwrap(), inner) {
		t.Error("expected unwrap to return inner error")
	}
}

func TestNewError_NilInner(t *testing.T) {
	e := NewError("E002", "no inner", nil)
	if e.Err != nil {
		t.Error("expected nil inner")
	}
}

func TestOffsetRange(t *testing.T) {
	r := OffsetRange{Start: 10, End: 20}

	if !r.Contains(10) {
		t.Error("should contain start")
	}
	if !r.Contains(15) {
		t.Error("should contain middle")
	}
	if !r.Contains(20) {
		t.Error("should contain end")
	}
	if r.Contains(9) {
		t.Error("should not contain before start")
	}
	if r.Contains(21) {
		t.Error("should not contain after end")
	}

	if r.String() != "[10-20]" {
		t.Errorf("expected '[10-20]', got %s", r.String())
	}
}

func TestTimestampRange(t *testing.T) {
	r := TimestampRange{Start: 1000, End: 2000}

	if !r.Contains(1000) {
		t.Error("should contain start")
	}
	if !r.Contains(1500) {
		t.Error("should contain middle")
	}
	if !r.Contains(2000) {
		t.Error("should contain end")
	}
	if r.Contains(999) {
		t.Error("should not contain before start")
	}
	if r.Contains(2001) {
		t.Error("should not contain after end")
	}

	if r.String() != "[1000-2000]" {
		t.Errorf("expected '[1000-2000]', got %s", r.String())
	}
}

func TestPartition_Fields(t *testing.T) {
	p := &Partition{
		ID:            1,
		Topic:         "orders",
		NextOffset:    100,
		HighWatermark: 99,
		Active:        true,
		CreatedTS:     1000,
		UpdatedTS:     2000,
	}
	if p.ID != 1 {
		t.Error("ID mismatch")
	}
	if p.Topic != "orders" {
		t.Error("Topic mismatch")
	}
}

func TestConsumerGroup_Fields(t *testing.T) {
	cg := &ConsumerGroup{
		GroupID:          "group-1",
		Topic:            "orders",
		Partitions:       []int32{0, 1, 2},
		CommittedOffsets: map[int32]int64{0: 100, 1: 200},
		Members:          map[string]*ConsumerMember{"m1": {MemberID: "m1"}},
	}
	if cg.GroupID != "group-1" {
		t.Error("GroupID mismatch")
	}
	if len(cg.Partitions) != 3 {
		t.Errorf("expected 3 partitions, got %d", len(cg.Partitions))
	}
}

func TestConsumerMember_Fields(t *testing.T) {
	m := &ConsumerMember{
		MemberID:          "member-1",
		Address:           "127.0.0.1",
		AssignedPartition: 0,
		Active:            true,
		LastSeenTS:        1000,
		ConnectedTS:       500,
	}
	if m.MemberID != "member-1" {
		t.Error("MemberID mismatch")
	}
	if !m.Active {
		t.Error("expected active")
	}
}

func TestConfig_Fields(t *testing.T) {
	cfg := &Config{
		NodeID:         "node-1",
		DataDir:        "/data",
		GPRCAddress:    "localhost:50051",
		PartitionCount: 8,
	}
	if cfg.NodeID != "node-1" {
		t.Error("NodeID mismatch")
	}
	if cfg.PartitionCount != 8 {
		t.Error("PartitionCount mismatch")
	}
}

func TestGenerateDeliveryID(t *testing.T) {
	id1 := GenerateDeliveryID()
	id2 := GenerateDeliveryID()

	if id1 == "" {
		t.Error("delivery ID should not be empty")
	}
	if id1 == id2 {
		t.Error("delivery IDs should be unique")
	}
	if len(id1) != 32 {
		t.Errorf("expected 32 hex chars, got %d", len(id1))
	}
}

func TestEvent_Getters(t *testing.T) {
	e := &Event{
		MessageId:   "msg-1",
		ScheduleTs:  1000,
		Payload:     []byte("data"),
		Topic:       "orders",
		Meta:        map[string]string{"key": "value"},
		CreatedTs:   500,
		Offset:      42,
		PartitionId: 1,
		Checksum:    12345,
	}

	if e.GetMessageId() != "msg-1" {
		t.Error("GetMessageId mismatch")
	}
	if e.GetScheduleTs() != 1000 {
		t.Error("GetScheduleTs mismatch")
	}
	if string(e.GetPayload()) != "data" {
		t.Error("GetPayload mismatch")
	}
	if e.GetTopic() != "orders" {
		t.Error("GetTopic mismatch")
	}
	if e.GetOffset() != 42 {
		t.Error("GetOffset mismatch")
	}
	if e.GetPartitionId() != 1 {
		t.Error("GetPartitionId mismatch")
	}
	if e.GetChecksum() != 12345 {
		t.Error("GetChecksum mismatch")
	}
}

func TestPublishRequest_Getters(t *testing.T) {
	req := &PublishRequest{
		Event:          e,
		AllowDuplicate: true,
	}
	if req.GetEvent() == nil {
		t.Error("GetEvent should not be nil")
	}
	if !req.GetAllowDuplicate() {
		t.Error("GetAllowDuplicate mismatch")
	}
}

var e = &Event{MessageId: "test"}

func TestPublishResponse_Getters(t *testing.T) {
	resp := &PublishResponse{
		Success:     true,
		Error:       "",
		Offset:      42,
		PartitionId: 1,
		ScheduleTs:  1000,
	}
	if !resp.GetSuccess() {
		t.Error("GetSuccess mismatch")
	}
	if resp.GetOffset() != 42 {
		t.Error("GetOffset mismatch")
	}
}

func TestDelivery_Getters(t *testing.T) {
	d := &Delivery{
		Event:        e,
		DeliveryId:   "d-1",
		Attempt:      3,
		AckTimeoutMs: 5000,
		Batch:        []*Event{e},
	}
	if d.GetDeliveryId() != "d-1" {
		t.Error("GetDeliveryId mismatch")
	}
	if d.GetAttempt() != 3 {
		t.Error("GetAttempt mismatch")
	}
	if len(d.GetBatch()) != 1 {
		t.Error("GetBatch mismatch")
	}
}

func TestReplayRequest_Getters(t *testing.T) {
	r := &ReplayRequest{
		Topic:          "orders",
		PartitionId:    0,
		StartTs:        1000,
		EndTs:          2000,
		StartOffset:    0,
		Count:          100,
		ConsumerGroup:  "cg-1",
		SubscriptionId: "sub-1",
		Speed:          1.5,
	}
	if r.GetTopic() != "orders" {
		t.Error("GetTopic mismatch")
	}
	if r.GetSpeed() != 1.5 {
		t.Error("GetSpeed mismatch")
	}
}

func TestPartitionInfo_Getters(t *testing.T) {
	pi := &PartitionInfo{
		PartitionId:    0,
		Topic:          "orders",
		LeaderId:       "node-1",
		ReplicaIds:     []string{"node-2"},
		HighWatermark:  100,
		LastOffset:     99,
		SegmentCount:   2,
		DiskUsageBytes: 1024,
	}
	if pi.GetLeaderId() != "node-1" {
		t.Error("GetLeaderId mismatch")
	}
	if pi.GetHighWatermark() != 100 {
		t.Error("GetHighWatermark mismatch")
	}
}

func TestRegionReplicateRequest_Getters(t *testing.T) {
	req := &RegionReplicateRequest{
		RegionId:    "us-west-2",
		PartitionId: 1,
		FirstOffset: 100,
		Events:      []*Event{e},
	}
	if req.GetRegionId() != "us-west-2" {
		t.Error("GetRegionId mismatch")
	}
	if req.GetFirstOffset() != 100 {
		t.Error("GetFirstOffset mismatch")
	}
}

func TestRegionReplicateResponse_Getters(t *testing.T) {
	resp := &RegionReplicateResponse{
		Success:    true,
		Error:      "",
		LastOffset: 200,
	}
	if !resp.GetSuccess() {
		t.Error("GetSuccess mismatch")
	}
	if resp.GetLastOffset() != 200 {
		t.Error("GetLastOffset mismatch")
	}
}

func TestTransactionRequests_Getters(t *testing.T) {
	begin := &BeginTransactionRequest{
		TransactionId:           "tx-1",
		ParticipantPartitionIds: []int32{0, 1},
	}
	if begin.GetTransactionId() != "tx-1" {
		t.Error("GetTransactionId mismatch")
	}

	prepare := &PrepareTransactionRequest{TransactionId: "tx-1"}
	if prepare.GetTransactionId() != "tx-1" {
		t.Error("GetTransactionId mismatch")
	}

	commit := &CommitTransactionRequest{TransactionId: "tx-1"}
	if commit.GetTransactionId() != "tx-1" {
		t.Error("GetTransactionId mismatch")
	}

	abort := &AbortTransactionRequest{TransactionId: "tx-1"}
	if abort.GetTransactionId() != "tx-1" {
		t.Error("GetTransactionId mismatch")
	}
}

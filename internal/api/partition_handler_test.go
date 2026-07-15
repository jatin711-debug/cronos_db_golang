package api

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestPartitionServiceHandler_Standalone(t *testing.T) {
	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
		TickMS:         10,
		WheelSize:      100,
	}

	pm := partition.NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	err := pm.CreatePartition(0, "topic-test")
	if err != nil {
		t.Fatalf("CreatePartition failed: %v", err)
	}

	h := NewPartitionServiceHandler(pm, nil, "node-1")
	ctx := context.Background()

	// 1. GetPartition
	getReq := &types.GetPartitionRequest{PartitionId: 0}
	getResp, err := h.GetPartition(ctx, getReq)
	if err != nil {
		t.Fatalf("GetPartition failed: %v", err)
	}
	if getResp.PartitionId != 0 {
		t.Errorf("Expected partition 0, got %d", getResp.PartitionId)
	}
	if getResp.Topic != "topic-test" {
		t.Errorf("Expected topic topic-test, got %s", getResp.Topic)
	}

	// 2. ListPartitions
	listResp, err := h.ListPartitions(ctx, &types.ListPartitionsRequest{})
	if err != nil {
		t.Fatalf("ListPartitions failed: %v", err)
	}
	if len(listResp.Partitions) != 1 {
		t.Errorf("Expected 1 partition, got %d", len(listResp.Partitions))
	}

	// 3. GetWALStatus
	walStatus, err := h.GetWALStatus(ctx, &types.GetWALStatusRequest{PartitionId: 0})
	if err != nil {
		t.Fatalf("GetWALStatus failed: %v", err)
	}
	if walStatus.PartitionId != 0 {
		t.Errorf("Expected partition 0, got %d", walStatus.PartitionId)
	}

	// 4. GetSchedulerStatus
	schedStatus, err := h.GetSchedulerStatus(ctx, &types.GetSchedulerStatusRequest{PartitionId: 0})
	if err != nil {
		t.Fatalf("GetSchedulerStatus failed: %v", err)
	}
	if schedStatus.PartitionId != 0 {
		t.Errorf("Expected partition 0, got %d", schedStatus.PartitionId)
	}
}

func TestPartitionServiceHandler_CompactAndRetentionImplemented(t *testing.T) {
	cfg := &types.Config{
		DataDir:          t.TempDir(),
		PartitionCount:   1,
		TickMS:           10,
		WheelSize:        100,
		SegmentSizeBytes: 512,
		IndexInterval:    1,
		FsyncMode:        "periodic",
		FlushIntervalMS:  100,
	}

	pm := partition.NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	if err := pm.CreatePartition(0, "topic-test"); err != nil {
		t.Fatalf("CreatePartition failed: %v", err)
	}

	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatalf("GetInternalPartition failed: %v", err)
	}

	for i := 0; i < 40; i++ {
		err := p.Wal.AppendEvent(&types.Event{
			MessageId:  fmt.Sprintf("compact-test-%d", i),
			ScheduleTs: time.Now().UnixMilli(),
			Payload:    make([]byte, 64),
			Topic:      "topic-test",
		})
		if err != nil {
			t.Fatalf("AppendEvent failed: %v", err)
		}
	}

	h := NewPartitionServiceHandler(pm, nil, "node-1")
	ctx := context.Background()

	compactResp, err := h.Compact(ctx, &types.CompactRequest{PartitionId: 0, Force: true})
	if err != nil {
		t.Fatalf("Compact returned gRPC error: %v", err)
	}
	if !compactResp.GetSuccess() {
		t.Fatalf("Compact failed: %s", compactResp.GetError())
	}

	retResp, err := h.RunRetention(ctx, &types.RetentionRequest{PartitionId: 0, MaxSizeBytes: 1024})
	if err != nil {
		t.Fatalf("RunRetention returned gRPC error: %v", err)
	}
	if !retResp.GetSuccess() {
		t.Fatalf("RunRetention failed: %s", retResp.GetError())
	}
}

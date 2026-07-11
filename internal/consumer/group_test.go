package consumer

import (
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestGroupManager_NewGroupManager(t *testing.T) {
	gm := NewGroupManager()
	if gm == nil {
		t.Fatal("NewGroupManager should not return nil")
	}
}

func TestGroupManager_NewGroupManagerWithStore(t *testing.T) {
	store := &OffsetStore{}
	gm := NewGroupManagerWithStore(store)
	if gm == nil {
		t.Fatal("NewGroupManagerWithStore should not return nil")
	}
	if gm.offsetStore != store {
		t.Error("offsetStore not set correctly")
	}
}

func TestGroupManager_CreateGroup(t *testing.T) {
	gm := NewGroupManager()

	err := gm.CreateGroup("group-1", "topic-1", []int32{0, 1, 2})
	if err != nil {
		t.Fatalf("CreateGroup failed: %v", err)
	}

	group, ok := gm.GetGroup("group-1")
	if !ok {
		t.Fatal("group should exist after creation")
	}

	if group.GroupID != "group-1" {
		t.Errorf("expected GroupID=group-1, got %s", group.GroupID)
	}
	if group.Topic != "topic-1" {
		t.Errorf("expected Topic=topic-1, got %s", group.Topic)
	}
	if len(group.Partitions) != 3 {
		t.Errorf("expected 3 partitions, got %d", len(group.Partitions))
	}
	if group.CommittedOffsets[0] != -1 {
		t.Errorf("expected offset -1 for partition 0, got %d", group.CommittedOffsets[0])
	}
}

func TestGroupManager_CreateGroup_Duplicate(t *testing.T) {
	gm := NewGroupManager()

	gm.CreateGroup("group-1", "topic-1", []int32{0})

	err := gm.CreateGroup("group-1", "topic-1", []int32{0})
	if err == nil {
		t.Error("expected error for duplicate group")
	}
}

func TestGroupManager_GetGroup(t *testing.T) {
	gm := NewGroupManager()

	gm.CreateGroup("group-1", "topic-1", []int32{0})

	group, ok := gm.GetGroup("group-1")
	if !ok {
		t.Error("expected to find group")
	}
	if group.GroupID != "group-1" {
		t.Errorf("expected group-1, got %s", group.GroupID)
	}

	_, ok = gm.GetGroup("nonexistent")
	if ok {
		t.Error("should not find nonexistent group")
	}
}

func TestGroupManager_JoinGroup(t *testing.T) {
	gm := NewGroupManager()
	gm.CreateGroup("group-1", "topic-1", []int32{0})

	err := gm.JoinGroup("group-1", "member-1", "localhost:9000", "topic-1", 0)
	if err != nil {
		t.Fatalf("JoinGroup failed: %v", err)
	}

	group, _ := gm.GetGroup("group-1")
	if len(group.Members) != 1 {
		t.Errorf("expected 1 member, got %d", len(group.Members))
	}
}

func TestGroupManager_JoinGroup_AutoCreate(t *testing.T) {
	gm := NewGroupManager()

	// Joining non-existent group should auto-create
	err := gm.JoinGroup("new-group", "member-1", "localhost:9000", "topic-1", 0)
	if err != nil {
		t.Fatalf("JoinGroup auto-create failed: %v", err)
	}

	group, ok := gm.GetGroup("new-group")
	_ = group // suppress unused warning
	if !ok {
		t.Error("group should be auto-created")
	}
}

func TestGroupManager_CommitOffset(t *testing.T) {
	gm := NewGroupManager()
	gm.CreateGroup("group-1", "topic-1", []int32{0, 1})
	gm.JoinGroup("group-1", "member-1", "localhost:9000", "topic-1", 0)

	err := gm.CommitOffset("group-1", 0, 100)
	if err != nil {
		t.Fatalf("CommitOffset failed: %v", err)
	}

	group, _ := gm.GetGroup("group-1")
	if group.CommittedOffsets[0] != 100 {
		t.Errorf("expected committed offset 100, got %d", group.CommittedOffsets[0])
	}
}

func TestGroupManager_CommitOffset_NonExistent(t *testing.T) {
	gm := NewGroupManager()

	err := gm.CommitOffset("nonexistent", 0, 100)
	if err == nil {
		t.Error("expected error for nonexistent group")
	}
}

func TestGroupManager_GetCommittedOffset(t *testing.T) {
	gm := NewGroupManager()
	gm.CreateGroup("group-1", "topic-1", []int32{0, 1})

	offset, err := gm.GetCommittedOffset("group-1", 0)
	if err != nil {
		t.Fatalf("GetCommittedOffset failed: %v", err)
	}
	if offset != -1 {
		t.Errorf("expected offset -1, got %d", offset)
	}

	// After commit
	gm.CommitOffset("group-1", 0, 50)
	offset, _ = gm.GetCommittedOffset("group-1", 0)
	if offset != 50 {
		t.Errorf("expected offset 50, got %d", offset)
	}
}

func TestSubscription_Struct(t *testing.T) {
	deliver := make(chan *types.Delivery)
	done := make(chan struct{})

	sub := &Subscription{
		ID:            "sub-1",
		ConsumerGroup: "group-1",
		Partition:     &types.Partition{ID: 0},
		StartOffset:   100,
		Delivery:      deliver,
		Done:          done,
		quit:          make(chan struct{}),
	}

	if sub.ID != "sub-1" {
		t.Errorf("expected ID=sub-1, got %s", sub.ID)
	}
	if sub.ConsumerGroup != "group-1" {
		t.Errorf("expected ConsumerGroup=group-1, got %s", sub.ConsumerGroup)
	}
	if sub.StartOffset != 100 {
		t.Errorf("expected StartOffset=100, got %d", sub.StartOffset)
	}
}

func TestGroupManager_GetGroup_MultipleGroups(t *testing.T) {
	gm := NewGroupManager()

	gm.CreateGroup("group-1", "topic-1", []int32{0})
	gm.CreateGroup("group-2", "topic-2", []int32{1, 2})

	group1, _ := gm.GetGroup("group-1")
	group2, _ := gm.GetGroup("group-2")

	if group1.Topic != "topic-1" {
		t.Errorf("expected topic-1, got %s", group1.Topic)
	}
	if group2.Topic != "topic-2" {
		t.Errorf("expected topic-2, got %s", group2.Topic)
	}
}

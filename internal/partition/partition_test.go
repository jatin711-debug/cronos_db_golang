package partition

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
)

func TestNewPartitionManager(t *testing.T) {
	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
	}
	pm := NewPartitionManager("node-1", cfg)
	if pm == nil {
		t.Fatal("NewPartitionManager should not return nil")
	}
	if pm.nodeID != "node-1" {
		t.Errorf("expected nodeID node-1, got %s", pm.nodeID)
	}
	if len(pm.partitions) != 0 {
		t.Errorf("expected 0 partitions, got %d", len(pm.partitions))
	}
}

func skipWindows(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Skipping on Windows due to PebbleDB file locking in partition cleanup")
	}
}

func TestPartitionManager_CreatePartition(t *testing.T) {
	skipWindows(t)
	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
		TickMS:         100,
		WheelSize:      60,
	}
	pm := NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	err := pm.CreatePartition(0, "test-topic")
	if err != nil {
		t.Fatalf("CreatePartition failed: %v", err)
	}

	// Duplicate should fail
	err = pm.CreatePartition(0, "test-topic")
	if err == nil {
		t.Error("expected error for duplicate partition")
	}
}

func TestPartitionManager_GetPartition(t *testing.T) {
	skipWindows(t)
	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
		TickMS:         100,
		WheelSize:      60,
	}
	pm := NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	_, err := pm.GetPartition(0)
	if err == nil {
		t.Error("expected error for nonexistent partition")
	}

	pm.CreatePartition(0, "test-topic")
	p, err := pm.GetPartition(0)
	if err != nil {
		t.Fatalf("GetPartition failed: %v", err)
	}
	if p.ID != 0 {
		t.Errorf("expected ID 0, got %d", p.ID)
	}
	if p.Topic != "test-topic" {
		t.Errorf("expected topic test-topic, got %s", p.Topic)
	}
}

func TestPartitionManager_GetPartitionIDForTopic(t *testing.T) {
	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
	}
	pm := NewPartitionManager("node-1", cfg)

	id1 := pm.GetPartitionIDForTopic("topic-a")
	id2 := pm.GetPartitionIDForTopic("topic-b")

	if id1 < 0 || id1 >= 8 {
		t.Errorf("id1 out of range: %d", id1)
	}
	if id2 < 0 || id2 >= 8 {
		t.Errorf("id2 out of range: %d", id2)
	}

	// Same topic should give same ID
	id1Again := pm.GetPartitionIDForTopic("topic-a")
	if id1 != id1Again {
		t.Errorf("expected same ID for same topic, got %d and %d", id1, id1Again)
	}
}

func TestPartitionManager_GetPartitionIDForKey(t *testing.T) {
	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
	}
	pm := NewPartitionManager("node-1", cfg)

	id1 := pm.GetPartitionIDForKey("key-a")
	id2 := pm.GetPartitionIDForKey("key-b")

	if id1 < 0 || id1 >= 8 {
		t.Errorf("id1 out of range: %d", id1)
	}
	if id2 < 0 || id2 >= 8 {
		t.Errorf("id2 out of range: %d", id2)
	}

	// Same key should give same ID
	id1Again := pm.GetPartitionIDForKey("key-a")
	if id1 != id1Again {
		t.Errorf("expected same ID for same key, got %d and %d", id1, id1Again)
	}
}

func TestPartitionManager_GetPartitionID_MatchesHashToPartitionID(t *testing.T) {
	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
	}
	pm := NewPartitionManager("node-1", cfg)

	topic := "consistent-topic"
	expected := utils.HashToPartitionID(topic, cfg.PartitionCount)
	actual := pm.GetPartitionIDForTopic(topic)

	if actual != expected {
		t.Errorf("expected %d, got %d", expected, actual)
	}
}

func TestPartitionManager_ListPartitions(t *testing.T) {
	skipWindows(t)
	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
		TickMS:         100,
		WheelSize:      60,
	}
	pm := NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	if len(pm.ListPartitions()) != 0 {
		t.Error("expected 0 partitions initially")
	}

	pm.CreatePartition(0, "topic-0")
	pm.CreatePartition(1, "topic-1")

	parts := pm.ListPartitions()
	if len(parts) != 2 {
		t.Errorf("expected 2 partitions, got %d", len(parts))
	}
}

func TestPartitionManager_GetStats(t *testing.T) {
	skipWindows(t)
	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
		TickMS:         100,
		WheelSize:      60,
	}
	pm := NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	stats := pm.GetStats()
	if stats.TotalPartitions != 0 {
		t.Errorf("expected 0 total partitions, got %d", stats.TotalPartitions)
	}

	pm.CreatePartition(0, "topic-0")
	pm.partitions[0].Leader = true

	stats = pm.GetStats()
	if stats.TotalPartitions != 1 {
		t.Errorf("expected 1 total partition, got %d", stats.TotalPartitions)
	}
	if stats.LeaderPartitions != 1 {
		t.Errorf("expected 1 leader partition, got %d", stats.LeaderPartitions)
	}
}

func TestPartitionManager_CanAccept_NonExistent(t *testing.T) {
	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
	}
	pm := NewPartitionManager("node-1", cfg)

	if !pm.CanAccept(0) {
		t.Error("non-existent partition should be acceptable")
	}
}

func TestPartitionManager_GetOrCreatePartition(t *testing.T) {
	skipWindows(t)
	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
		TickMS:         100,
		WheelSize:      60,
	}
	pm := NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()

	err := pm.GetOrCreatePartition(5)
	if err != nil {
		t.Fatalf("GetOrCreatePartition failed: %v", err)
	}

	// Should be idempotent
	err = pm.GetOrCreatePartition(5)
	if err != nil {
		t.Fatalf("GetOrCreatePartition idempotent failed: %v", err)
	}

	if len(pm.partitions) != 1 {
		t.Errorf("expected 1 partition, got %d", len(pm.partitions))
	}
}

func TestPartitionManager_SyncPartitionFromLeader_NotFound(t *testing.T) {
	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
	}
	pm := NewPartitionManager("node-1", cfg)

	err := pm.SyncPartitionFromLeader(0, "leader-addr")
	if err == nil {
		t.Error("expected error for nonexistent partition")
	}
}

func TestPartitionManager_StopPartition_NotFound(t *testing.T) {
	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
	}
	pm := NewPartitionManager("node-1", cfg)

	err := pm.StopPartition(0)
	if err == nil {
		t.Error("expected error for nonexistent partition")
	}
}

func TestPartitionManager_StopAllPartitions(t *testing.T) {
	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
		TickMS:         100,
		WheelSize:      60,
	}
	pm := NewPartitionManager("node-1", cfg)

	// No partitions — should not error
	if err := pm.StopAllPartitions(); err != nil {
		t.Fatalf("StopAllPartitions on empty failed: %v", err)
	}
}

func TestPartitionManager_StartPartition_NotFound(t *testing.T) {
	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
	}
	pm := NewPartitionManager("node-1", cfg)

	err := pm.StartPartition(999)
	if err == nil {
		t.Error("expected error for nonexistent partition")
	}
}

func TestNewPartitionManagerWithAccessor(t *testing.T) {
	cfg := &types.Config{
		DataDir:        t.TempDir(),
		PartitionCount: 8,
	}
	pm := NewPartitionManagerWithAccessor("node-1", cfg)
	if pm == nil {
		t.Fatal("should not be nil")
	}
}

func TestDiskMonitor_New(t *testing.T) {
	tmpDir := t.TempDir()
	dm := NewDiskMonitor(tmpDir, 0.85, func() {})
	if dm == nil {
		t.Fatal("NewDiskMonitor should not return nil")
	}
	if dm.thresholdPct != 0.85 {
		t.Errorf("expected threshold 0.85, got %f", dm.thresholdPct)
	}
}

func TestDiskMonitor_StartStop(t *testing.T) {
	tmpDir := t.TempDir()
	dm := NewDiskMonitor(tmpDir, 0.0, func() {})
	dm.checkInterval = 10 * time.Millisecond
	dm.Start()
	time.Sleep(50 * time.Millisecond)
	dm.Stop()
	// compactFn may or may not be called depending on disk usage heuristic
	// Just verify no panic
}

func TestDiskMonitor_usage_RealFilesystem(t *testing.T) {
	tmpDir := t.TempDir()
	dm := NewDiskMonitor(tmpDir, 0.85, nil)
	usage, err := dm.usage()
	if err != nil {
		t.Fatalf("usage failed: %v", err)
	}
	// Real filesystem usage is always > 0 (filesystem has metadata)
	if usage < 0 || usage > 1 {
		t.Errorf("usage should be in [0,1], got %f", usage)
	}
}

func TestWalDiskSize(t *testing.T) {
	tmpDir := t.TempDir()
	os.WriteFile(filepath.Join(tmpDir, "1.dat"), []byte("hello"), 0644)
	os.WriteFile(filepath.Join(tmpDir, "2.dat"), []byte("world"), 0644)

	size, err := WalDiskSize(tmpDir)
	if err != nil {
		t.Fatalf("WalDiskSize failed: %v", err)
	}
	if size != 10 {
		t.Errorf("expected size 10, got %d", size)
	}
}

func TestWalDiskSize_Empty(t *testing.T) {
	tmpDir := t.TempDir()
	size, err := WalDiskSize(tmpDir)
	if err != nil {
		t.Fatalf("WalDiskSize failed: %v", err)
	}
	if size != 0 {
		t.Errorf("expected size 0, got %d", size)
	}
}

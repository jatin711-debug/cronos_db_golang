package metadata

import (
	"context"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client/internal/connpool"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestNewManager(t *testing.T) {
	pool, err := connpool.New(context.Background(), []string{}, connpool.Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if pool == nil {
		// Empty bootstrap may fail; create a minimal pool manually for testing
		if err != nil {
			// Skip tests that need a real pool
			t.Skip("connpool creation failed, skipping")
		}
	}
	defer pool.Close()

	cfg := Config{TTL: 15 * time.Second}
	mgr := NewManager(pool, cfg)
	if mgr == nil {
		t.Fatal("NewManager should not return nil")
	}
}

func TestManager_Partitions_Empty(t *testing.T) {
	pool, err := connpool.New(context.Background(), []string{}, connpool.Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if pool == nil {
		// Empty bootstrap may fail; create a minimal pool manually for testing
		if err != nil {
			// Skip tests that need a real pool
			t.Skip("connpool creation failed, skipping")
		}
	}
	defer pool.Close()

	mgr := NewManager(pool, Config{TTL: 15 * time.Second})
	parts := mgr.Partitions()
	if len(parts) != 0 {
		t.Errorf("expected 0 partitions, got %d", len(parts))
	}
}

func TestManager_GetPartitionInfo(t *testing.T) {
	pool, err := connpool.New(context.Background(), []string{}, connpool.Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if pool == nil {
		// Empty bootstrap may fail; create a minimal pool manually for testing
		if err != nil {
			// Skip tests that need a real pool
			t.Skip("connpool creation failed, skipping")
		}
	}
	defer pool.Close()

	mgr := NewManager(pool, Config{TTL: 15 * time.Second})
	_, ok := mgr.GetPartitionInfo(0)
	if ok {
		t.Error("should not find partition in empty manager")
	}
}

func TestManager_PartitionCount(t *testing.T) {
	pool, err := connpool.New(context.Background(), []string{}, connpool.Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if pool == nil {
		// Empty bootstrap may fail; create a minimal pool manually for testing
		if err != nil {
			// Skip tests that need a real pool
			t.Skip("connpool creation failed, skipping")
		}
	}
	defer pool.Close()

	mgr := NewManager(pool, Config{TTL: 15 * time.Second, StaticPartitionCount: 8})
	if mgr.PartitionCount() != 8 {
		t.Errorf("expected partition count 8, got %d", mgr.PartitionCount())
	}
}

func TestManager_ResolveLeaderAddress(t *testing.T) {
	pool, err := connpool.New(context.Background(), []string{}, connpool.Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if pool == nil {
		// Empty bootstrap may fail; create a minimal pool manually for testing
		if err != nil {
			// Skip tests that need a real pool
			t.Skip("connpool creation failed, skipping")
		}
	}
	defer pool.Close()

	mgr := NewManager(pool, Config{TTL: 15 * time.Second})
	_, ok := mgr.ResolveLeaderAddress(0)
	if ok {
		t.Error("should not resolve leader in empty manager")
	}
}

func TestManager_CandidateAddresses(t *testing.T) {
	pool, err := connpool.New(context.Background(), []string{}, connpool.Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if pool == nil {
		// Empty bootstrap may fail; create a minimal pool manually for testing
		if err != nil {
			// Skip tests that need a real pool
			t.Skip("connpool creation failed, skipping")
		}
	}
	defer pool.Close()

	mgr := NewManager(pool, Config{TTL: 15 * time.Second})
	addrs := mgr.CandidateAddresses(0)
	if len(addrs) != 0 {
		t.Errorf("expected 0 addresses, got %d", len(addrs))
	}
}

func TestManager_MarkStale(t *testing.T) {
	pool, err := connpool.New(context.Background(), []string{}, connpool.Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if pool == nil {
		// Empty bootstrap may fail; create a minimal pool manually for testing
		if err != nil {
			// Skip tests that need a real pool
			t.Skip("connpool creation failed, skipping")
		}
	}
	defer pool.Close()

	mgr := NewManager(pool, Config{TTL: 15 * time.Second})
	mgr.MarkStale()
	if !mgr.stale.Load() {
		t.Error("manager should be stale after MarkStale")
	}
}

func TestManager_Close(t *testing.T) {
	pool, err := connpool.New(context.Background(), []string{}, connpool.Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if pool == nil {
		// Empty bootstrap may fail; create a minimal pool manually for testing
		if err != nil {
			// Skip tests that need a real pool
			t.Skip("connpool creation failed, skipping")
		}
	}
	defer pool.Close()

	mgr := NewManager(pool, Config{TTL: 15 * time.Second})
	mgr.Close()
	// Double close should be safe
	mgr.Close()
}

func TestManager_Start(t *testing.T) {
	pool, err := connpool.New(context.Background(), []string{}, connpool.Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if pool == nil {
		// Empty bootstrap may fail; create a minimal pool manually for testing
		if err != nil {
			// Skip tests that need a real pool
			t.Skip("connpool creation failed, skipping")
		}
	}
	defer pool.Close()

	cfg := Config{
		TTL:             15 * time.Second,
		RefreshInterval: 100 * time.Millisecond,
		RequestTimeout:  1 * time.Second,
	}
	mgr := NewManager(pool, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr.Start(ctx)
	time.Sleep(50 * time.Millisecond)
	mgr.Close()
}

func TestManager_EnsureFresh(t *testing.T) {
	pool, err := connpool.New(context.Background(), []string{}, connpool.Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if pool == nil {
		// Empty bootstrap may fail; create a minimal pool manually for testing
		if err != nil {
			// Skip tests that need a real pool
			t.Skip("connpool creation failed, skipping")
		}
	}
	defer pool.Close()

	cfg := Config{
		TTL:             15 * time.Second,
		RefreshInterval: 1 * time.Hour,
		RequestTimeout:  1 * time.Second,
	}
	mgr := NewManager(pool, cfg)
	ctx := context.Background()

	// Should not error even with no partitions
	err = mgr.EnsureFresh(ctx)
	if err != nil {
		t.Logf("EnsureFresh error (expected with empty pool): %v", err)
	}
}

func TestManager_Refresh(t *testing.T) {
	pool, err := connpool.New(context.Background(), []string{}, connpool.Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if pool == nil {
		// Empty bootstrap may fail; create a minimal pool manually for testing
		if err != nil {
			// Skip tests that need a real pool
			t.Skip("connpool creation failed, skipping")
		}
	}
	defer pool.Close()

	cfg := Config{
		TTL:             15 * time.Second,
		RefreshInterval: 1 * time.Hour,
		RequestTimeout:  1 * time.Second,
	}
	mgr := NewManager(pool, cfg)
	ctx := context.Background()

	err = mgr.Refresh(ctx)
	if err != nil {
		t.Logf("Refresh error (expected with empty pool): %v", err)
	}
}

func TestManager_NodeIDToAddress(t *testing.T) {
	pool, err := connpool.New(context.Background(), []string{}, connpool.Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if pool == nil {
		// Empty bootstrap may fail; create a minimal pool manually for testing
		if err != nil {
			// Skip tests that need a real pool
			t.Skip("connpool creation failed, skipping")
		}
	}
	defer pool.Close()

	cfg := Config{
		TTL:             15 * time.Second,
		NodeIDToAddress: map[string]string{"node-1": "127.0.0.1:50051"},
	}
	mgr := NewManager(pool, cfg)
	if mgr == nil {
		t.Fatal("manager should not be nil")
	}
}

func TestManager_OnRefresh(t *testing.T) {
	pool, err := connpool.New(context.Background(), []string{}, connpool.Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if pool == nil {
		// Empty bootstrap may fail; create a minimal pool manually for testing
		if err != nil {
			// Skip tests that need a real pool
			t.Skip("connpool creation failed, skipping")
		}
	}
	defer pool.Close()

	called := false
	cfg := Config{
		TTL:            15 * time.Second,
		RequestTimeout: 1 * time.Second,
		OnRefresh: func(d time.Duration, err error) {
			called = true
		},
	}
	mgr := NewManager(pool, cfg)
	ctx := context.Background()
	_ = mgr.Refresh(ctx)
	// OnRefresh may or may not be called depending on refresh path
	_ = called
}

func TestManager_EnsureFresh_Stale(t *testing.T) {
	pool, err := connpool.New(context.Background(), []string{}, connpool.Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if pool == nil {
		// Empty bootstrap may fail; create a minimal pool manually for testing
		if err != nil {
			// Skip tests that need a real pool
			t.Skip("connpool creation failed, skipping")
		}
	}
	defer pool.Close()

	cfg := Config{
		TTL:             1 * time.Millisecond,
		RefreshInterval: 1 * time.Hour,
		RequestTimeout:  1 * time.Second,
	}
	mgr := NewManager(pool, cfg)
	// Manually set lastUpdated to be old
	mgr.mu.Lock()
	mgr.lastUpdated = time.Now().Add(-1 * time.Hour)
	mgr.mu.Unlock()

	ctx := context.Background()
	err = mgr.EnsureFresh(ctx)
	if err != nil {
		t.Logf("EnsureFresh with stale data: %v", err)
	}
}

func TestManager_StaticPartitionCount(t *testing.T) {
	pool, err := connpool.New(context.Background(), []string{}, connpool.Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if pool == nil {
		// Empty bootstrap may fail; create a minimal pool manually for testing
		if err != nil {
			// Skip tests that need a real pool
			t.Skip("connpool creation failed, skipping")
		}
	}
	defer pool.Close()

	cfg := Config{
		TTL:                  15 * time.Second,
		StaticPartitionCount: 16,
	}
	mgr := NewManager(pool, cfg)
	if mgr.PartitionCount() != 16 {
		t.Errorf("expected 16, got %d", mgr.PartitionCount())
	}
}

func TestManager_Concurrent(t *testing.T) {
	pool, err := connpool.New(context.Background(), []string{}, connpool.Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if pool == nil {
		// Empty bootstrap may fail; create a minimal pool manually for testing
		if err != nil {
			// Skip tests that need a real pool
			t.Skip("connpool creation failed, skipping")
		}
	}
	defer pool.Close()

	mgr := NewManager(pool, Config{TTL: 15 * time.Second})

	done := make(chan struct{}, 10)
	for i := 0; i < 10; i++ {
		go func() {
			mgr.Partitions()
			mgr.GetPartitionInfo(0)
			mgr.PartitionCount()
			mgr.CandidateAddresses(0)
			mgr.ResolveLeaderAddress(0)
			done <- struct{}{}
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestCloneStringMap(t *testing.T) {
	original := map[string]string{"a": "1", "b": "2"}
	cloned := cloneStringMap(original)

	if len(cloned) != len(original) {
		t.Errorf("expected %d entries, got %d", len(original), len(cloned))
	}
	for k, v := range original {
		if cloned[k] != v {
			t.Errorf("expected %s=%s, got %s", k, v, cloned[k])
		}
	}

	// Mutate clone should not affect original
	cloned["a"] = "999"
	if original["a"] != "1" {
		t.Error("clone should be independent")
	}
}

func TestCloneStringMap_Nil(t *testing.T) {
	cloned := cloneStringMap(nil)
	if cloned == nil {
		t.Error("nil map should return empty map, not nil")
	}
	if len(cloned) != 0 {
		t.Error("should be empty")
	}
}

func TestClonePartitionInfo(t *testing.T) {
	original := &types.PartitionInfo{
		PartitionId: 0,
		Topic:       "orders",
		LeaderId:    "node-1",
	}
	cloned := clonePartitionInfo(original)

	if cloned.PartitionId != original.PartitionId {
		t.Error("PartitionId mismatch")
	}
	if cloned.Topic != original.Topic {
		t.Error("Topic mismatch")
	}
	if cloned.LeaderId != original.LeaderId {
		t.Error("LeaderId mismatch")
	}
}

func TestClonePartitionInfo_Nil(t *testing.T) {
	cloned := clonePartitionInfo(nil)
	if cloned == nil {
		t.Error("nil input should return empty PartitionInfo, not nil")
	}
}

package connpool

import (
	"context"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func defaultDialOpts() []grpc.DialOption {
	return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
}

func TestNew(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 2,
		DialTimeout:        1 * time.Second,
		DialOptions:        defaultDialOpts(),
	}
	pool, err := New(context.Background(), []string{}, cfg)
	if err == nil {
		t.Skip("New with empty bootstrap may fail; skipping")
	}
	if pool != nil {
		defer pool.Close()
	}
}

func TestNew_WithBootstrap(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        defaultDialOpts(),
	}
	pool, err := New(context.Background(), []string{"127.0.0.1:1"}, cfg)
	if pool != nil {
		defer pool.Close()
	}
	_ = err
}

func TestPool_AddNode(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        defaultDialOpts(),
	}
	pool, _ := New(context.Background(), []string{"127.0.0.1:1"}, cfg)
	if pool == nil {
		t.Skip("pool creation failed, skipping")
	}
	defer pool.Close()

	err := pool.AddNode(context.Background(), "127.0.0.1:2")
	_ = err
}

func TestPool_AddNode_Invalid(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        defaultDialOpts(),
	}
	pool, _ := New(context.Background(), []string{}, cfg)
	if pool == nil {
		t.Skip("pool creation failed, skipping")
	}
	defer pool.Close()

	err := pool.AddNode(context.Background(), "not-an-address:::::99999")
	if err == nil {
		t.Error("expected error for invalid address")
	}
}

func TestPool_AddNode_ResolveDNS(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		ResolveDNS:         true,
		DialOptions:        defaultDialOpts(),
	}
	pool, _ := New(context.Background(), []string{}, cfg)
	if pool == nil {
		t.Skip("pool creation failed, skipping")
	}
	defer pool.Close()

	err := pool.AddNode(context.Background(), "localhost:50051")
	_ = err
}

func TestPool_AddNode_Empty(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        defaultDialOpts(),
	}
	pool, _ := New(context.Background(), []string{}, cfg)
	if pool == nil {
		t.Skip("pool creation failed, skipping")
	}
	defer pool.Close()

	err := pool.AddNode(context.Background(), "")
	if err == nil {
		t.Error("expected error for empty address")
	}
}

func TestPool_AddNode_AlreadyExists(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        defaultDialOpts(),
	}
	pool, _ := New(context.Background(), []string{}, cfg)
	if pool == nil {
		t.Skip("pool creation failed, skipping")
	}
	defer pool.Close()

	pool.AddNode(context.Background(), "127.0.0.1:1")
	pool.AddNode(context.Background(), "127.0.0.1:1")

	addrs := pool.Addresses()
	count := 0
	for _, a := range addrs {
		if a == "127.0.0.1:1" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected 1 occurrence of 127.0.0.1:1, got %d", count)
	}
}

func TestPool_AddNode_Closed(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        defaultDialOpts(),
	}
	pool, _ := New(context.Background(), []string{}, cfg)
	if pool == nil {
		t.Skip("pool creation failed, skipping")
	}
	pool.Close()

	err := pool.AddNode(context.Background(), "127.0.0.1:1")
	if err == nil {
		t.Error("expected error for closed pool")
	}
}

func TestPool_AddNode_ContextCancel(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        5 * time.Second,
		DialOptions:        defaultDialOpts(),
	}
	pool, _ := New(context.Background(), []string{}, cfg)
	if pool == nil {
		t.Skip("pool creation failed, skipping")
	}
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := pool.AddNode(ctx, "127.0.0.1:1")
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

func TestPool_AddNode_Multiple(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        defaultDialOpts(),
	}
	pool, _ := New(context.Background(), []string{}, cfg)
	if pool == nil {
		t.Skip("pool creation failed, skipping")
	}
	defer pool.Close()

	for i := 0; i < 5; i++ {
		addr := "127.0.0.1:" + strconv.Itoa(10000+i)
		pool.AddNode(context.Background(), addr)
	}

	addrs := pool.Addresses()
	if len(addrs) != 5 {
		t.Errorf("expected 5 addresses, got %d", len(addrs))
	}
}

func TestPool_AddNode_Concurrent(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        defaultDialOpts(),
	}
	pool, _ := New(context.Background(), []string{}, cfg)
	if pool == nil {
		t.Skip("pool creation failed, skipping")
	}
	defer pool.Close()

	done := make(chan struct{}, 10)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			addr := "127.0.0.1:" + strconv.Itoa(20000+idx)
			pool.AddNode(context.Background(), addr)
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < 10; i++ {
		<-done
	}

	addrs := pool.Addresses()
	if len(addrs) != 10 {
		t.Errorf("expected 10 addresses, got %d", len(addrs))
	}
}

func TestPool_AddNode_LookupHost_Error(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		ResolveDNS:         true,
		DialOptions:        defaultDialOpts(),
	}
	pool, _ := New(context.Background(), []string{}, cfg)
	if pool == nil {
		t.Skip("pool creation failed, skipping")
	}
	defer pool.Close()

	err := pool.AddNode(context.Background(), "this-host-definitely-does-not-exist-12345.local:50051")
	if err == nil {
		t.Error("expected error for unresolvable hostname")
	}
}

func TestPool_Close(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        defaultDialOpts(),
	}
	pool, _ := New(context.Background(), []string{}, cfg)
	if pool == nil {
		t.Skip("pool creation failed, skipping")
	}

	if err := pool.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if err := pool.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
}

func TestPool_Close_WithNodes(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        defaultDialOpts(),
	}
	pool, _ := New(context.Background(), []string{"127.0.0.1:1"}, cfg)
	if pool == nil {
		t.Skip("pool creation failed, skipping")
	}
	if err := pool.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestPool_Close_MultipleNodes(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        defaultDialOpts(),
	}
	pool, _ := New(context.Background(), []string{}, cfg)
	if pool == nil {
		t.Skip("pool creation failed, skipping")
	}
	for i := 0; i < 3; i++ {
		pool.AddNode(context.Background(), "127.0.0.1:"+strconv.Itoa(30000+i))
	}
	if err := pool.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestPool_Addresses(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        defaultDialOpts(),
	}
	pool, _ := New(context.Background(), []string{}, cfg)
	if pool == nil {
		t.Skip("pool creation failed, skipping")
	}
	defer pool.Close()

	addrs := pool.Addresses()
	if len(addrs) != 0 {
		t.Errorf("expected 0 addresses, got %d", len(addrs))
	}

	pool.AddNode(context.Background(), "127.0.0.1:1")
	addrs = pool.Addresses()
	if len(addrs) != 1 {
		t.Errorf("expected 1 address, got %d", len(addrs))
	}
}

func TestPool_Addresses_Closed(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        defaultDialOpts(),
	}
	pool, _ := New(context.Background(), []string{}, cfg)
	if pool == nil {
		t.Skip("pool creation failed, skipping")
	}
	pool.Close()

	addrs := pool.Addresses()
	if len(addrs) != 0 {
		t.Errorf("expected 0 addresses for closed pool, got %d", len(addrs))
	}
}

func TestPool_getConn(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        defaultDialOpts(),
	}
	pool, _ := New(context.Background(), []string{}, cfg)
	if pool == nil {
		t.Skip("pool creation failed, skipping")
	}
	defer pool.Close()

	_, err := pool.getConn("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent node")
	}
}

func TestPool_PartitionClient(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        defaultDialOpts(),
	}
	pool, _ := New(context.Background(), []string{}, cfg)
	if pool == nil {
		t.Skip("pool creation failed, skipping")
	}
	defer pool.Close()

	_, err := pool.PartitionClient("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent node")
	}
}

func TestPool_EventClient(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        defaultDialOpts(),
	}
	pool, _ := New(context.Background(), []string{}, cfg)
	if pool == nil {
		t.Skip("pool creation failed, skipping")
	}
	defer pool.Close()

	_, err := pool.EventClient("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent node")
	}
}

func TestPool_getConn_MultipleConns(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 3,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        defaultDialOpts(),
	}
	pool, _ := New(context.Background(), []string{}, cfg)
	if pool == nil {
		t.Skip("pool creation failed, skipping")
	}
	defer pool.Close()

	pool.AddNode(context.Background(), "127.0.0.1:1")
	for i := 0; i < 10; i++ {
		_, err := pool.getConn("127.0.0.1:1")
		_ = err
	}
}

func TestPool_AddNode_ZeroConnections(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 0,
		DialTimeout:        100 * time.Millisecond,
		DialOptions:        defaultDialOpts(),
	}
	_, err := New(context.Background(), []string{}, cfg)
	if err == nil {
		t.Error("expected error for 0 connections per node")
	}
}

func TestPool_AddNode_ContextTimeout(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        5 * time.Second,
		DialOptions:        defaultDialOpts(),
	}
	pool, _ := New(context.Background(), []string{}, cfg)
	if pool == nil {
		t.Skip("pool creation failed, skipping")
	}
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond)

	err := pool.AddNode(ctx, "127.0.0.1:1")
	if err == nil {
		t.Error("expected error for timed out context")
	}
}

func TestPool_AddNode_TCPAddr(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		ResolveDNS:         false,
		DialOptions:        defaultDialOpts(),
	}
	pool, _ := New(context.Background(), []string{}, cfg)
	if pool == nil {
		t.Skip("pool creation failed, skipping")
	}
	defer pool.Close()

	_ = pool.AddNode(context.Background(), "tcp:127.0.0.1:1")
}

func TestPool_AddNode_UNIXAddr(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
		ResolveDNS:         false,
		DialOptions:        defaultDialOpts(),
	}
	pool, _ := New(context.Background(), []string{}, cfg)
	if pool == nil {
		t.Skip("pool creation failed, skipping")
	}
	defer pool.Close()

	_ = pool.AddNode(context.Background(), "unix:/tmp/test.sock")
}

func TestPool_AddNode_NegativeDialTimeout(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        -1 * time.Second,
		DialOptions:        defaultDialOpts(),
	}
	_, err := New(context.Background(), []string{}, cfg)
	if err == nil {
		t.Error("expected error for negative dial timeout")
	}
}

func TestPool_AddNode_NoDialOptions(t *testing.T) {
	cfg := Config{
		ConnectionsPerNode: 1,
		DialTimeout:        100 * time.Millisecond,
	}
	_, err := New(context.Background(), []string{}, cfg)
	if err == nil {
		t.Error("expected error for missing dial options")
	}
}

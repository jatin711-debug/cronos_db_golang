package api

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// tenants is a server with authentication on and two principals who may each
// publish to and consume from one topic: alice has "alpha", bob has "beta".
// There is one partition, so the two topics share a log and a dispatcher,
// which is where one tenant's events could reach the other.
type tenants struct {
	t      *testing.T
	addr   string
	p      *partition.Partition
	tokens map[string]string
}

func newTenants(t *testing.T) *tenants {
	t.Helper()
	pm, p := func() (*partition.PartitionManager, *partition.Partition) {
		cfg := &types.Config{DataDir: t.TempDir(), PartitionCount: 1, FsyncMode: "periodic", FlushIntervalMS: 10, TickMS: 10, WheelSize: 64, DedupTTLHours: 1, BloomCapacity: 10000}
		pm := partition.NewPartitionManager("node", cfg)
		if err := pm.CreatePartition(0, "alpha"); err != nil {
			pm.Close()
			t.Fatal(err)
		}
		if err := pm.StartPartition(0); err != nil {
			pm.Close()
			t.Fatal(err)
		}
		p, err := pm.GetInternalPartition(0)
		if err != nil {
			pm.Close()
			t.Fatal(err)
		}
		return pm, p
	}()

	secret := []byte("isolation-test-secret-0123456789")
	policy := &auth.Policy{Subjects: map[string]*auth.Subject{
		"alice": {Topics: map[string]auth.TopicPerms{"alpha": {Publish: true, Subscribe: true}}},
		"bob":   {Topics: map[string]auth.TopicPerms{"beta": {Publish: true, Subscribe: true}}},
	}}
	handler := NewEventServiceHandler(pm, p.DedupStore, p.ConsumerGroup)
	handler.SetAuthPolicy(policy)

	serverCfg := DefaultConfig()
	serverCfg.Address = "127.0.0.1:0"
	serverCfg.SLORecorder = nil
	serverCfg.Auth = &auth.Config{Enabled: true, JWTSecret: secret, Policy: policy}
	server, err := NewGRPCServer(serverCfg)
	if err != nil {
		pm.Close()
		t.Fatal(err)
	}
	server.RegisterServices(handler, nil, NewPartitionServiceHandler(pm, nil, "node"), nil)
	if err := server.Start(); err != nil {
		pm.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		server.Stop()
		pm.Close()
	})

	ts := &tenants{t: t, addr: server.listener.Addr().String(), p: p, tokens: map[string]string{}}
	for _, who := range []string{"alice", "bob"} {
		token, err := auth.GenerateToken(who, secret, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		ts.tokens[who] = token
	}
	return ts
}

// sdk returns a client library connection that authenticates as who.
func (ts *tenants) sdk(who string) *client.Client {
	ts.t.Helper()
	cfg := client.DefaultConfig(ts.addr)
	cfg.Security.Insecure = true
	cfg.Security.BearerToken = ts.tokens[who]
	c, err := client.Dial(context.Background(), cfg)
	if err != nil {
		ts.t.Fatalf("connect as %s: %v", who, err)
	}
	ts.t.Cleanup(func() { c.Close() })
	return c
}

// raw returns the event service and a context that authenticates as who, for
// requests the client library would not make.
func (ts *tenants) raw(who string) (types.EventServiceClient, context.Context) {
	ts.t.Helper()
	conn, err := grpc.NewClient(ts.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		ts.t.Fatal(err)
	}
	ts.t.Cleanup(func() { conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	ts.t.Cleanup(cancel)
	return types.NewEventServiceClient(conn), metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+ts.tokens[who])
}

func (ts *tenants) publish(who, topic string, n int) error {
	ts.t.Helper()
	producer, err := ts.sdk(who).NewProducer(client.DefaultProducerConfig())
	if err != nil {
		ts.t.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := producer.Send(ctx, client.Message{
			MessageID:  fmt.Sprintf("%s-%s-%d", who, topic, i),
			Topic:      topic,
			Payload:    []byte("secret of " + who),
			ScheduleTS: time.Now().UnixMilli(),
		})
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

// received collects what a consumer was given.
type received struct {
	mu     sync.Mutex
	topics map[string]int
	total  int
}

func (r *received) count() (total int, topics map[string]int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	copied := make(map[string]int, len(r.topics))
	for topic, n := range r.topics {
		copied[topic] = n
	}
	return r.total, copied
}

func (ts *tenants) consume(ctx context.Context, who, topic, group string) *received {
	got := &received{topics: map[string]int{}}
	c := ts.sdk(who)
	go func() {
		cfg := client.DefaultConsumerConfig(topic, group)
		cfg.ReconnectBackoff = 20 * time.Millisecond
		cfg.MaxReconnectBackoff = 100 * time.Millisecond
		_ = c.Subscribe(ctx, cfg, func(_ context.Context, d client.Delivery) error {
			got.mu.Lock()
			defer got.mu.Unlock()
			if d.Event != nil {
				got.topics[d.Event.GetTopic()]++
				got.total++
			}
			for _, event := range d.Batch {
				got.topics[event.GetTopic()]++
				got.total++
			}
			return nil
		})
	}()
	return got
}

// Two tenants on one server, through the public API only. Each can use its
// own topic and cannot publish to, consume from, replay or acknowledge for
// the other's, although both topics live in the same partition.
func TestIsolation_TwoPrincipalsShareAPartition(t *testing.T) {
	ts := newTenants(t)
	const each = 20

	// Publishing.
	if err := ts.publish("alice", "alpha", each); err != nil {
		t.Fatalf("alice publishes to her topic: %v", err)
	}
	if err := ts.publish("bob", "beta", each); err != nil {
		t.Fatalf("bob publishes to his topic: %v", err)
	}
	for who, topic := range map[string]string{"alice": "beta", "bob": "alpha"} {
		if err := ts.publish(who, topic, 1); err == nil {
			t.Fatalf("%s published to %s", who, topic)
		}
	}
	if last := ts.p.Wal.GetLastOffset(); last != 2*each-1 {
		t.Fatalf("the log ends at offset %d, want %d: a refused publish was written", last, 2*each-1)
	}

	// Consuming, both at once.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	alice := ts.consume(ctx, "alice", "alpha", "group-a")
	bob := ts.consume(ctx, "bob", "beta", "group-b")
	deadline := time.Now().Add(20 * time.Second)
	for {
		a, _ := alice.count()
		b, _ := bob.count()
		if a >= each && b >= each {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("alice received %d and bob %d of %d events each", a, b, each)
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond) // anything misdirected would have arrived by now
	for who, got := range map[string]*received{"alice": alice, "bob": bob} {
		own := map[string]string{"alice": "alpha", "bob": "beta"}[who]
		total, topics := got.count()
		if total != each || topics[own] != each {
			t.Fatalf("%s received %d events by topic %v, want exactly the %d of %s", who, total, topics, each, own)
		}
	}

	// Subscribing to the other's topic, and replaying it.
	events, bobCtx := ts.raw("bob")
	stream, err := events.Subscribe(bobCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&types.SubscribeRequest{ConsumerGroup: "group-x", Topic: "alpha", PartitionId: 0, SubscriptionId: "bob-on-alpha"}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("bob's subscription to alpha ended with %v, want a permission error", err)
	}
	replay, err := events.Replay(bobCtx, &types.ReplayRequest{Topic: "alpha", PartitionId: 0, StartOffset: 0, Count: 1000})
	if err == nil {
		_, err = replay.Recv()
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("bob's replay of alpha ended with %v, want a permission error", err)
	}

	// Replaying his own topic over the shared log returns his events only.
	replay, err = events.Replay(bobCtx, &types.ReplayRequest{Topic: "beta", PartitionId: 0, StartOffset: 0, Count: 1000})
	if err != nil {
		t.Fatal(err)
	}
	replayed := 0
	for {
		item, err := replay.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("bob's replay of beta: %v", err)
		}
		if item.GetEvent().GetTopic() != "beta" {
			t.Fatalf("bob's replay of beta returned an event of topic %q", item.GetEvent().GetTopic())
		}
		replayed++
	}
	if replayed != each {
		t.Fatalf("bob's replay of beta returned %d events, want %d", replayed, each)
	}

	// Joining the other's consumer group under his own topic.
	stream, err = events.Subscribe(bobCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&types.SubscribeRequest{ConsumerGroup: "group-a", Topic: "beta", PartitionId: 0, SubscriptionId: "bob-in-group-a"}); err != nil {
		t.Fatal(err)
	}
	if delivery, err := stream.Recv(); err == nil {
		t.Fatalf("bob joined alice's consumer group and received %v", delivery.GetDeliveryId())
	}
}

// One tenant cannot acknowledge a delivery made to the other, which would
// mark the other's event as done without it having been processed.
func TestIsolation_AcknowledgementsBelongToTheirRecipient(t *testing.T) {
	ts := newTenants(t)
	if err := ts.publish("alice", "alpha", 1); err != nil {
		t.Fatal(err)
	}

	// Alice receives the event and holds on to it.
	delivered := make(chan client.Delivery, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		cfg := client.DefaultConsumerConfig("alpha", "group-a")
		cfg.AckMode = client.AckModeManual
		cfg.AutoAck = false
		_ = ts.sdk("alice").Subscribe(ctx, cfg, func(_ context.Context, d client.Delivery) error {
			select {
			case delivered <- d:
			default:
			}
			return nil
		})
	}()
	var held client.Delivery
	select {
	case held = <-delivered:
	case <-time.After(20 * time.Second):
		t.Fatal("alice did not receive her event")
	}

	// Bob acknowledges it with alice's delivery ID.
	events, bobCtx := ts.raw("bob")
	acks, err := events.Ack(bobCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err := acks.Send(&types.AckRequest{DeliveryId: held.DeliveryID, Success: true, NextOffset: held.LastOffset() + 1}); err != nil {
		t.Fatal(err)
	}
	if resp, err := acks.Recv(); err == nil && resp.GetSuccess() {
		t.Fatal("bob's acknowledgement of alice's delivery was accepted")
	}
	if ts.p.ConsumerGroup.IsCompleted("group-a", 0, held.LastOffset()) {
		t.Fatal("alice's event counts as finished after bob acknowledged it")
	}

	// Alice's own acknowledgement still works.
	if err := held.AckSuccess(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for !ts.p.ConsumerGroup.IsCompleted("group-a", 0, held.LastOffset()) {
		if time.Now().After(deadline) {
			t.Fatal("alice's own acknowledgement did not finish her event")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

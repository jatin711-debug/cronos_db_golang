package client

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client/internal/circuitbreaker"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// clusterNode stands in for one node of a cluster in which every node holds
// partition 0 and one leads it. A node that does not lead answers the way the
// server does: FailedPrecondition to a publish, the same text inside the
// response to a batch, and an error to a subscription.
type clusterNode struct {
	types.UnimplementedEventServiceServer
	name  string
	leads atomic.Bool

	publishCalls atomic.Int64
	batchCalls   atomic.Int64
	nextOffset   atomic.Int64

	mu            sync.Mutex
	subscriptions []string
	// endCleanly makes the node end its subscriptions without an error when
	// it stops leading, as a server that shuts down does.
	endCleanly bool
}

func (n *clusterNode) notLeader() error {
	return status.Error(codes.FailedPrecondition,
		"partition 0 is local but this node is not the leader; retry against the partition leader")
}

func (n *clusterNode) Publish(_ context.Context, req *types.PublishRequest) (*types.PublishResponse, error) {
	n.publishCalls.Add(1)
	if !n.leads.Load() {
		return nil, n.notLeader()
	}
	return &types.PublishResponse{Success: true, PartitionId: 0, Offset: n.nextOffset.Add(1) - 1}, nil
}

func (n *clusterNode) PublishBatch(_ context.Context, req *types.PublishBatchRequest) (*types.PublishBatchResponse, error) {
	n.batchCalls.Add(1)
	count := int64(len(req.GetEvents()))
	if !n.leads.Load() {
		return &types.PublishBatchResponse{Error: n.notLeader().Error(), ErrorCount: int32(count)}, nil
	}
	last := n.nextOffset.Add(count) - 1
	return &types.PublishBatchResponse{Success: true, PublishedCount: int32(count), FirstOffset: last - count + 1, LastOffset: last}, nil
}

func (n *clusterNode) Subscribe(stream grpc.BidiStreamingServer[types.SubscribeRequest, types.Delivery]) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	if !n.leads.Load() {
		return n.notLeader()
	}
	n.mu.Lock()
	n.subscriptions = append(n.subscriptions, req.GetSubscriptionId())
	n.mu.Unlock()

	offset := n.nextOffset.Add(1) - 1
	err = stream.Send(&types.Delivery{
		DeliveryId: fmt.Sprintf("%s:0:%d", req.GetConsumerGroup(), offset),
		Event:      &types.Event{MessageId: fmt.Sprintf("%s-%d", n.name, offset), Topic: req.GetTopic(), Offset: offset},
	})
	if err != nil {
		return err
	}
	for n.leads.Load() {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
	n.mu.Lock()
	clean := n.endCleanly
	n.mu.Unlock()
	if clean {
		return nil
	}
	return n.notLeader()
}

func (n *clusterNode) Ack(stream grpc.BidiStreamingServer[types.AckRequest, types.AckResponse]) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		if err := stream.Send(&types.AckResponse{Success: true, CommittedOffset: req.GetNextOffset()}); err != nil {
			return err
		}
	}
}

// twoNodes starts two nodes, the second of which leads, and a client that was
// given their addresses and nothing else: partition metadata names the leader
// by node ID, which says nothing about where to find it.
func twoNodes(t *testing.T) (a, b *clusterNode, addrA, addrB string, cl *Client) {
	t.Helper()
	a, b = &clusterNode{name: "a"}, &clusterNode{name: "b"}
	b.leads.Store(true)
	addrA = startTestEventServer(t, a, &testPartitionServer{})
	addrB = startTestEventServer(t, b, &testPartitionServer{})
	cfg := DefaultConfig(addrA, addrB)
	// One failure closes a node off for a minute, so a test notices any
	// answer that is wrongly counted as one.
	cfg.CircuitBreaker = circuitbreaker.Config{FailureThreshold: 1, SuccessThreshold: 1, Timeout: time.Minute}
	cl, err := Dial(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	return a, b, addrA, addrB, cl
}

// A publish that reaches a node which does not lead the partition goes on to
// the node that does, and the next one goes there directly.
func TestProducer_FindsAndRemembersThePartitionLeader(t *testing.T) {
	a, b, addrA, addrB, cl := twoNodes(t)
	producer, err := cl.NewProducer(DefaultProducerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	partition := int32(0)
	send := func(id string) *SendResult {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result, err := producer.Send(ctx, Message{MessageID: id, Topic: "orders", Payload: []byte(id), PartitionID: &partition, ScheduleAt: time.Now().Add(time.Second)})
		if err != nil {
			t.Fatalf("publish %s: %v", id, err)
		}
		return result
	}

	if result := send("first"); result.NodeAddress != addrB {
		t.Fatalf("published through %s, want the leader %s", result.NodeAddress, addrB)
	}
	asked := a.publishCalls.Load()
	for i := 0; i < 5; i++ {
		send(fmt.Sprintf("next-%d", i))
	}
	if got := a.publishCalls.Load(); got != asked {
		t.Fatalf("the node that does not lead was asked %d more times after the leader was known", got-asked)
	}

	// The partition moves to the other node.
	b.leads.Store(false)
	a.leads.Store(true)
	if result := send("after-the-move"); result.NodeAddress != addrA {
		t.Fatalf("published through %s after the move, want %s", result.NodeAddress, addrA)
	}
	asked = b.publishCalls.Load()
	// Being told "not the leader" must not have closed the node off: it
	// takes publishes the moment it leads.
	for i := 0; i < 10; i++ {
		send(fmt.Sprintf("moved-%d", i))
	}
	if got := b.publishCalls.Load(); got != asked {
		t.Fatalf("the old leader was asked %d more times after the new one was known", got-asked)
	}
}

// A batch that reaches a node which does not lead the partition is refused as
// a whole. It goes on to the leader, as a batch.
func TestProducer_BatchGoesOnToThePartitionLeader(t *testing.T) {
	a, b, _, addrB, cl := twoNodes(t)
	producer, err := cl.NewProducer(DefaultProducerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	partition := int32(0)
	batch := make([]Message, 4)
	for i := range batch {
		batch[i] = Message{MessageID: fmt.Sprintf("m-%d", i), Topic: "orders", Payload: []byte("x"), PartitionID: &partition, ScheduleAt: time.Now().Add(time.Second)}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := producer.SendBatch(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	if result.PublishedCount != 4 || result.ErrorCount != 0 {
		t.Fatalf("published %d with %d errors, want 4 and 0", result.PublishedCount, result.ErrorCount)
	}
	if b.batchCalls.Load() != 1 || b.publishCalls.Load() != 0 {
		t.Fatalf("the leader got %d batch calls and %d single publishes, want one batch", b.batchCalls.Load(), b.publishCalls.Load())
	}
	for _, sent := range result.Results {
		if sent.NodeAddress != addrB {
			t.Fatalf("%s is reported as published through %s, want %s", sent.MessageID, sent.NodeAddress, addrB)
		}
	}
	if _, err := producer.SendBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if got := a.batchCalls.Load(); got != 1 {
		t.Fatalf("the node that does not lead got %d batch calls, want only the first", got)
	}
}

// A subscription follows its partition. The node it is attached to ending
// the stream, with an error or without one, is not the end of the
// subscription: it continues on the node that leads now.
func TestConsumer_FollowsThePartitionToItsNewLeader(t *testing.T) {
	for _, clean := range []bool{false, true} {
		name := "stream ended with an error"
		if clean {
			name = "stream ended without an error"
		}
		t.Run(name, func(t *testing.T) {
			a, b, _, _, cl := twoNodes(t)
			b.endCleanly = clean

			received := make(chan string, 16)
			cfg := DefaultConsumerConfig("orders", "workers")
			cfg.SubscriptionID = "worker-1"
			cfg.ReconnectBackoff = 10 * time.Millisecond
			cfg.MaxReconnectBackoff = 50 * time.Millisecond
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- cl.Subscribe(ctx, cfg, func(_ context.Context, d Delivery) error {
					received <- d.Event.GetMessageId()
					return nil
				})
			}()
			expect := func(prefix string) {
				t.Helper()
				select {
				case id := <-received:
					if !strings.HasPrefix(id, prefix) {
						t.Fatalf("received %s, want a delivery from node %s", id, prefix)
					}
				case err := <-done:
					t.Fatalf("the subscription ended by itself: %v", err)
				case <-time.After(5 * time.Second):
					t.Fatalf("no delivery from node %s", prefix)
				}
			}

			expect("b-")
			b.leads.Store(false)
			a.leads.Store(true)
			expect("a-")
			a.leads.Store(false)
			b.leads.Store(true)
			expect("b-")

			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("the subscription did not stop")
			}

			// Every connection carries its own ID, so one the server has not
			// noticed is gone cannot refuse the next as a duplicate.
			seen := map[string]bool{}
			for _, node := range []*clusterNode{a, b} {
				node.mu.Lock()
				for _, id := range node.subscriptions {
					if !strings.HasPrefix(id, "worker-1") {
						t.Errorf("subscription ID %q does not carry the configured one", id)
					}
					if seen[id] {
						t.Errorf("subscription ID %q was used for two connections", id)
					}
					seen[id] = true
				}
				node.mu.Unlock()
			}
			if len(seen) != 3 {
				t.Errorf("%d subscriptions were accepted, want 3", len(seen))
			}
		})
	}
}

// Two consumers that leave the subscription ID to the client do not end up
// with the same one, in the same process or in two.
func TestConsumer_DefaultSubscriptionIDsAreUnique(t *testing.T) {
	first := DefaultConsumerConfig("orders", "workers").withDefaults().SubscriptionID
	second := DefaultConsumerConfig("orders", "workers").withDefaults().SubscriptionID
	if first == second {
		t.Fatalf("two consumers got the subscription ID %q", first)
	}
	if !strings.Contains(first, clientInstance) {
		t.Fatalf("subscription ID %q does not identify this process", first)
	}
	if other := newInstanceID(); other == clientInstance || len(other) < 8 {
		t.Fatalf("process IDs %q and %q do not tell two processes apart", clientInstance, other)
	}
}

package delivery

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/consumer"
)

type immediateAckStream struct {
	*mockStream
	ack func(*DeliveryMessage) error
	err error
}

func (s *immediateAckStream) Send(msg *DeliveryMessage) error {
	if err := s.mockStream.Send(msg); err != nil {
		return err
	}
	s.err = s.ack(msg)
	return nil
}

// A live event can become due while a subscription's consumer group is still
// being created. It must wait: otherwise its first ACK is refused because the
// group does not exist, and the delivery stays in flight until its timeout.
func TestDispatcher_FirstDeliveryWaitsForConsumerGroup(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()
	groups := consumer.NewGroupManager()
	stream := &immediateAckStream{mockStream: newMockStream()}
	sub := makeTestSubscription("sub-1", 0, stream.mockStream)
	sub.Stream = stream
	stream.ack = func(msg *DeliveryMessage) error {
		group, _, events, err := d.ValidateAck(msg.DeliveryID, "", 1, true)
		if err != nil {
			return err
		}
		if err := groups.CommitDelivery(group, 0, events); err != nil {
			return err
		}
		return d.HandleAck(msg.DeliveryID, true, 1)
	}

	initializing := make(chan struct{})
	allowJoin := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(allowJoin) })
	defer unblock()
	registered := make(chan error, 1)
	go func() {
		registered <- d.SubscribeWithInit(sub, func() error {
			close(initializing)
			<-allowJoin
			return groups.JoinGroup(sub.ConsumerGroup, sub.ID, "", "test-topic", 0)
		})
	}()
	select {
	case <-initializing:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer group initialization did not start")
	}

	event := makeTestEvent(0, 0, "first")
	if err := d.Dispatch(event); err != nil {
		t.Fatal(err)
	}
	if stream.SendCount() != 0 {
		t.Fatalf("event delivered before its group exists; first ACK: %v", stream.err)
	}

	unblock()
	select {
	case err := <-registered:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("subscription registration did not finish")
	}
	if err := d.Dispatch(event); err != nil {
		t.Fatal(err)
	}
	if stream.err != nil || stream.SendCount() != 1 || !groups.IsCompleted(sub.ConsumerGroup, 0, 0) {
		t.Fatalf("first delivery did not complete: sent=%d, ACK error=%v", stream.SendCount(), stream.err)
	}
	if stats := d.GetStats(); stats.ActiveDeliveries != 0 || stats.CreditsInUse != 0 {
		t.Fatalf("acknowledged delivery still holds credits: %+v", stats)
	}
}

func TestDispatcher_FailedInitializationDoesNotSubscribe(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()
	stream := newMockStream()
	sub := makeTestSubscription("sub-1", 0, stream)
	rejected := errors.New("consumer group belongs to a different topic")
	if err := d.SubscribeWithInit(sub, func() error { return rejected }); !errors.Is(err, rejected) {
		t.Fatalf("registration error: %v, want %v", err, rejected)
	}
	if err := d.Dispatch(makeTestEvent(0, 0, "first")); err != nil {
		t.Fatal(err)
	}
	if stream.SendCount() != 0 || d.GetStats().ActiveSubscriptions != 0 {
		t.Fatal("rejected subscription was exposed to deliveries")
	}
}

func TestDispatcher_DuplicateDoesNotInitializeAgain(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()
	sub := makeTestSubscription("sub-1", 0, newMockStream())
	if err := d.Subscribe(sub); err != nil {
		t.Fatal(err)
	}
	if err := d.SubscribeWithInit(sub, func() error {
		t.Error("duplicate registration changed existing group membership")
		return nil
	}); err == nil {
		t.Fatal("duplicate subscription was accepted")
	}
	if d.GetStats().ActiveSubscriptions != 1 {
		t.Fatal("duplicate registration changed existing subscription")
	}
}

var _ Stream = (*immediateAckStream)(nil)

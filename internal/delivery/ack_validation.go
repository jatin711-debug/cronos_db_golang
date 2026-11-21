package delivery

import (
	"fmt"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// ValidateAck binds an ACK to a currently tracked delivery and its principal.
// Returned records are copied as a slice so dispatch bookkeeping cannot reuse it.
func (d *Dispatcher) ValidateAck(id, subject string, next int64, success bool) (string, string, []*types.Event, error) {
	subID, err := parseSubIDFromDeliveryID(id)
	if err != nil {
		return "", "", nil, err
	}
	shard := d.getShard(subID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	active := shard.activeDeliveries[id]
	if active == nil || active.Subscription.Subject != subject {
		return "", "", nil, fmt.Errorf("unknown delivery or invalid delivery owner")
	}
	events := active.Delivery.Batch
	if active.Delivery.Event != nil {
		events = []*types.Event{active.Delivery.Event}
	}
	expected := int64(0)
	for _, event := range events {
		if event.Offset >= expected {
			expected = event.Offset + 1
		}
	}
	if success && next != expected {
		return "", "", nil, fmt.Errorf("ack offset %d does not match delivered records (want %d)", next, expected)
	}
	return active.Subscription.ConsumerGroup, active.Subscription.Topic, append([]*types.Event(nil), events...), nil
}

// ReleaseAckedCredits gives a subscription back the credits of a delivery its
// consumer has acknowledged, before the completion is recorded, and tells the
// group's subscriptions that there is room again. It does nothing for a
// delivery that is not on its way, or whose credits went back already.
//
// A credit bounds what a consumer holds unacknowledged, and it has
// acknowledged these. Recording the completion is a durable write, and the
// credits used to come back only after it: a consumer was sent its next
// window once the disk had synced the last one, so a group with a window of
// four took four events per sync however fast it was. The delivery itself
// stays on its way until the completion is recorded (HandleAck), so nothing
// hands its events out again in between. If recording fails, it is sent again
// when it times out, as before, and holds no credits then.
func (d *Dispatcher) ReleaseAckedCredits(deliveryID string) {
	subID, err := parseSubIDFromDeliveryID(deliveryID)
	if err != nil {
		return
	}
	shard := d.getShard(subID)
	shard.mu.Lock()
	active := shard.activeDeliveries[deliveryID]
	credits := int32(0)
	if active != nil {
		// Taken here, under the lock that guards the delivery while it is on
		// its way: whoever ends it later finds none to give back.
		credits, active.CreditsConsumed = active.CreditsConsumed, 0
	}
	shard.mu.Unlock()
	if credits == 0 {
		return
	}
	d.releaseCredits(active.Subscription, credits)
	d.wakeRedrive(active.Subscription.ConsumerGroup)
}

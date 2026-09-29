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

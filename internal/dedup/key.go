package dedup

import (
	"strconv"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// Key is what the store records for a message ID published to topic. A
// message ID is unique within its topic: the same ID on two topics names two
// events.
//
// IDs used to be recorded per partition alone, and a publish is placed by its
// message ID unless it names a partition key, so the same ID on two topics met
// in one partition. The second publish was refused as a duplicate, and a
// principal allowed to publish to one topic could block IDs used on another,
// or learn that they were in use.
//
// The topic's length comes first, so that no topic and ID run together into
// the key of another pair.
func Key(topic, messageID string) string {
	return strconv.Itoa(len(topic)) + ":" + topic + messageID
}

// EventKey is Key for an event.
func EventKey(e *types.Event) string {
	return Key(e.GetTopic(), e.GetMessageId())
}

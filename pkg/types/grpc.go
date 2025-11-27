package types

import "context"

// ============================================================================
// Publish Types
// ============================================================================

// PublishRequest is used to publish an event
type PublishRequest struct {
	Event         *Event
	AllowDuplicate bool
}

// PublishResponse is the response from a publish operation
type PublishResponse struct {
	Success       bool
	Error         string
	Offset        int64
	PartitionID   int32
	ScheduleTS    int64
}

// ============================================================================
// Subscribe Types
// ============================================================================

// SubscribeRequest is used to subscribe to events
type SubscribeRequest struct {
	ConsumerGroup  string
	Topic          string
	PartitionID    int32
	StartOffset    int64
	MaxBufferSize  int32
	SubscriptionID string
	Replay         bool
}

// EventService_SubscribeServer is an interface for streaming events
type EventService_SubscribeServer interface {
	Send(*Delivery) error
	Context() context.Context
}

// ============================================================================
// Ack Types
// ============================================================================

// AckRequest is used to acknowledge event processing
type AckRequest struct {
	DeliveryID  string
	Success     bool
	Error       string
	NextOffset  int64
}

// AckResponse is the response from an ack operation
type AckResponse struct {
	Success          bool
	Error            string
	CommittedOffset  int64
}

// ============================================================================
// Replay Types
// ============================================================================

// ReplayEvent represents an event being replayed
type ReplayEvent struct {
	Event        *Event
	ReplayOffset int64
}

// EventService_ReplayServer is an interface for streaming replay events
type EventService_ReplayServer interface {
	Send(*ReplayEvent) error
	Context() context.Context
}

// ============================================================================
// Partition Manager Interface
// ============================================================================

// PartitionManager interface
type PartitionManager interface {
	GetPartitionForTopic(topic string) (*Partition, error)
	GetPartition(partitionID int32) (*Partition, error)
}

// EventServiceServer is the interface for the EventService
type EventServiceServer interface {
	Publish(context.Context, *PublishRequest) (*PublishResponse, error)
	Subscribe(*SubscribeRequest, EventService_SubscribeServer) error
	Ack(*AckRequest) (*AckResponse, error)
	Replay(*ReplayRequest, EventService_ReplayServer) error
}

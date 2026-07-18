package client

import "time"

// Hooks receives low-overhead client instrumentation events.
// Implementations must be safe for concurrent use and should not block.
type Hooks interface {
	// OnMetadataRefresh is called after a metadata refresh attempt completes.
	OnMetadataRefresh(duration time.Duration, err error)
	// OnRequest is called after a client RPC completes (success or failure).
	OnRequest(op string, nodeAddr string, duration time.Duration, err error)
}

// ErrorHook is an optional hook for typed error observation.
// Clients that implement both Hooks and ErrorHook receive OnError callbacks.
type ErrorHook interface {
	// OnError is called when the client classifies a failure with ErrorKind.
	OnError(op string, kind ErrorKind, err error)
}

// ProducerStateHook is an optional hook for producer queue depth signals.
type ProducerStateHook interface {
	// OnProducerState reports async queue depth, queued payload bytes, and in-flight sends.
	OnProducerState(queueDepth int, queuedBytes int64, inFlight int)
}

// ConsumerStateHook is an optional hook for consumer queue depth signals.
type ConsumerStateHook interface {
	// OnConsumerState reports delivery and ack queue depths.
	OnConsumerState(deliveryQueueDepth int, ackQueueDepth int)
}

// NopHooks is the default no-op Hooks implementation used when none is configured.
type NopHooks struct{}

// OnMetadataRefresh implements Hooks.
func (NopHooks) OnMetadataRefresh(time.Duration, error) {}

// OnRequest implements Hooks.
func (NopHooks) OnRequest(string, string, time.Duration, error) {}

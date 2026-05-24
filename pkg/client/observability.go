package client

import "time"

// Hooks receives low-overhead client instrumentation events.
type Hooks interface {
	OnMetadataRefresh(duration time.Duration, err error)
	OnRequest(op string, nodeAddr string, duration time.Duration, err error)
}

// ErrorHook is an optional hook for typed error observation.
type ErrorHook interface {
	OnError(op string, kind ErrorKind, err error)
}

// ProducerStateHook is an optional hook for producer queue state.
type ProducerStateHook interface {
	OnProducerState(queueDepth int, queuedBytes int64, inFlight int)
}

// ConsumerStateHook is an optional hook for consumer queue state.
type ConsumerStateHook interface {
	OnConsumerState(deliveryQueueDepth int, ackQueueDepth int)
}

// NopHooks is the default no-op hooks implementation.
type NopHooks struct{}

func (NopHooks) OnMetadataRefresh(time.Duration, error)         {}
func (NopHooks) OnRequest(string, string, time.Duration, error) {}

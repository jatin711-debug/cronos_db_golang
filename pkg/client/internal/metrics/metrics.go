package metrics

import "time"

// Hooks receives low-overhead client instrumentation events.
type Hooks interface {
	OnMetadataRefresh(duration time.Duration, err error)
	OnRequest(op string, nodeAddr string, duration time.Duration, err error)
}

// Noop is the default no-op hooks implementation.
type Noop struct{}

func (Noop) OnMetadataRefresh(time.Duration, error)         {}
func (Noop) OnRequest(string, string, time.Duration, error) {}

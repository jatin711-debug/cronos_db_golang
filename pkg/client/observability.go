package client

import "time"

// Hooks receives low-overhead client instrumentation events.
type Hooks interface {
	OnMetadataRefresh(duration time.Duration, err error)
	OnRequest(op string, nodeAddr string, duration time.Duration, err error)
}

// NopHooks is the default no-op hooks implementation.
type NopHooks struct{}

func (NopHooks) OnMetadataRefresh(time.Duration, error)         {}
func (NopHooks) OnRequest(string, string, time.Duration, error) {}

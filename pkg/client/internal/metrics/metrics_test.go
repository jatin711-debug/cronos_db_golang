package metrics

import (
	"testing"
	"time"
)

func TestNoop_OnMetadataRefresh(t *testing.T) {
	var n Noop
	// Should not panic
	n.OnMetadataRefresh(100*time.Millisecond, nil)
	n.OnMetadataRefresh(0, nil)
}

func TestNoop_OnRequest(t *testing.T) {
	var n Noop
	// Should not panic
	n.OnRequest("publish", "node-1", 50*time.Millisecond, nil)
	n.OnRequest("subscribe", "node-2", 0, nil)
}

func TestNoop_MultipleCalls(t *testing.T) {
	var n Noop
	for i := 0; i < 100; i++ {
		n.OnMetadataRefresh(time.Duration(i)*time.Millisecond, nil)
		n.OnRequest("op", "addr", time.Duration(i)*time.Millisecond, nil)
	}
}

//go:build acceptance

package acceptance

import (
	"errors"
	"fmt"
	"net"
	"testing"
)

func TestPortAllocationRejectsAPreviouslyAssignedPort(t *testing.T) {
	var allocator portAllocator
	listen := func() (net.Listener, error) { return net.Listen("tcp", "127.0.0.1:0") }
	first, err := allocator.take(1, listen)
	if err != nil {
		t.Fatal(err)
	}
	// The initial cluster's sockets have been closed but its nodes have not
	// started yet. Force the OS to offer one of those ports to addNode.
	attempts := 0
	var offered []net.Listener
	next, err := allocator.take(2, func() (net.Listener, error) {
		attempts++
		var l net.Listener
		var err error
		if attempts == 1 {
			l, err = net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", first[0]))
		} else {
			l, err = listen()
		}
		if err == nil {
			offered = append(offered, l)
		}
		return l, err
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, port := range next {
		if port == first[0] {
			t.Fatalf("new node reused port %d assigned to the initial cluster", port)
		}
	}
	if next[0] == next[1] {
		t.Fatalf("new node reused a port within its allocation: %v", next)
	}
	// The rejected listener and the accepted ones all have to be released
	// before the child servers try to bind them.
	for _, l := range offered {
		if err := l.Close(); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("allocator did not close listener %s: %v", l.Addr(), err)
		}
	}
}

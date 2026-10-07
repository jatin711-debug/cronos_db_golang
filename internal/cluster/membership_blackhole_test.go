package cluster

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blackhole stands between two nodes and can stop carrying what they send,
// the way a failed network does: nothing is refused and nothing is closed.
// What is sent into a connection it has cut goes nowhere, for as long as that
// connection lives, also after the network has returned. Connections opened
// after that work.
type blackhole struct {
	listener net.Listener
	target   string

	mu    sync.Mutex
	cut   bool
	conns []*atomic.Bool // one per connection: true once it carries nothing
}

func newBlackhole(t *testing.T, target string) *blackhole {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &blackhole{listener: listener, target: target}
	t.Cleanup(func() { _ = listener.Close() })
	go b.serve()
	return b
}

func (b *blackhole) addr() string { return b.listener.Addr().String() }

// fail cuts every connection there is, for good, and those opened from now
// on. restore lets connections opened from then on through again.
func (b *blackhole) fail() {
	b.mu.Lock()
	b.cut = true
	for _, dead := range b.conns {
		dead.Store(true)
	}
	b.mu.Unlock()
}

func (b *blackhole) restore() {
	b.mu.Lock()
	b.cut = false
	b.mu.Unlock()
}

func (b *blackhole) serve() {
	for {
		from, err := b.listener.Accept()
		if err != nil {
			return
		}
		dead := &atomic.Bool{}
		b.mu.Lock()
		dead.Store(b.cut)
		b.conns = append(b.conns, dead)
		b.mu.Unlock()

		go func() {
			defer from.Close()
			var to net.Conn
			if !dead.Load() {
				if to, err = net.DialTimeout("tcp", b.target, 2*time.Second); err != nil {
					return
				}
				defer to.Close()
				go func() { // answers, for the exchanges that have one
					buf := make([]byte, 4096)
					for {
						n, err := to.Read(buf)
						if err != nil {
							return
						}
						if !dead.Load() {
							_, _ = from.Write(buf[:n])
						}
					}
				}()
			}
			buf := make([]byte, 4096)
			for {
				n, err := from.Read(buf)
				if err != nil {
					return
				}
				if to != nil && !dead.Load() {
					_, _ = to.Write(buf[:n])
				}
			}
		}()
	}
}

// When the network between two nodes loses everything for a while, their
// heartbeat connections stay open and carry nothing, and go on carrying
// nothing after the network has returned. Each node wrote its heartbeats into
// such a connection without an error, so neither opened a new one, and the
// two stayed dead to each other. A node now stops using a connection when it
// no longer hears from the node at its other end.
func TestMembership_ConnectionThatCarriesNothingIsReplaced(t *testing.T) {
	realA, realB := freeAddr(t), freeAddr(t)
	toA, toB := newBlackhole(t, realA), newBlackhole(t, realB)
	a := quickMember(t, "node-a", realA, nil)
	b := quickMember(t, "node-b", realB, []string{toA.addr()})
	// Each is known to the other by the address of what stands in front of it.
	a.localNode.GossipAddr, b.localNode.GossipAddr = toA.addr(), toB.addr()
	startMember(t, a)
	startMember(t, b)
	eventually(t, "the two nodes know each other", func() bool { return alive(a, "node-b") && alive(b, "node-a") })

	toA.fail()
	toB.fail()
	eventually(t, "each node counts the other as failed", func() bool { return !alive(a, "node-b") && !alive(b, "node-a") })

	toA.restore()
	toB.restore()
	eventually(t, "the two nodes hear each other again", func() bool { return alive(a, "node-b") && alive(b, "node-a") })
}

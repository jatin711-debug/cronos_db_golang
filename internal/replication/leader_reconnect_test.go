package replication

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
)

// countingListener counts the connections that were opened to it.
type countingListener struct {
	net.Listener
	opened atomic.Int32
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.opened.Add(1)
	}
	return conn, err
}

// A call to a follower that gets no answer may have gone into a connection
// that carries nothing any more. The leader used to keep that connection for
// the next call, and after a network failure a connection like that stays
// silent long after the network has returned. The next call now gets a
// connection of its own.
func TestLeader_OpensANewConnectionAfterACallWithoutAnswer(t *testing.T) {
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener := &countingListener{Listener: inner}
	// A follower that never answers: its gate stays shut.
	follower := &gatedReplServer{gate: make(chan struct{})}
	server := grpc.NewServer()
	types.RegisterReplicationServiceServer(server, follower)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	l := NewLeader(0, 500, 20*time.Millisecond, nil, 2, "leader", nil)
	l.SetReplicateTimeout(150 * time.Millisecond)
	if err := l.AddFollower("f1", inner.Addr().String()); err != nil {
		t.Fatal(err)
	}
	l.Start()
	defer l.Stop()

	if err := l.Replicate(batchAt(0, 1)); err == nil {
		t.Fatal("a publish was replicated to a follower that never answers")
	}
	if opened := listener.opened.Load(); opened != 1 {
		t.Fatalf("%d connections were opened for the first call, want 1", opened)
	}
	deadline := time.Now().Add(5 * time.Second)
	for listener.opened.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("the leader went on using the connection of a call that got no answer")
		}
		_ = l.Replicate(batchAt(0, 1))
		time.Sleep(10 * time.Millisecond)
	}
}

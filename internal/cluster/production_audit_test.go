package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAuditRaftInitializationFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(path, []byte("occupied"), 0600); err != nil {
		t.Fatal(err)
	}
	m := NewManager(&Config{NodeID: "audit", RaftAddr: "127.0.0.1:0", RaftDir: path, GossipAddr: "127.0.0.1:0"})
	defer m.cancel()
	err := m.Start()
	if err == nil || !strings.Contains(err.Error(), "Raft authority") {
		t.Fatalf("continued without required Raft: %v", err)
	}
	if m.started || m.membership != nil || m.router != nil {
		t.Fatal("serving cluster state after Raft initialization failure")
	}
}

package replication

import (
	"strings"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// makeLeaderForTest creates a Leader with the given minISR and a follower map
// already seeded. This lets us test the ISR/durability policy without a network.
func makeLeaderForTest(minISR int, followers map[string]*FollowerInfo) *Leader {
	l := NewLeader(0, 100, 100*time.Millisecond, nil, minISR, "test-node", nil)
	l.followers = followers
	return l
}

func TestReplicate_MinISR_InsufficientFollowers(t *testing.T) {
	// minISR=2, 1 follower configured but not connected/in-sync.
	followers := map[string]*FollowerInfo{
		"f1": {ID: "f1", Connected: false, InSync: false},
	}
	l := makeLeaderForTest(2, followers)

	events := []*types.Event{{Offset: 1, Payload: []byte("data")}}
	err := l.Replicate(events)
	if err == nil {
		t.Fatal("expected error when minISR=2 but follower is not in ISR, got nil")
	}
	if !strings.Contains(err.Error(), "not enough replicas") {
		t.Errorf("expected 'not enough replicas' error, got: %v", err)
	}
}

func TestReplicate_MinISR_InsufficientActive(t *testing.T) {
	// minISR=2, 1 follower connected but not in ISR, so len(isr)+1 = 1 < 2.
	// Should fail before any network send.
	followers := map[string]*FollowerInfo{
		"f1": {ID: "f1", Connected: true, InSync: false},
	}
	l := makeLeaderForTest(2, followers)

	events := []*types.Event{{Offset: 1, Payload: []byte("data")}}
	err := l.Replicate(events)
	if err == nil {
		t.Fatal("expected error when minISR=2 but only 1 ISR member (incl. leader), got nil")
	}
	if !strings.Contains(err.Error(), "not enough replicas") {
		t.Errorf("expected 'not enough replicas' error, got: %v", err)
	}
}

func TestReplicate_MinISR_SingleReplicaMode(t *testing.T) {
	// minISR=1 (default), no followers. Leader-only writes are allowed.
	l := makeLeaderForTest(1, nil)
	events := []*types.Event{{Offset: 1, Payload: []byte("data")}}
	if err := l.Replicate(events); err != nil {
		t.Errorf("expected nil for minISR=1 with no followers, got: %v", err)
	}
}

func TestReplicate_MinISR_ZeroTreatedAsOne(t *testing.T) {
	l := makeLeaderForTest(0, nil)
	if l.minInSyncReplicas != 1 {
		t.Fatalf("minISR=0 should be normalized to 1, got %d", l.minInSyncReplicas)
	}
}

func TestReplicate_NeededAcks_Math(t *testing.T) {
	// minISR=1, RF=3 (3 ISR followers). Majority of ISR = (3+1)/2 = 2, minISR-1
	// = 0, so neededAcks = max(2,1) = 2. Need 2 active connected followers.
	followers := map[string]*FollowerInfo{
		"f1": {ID: "f1", Connected: true, InSync: true},
		"f2": {ID: "f2", Connected: true, InSync: true},
		"f3": {ID: "f3", Connected: false, InSync: true},
	}
	l := makeLeaderForTest(1, followers)
	active := 0
	for _, f := range followers {
		if f.InSync && f.Connected {
			active++
		}
	}
	if active != 2 {
		t.Fatalf("test setup: expected 2 active followers, got %d", active)
	}

	// The call should proceed past the ISR checks (it will later fail on
	// network send, which is fine). We just need to prove the guard doesn't
	// reject it.
	events := []*types.Event{{Offset: 1, Payload: []byte("data")}}
	err := l.Replicate(events)
	// We expect an error from sendBatchLocked because transports are nil, but
	// not the earlier "not enough replicas" / "quorum failed" error.
	if err != nil && strings.Contains(err.Error(), "not enough replicas") {
		t.Errorf("did not expect 'not enough replicas', got: %v", err)
	}
	if err != nil && strings.Contains(err.Error(), "quorum failed") {
		t.Errorf("did not expect 'quorum failed', got: %v", err)
	}
}

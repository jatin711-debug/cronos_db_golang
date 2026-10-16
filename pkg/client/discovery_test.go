package client

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

type discoveryServer struct {
	types.UnimplementedPartitionServiceServer
	mu        sync.Mutex
	leader    string
	addresses map[string]string
	legacy    bool
}

func (s *discoveryServer) ListPartitions(context.Context, *types.ListPartitionsRequest) (*types.ListPartitionsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := &types.ListPartitionsResponse{Partitions: []*types.PartitionInfo{{PartitionId: 0, LeaderId: s.leader}}}
	if !s.legacy {
		r.DiscoveryVersion = 1
		r.NodeAddresses = make(map[string]string)
		for id, addr := range s.addresses {
			r.NodeAddresses[id] = addr
		}
	}
	return r, nil
}

func TestClientDiscoversNewLeaderAfterConnecting(t *testing.T) {
	a, b := &clusterNode{name: "seed"}, &clusterNode{name: "new-node"}
	a.leads.Store(true)
	meta := &discoveryServer{leader: "seed"}
	seed := startTestEventServer(t, a, meta)
	meta.addresses = map[string]string{"seed": seed}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cl, err := Dial(ctx, DefaultConfig(seed))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	producer, err := cl.NewProducer(DefaultProducerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	partitionID := int32(0)
	send := func(id, want string) {
		t.Helper()
		result, err := producer.Send(ctx, Message{MessageID: id, Topic: "orders", Payload: []byte(id), PartitionID: &partitionID, ScheduleAt: time.Now().Add(time.Second)})
		if err != nil {
			t.Fatal(err)
		}
		if result.NodeAddress != want {
			t.Fatalf("sent through %s, want %s", result.NodeAddress, want)
		}
	}
	send("before", seed)
	joined := startTestEventServer(t, b, meta)
	a.leads.Store(false)
	b.leads.Store(true)
	meta.mu.Lock()
	meta.leader = "new-node"
	meta.addresses["new-node"] = joined
	meta.mu.Unlock()
	if err := cl.ForceMetadataRefresh(ctx); err != nil {
		t.Fatal(err)
	}
	send("after", joined)
	// A refresh through a legacy peer must not erase a discovered endpoint.
	meta.mu.Lock()
	meta.legacy = true
	meta.mu.Unlock()
	if err := cl.ForceMetadataRefresh(ctx); err != nil {
		t.Fatal(err)
	}
	send("mixed-version", joined)
}

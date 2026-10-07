package api

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// leadershipView is the cluster's view of partition 0 as one node sees it.
type leadershipView struct {
	leads atomic.Bool
	epoch atomic.Int64
}

func (v *leadershipView) IsLocalPartition(int32) bool    { return true }
func (v *leadershipView) IsPartitionLeader(int32) bool   { return v.leads.Load() }
func (v *leadershipView) PartitionHasLeader(int32) bool  { return true }
func (v *leadershipView) IsPartitionWritable(int32) bool { return v.leads.Load() }
func (v *leadershipView) GetPartitionEpoch(int32) int64  { return v.epoch.Load() }

// A subscription is served by the leader of its partition. When the node it
// is attached to stops leading, the stream ends with an error that says so:
// left open, the consumer would wait there while its events are delivered by
// another node, or by nobody.
func TestSubscriptionEndsWhenTheNodeStopsLeading(t *testing.T) {
	for _, tc := range []struct {
		name string
		// lose takes leadership away from the node.
		lose func(pm *partition.PartitionManager, view *leadershipView)
	}{
		{"the cluster names another leader", func(_ *partition.PartitionManager, view *leadershipView) {
			view.leads.Store(false)
		}},
		{"the node steps down for a newer leader", func(pm *partition.PartitionManager, _ *leadershipView) {
			if err := pm.DemoteFromLeader(0); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pm := partition.NewPartitionManager("node-a", &types.Config{
				DataDir: t.TempDir(), PartitionCount: 1, FsyncMode: "periodic", FlushIntervalMS: 10,
				TickMS: 10, WheelSize: 64, DedupTTLHours: 1, BloomCapacity: 10000,
				ClusterEnabled: true, ReplicationFactor: 1,
			})
			if err := pm.CreatePartition(0, "orders"); err != nil {
				pm.Close()
				t.Fatal(err)
			}
			if err := pm.PromoteToLeader(0, 1); err != nil {
				pm.Close()
				t.Fatal(err)
			}
			p, err := pm.GetInternalPartition(0)
			if err != nil {
				pm.Close()
				t.Fatal(err)
			}
			view := &leadershipView{}
			view.leads.Store(true)
			view.epoch.Store(1)
			handler := NewEventServiceHandler(pm, p.DedupStore, p.ConsumerGroup)
			handler.SetClusterRouter(view)

			serverCfg := DefaultConfig()
			serverCfg.Address = "127.0.0.1:0"
			serverCfg.SLORecorder = nil
			server, err := NewGRPCServer(serverCfg)
			if err != nil {
				pm.Close()
				t.Fatal(err)
			}
			server.RegisterServices(handler, nil, NewPartitionServiceHandler(pm, nil, "node-a"), nil)
			if err := server.Start(); err != nil {
				pm.Close()
				t.Fatal(err)
			}
			t.Cleanup(func() {
				server.Stop()
				pm.Close()
			})

			conn, err := grpc.NewClient(server.listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			stream, err := types.NewEventServiceClient(conn).Subscribe(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := stream.Send(&types.SubscribeRequest{ConsumerGroup: "workers", Topic: "orders", PartitionId: 0, SubscriptionId: "worker-1"}); err != nil {
				t.Fatal(err)
			}
			ended := make(chan error, 1)
			go func() {
				_, err := stream.Recv()
				ended <- err
			}()

			// While the node leads, the subscription stays open.
			select {
			case err := <-ended:
				t.Fatalf("the subscription ended while the node leads: %v", err)
			case <-time.After(3 * servedCheckInterval):
			}

			tc.lose(pm, view)
			select {
			case err := <-ended:
				if err == nil || !strings.Contains(err.Error(), "not the leader") {
					t.Fatalf("the subscription ended with %v, want an error naming the leader change", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the subscription stayed open on a node that no longer leads the partition")
			}
		})
	}
}

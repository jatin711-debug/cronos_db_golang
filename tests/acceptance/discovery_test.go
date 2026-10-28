//go:build acceptance

package acceptance

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
)

// A client given one node of the cluster publishes to every partition, which
// the three nodes lead between them. It reaches the leaders it was not given
// through the addresses partition metadata names, which have to be the ones
// clients use. Metadata used to name each node's cluster address, where
// replication and Raft are served: a client sent there was refused, either
// at the TLS handshake or with "unknown service cronos_db.EventService".
func TestClientsFindTheOtherNodesThroughPartitionMetadata(t *testing.T) {
	c := newCluster(t)
	c.startAll()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	conn, err := grpc.NewClient(c.nodes[0].grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	metadata, err := types.NewPartitionServiceClient(conn).ListPartitions(ctx, &types.ListPartitionsRequest{})
	if err != nil {
		t.Fatalf("list partitions: %v", err)
	}
	for _, n := range c.nodes {
		if got := metadata.GetNodeAddresses()[n.id]; got != n.grpcAddr {
			t.Fatalf("partition metadata names %s at %q, want its client address %s (all: %v)", n.id, got, n.grpcAddr, metadata.GetNodeAddresses())
		}
	}

	cl, err := client.Dial(ctx, client.DefaultConfig(c.nodes[0].grpcAddr))
	if err != nil {
		t.Fatalf("connect to %s: %v", c.nodes[0].id, err)
	}
	defer cl.Close()
	producer, err := client.NewProducer(cl, client.DefaultProducerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	servedBy := map[string]int{}
	for p := int32(0); p < partitionCount; p++ {
		// The server hashes the key independently of the client's route.
		// Choose a key for each partition so this exercises its actual leader.
		messageID := fmt.Sprintf("discovery-%d", p)
		for suffix := 0; utils.HashToPartitionID(messageID, partitionCount) != p; suffix++ {
			messageID = fmt.Sprintf("discovery-%d-%d", p, suffix)
		}
		res, err := sendWhileLeadershipSettles(ctx, t, producer, client.Message{
			Topic:      "discovery",
			MessageID:  messageID,
			Payload:    []byte("x"),
			ScheduleAt: time.Now().Add(time.Second),
		})
		if err != nil {
			t.Fatalf("publish to partition %d through a client given only %s: %v", p, c.nodes[0].id, err)
		}
		if res.PartitionID != p {
			t.Fatalf("publish for partition %d was accepted by partition %d", p, res.PartitionID)
		}
		servedBy[res.NodeAddress]++
	}
	leaders := map[string]bool{}
	for p := int32(0); p < partitionCount; p++ {
		leaders[c.leaderOf(cl, p).grpcAddr] = true
	}
	for addr := range leaders {
		if servedBy[addr] == 0 {
			t.Fatalf("no publish went to %s, which leads a partition; published through %v", addr, servedBy)
		}
	}
	t.Logf("published through %v; the partitions are led at %v", servedBy, leaders)
}

// sendWhileLeadershipSettles publishes msg, and publishes it again while the
// cluster refuses it as unavailable. Right after start-up the leadership of a
// partition can still be moving to the node the ring prefers, and the node
// giving it up refuses publishes meanwhile ("partition 1 is local but this node
// is not the leader"). The producer's own retries could run out before that
// ended, and the test failed now and then in CI.
func sendWhileLeadershipSettles(ctx context.Context, t *testing.T, producer *client.Producer, msg client.Message) (*client.SendResult, error) {
	t.Helper()
	for {
		res, err := producer.Send(ctx, msg)
		var refused *client.Error
		if err == nil || !errors.As(err, &refused) || refused.Kind != client.ErrorKindUnavailable {
			return res, err
		}
		t.Logf("publish of %s refused while leadership settles; trying again: %v", msg.MessageID, err)
		select {
		case <-ctx.Done():
			return res, err
		case <-time.After(500 * time.Millisecond):
		}
	}
}

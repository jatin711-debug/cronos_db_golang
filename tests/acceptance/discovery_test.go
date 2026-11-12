//go:build acceptance

package acceptance

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
	// Node health can turn green before startup placement has connected the
	// followers. Discovery needs a writable cluster, including its replicas.
	c.waitPlacement(c.running(), "before client discovery", readyTimeout)

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
		res, err := sendWhileLeadershipSettles(ctx, t, producer.Send, client.Message{
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

func TestSendWhileLeadershipSettles(t *testing.T) {
	for _, tc := range []struct {
		kind   client.ErrorKind
		reason string
		retry  bool
	}{
		{client.ErrorKindUnavailable, "unreachable", true},
		{client.ErrorKindLeaderChange, "not leader", true},
		{client.ErrorKindInternal, "replication: not enough replicas: min-insync-replicas=2", true},
		{client.ErrorKindInternal, "replication: replication quorum not met: 1 of 2 required in-sync replicas acked", true},
		{client.ErrorKindAuth, "denied", false},
		{client.ErrorKindValidation, "invalid", false},
		{client.ErrorKindInternal, "disk write failed", false},
	} {
		t.Run(string(tc.kind)+"/"+tc.reason, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			refusal := &client.Error{Kind: tc.kind, Err: errors.New(tc.reason)}
			want := &client.SendResult{MessageID: "same-id"}
			calls := 0
			res, err := sendWhileLeadershipSettles(ctx, t, func(_ context.Context, msg client.Message) (*client.SendResult, error) {
				calls++
				if msg.MessageID != want.MessageID {
					t.Fatal("retry changed the message ID")
				}
				if calls == 1 {
					return nil, fmt.Errorf("wrapped: %w", refusal)
				}
				return want, nil
			}, client.Message{MessageID: want.MessageID})
			if tc.retry {
				if err != nil || res != want || calls != 2 {
					t.Fatalf("transient refusal: result=%v err=%v calls=%d", res, err, calls)
				}
			} else if !errors.Is(err, refusal) || calls != 1 {
				t.Fatalf("permanent refusal: err=%v calls=%d", err, calls)
			}
		})
	}
}

// sendWhileLeadershipSettles publishes msg, and publishes it again while the
// cluster refuses it as unavailable or changing leaders. Right after start-up
// the leadership can still be moving to the node the ring prefers, and the node
// giving it up refuses publishes meanwhile ("partition 1 is local but this node
// is not the leader"). The producer's own retries could run out before that
// ended, and the test failed now and then in CI. Startup format upgrades can
// also temporarily refuse replication quorum. Retry those specific refusals
// with the same ID; all other internal errors still fail the test immediately.
func sendWhileLeadershipSettles(ctx context.Context, t *testing.T, send func(context.Context, client.Message) (*client.SendResult, error), msg client.Message) (*client.SendResult, error) {
	t.Helper()
	for {
		res, err := send(ctx, msg)
		var refused *client.Error
		if err == nil || !errors.As(err, &refused) {
			return res, err
		}
		quorumRefused := refused.Kind == client.ErrorKindInternal && refused.Err != nil &&
			(strings.HasPrefix(refused.Err.Error(), "replication: not enough replicas:") ||
				strings.HasPrefix(refused.Err.Error(), "replication: replication quorum not met:"))
		if refused.Kind != client.ErrorKindUnavailable && refused.Kind != client.ErrorKindLeaderChange && !quorumRefused {
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

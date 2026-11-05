// dashboard_journey creates disposable local fixtures and verifies the public RPCs
// behind a populated dashboard. Never point it at a production server.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
	"github.com/jatin711-debug/cronos_db_golang/internal/schema"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"os"
	"path/filepath"
	"time"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	dir := flag.String("dir", "build/dashboard-journey", "disposable fixture directory")
	seed := flag.Bool("seed", false, "exercise the local server after startup")
	flag.Parse()
	if !*seed {
		must(os.MkdirAll(*dir, 0700))
		raw := make([]byte, 32)
		_, err := rand.Read(raw)
		must(err)
		secret := hex.EncodeToString(raw)
		must(os.WriteFile(filepath.Join(*dir, "server.env"), []byte("CRONOS_AUTH_JWT_SECRET="+secret+"\n"), 0600))
		policy := `{"ops":{"admin":true,"topics":{"journey":{"publish":true,"subscribe":true}}},"viewer":{"topics":{"journey":{"subscribe":true}}}}`
		must(os.WriteFile(filepath.Join(*dir, "policy.json"), []byte(policy), 0600))
		must(os.WriteFile(filepath.Join(*dir, "limits.json"), []byte(`{"ops":{"max_events_per_second":100}}`), 0600))
		for _, subject := range []string{"ops", "viewer"} {
			token, err := auth.GenerateToken(subject, []byte(secret), time.Hour)
			must(err)
			must(os.WriteFile(filepath.Join(*dir, subject+".token"), []byte(token), 0600))
		}
		expired, err := auth.GenerateToken("ops", []byte(secret), -time.Hour)
		must(err)
		must(os.WriteFile(filepath.Join(*dir, "expired.token"), []byte(expired), 0600))
		registry, err := schema.NewRegistry(filepath.Join(*dir, "data"))
		must(err)
		_, err = registry.Register("journey", schema.TypeJSON, `{"type":"object","properties":{"order_id":{"type":"string"}},"required":["order_id"]}`)
		must(err)
		fmt.Println("Disposable auth and persisted schema fixtures created")
		return
	}
	token, err := os.ReadFile(filepath.Join(*dir, "ops.token"))
	must(err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+string(token))
	conn, err := grpc.NewClient("127.0.0.1:19000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	must(err)
	defer conn.Close()
	events := types.NewEventServiceClient(conn)
	due := time.Now().Add(2 * time.Second).UnixMilli()
	event := &types.Event{MessageId: "journey-order", Topic: "journey", PartitionId: 0, Payload: []byte(`{"order_id":"order-1"}`), ScheduleTs: due}
	published, err := events.Publish(ctx, &types.PublishRequest{Event: event})
	must(err)
	if !published.Success {
		panic(published.Error)
	}
	// The server routes by message id, so the group follows the partition it chose.
	group, err := types.NewConsumerGroupServiceClient(conn).CreateConsumerGroup(ctx, &types.CreateConsumerGroupRequest{GroupId: "journey-workers", Topic: "journey", Partitions: []int32{published.PartitionId}})
	must(err)
	if !group.Success {
		panic(group.Error)
	}
	duplicate, err := events.Publish(ctx, &types.PublishRequest{Event: event})
	if err == nil && duplicate.Success && duplicate.Offset != published.Offset {
		panic("duplicate appended")
	}
	invalid, err := events.Publish(ctx, &types.PublishRequest{Event: &types.Event{MessageId: "invalid", Topic: "journey", Payload: []byte(`{}`), ScheduleTs: due}})
	if err == nil && invalid.Success {
		panic("invalid schema accepted")
	}
	sub, err := events.Subscribe(ctx)
	must(err)
	must(sub.Send(&types.SubscribeRequest{ConsumerGroup: "journey-workers", Topic: "journey", PartitionId: published.PartitionId, StartOffset: -1, MaxBufferSize: 10}))
	delivery, err := sub.Recv()
	must(err)
	if time.Now().UnixMilli() < due {
		panic("early delivery")
	}
	if delivery.Event == nil || delivery.Event.MessageId != event.MessageId {
		panic("unexpected delivery")
	}
	ack, err := events.Ack(ctx)
	must(err)
	must(ack.Send(&types.AckRequest{DeliveryId: delivery.DeliveryId, Success: true, NextOffset: delivery.Event.Offset + 1}))
	acked, err := ack.Recv()
	must(err)
	if !acked.Success {
		panic(acked.Error)
	}
	replay, err := events.Replay(ctx, &types.ReplayRequest{Topic: "journey", PartitionId: published.PartitionId, StartOffset: published.Offset, Count: 1})
	must(err)
	item, err := replay.Recv()
	must(err)
	if item.Event.MessageId != event.MessageId {
		panic("replay mismatch")
	}
	result := map[string]any{"publish": true, "deduplication": true, "schema_rejection": true, "scheduled_delivery": true, "acknowledgement": true, "replay": true, "partition": published.PartitionId, "offset": published.Offset}
	out, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(out))
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	serverAddr string
	client     types.EventServiceClient
	partClient types.PartitionServiceClient
	cgClient   types.ConsumerGroupServiceClient
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "cronos-admin",
		Short: "CronosDB administrative CLI",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return dial()
		},
		PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
			return closeConn()
		},
	}
	rootCmd.PersistentFlags().StringVarP(&serverAddr, "server", "s", "localhost:9000", "CronosDB server gRPC address")

	rootCmd.AddCommand(
		partitionCmd(),
		consumerCmd(),
		clusterCmd(),
		debugCmd(),
		generateTokenCmd(),
	)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func dial() error {
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial %s: %w", serverAddr, err)
	}
	client = types.NewEventServiceClient(conn)
	partClient = types.NewPartitionServiceClient(conn)
	cgClient = types.NewConsumerGroupServiceClient(conn)
	return nil
}

func closeConn() error {
	// Connection is shared across clients; no direct close on clients needed in this simple impl
	return nil
}

func partitionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "partition",
		Short: "Partition management commands",
	}

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List partitions",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := partClient.ListPartitions(ctx, &types.ListPartitionsRequest{})
			if err != nil {
				return err
			}
			for _, p := range resp.Partitions {
				fmt.Printf("partition-%d topic=%s leader=%s watermark=%d\n", p.GetPartitionId(), p.GetTopic(), p.GetLeaderId(), p.GetHighWatermark())
			}
			return nil
		},
	}

	infoCmd := &cobra.Command{
		Use:   "info [partition-id]",
		Short: "Get partition info",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := strconv.ParseInt(args[0], 10, 32)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := partClient.GetPartition(ctx, &types.GetPartitionRequest{PartitionId: int32(id)})
			if err != nil {
				return err
			}
			b, _ := json.MarshalIndent(resp, "", "  ")
			fmt.Println(string(b))
			return nil
		},
	}

	walCmd := &cobra.Command{
		Use:   "wal [partition-id]",
		Short: "Get WAL status",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := strconv.ParseInt(args[0], 10, 32)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := partClient.GetWALStatus(ctx, &types.GetWALStatusRequest{PartitionId: int32(id)})
			if err != nil {
				return err
			}
			fmt.Printf("LastOffset: %d\nSegmentFiles: %d\nTotalSize: %d\n", resp.GetLastOffset(), len(resp.GetSegmentFiles()), resp.GetTotalSizeBytes())
			return nil
		},
	}

	cmd.AddCommand(listCmd, infoCmd, walCmd)
	return cmd
}

func consumerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "consumer",
		Short: "Consumer group commands",
	}

	lagCmd := &cobra.Command{
		Use:   "lag [group-id]",
		Short: "Show consumer group lag (approximate)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := cgClient.GetConsumerGroup(ctx, &types.GetConsumerGroupRequest{GroupId: args[0]})
			if err != nil {
				return err
			}
			fmt.Printf("Group: %s\nTopic: %s\nPartitions: %v\n", resp.GetGroupId(), resp.GetTopic(), resp.GetPartitions())
			for pid, off := range resp.GetCommittedOffsets() {
				fmt.Printf("  Partition %d -> Offset %d\n", pid, off)
			}
			return nil
		},
	}

	listGroupsCmd := &cobra.Command{
		Use:   "list",
		Short: "List consumer groups",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := cgClient.ListConsumerGroups(ctx, &types.ListConsumerGroupsRequest{})
			if err != nil {
				return err
			}
			for _, g := range resp.Groups {
				fmt.Printf("%s topic=%s partitions=%v\n", g.GetGroupId(), g.GetTopic(), g.GetPartitions())
			}
			return nil
		},
	}

	cmd.AddCommand(lagCmd, listGroupsCmd)
	return cmd
}

func clusterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Cluster commands",
	}

	statusCmd := &cobra.Command{
		Use:   "status",
		Short: "Get cluster status",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Use HTTP health endpoint for simple cluster status
			addr := strings.Replace(serverAddr, ":9000", ":8080", 1)
			if addr == serverAddr {
				addr = addr + ":8080"
			}
			fmt.Printf("Cluster status endpoint: http://%s/health/deep\n", addr)
			return nil
		},
	}

	cmd.AddCommand(statusCmd)
	return cmd
}

func debugCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "debug",
		Short: "Debug commands",
	}

	publishCmd := &cobra.Command{
		Use:   "publish [topic] [payload]",
		Short: "Publish a single test event",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := client.Publish(ctx, &types.PublishRequest{
				Event: &types.Event{
					MessageId:  fmt.Sprintf("admin-%d", time.Now().UnixNano()),
					ScheduleTs: time.Now().Add(5 * time.Second).UnixMilli(),
					Payload:    []byte(args[1]),
					Topic:      args[0],
				},
			})
			if err != nil {
				return err
			}
			fmt.Printf("Published: success=%v offset=%d partition=%d\n", resp.Success, resp.Offset, resp.PartitionId)
			return nil
		},
	}

	cmd.AddCommand(publishCmd)
	return cmd
}

func generateTokenCmd() *cobra.Command {
	var secret string
	var subject string
	var ttl time.Duration

	cmd := &cobra.Command{
		Use:   "generate-token",
		Short: "Generate a JWT token for testing",
		RunE: func(cmd *cobra.Command, args []string) error {
			if secret == "" {
				return fmt.Errorf("--secret is required")
			}
			token, err := auth.GenerateToken(subject, []byte(secret), ttl)
			if err != nil {
				return err
			}
			fmt.Println(token)
			return nil
		},
	}
	cmd.Flags().StringVar(&secret, "secret", "", "HMAC secret")
	cmd.Flags().StringVar(&subject, "subject", "admin", "Token subject")
	cmd.Flags().DurationVar(&ttl, "ttl", 24*time.Hour, "Token TTL")
	return cmd
}

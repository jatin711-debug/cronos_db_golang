package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

var (
	serverAddr    string
	tlsEnabled    bool
	tlsCAFile     string
	tlsCertFile   string
	tlsKeyFile    string
	tlsSkipVerify bool
	jwtToken      string
	client        types.EventServiceClient
	partClient    types.PartitionServiceClient
	cgClient      types.ConsumerGroupServiceClient
	adminClient   types.AdminServiceClient
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
	rootCmd.PersistentFlags().BoolVar(&tlsEnabled, "tls", false, "Enable TLS for gRPC connection")
	rootCmd.PersistentFlags().StringVar(&tlsCAFile, "tls-ca", "", "Path to CA certificate file")
	rootCmd.PersistentFlags().StringVar(&tlsCertFile, "tls-cert", "", "Path to client TLS certificate file (mTLS)")
	rootCmd.PersistentFlags().StringVar(&tlsKeyFile, "tls-key", "", "Path to client TLS private key file (mTLS)")
	rootCmd.PersistentFlags().BoolVar(&tlsSkipVerify, "tls-skip-verify", false, "Skip server certificate verification (development only)")
	rootCmd.PersistentFlags().StringVar(&jwtToken, "jwt-token", "", "Bearer token for authenticated RPCs (sent as `authorization: Bearer <token>` metadata)")

	rootCmd.AddCommand(
		partitionCmd(),
		consumerCmd(),
		clusterCmd(),
		adminCmd(),
		debugCmd(),
		generateTokenCmd(),
	)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// dial connects to the CronosDB node, attaching the bearer token (if
// --jwt-token is set) as `authorization: Bearer <token>` metadata on
// every unary and streaming RPC via small client interceptors. Admin RPCs
// require Subject.Admin=true; the token is what carries the subject claim.
func dial() error {
	var creds credentials.TransportCredentials
	if tlsEnabled {
		tlsCfg := &tls.Config{
			InsecureSkipVerify: tlsSkipVerify,
			MinVersion:         tls.VersionTLS12,
		}
		if tlsCAFile != "" {
			caPEM, err := os.ReadFile(tlsCAFile)
			if err != nil {
				return fmt.Errorf("read CA cert: %w", err)
			}
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(caPEM) {
				return fmt.Errorf("parse CA cert: no certificates loaded")
			}
			tlsCfg.RootCAs = roots
		}
		if tlsCertFile != "" && tlsKeyFile != "" {
			cert, err := tls.LoadX509KeyPair(tlsCertFile, tlsKeyFile)
			if err != nil {
				return fmt.Errorf("load client cert/key: %w", err)
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		}
		creds = credentials.NewTLS(tlsCfg)
	} else {
		creds = insecure.NewCredentials()
	}

	opts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}
	if jwtToken != "" {
		opts = append(opts,
			grpc.WithUnaryInterceptor(bearerUnaryInterceptor(jwtToken)),
			grpc.WithStreamInterceptor(bearerStreamInterceptor(jwtToken)),
		)
	}

	conn, err := grpc.NewClient(serverAddr, opts...)
	if err != nil {
		return fmt.Errorf("dial %s: %w", serverAddr, err)
	}
	client = types.NewEventServiceClient(conn)
	partClient = types.NewPartitionServiceClient(conn)
	cgClient = types.NewConsumerGroupServiceClient(conn)
	adminClient = types.NewAdminServiceClient(conn)
	return nil
}

// bearerUnaryInterceptor attaches `authorization: Bearer <token>` to the
// outgoing metadata of every unary RPC.
func bearerUnaryInterceptor(token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// bearerStreamInterceptor attaches `authorization: Bearer <token>` to the
// outgoing metadata of every streaming RPC.
func bearerStreamInterceptor(token string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		return streamer(ctx, desc, cc, method, opts...)
	}
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

// ---------------------------------------------------------------------------
// adminCmd — operator-facing subcommand group calling AdminService.
//
// All commands require --jwt-token (Subject.Admin=true) when the server
// runs with auth enabled. In --dev mode auth is bypassed and the token
// is optional.
// ---------------------------------------------------------------------------

func adminCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "admin",
		Short: "AdminService RPCs (operator-facing; require admin role)",
	}

	cmd.AddCommand(
		adminTopologyCmd(),
		adminPartitionHealthCmd(),
		adminReplicationLagCmd(),
		adminConsumerGroupsCmd(),
		adminConsumerGroupLagCmd(),
		adminSchemaListCmd(),
		adminSchemaGetCmd(),
		adminTenantUsageCmd(),
		adminRetentionRunCmd(),
		adminCompactionRunCmd(),
		adminRebalanceCmd(),
	)
	return cmd
}

func adminTopologyCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "topology",
		Short: "Show cluster topology (nodes, partitions, ISR, follower offsets)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := adminClient.GetClusterTopology(ctx, &types.GetClusterTopologyRequest{})
			if err != nil {
				return err
			}
			fmt.Printf("local=%s cluster_mode=%v total_nodes=%d alive_nodes=%d total_partitions=%d leader_partitions=%d captured_at=%d\n",
				resp.GetLocalNodeId(), resp.GetIsClusterMode(),
				resp.GetTotalNodes(), resp.GetAliveNodes(),
				resp.GetTotalPartitions(), resp.GetLeaderPartitions(),
				resp.GetCapturedAtUnixMs())
			for _, n := range resp.GetNodes() {
				fmt.Printf("  node id=%s addr=%s is_local=%v is_alive=%v state=%s\n",
					n.GetNodeId(), n.GetAddress(), n.GetIsLocal(), n.GetIsAlive(), n.GetState())
			}
			for _, p := range resp.GetPartitions() {
				fmt.Printf("  partition id=%d leader=%s replicas=%v isr=%v hwm=%d\n",
					p.GetPartitionId(), p.GetLeaderId(), p.GetReplicas(), p.GetIsr(), p.GetLeaderHighWatermark())
			}
			return nil
		},
	}
}

func adminPartitionHealthCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "partition-health [partition-id]",
		Short: "Get per-partition runtime stats (WAL, scheduler, dedup, replay status)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := strconv.ParseInt(args[0], 10, 32)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := adminClient.GetPartitionHealth(ctx, &types.PartitionHealth{PartitionId: int32(id)})
			if err != nil {
				return err
			}
			fmt.Printf("partition=%d hwm=%d segments=%d disk=%d active_timers=%d ready=%d cold=%d replay_err=%v\n",
				resp.GetPartitionId(), resp.GetHighWatermark(),
				resp.GetSegmentCount(), resp.GetDiskUsageBytes(),
				resp.GetActiveTimers(), resp.GetReadyEvents(), resp.GetColdStoreEntries(),
				resp.GetReplayError())
			if resp.GetLastError() != "" {
				fmt.Printf("  last_error=%s\n", resp.GetLastError())
			}
			return nil
		},
	}
}

func adminReplicationLagCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "replication-lag [partition-id]",
		Short: "Show leader HWM and per-follower lag (0 = all local leaders)",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pid := int32(0)
			if len(args) == 1 {
				v, err := strconv.ParseInt(args[0], 10, 32)
				if err != nil {
					return err
				}
				pid = int32(v)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := adminClient.GetReplicationLag(ctx, &types.ReplicationLag{PartitionId: pid})
			if err != nil {
				return err
			}
			fmt.Printf("partition=%d hwm=%d\n", resp.GetPartitionId(), resp.GetLeaderHighWatermark())
			for _, f := range resp.GetFollowers() {
				fmt.Printf("  follower=%s offset=%d lag_events=%d in_sync=%v\n",
					f.GetFollowerId(), f.GetOffset(), f.GetLagEvents(), f.GetInSync())
			}
			return nil
		},
	}
}

func adminConsumerGroupsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "consumer-groups",
		Short: "Consumer group admin commands",
	}
	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List consumer groups known to this node",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := adminClient.ListConsumerGroups(ctx, &types.AdminListConsumerGroupsRequest{})
			if err != nil {
				return err
			}
			for _, g := range resp.GetGroups() {
				fmt.Printf("group=%s topic=%s partitions=%v members=%d\n",
					g.GetGroupId(), g.GetTopic(), g.GetPartitions(), g.GetMemberCount())
			}
			return nil
		},
	}
	cmd.AddCommand(listCmd)
	return cmd
}

func adminConsumerGroupLagCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "consumer-group-lag [group-id] [partition-id]",
		Short: "Show committed-vs-high-watermark lag for a (group, partition)",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			pid, err := strconv.ParseInt(args[1], 10, 32)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := adminClient.GetConsumerGroupLag(ctx, &types.AdminConsumerGroupLag{
				GroupId: args[0], PartitionId: int32(pid),
			})
			if err != nil {
				return err
			}
			fmt.Printf("group=%s partition=%d committed=%d hwm=%d lag=%d\n",
				resp.GetGroupId(), resp.GetPartitionId(),
				resp.GetCommittedOffset(), resp.GetPartitionHighWatermark(), resp.GetLag())
			return nil
		},
	}
}

func adminSchemaListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "schema-list",
		Short: "List registered topic schemas",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := adminClient.ListSchemas(ctx, &types.ListSchemasRequest{})
			if err != nil {
				return err
			}
			if len(resp.GetSchemas()) == 0 {
				fmt.Println("(no schemas registered)")
				return nil
			}
			for _, s := range resp.GetSchemas() {
				fmt.Printf("topic=%s version=%d compat=%s\n",
					s.GetTopic(), s.GetLatestVersion(), s.GetCompatibilityMode())
			}
			return nil
		},
	}
}

func adminSchemaGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "schema-get [topic] [version]",
		Short: "Get a specific schema version (0 = latest)",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ver, err := strconv.ParseInt(args[1], 10, 32)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := adminClient.GetSchema(ctx, &types.GetSchemaRequest{
				Topic: args[0], Version: int32(ver),
			})
			if err != nil {
				return err
			}
			fmt.Printf("topic=%s version=%d type=%s definition=%s\n",
				resp.GetTopic(), resp.GetVersion(), resp.GetSchemaType(), resp.GetDefinition())
			return nil
		},
	}
}

func adminTenantUsageCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "tenant-usage [tenant-id]",
		Short: "Show per-tenant in-flight / storage usage (empty = all tenants)",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			tenantID := ""
			if len(args) == 1 {
				tenantID = args[0]
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := adminClient.GetTenantUsage(ctx, &types.TenantUsage{TenantId: tenantID})
			if err != nil {
				return err
			}
			fmt.Printf("tenant=%s in_flight=%d storage=%d limits_configured=%v\n",
				resp.GetTenantId(), resp.GetInFlight(), resp.GetStorageBytes(), resp.GetLimitsConfigured())
			if !resp.GetLimitsConfigured() {
				fmt.Println("(tenant limits not configured)")
			}
			return nil
		},
	}
}

func adminRetentionRunCmd() *cobra.Command {
	var partitionID int64
	var maxAgeHours int64
	var maxSizeBytes int64

	cmd := &cobra.Command{
		Use:   "retention-run",
		Short: "Run age/size retention once (RBAC: Subject.Admin=true)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			resp, err := adminClient.RunRetention(ctx, &types.RunRetentionRequest{
				PartitionId:  int32(partitionID),
				MaxAgeHours:  maxAgeHours,
				MaxSizeBytes: maxSizeBytes,
			})
			if err != nil {
				return err
			}
			if !resp.GetSuccess() {
				fmt.Printf("retention: success=false error=%s\n", resp.GetError())
				return nil
			}
			fmt.Printf("retention: success=true segments_deleted=%d bytes_freed=%d\n",
				resp.GetSegmentsDeleted(), resp.GetBytesFreed())
			return nil
		},
	}
	cmd.Flags().Int64Var(&partitionID, "partition-id", 0, "Partition ID (0 = all)")
	cmd.Flags().Int64Var(&maxAgeHours, "max-age-hours", 0, "Max age in hours (0 = no limit)")
	cmd.Flags().Int64Var(&maxSizeBytes, "max-size-bytes", 0, "Max size in bytes (0 = no limit)")
	return cmd
}

func adminCompactionRunCmd() *cobra.Command {
	var partitionID int64

	cmd := &cobra.Command{
		Use:   "compaction-run",
		Short: "Run consumer-offset-bounded compaction once (RBAC: Subject.Admin=true)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			resp, err := adminClient.RunCompaction(ctx, &types.RunCompactionRequest{
				PartitionId: int32(partitionID),
			})
			if err != nil {
				return err
			}
			if !resp.GetSuccess() {
				fmt.Printf("compaction: success=false error=%s\n", resp.GetError())
				return nil
			}
			fmt.Printf("compaction: success=true segments_compacted=%d bytes_reclaimed=%d\n",
				resp.GetSegmentsCompacted(), resp.GetBytesReclaimed())
			return nil
		},
	}
	cmd.Flags().Int64Var(&partitionID, "partition-id", 0, "Partition ID (required; per-partition primitive)")
	return cmd
}

func adminRebalanceCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cluster-rebalance",
		Short: "Trigger a router rebalance (RBAC: Subject.Admin=true; deferred — see error)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := adminClient.TriggerRebalance(ctx, &types.RebalanceRequest{})
			if err != nil {
				return err
			}
			fmt.Printf("rebalance: success=%v moved=%d error=%q\n",
				resp.GetSuccess(), resp.GetPartitionsMoved(), resp.GetError())
			return nil
		},
	}
}

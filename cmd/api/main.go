package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/api"
	"github.com/jatin711-debug/cronos_db_golang/internal/audit"
	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
	"github.com/jatin711-debug/cronos_db_golang/internal/cdc"
	"github.com/jatin711-debug/cronos_db_golang/internal/cluster"
	"github.com/jatin711-debug/cronos_db_golang/internal/compliance"
	"github.com/jatin711-debug/cronos_db_golang/internal/config"
	"github.com/jatin711-debug/cronos_db_golang/internal/metrics"
	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/internal/replication"
	"github.com/jatin711-debug/cronos_db_golang/internal/schema"
	"github.com/jatin711-debug/cronos_db_golang/internal/slo"
	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/internal/tenant"
	"github.com/jatin711-debug/cronos_db_golang/internal/tracing"
	"github.com/jatin711-debug/cronos_db_golang/internal/tx"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"

	"github.com/cockroachdb/pebble"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	// Setup structured logging
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		slog.Error("Failed to load config", "error", err)
		os.Exit(1)
	}

	slog.Info("Configuration loaded",
		"auth_enabled", cfg.AuthEnabled,
		"auth_jwt_secret_set", cfg.AuthJWTSecret != "",
		"auth_policy_file", cfg.AuthPolicyFile,
		"auth_explicitly_disabled_by_flag", !cfg.AuthEnabled,
	)

	// Wrap config for hot reload
	reloadableCfg := config.NewReloadableConfig(cfg)
	reloadableCfg.StartSIGHUPListener(context.Background())

	// Initialize audit logging
	auditLogger, err := audit.NewLogger(cfg.DataDir)
	if err != nil {
		slog.Warn("Failed to initialize audit logger", "error", err)
	}
	defer func() {
		if auditLogger != nil {
			_ = auditLogger.Close()
		}
	}()

	// Initialize schema registry
	schemaRegistry, err := schema.NewRegistry(cfg.DataDir)
	if err != nil {
		slog.Warn("Failed to initialize schema registry", "error", err)
	}
	_ = schemaRegistry

	// Initialize tenant accountant
	tenantAccountant := tenant.NewAccountant()

	// Initialize CDC manager
	cdcManager := cdc.NewManager()
	defer func() { _ = cdcManager.Close() }()

	// Register CDC sinks from environment
	if kafkaBrokers := os.Getenv("CRONOS_CDC_KAFKA_BROKERS"); kafkaBrokers != "" {
		kafkaTopic := os.Getenv("CRONOS_CDC_KAFKA_TOPIC")
		if kafkaTopic == "" {
			kafkaTopic = "cronos-cdc"
		}
		brokers := strings.Split(kafkaBrokers, ",")
		for i := range brokers {
			brokers[i] = strings.TrimSpace(brokers[i])
		}
		cdcManager.RegisterSink(cdc.NewKafkaSink(brokers, kafkaTopic))
		slog.Info("CDC Kafka sink registered", "brokers", brokers, "topic", kafkaTopic)
	}
	if webhookURL := os.Getenv("CRONOS_CDC_WEBHOOK_URL"); webhookURL != "" {
		cdcManager.RegisterSink(cdc.NewWebhookSink(webhookURL))
		slog.Info("CDC webhook sink registered", "url", webhookURL)
	}

	// Initialize cross-region replicator
	crossRegionReplicator := replication.NewCrossRegionReplicator(replication.RegionID(cfg.NodeRegion))
	defer func() { _ = crossRegionReplicator.Close() }()

	// Initialize SLO recorder
	sloRecorder := slo.NewRecorder(slo.Window{
		Duration:     time.Minute,
		TargetP99:    500 * time.Millisecond,
		MaxErrorRate: 0.001,
	})
	sloRecorder.Start()
	defer sloRecorder.Stop()

	// Register SLO collector with Prometheus
	prometheus.MustRegister(sloRecorder.PrometheusCollector())

	if err := tracing.InitTracing(&tracing.Config{
		ServiceName:  "cronos-api-" + cfg.NodeID,
		Enabled:      cfg.TracingEnabled,
		ExporterType: cfg.TracingExporter,
		OTLPEndpoint: cfg.TracingOTLPEndpoint,
		SampleRatio:  cfg.TracingSampleRatio,
		OTLPInsecure: cfg.TracingInsecure,
	}); err != nil {
		slog.Warn("Failed to initialize tracing; continuing without tracing", "error", err)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer shutdownCancel()
		if err := tracing.Shutdown(shutdownCtx); err != nil {
			slog.Warn("Tracing shutdown error", "error", err)
		}
	}()

	// Create shared PebbleDB block cache sized to ~25% of available RAM.
	// This prevents memory fragmentation when running many partitions.
	sharedCache := pebble.NewCache(256 << 20) // 256MB default; scales with partition count
	defer sharedCache.Unref()

	// Create partition manager with shared cache
	pm := partition.NewPartitionManagerWithCache(cfg.NodeID, cfg, sharedCache)

	// Start disk pressure monitor
	diskMonitor := partition.NewDiskMonitor(cfg.DataDir, 0.85, func() {
		slog.Info("Disk pressure detected: triggering emergency compaction")
		for i := int32(0); i < int32(cfg.PartitionCount); i++ {
			if p, err := pm.GetInternalPartition(i); err == nil && p != nil {
				p.RunCompaction()
			}
		}
	})
	diskMonitor.Start()
	defer diskMonitor.Stop()

	// Start WAL backup scheduler
	backupScheduler := storage.NewBackupScheduler(
		cfg.DataDir+"/wal",
		cfg.DataDir+"/backups",
		1*time.Hour,
		7*24*time.Hour,
	)
	backupScheduler.Start()
	defer backupScheduler.Stop()

	// Start compliance retention enforcer
	retentionEnforcer := compliance.NewEnforcer(cfg.DataDir, compliance.RetentionPolicy{
		MaxAge:       time.Duration(cfg.RetentionMaxAgeHours) * time.Hour,
		MaxSizeBytes: cfg.RetentionMaxSizeGB << 30,
	})
	utils.GoSafe("retention-enforcer", func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for range ticker.C {
			if err := retentionEnforcer.Run(context.Background()); err != nil {
				slog.Warn("Retention enforcement failed", "error", err)
			}
		}
	})

	// Create cluster manager (if enabled)
	var clusterMgr *cluster.Manager
	if cfg.ClusterEnabled {
		// Use node-specific raft directory
		raftDir := cfg.RaftDir
		if raftDir == "./raft" || raftDir == "raft" {
			// Default raft dir - make it node-specific
			raftDir = cfg.DataDir + "/raft"
		}

		clusterConfig := &cluster.Config{
			NodeID:            cfg.NodeID,
			DataDir:           cfg.DataDir,
			GossipAddr:        cfg.ClusterGossipAddr,
			GRPCAddr:          cfg.ClusterGRPCAddr,
			RaftAddr:          cfg.ClusterRaftAddr,
			RaftDir:           raftDir,
			SeedNodes:         cfg.ClusterSeeds,
			VirtualNodes:      cfg.VirtualNodes,
			HeartbeatInterval: cfg.HeartbeatInterval,
			FailureTimeout:    cfg.FailureTimeout,
			SuspectTimeout:    cfg.SuspectTimeout,
			PartitionCount:    cfg.PartitionCount,
			ReplicationFactor: cfg.ReplicationFactor,
			Rack:              cfg.NodeRack,
			Zone:              cfg.NodeZone,
			Region:            cfg.NodeRegion,
		}

		clusterMgr = cluster.NewManager(clusterConfig)
		// Wire partition state transfer hooks before starting cluster services.
		clusterMgr.SetPartitionAccessor(pm)
		if err := clusterMgr.Start(); err != nil {
			slog.Error("Failed to start cluster manager", "error", err)
			os.Exit(1)
		}

		// Join cluster if seeds provided
		if len(cfg.ClusterSeeds) > 0 {
			for _, seed := range cfg.ClusterSeeds {
				if err := clusterMgr.JoinCluster(seed); err != nil {
					slog.Warn("Failed to join cluster", "seed", seed, "error", err)
				} else {
					slog.Info("Joined cluster", "seed", seed)
					break
				}
			}
		}

		slog.Info("Cluster mode enabled", "gossip_addr", cfg.ClusterGossipAddr, "raft_addr", cfg.ClusterRaftAddr)
	}

	// Create partitions.
	// In standalone mode: create all partitions locally.
	// In cluster mode: create only partition 0 for shared dedup/consumer group state;
	// remaining partitions are lazy-created on first write via GetPartitionForKey/Topic.
	partitionsToCreate := cfg.PartitionCount
	if cfg.ClusterEnabled {
		partitionsToCreate = 1
	}
	for i := int32(0); i < int32(partitionsToCreate); i++ {
		topic := fmt.Sprintf("partition-%d", i)
		if err := pm.CreatePartition(i, topic); err != nil {
			slog.Warn("Failed to create partition", "partition_id", i, "error", err)
			continue
		}
		if err := pm.StartPartition(i); err != nil {
			slog.Warn("Failed to start partition", "partition_id", i, "error", err)
		} else {
			slog.Info("Created and started partition", "partition_id", i)
		}
	}

	// Wire CDC and cross-region replication hooks into all partitions
	for i := int32(0); i < int32(cfg.PartitionCount); i++ {
		p, err := pm.GetInternalPartition(i)
		if err != nil || p == nil || p.Wal == nil {
			continue
		}
		p.Wal.SetAppendHook(func(event *types.Event) {
			// Fast path: skip the ChangeEvent allocation + time.Now() syscall
			// entirely when there are no CDC sinks and no cross-region
			// replicator configured. This is the common case in dev / load
			// test mode and avoids per-event heap pressure at 1M+ events/sec.
			if cdcManager != nil && cdcManager.HasSinks() {
				cdcManager.Emit(context.Background(), &cdc.ChangeEvent{
					Timestamp:   time.Now(),
					Op:          "append",
					PartitionID: event.PartitionId,
					Topic:       event.Topic,
					Offset:      event.Offset,
					Event:       event,
				})
			}
			if crossRegionReplicator != nil && cfg.NodeRegion != "" {
				crossRegionReplicator.ReplicateAsync(event)
			}
		})
	}

	// Get any available partition for handler setup (dedup and consumer group are shared)
	var part *partition.Partition
	for i := int32(0); i < int32(cfg.PartitionCount); i++ {
		p, err := pm.GetInternalPartition(i)
		if err == nil && p != nil {
			part = p
			break
		}
	}
	if part == nil && !cfg.ClusterEnabled {
		slog.Error("Failed to get any partition")
		os.Exit(1)
	}

	// Build auth config if enabled
	var authConfig *auth.Config
	if cfg.AuthEnabled {
		authConfig = &auth.Config{
			Enabled: cfg.AuthEnabled,
			Policy:  auth.AllowAllPolicy(),
		}
		if cfg.AuthJWTSecret != "" {
			authConfig.JWTSecret = []byte(cfg.AuthJWTSecret)
		}
		if cfg.AuthJWTPublicKey != "" {
			pubKey, err := auth.LoadPublicKey(cfg.AuthJWTPublicKey)
			if err != nil {
				slog.Warn("Failed to load JWT public key", "error", err)
			} else {
				authConfig.JWTPublicKey = pubKey
			}
		}
		if cfg.AuthPolicyFile != "" {
			policy, err := auth.NewPolicyFromFile(cfg.AuthPolicyFile)
			if err != nil {
				slog.Warn("Failed to load auth policy", "error", err)
			} else {
				authConfig.Policy = policy
			}
		}
	}

	// Create version gate for zero-downtime upgrades
	versionGate := api.NewVersionGate()

	// Create topic rate limiter if configured
	var topicRateLimiter *api.TopicRateLimiter
	if cfg.TopicRateLimitPerSecond > 0 && cfg.TopicRateLimitBurst > 0 {
		topicRateLimiter = api.NewTopicRateLimiter(cfg.TopicRateLimitPerSecond, cfg.TopicRateLimitBurst)
	}

	// Create gRPC server
	grpcConfig := api.DefaultConfig()
	grpcConfig.Address = cfg.GPRCAddress
	if cfg.TLSEnabled {
		grpcConfig.TLS = &api.TLSConfig{
			Enabled:    cfg.TLSEnabled,
			CAFile:     cfg.TLSCAFile,
			CertFile:   cfg.TLSCertFile,
			KeyFile:    cfg.TLSKeyFile,
			ClientAuth: cfg.TLSClientAuth,
		}
	}
	grpcConfig.Auth = authConfig
	grpcConfig.AuditLogger = auditLogger
	grpcConfig.VersionGate = versionGate
	grpcConfig.TopicRateLimiter = topicRateLimiter
	grpcConfig.SLORecorder = sloRecorder

	grpcServer, err := api.NewGRPCServer(grpcConfig)
	if err != nil {
		slog.Error("Failed to create gRPC server", "error", err)
		os.Exit(1)
	}

	// Create event service handler
	var eventHandler *api.EventServiceHandler
	if part != nil {
		eventHandler = api.NewEventServiceHandler(
			pm,
			part.DedupStore,
			part.ConsumerGroup,
		)
	} else {
		// Cluster mode - partition might be on another node
		eventHandler = api.NewEventServiceHandler(pm, nil, nil)
	}

	// Wire framework components into event handler
	if auditLogger != nil {
		eventHandler.SetAuditLogger(auditLogger)
	}
	if schemaRegistry != nil {
		eventHandler.SetSchemaRegistry(schemaRegistry)
	}
	if tenantAccountant != nil {
		eventHandler.SetTenantAccountant(tenantAccountant)
		pm.SetTenantAccountant(tenantAccountant)
	}

	// Wire cluster router for partition-aware request routing
	if clusterMgr != nil {
		eventHandler.SetClusterRouter(clusterMgr)
	}

	// Wire topic-level RBAC policy (nil when auth is disabled)
	if authConfig != nil {
		eventHandler.SetAuthPolicy(authConfig.Policy)
	}

	// Create consumer group service handler
	var consumerHandler *api.ConsumerGroupServiceHandler
	if part != nil {
		consumerHandler = api.NewConsumerGroupServiceHandler(part.ConsumerGroup)
	}

	// Create partition metadata service handler
	partitionHandler := api.NewPartitionServiceHandler(pm, clusterMgr, cfg.NodeID)

	// Create transaction service handler (2PC)
	transactionHandler := tx.NewHandler(pm)
	grpcServer.SetTransactionHandler(transactionHandler)

	// Register cross-region replication server
	crossRegionServer := api.NewCrossRegionServer(pm)
	grpcServer.RegisterCrossRegionServer(crossRegionServer)

	// Register internal replication and raft metadata services
	replicationServer := api.NewReplicationServiceHandler(pm)
	grpcServer.RegisterReplicationServer(replicationServer)
	if clusterMgr != nil {
		raftServer := api.NewRaftServiceHandler(clusterMgr, cfg.NodeID)
		grpcServer.RegisterRaftServer(raftServer)
	}

	// Load remote regions from environment
	if regions := os.Getenv("CRONOS_REGIONS"); regions != "" {
		for r := range strings.SplitSeq(regions, ",") {
			parts := strings.SplitN(strings.TrimSpace(r), "=", 2)
			if len(parts) == 2 {
				crossRegionReplicator.AddRegion(&replication.RegionConnection{
					RegionID: replication.RegionID(parts[0]),
					Endpoint: parts[1],
				})
			}
		}
	}

	// Register services
	grpcServer.RegisterServices(eventHandler, consumerHandler, partitionHandler)

	// Start gRPC server
	slog.Info("Starting gRPC server", "address", cfg.GPRCAddress)
	if err := grpcServer.Start(); err != nil {
		slog.Error("Failed to start gRPC server", "error", err)
		os.Exit(1)
	}

	// Propagate unexpected gRPC server exits as fatal.
	utils.GoSafe("grpc-serve-monitor", func() {
		if err := <-grpcServer.ServeError(); err != nil {
			slog.Error("gRPC server exited unexpectedly", "error", err)
			os.Exit(1)
		}
	})

	// Health check endpoint with cluster status
	mux := http.NewServeMux()

	healthChecker := api.NewHealthChecker(cfg, pm, clusterMgr)
	healthChecker.Register(mux)

	mux.Handle("/metrics", promhttp.Handler())

	healthServer := &http.Server{
		Addr:    cfg.HTTPAddress,
		Handler: mux,
	}

	healthErr := make(chan error, 1)
	utils.GoSafe("health-server", func() {
		slog.Info("Starting health check server", "address", cfg.HTTPAddress)
		if err := healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			healthErr <- err
		}
	})

	// Fail fast if the health server cannot bind or starts serving errors.
	utils.GoSafe("health-start-monitor", func() {
		if err := <-healthErr; err != nil {
			slog.Error("Health server failed", "error", err)
			os.Exit(1)
		}
	})

	// Start auto-scaler with real system metrics
	autoScaler := cluster.NewAutoScaler(cluster.NewSystemMetrics(cfg.DataDir))
	autoScaler.Start()
	defer autoScaler.Stop()

	// Setup signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Print stats periodically using config interval
	statsPrintInterval := cfg.StatsPrintInterval
	if statsPrintInterval == 0 {
		statsPrintInterval = 30 * time.Second
	}

	utils.GoSafe("stats-printer", func() {
		ticker := time.NewTicker(statsPrintInterval)
		defer ticker.Stop()

		// To keep the stats loop O(1) regardless of partition count, we sample
		// a fixed window of partitions each tick and rotate the window.
		const sampleSize int32 = 16
		partitionCount := int32(cfg.PartitionCount)
		window := sampleSize
		if partitionCount < window {
			window = partitionCount
		}
		var offset int32

		for {
			select {
			case <-ticker.C:
				// Refresh low-frequency gauges on the same interval as stats logging.
				sampled := 0
				for j := int32(0); j < window; j++ {
					i := (offset + j) % partitionCount
					p, err := pm.GetInternalPartition(i)
					if err != nil || p == nil {
						continue
					}
					sampled++

					partitionLabel := strconv.FormatInt(int64(i), 10)

					if p.Scheduler != nil {
						schedulerStats := p.Scheduler.GetStats()
						metrics.SetTimingWheelMetrics(partitionLabel, schedulerStats.ActiveTimers, int64(schedulerStats.OverflowLevel))
					}

					if p.Wal != nil {
						activeSegmentSize := int64(0)
						if activeSegment := p.Wal.GetActiveSegment(); activeSegment != nil {
							activeSegmentSize = activeSegment.GetSize()
						}
						metrics.SetWALMetrics(partitionLabel, len(p.Wal.GetSegments()), activeSegmentSize, p.Wal.GetHighWatermark())
					}

					if p.DedupStore != nil {
						if dedupStats, err := p.DedupStore.GetStats(); err == nil && dedupStats != nil {
							falsePositiveRate := 0.0
							totalBloomPositives := dedupStats.BloomHits + dedupStats.BloomFalsePositives
							if totalBloomPositives > 0 {
								falsePositiveRate = float64(dedupStats.BloomFalsePositives) / float64(totalBloomPositives)
							}
							metrics.SetDedupMetrics(partitionLabel, dedupStats.BloomMemoryBytes, falsePositiveRate, dedupStats.PebbleHits)
						}
					}

					if p.Dispatcher != nil {
						dispatcherStats := p.Dispatcher.GetStats()
						workerQueueDepth := int64(0)
						if p.Worker != nil {
							workerQueueDepth = p.Worker.GetStats().QueueLength
						}
						metrics.SetDeliveryMetrics(partitionLabel, dispatcherStats.ActiveDeliveries, dispatcherStats.CreditsInUse, workerQueueDepth)
					}

					if p.ConsumerGroup != nil && p.Wal != nil {
						hwm := p.Wal.GetHighWatermark()
						for _, group := range p.ConsumerGroup.ListGroups() {
							var lag int64
							for _, part := range group.Partitions {
								committed := group.CommittedOffsets[part]
								if committed < 0 {
									committed = -1
								}
								partLag := hwm - committed
								if partLag < 0 {
									partLag = 0
								}
								lag += partLag
								metrics.SetConsumerGroupMetrics(group.GroupID, strconv.FormatInt(int64(part), 10), partLag, len(group.Members))
							}
						}
					}
				}

				offset = (offset + window) % partitionCount

				stats := pm.GetStats()
				slog.Info("Stats", "stats", stats, "sampled_partitions", sampled)
				if cfg.ClusterEnabled && clusterMgr != nil {
					clusterStats := clusterMgr.GetStats()
					metrics.SetClusterMetrics(
						int64(clusterStats.TotalNodes),
						int64(clusterStats.AliveNodes),
						int64(clusterStats.NumPartitions),
						int64(clusterStats.LeaderPartitions),
					)
					slog.Info("Cluster Stats", "stats", clusterStats)
				} else {
					metrics.SetClusterMetrics(1, 1, stats.TotalPartitions, stats.LeaderPartitions)
				}
			case <-ctx.Done():
				return
			}
		}
	})

	// Wait for shutdown signal
	<-sigChan
	slog.Info("Shutting down...", "node_id", cfg.NodeID)
	cancel() // Stop background tasks (stats, etc.)

	// Create an overall shutdown context with generous timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer shutdownCancel()

	shutdownDone := make(chan struct{})
	utils.GoSafe("shutdown", func() {
		defer close(shutdownDone)

		// 1. Stop accepting NEW requests first (drain phase)
		slog.Info("Shutdown phase 1: Draining incoming requests...")
		grpcDrainCtx, grpcDrainCancel := context.WithTimeout(shutdownCtx, 25*time.Second)
		defer grpcDrainCancel()
		grpcServer.GracefulStopWithTimeout(grpcDrainCtx) // stop accepting new gRPC connections

		// 2. Shutdown health server to stop new HTTP health checks
		healthShutdownCtx, healthShutdownCancel := context.WithTimeout(shutdownCtx, 5*time.Second)
		defer healthShutdownCancel()
		if err := healthServer.Shutdown(healthShutdownCtx); err != nil {
			slog.Warn("Health server shutdown error", "error", err)
		}

		// 3. Stop all partitions gracefully (drains in-flight deliveries, flushes WAL)
		slog.Info("Shutdown phase 2: Stopping partitions (draining deliveries, flushing WAL)...")
		if err := pm.Close(); err != nil {
			slog.Error("Failed to cleanly stop all partitions", "error", err)
		}

		// 4. Shutdown cluster manager
		if clusterMgr != nil {
			slog.Info("Shutdown phase 3: Stopping cluster manager...")
			if err := clusterMgr.Stop(); err != nil {
				slog.Error("Failed to stop cluster manager", "error", err)
			}
		}

		// 5. Final WAL flush for all partitions (extra safety)
		slog.Info("Shutdown phase 4: Final WAL flush...")
		for i := int32(0); i < int32(cfg.PartitionCount); i++ {
			p, err := pm.GetInternalPartition(i)
			if err != nil || p == nil || p.Wal == nil {
				continue
			}
			if err := p.Wal.Flush(); err != nil {
				slog.Warn("Final WAL flush failed", "partition", i, "error", err)
			}
		}

		slog.Info("Shutdown complete", "node_id", cfg.NodeID)
	})

	// Wait for shutdown or timeout
	select {
	case <-shutdownDone:
		slog.Info("Graceful shutdown completed successfully")
	case <-shutdownCtx.Done():
		slog.Error("Shutdown timed out after 60s, forcing exit")
		// Force stop gRPC if still running
		grpcServer.Stop()
	}
}

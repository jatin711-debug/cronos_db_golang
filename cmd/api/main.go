package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/api"
	"github.com/jatin711-debug/cronos_db_golang/internal/audit"
	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
	"github.com/jatin711-debug/cronos_db_golang/internal/cdc"
	"github.com/jatin711-debug/cronos_db_golang/internal/cluster"
	"github.com/jatin711-debug/cronos_db_golang/internal/compliance"
	"github.com/jatin711-debug/cronos_db_golang/internal/config"
	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/internal/replication"
	"github.com/jatin711-debug/cronos_db_golang/internal/schema"
	"github.com/jatin711-debug/cronos_db_golang/internal/slo"
	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/internal/tenant"
	"github.com/jatin711-debug/cronos_db_golang/internal/tracing"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"

	"github.com/cockroachdb/pebble"
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

	// Initialize cross-region replicator
	crossRegionReplicator := replication.NewCrossRegionReplicator(replication.RegionID(cfg.NodeRegion))
	defer func() { _ = crossRegionReplicator.Close() }()

	// Initialize SLO recorder
	sloRecorder := slo.NewRecorder(slo.Window{
		Duration:     time.Minute,
		TargetP99:    500 * time.Millisecond,
		MaxErrorRate: 0.001,
	})
	_ = sloRecorder

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
		MaxAge:       30 * 24 * time.Hour,
		MaxSizeBytes: 100 << 30, // 100GB
	})
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for range ticker.C {
			if err := retentionEnforcer.Run(context.Background()); err != nil {
				slog.Warn("Retention enforcement failed", "error", err)
			}
		}
	}()

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
			if cdcManager != nil {
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
				crossRegionReplicator.ReplicateAsync(event.PartitionId, event.Offset, event.Payload)
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

	grpcServer := api.NewGRPCServer(grpcConfig)

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
	}

	// Wire cluster router for partition-aware request routing
	if clusterMgr != nil {
		eventHandler.SetClusterRouter(clusterMgr)
	}

	// Create consumer group service handler
	var consumerHandler *api.ConsumerGroupServiceHandler
	if part != nil {
		consumerHandler = api.NewConsumerGroupServiceHandler(part.ConsumerGroup)
	}

	// Create partition metadata service handler
	partitionHandler := api.NewPartitionServiceHandler(pm, clusterMgr, cfg.NodeID)

	// Register services
	grpcServer.RegisterServices(eventHandler, consumerHandler, partitionHandler)

	// Start gRPC server
	slog.Info("Starting gRPC server", "address", cfg.GPRCAddress)
	if err := grpcServer.Start(); err != nil {
		slog.Error("Failed to start gRPC server", "error", err)
		os.Exit(1)
	}

	// Health check endpoint with cluster status
	mux := http.NewServeMux()

	healthChecker := api.NewHealthChecker(cfg, pm, clusterMgr)
	healthChecker.Register(mux)

	mux.Handle("/metrics", promhttp.Handler())

	healthServer := &http.Server{
		Addr:    cfg.HTTPAddress,
		Handler: mux,
	}

	go func() {
		slog.Info("Starting health check server", "address", cfg.HTTPAddress)
		if err := healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Failed to start health server", "error", err)
			os.Exit(1)
		}
	}()

	// Start auto-scaler
	autoScaler := cluster.NewAutoScaler(cluster.SimpleMetrics{})
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

	go func() {
		ticker := time.NewTicker(statsPrintInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Refresh low-frequency gauges on the same interval as stats logging.
				for i := int32(0); i < int32(cfg.PartitionCount); i++ {
					p, err := pm.GetInternalPartition(i)
					if err != nil || p == nil {
						continue
					}

					partitionLabel := strconv.FormatInt(int64(i), 10)

					if p.Scheduler != nil {
						schedulerStats := p.Scheduler.GetStats()
						api.SetTimingWheelMetrics(partitionLabel, schedulerStats.ActiveTimers, int64(schedulerStats.OverflowLevel))
					}

					if p.Wal != nil {
						activeSegmentSize := int64(0)
						if activeSegment := p.Wal.GetActiveSegment(); activeSegment != nil {
							activeSegmentSize = activeSegment.GetSize()
						}
						api.SetWALMetrics(partitionLabel, len(p.Wal.GetSegments()), activeSegmentSize, p.Wal.GetHighWatermark())
					}

					if p.DedupStore != nil {
						if dedupStats, err := p.DedupStore.GetStats(); err == nil && dedupStats != nil {
							falsePositiveRate := 0.0
							totalBloomPositives := dedupStats.BloomHits + dedupStats.BloomFalsePositives
							if totalBloomPositives > 0 {
								falsePositiveRate = float64(dedupStats.BloomFalsePositives) / float64(totalBloomPositives)
							}
							api.SetDedupMetrics(partitionLabel, dedupStats.BloomMemoryBytes, falsePositiveRate, dedupStats.PebbleHits)
						}
					}

					if p.Dispatcher != nil {
						dispatcherStats := p.Dispatcher.GetStats()
						workerQueueDepth := int64(0)
						if p.Worker != nil {
							workerQueueDepth = p.Worker.GetStats().QueueLength
						}
						api.SetDeliveryMetrics(partitionLabel, dispatcherStats.ActiveDeliveries, dispatcherStats.CreditsInUse, workerQueueDepth)
					}
				}

				stats := pm.GetStats()
				slog.Info("Stats", "stats", stats)
				if cfg.ClusterEnabled && clusterMgr != nil {
					clusterStats := clusterMgr.GetStats()
					api.SetClusterMetrics(
						int64(clusterStats.TotalNodes),
						int64(clusterStats.AliveNodes),
						int64(clusterStats.NumPartitions),
						int64(clusterStats.LeaderPartitions),
					)
					slog.Info("Cluster Stats", "stats", clusterStats)
				} else {
					api.SetClusterMetrics(1, 1, stats.TotalPartitions, stats.LeaderPartitions)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	slog.Info("Shutting down...")
	cancel() // Stop background tasks

	// 1. Graceful gRPC shutdown: stop accepting new requests but finish in-flight RPCs
	grpcShutdownCtx, grpcShutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer grpcShutdownCancel()
	go func() {
		<-grpcShutdownCtx.Done()
		grpcServer.Stop() // Force stop if graceful takes too long
	}()
	grpcServer.GracefulStop()

	// 2. Shutdown health server
	healthShutdownCtx, healthShutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer healthShutdownCancel()
	if err := healthServer.Shutdown(healthShutdownCtx); err != nil {
		slog.Error("Health server shutdown error", "error", err)
	}

	// 3. Stop all partitions gracefully (drains in-flight deliveries, flushes WAL)
	slog.Info("Stopping all partitions...")
	if err := pm.StopAllPartitions(); err != nil {
		slog.Error("Failed to cleanly stop all partitions", "error", err)
	}

	// 4. Shutdown cluster manager
	if clusterMgr != nil {
		if err := clusterMgr.Stop(); err != nil {
			slog.Error("Failed to stop cluster manager", "error", err)
		}
	}

	slog.Info("Shutdown complete")
}

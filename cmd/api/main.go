package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cronos_db/internal/api"
	"cronos_db/internal/cluster"
	"cronos_db/internal/config"
	"cronos_db/internal/partition"
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

	// Create partition manager
	pm := partition.NewPartitionManager(cfg.NodeID, cfg)

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
			SeedNodes:         cfg.ClusterSeeds, // Pass seed nodes for join vs bootstrap decision
			VirtualNodes:      cfg.VirtualNodes,
			HeartbeatInterval: cfg.HeartbeatInterval,
			FailureTimeout:    cfg.FailureTimeout,
			SuspectTimeout:    cfg.SuspectTimeout,
			PartitionCount:    cfg.PartitionCount,
			ReplicationFactor: cfg.ReplicationFactor,
		}

		clusterMgr = cluster.NewManager(clusterConfig)
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

	// Create all partitions locally
	// Each node creates all partitions so it can handle any routed events
	// The cluster router determines which node owns each partition for consistency
	for i := int32(0); i < int32(cfg.PartitionCount); i++ {
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

	// Create gRPC server
	grpcConfig := api.DefaultConfig()
	grpcConfig.Address = cfg.GPRCAddress

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

	// Create consumer group service handler
	var consumerHandler *api.ConsumerGroupServiceHandler
	if part != nil {
		consumerHandler = api.NewConsumerGroupServiceHandler(part.ConsumerGroup)
	}

	// Register services
	grpcServer.RegisterServices(eventHandler, consumerHandler)

	// Start gRPC server
	slog.Info("Starting gRPC server", "address", cfg.GPRCAddress)
	if err := grpcServer.Start(); err != nil {
		slog.Error("Failed to start gRPC server", "error", err)
		os.Exit(1)
	}

	// Health check endpoint with cluster status
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if cfg.ClusterEnabled && clusterMgr != nil {
			fmt.Fprintf(w, "OK - Cluster Mode\n")
			fmt.Fprintf(w, "Node: %s\n", cfg.NodeID)
		} else {
			fmt.Fprintf(w, "OK - Standalone Mode\n")
		}
	})

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
				stats := pm.GetStats()
				slog.Info("Stats", "stats", stats)
				if cfg.ClusterEnabled && clusterMgr != nil {
					clusterStats := clusterMgr.GetStats()
					slog.Info("Cluster Stats", "stats", clusterStats)
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

	// Shutdown cluster manager
	if clusterMgr != nil {
		if err := clusterMgr.Stop(); err != nil {
			slog.Error("Failed to stop cluster manager", "error", err)
		}
	}

	// Shutdown gRPC server
	grpcServer.Stop()

	// Shutdown health server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	healthServer.Shutdown(shutdownCtx)

	// Stop partition
	if part != nil {
		if err := pm.StopPartition(0); err != nil {
			slog.Error("Failed to stop partition", "error", err)
		}
	}

	slog.Info("Shutdown complete")
}

package main

import (
	"context"
	"fmt"
	"log"
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
	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
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
			log.Fatalf("Failed to start cluster manager: %v", err)
		}

		// Join cluster if seeds provided
		if len(cfg.ClusterSeeds) > 0 {
			for _, seed := range cfg.ClusterSeeds {
				if err := clusterMgr.JoinCluster(seed); err != nil {
					log.Printf("Warning: Failed to join cluster via %s: %v", seed, err)
				} else {
					log.Printf("Joined cluster via seed %s", seed)
					break
				}
			}
		}

		log.Printf("Cluster mode enabled - Gossip: %s, Raft: %s", cfg.ClusterGossipAddr, cfg.ClusterRaftAddr)
	}

	// Create all partitions locally
	// Each node creates all partitions so it can handle any routed events
	// The cluster router determines which node owns each partition for consistency
	for i := int32(0); i < int32(cfg.PartitionCount); i++ {
		topic := fmt.Sprintf("partition-%d", i)
		if err := pm.CreatePartition(i, topic); err != nil {
			log.Printf("Warning: Failed to create partition %d: %v", i, err)
			continue
		}
		if err := pm.StartPartition(i); err != nil {
			log.Printf("Warning: Failed to start partition %d: %v", i, err)
		} else {
			log.Printf("Created and started partition %d", i)
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
		log.Fatalf("Failed to get any partition")
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
	log.Printf("Starting gRPC server on %s", cfg.GPRCAddress)
	if err := grpcServer.Start(); err != nil {
		log.Fatalf("Failed to start gRPC server: %v", err)
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
		log.Printf("Starting health check server on %s", cfg.HTTPAddress)
		if err := healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start health server: %v", err)
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
				log.Printf("Stats: %+v", stats)
				if cfg.ClusterEnabled && clusterMgr != nil {
					clusterStats := clusterMgr.GetStats()
					log.Printf("Cluster Stats: %+v", clusterStats)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	log.Println("Shutting down...")

	// Shutdown cluster manager
	if clusterMgr != nil {
		if err := clusterMgr.Stop(); err != nil {
			log.Printf("Failed to stop cluster manager: %v", err)
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
			log.Printf("Failed to stop partition: %v", err)
		}
	}

	log.Println("Shutdown complete")
}

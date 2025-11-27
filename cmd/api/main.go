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

	// Create default partition
	if err := pm.CreatePartition(0, "default-topic"); err != nil {
		log.Fatalf("Failed to create partition: %v", err)
	}

	// Start partition
	if err := pm.StartPartition(0); err != nil {
		log.Fatalf("Failed to start partition: %v", err)
	}

	// Get partition
	partition, err := pm.GetInternalPartition(0)
	if err != nil {
		log.Fatalf("Failed to get partition: %v", err)
	}

	// Create gRPC server
	grpcConfig := api.DefaultConfig()
	grpcConfig.Address = cfg.GPRCAddress

	grpcServer := api.NewGRPCServer(grpcConfig)

	// Create event service handler
	eventHandler := api.NewEventServiceHandler(
		pm,
		partition.DedupStore,
		partition.ConsumerGroup,
	)

	// Register services
	grpcServer.RegisterServices(eventHandler)

	// Start gRPC server
	log.Printf("Starting gRPC server on %s", cfg.GPRCAddress)
	if err := grpcServer.Start(); err != nil {
		log.Fatalf("Failed to start gRPC server: %v", err)
	}

	// Health check endpoint
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "OK")
	})

	healthServer := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		log.Printf("Starting health check server on :8080")
		if err := healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start health server: %v", err)
		}
	}()

	// Setup signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Print stats periodically
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				stats := pm.GetStats()
				log.Printf("Stats: %+v", stats)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	log.Println("Shutting down...")

	// Shutdown gRPC server
	grpcServer.Stop()

	// Shutdown health server
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	healthServer.Shutdown(ctx)

	// Stop partition
	if err := pm.StopPartition(0); err != nil {
		log.Printf("Failed to stop partition: %v", err)
	}

	log.Println("Shutdown complete")
}

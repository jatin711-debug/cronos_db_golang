//go:build clustertest
// +build clustertest

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"cronos_db/pkg/types"
)

// ClusterNode represents a node in the cluster
type ClusterNode struct {
	ID       string
	GRPCAddr string
	HTTPAddr string
	conn     *grpc.ClientConn
	client   types.EventServiceClient
}

// ClusterLoadTestConfig holds cluster load test configuration
type ClusterLoadTestConfig struct {
	Nodes          []ClusterNode
	NumPublishers  int           // Publishers per node
	EventsPerPub   int           // Events per publisher
	PayloadSize    int           // Payload size in bytes
	ScheduleDelay  time.Duration // How far in future to schedule
	Topic          string
	RoundRobin     bool // Distribute events round-robin across nodes
	ReportInterval time.Duration
	TestFailover   bool          // Test node failover scenario
	FailoverAfter  time.Duration // When to simulate node failure
	BatchMode      bool          // Use batch publish API
	BatchSize      int           // Events per batch
}

// ClusterMetrics holds cluster-wide metrics
type ClusterMetrics struct {
	mu sync.Mutex

	// Per-node metrics
	NodePublished map[string]int64
	NodeErrors    map[string]int64
	NodeLatencies map[string][]time.Duration

	// Aggregate metrics
	TotalPublished int64
	TotalErrors    int64
	AllLatencies   []time.Duration
	StartTime      time.Time
	EndTime        time.Time

	// Partition distribution
	PartitionCounts map[int32]int64
}

func main() {
	// Parse flags
	node1GRPC := flag.String("node1-grpc", "127.0.0.1:9000", "Node 1 gRPC address")
	node1HTTP := flag.String("node1-http", "127.0.0.1:8080", "Node 1 HTTP address")
	node2GRPC := flag.String("node2-grpc", "127.0.0.1:9001", "Node 2 gRPC address")
	node2HTTP := flag.String("node2-http", "127.0.0.1:8081", "Node 2 HTTP address")
	node3GRPC := flag.String("node3-grpc", "127.0.0.1:9002", "Node 3 gRPC address")
	node3HTTP := flag.String("node3-http", "127.0.0.1:8082", "Node 3 HTTP address")
	numNodes := flag.Int("nodes", 3, "Number of nodes to test (1-3)")
	publishers := flag.Int("publishers", 10, "Publishers per node")
	events := flag.Int("events", 10000, "Events per publisher")
	payloadSize := flag.Int("payload", 256, "Payload size in bytes")
	scheduleDelay := flag.Duration("delay", 5*time.Second, "Schedule delay from now")
	topic := flag.String("topic", "cluster-loadtest", "Topic name")
	roundRobin := flag.Bool("round-robin", true, "Distribute events round-robin across nodes")
	testFailover := flag.Bool("failover", false, "Test failover scenario")
	failoverAfter := flag.Duration("failover-after", 10*time.Second, "When to simulate failover")
	batchMode := flag.Bool("batch", false, "Use batch publish API for higher throughput")
	batchSize := flag.Int("batch-size", 100, "Events per batch when using batch mode")

	flag.Parse()

	// Build node list
	allNodes := []ClusterNode{
		{ID: "node1", GRPCAddr: *node1GRPC, HTTPAddr: *node1HTTP},
		{ID: "node2", GRPCAddr: *node2GRPC, HTTPAddr: *node2HTTP},
		{ID: "node3", GRPCAddr: *node3GRPC, HTTPAddr: *node3HTTP},
	}

	nodes := allNodes[:*numNodes]

	config := ClusterLoadTestConfig{
		Nodes:          nodes,
		NumPublishers:  *publishers,
		EventsPerPub:   *events,
		PayloadSize:    *payloadSize,
		ScheduleDelay:  *scheduleDelay,
		Topic:          *topic,
		RoundRobin:     *roundRobin,
		ReportInterval: 5 * time.Second,
		TestFailover:   *testFailover,
		FailoverAfter:  *failoverAfter,
		BatchMode:      *batchMode,
		BatchSize:      *batchSize,
	}

	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║           CronosDB Cluster Load Test                          ║")
	fmt.Println("╠═══════════════════════════════════════════════════════════════╣")
	fmt.Printf("║ Nodes:        %-48d ║\n", len(nodes))
	for _, n := range nodes {
		fmt.Printf("║   %s:       %-48s ║\n", n.ID, n.GRPCAddr)
	}
	fmt.Printf("║ Publishers:   %-48d ║\n", config.NumPublishers*len(nodes))
	fmt.Printf("║ Events/Pub:   %-48d ║\n", config.EventsPerPub)
	fmt.Printf("║ Total Events: %-48d ║\n", config.NumPublishers*config.EventsPerPub*len(nodes))
	fmt.Printf("║ Payload Size: %-48d ║\n", config.PayloadSize)
	fmt.Printf("║ Round Robin:  %-48v ║\n", config.RoundRobin)
	fmt.Printf("║ Batch Mode:   %-48v ║\n", config.BatchMode)
	fmt.Printf("║ Batch Size:   %-48d ║\n", config.BatchSize)
	fmt.Printf("║ Failover:     %-48v ║\n", config.TestFailover)
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// Check cluster health first
	fmt.Println("🔍 Checking cluster health...")
	if err := checkClusterHealth(nodes); err != nil {
		log.Fatalf("Cluster health check failed: %v", err)
	}
	fmt.Println("✅ All nodes healthy")
	fmt.Println()

	// Run load test
	metrics := runClusterLoadTest(config)

	// Print results
	printClusterResults(config, metrics)
}

// checkClusterHealth verifies all nodes are responding
func checkClusterHealth(nodes []ClusterNode) error {
	for _, node := range nodes {
		resp, err := http.Get(fmt.Sprintf("http://%s/health", node.HTTPAddr))
		if err != nil {
			return fmt.Errorf("node %s health check failed: %w", node.ID, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("node %s unhealthy: status %d", node.ID, resp.StatusCode)
		}
		fmt.Printf("  ✓ %s (%s) - healthy\n", node.ID, node.GRPCAddr)
	}
	return nil
}

// runClusterLoadTest runs the distributed load test
func runClusterLoadTest(config ClusterLoadTestConfig) *ClusterMetrics {
	metrics := &ClusterMetrics{
		NodePublished:   make(map[string]int64),
		NodeErrors:      make(map[string]int64),
		NodeLatencies:   make(map[string][]time.Duration),
		PartitionCounts: make(map[int32]int64),
		StartTime:       time.Now(),
	}

	// Connect to all nodes
	for i := range config.Nodes {
		conn, err := grpc.Dial(config.Nodes[i].GRPCAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithTimeout(5*time.Second))
		if err != nil {
			log.Fatalf("Failed to connect to %s: %v", config.Nodes[i].ID, err)
		}
		config.Nodes[i].conn = conn
		config.Nodes[i].client = types.NewEventServiceClient(conn)
		defer conn.Close()
	}

	// Progress tracking
	var totalPublished int64
	totalEvents := int64(config.NumPublishers * config.EventsPerPub * len(config.Nodes))

	// Start progress reporter
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(config.ReportInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				published := atomic.LoadInt64(&totalPublished)
				elapsed := time.Since(metrics.StartTime)
				rate := float64(published) / elapsed.Seconds()
				pct := float64(published) / float64(totalEvents) * 100
				fmt.Printf("📊 Progress: %d/%d (%.1f%%) | Rate: %.0f events/sec\n",
					published, totalEvents, pct, rate)
			case <-done:
				return
			}
		}
	}()

	// Run publishers for each node
	var wg sync.WaitGroup

	if config.BatchMode {
		// Batch mode: use PublishBatch API
		for pubIdx := 0; pubIdx < config.NumPublishers*len(config.Nodes); pubIdx++ {
			wg.Add(1)
			go func(publisherID int) {
				defer wg.Done()
				runBatchPublisher(config, metrics, publisherID, &totalPublished)
			}(pubIdx)
		}
	} else if config.RoundRobin {
		// Round-robin: each publisher sends to all nodes in rotation
		for pubIdx := 0; pubIdx < config.NumPublishers*len(config.Nodes); pubIdx++ {
			wg.Add(1)
			go func(publisherID int) {
				defer wg.Done()
				runRoundRobinPublisher(config, metrics, publisherID, &totalPublished)
			}(pubIdx)
		}
	} else {
		// Per-node: publishers are dedicated to specific nodes
		for _, node := range config.Nodes {
			for pubIdx := 0; pubIdx < config.NumPublishers; pubIdx++ {
				wg.Add(1)
				go func(n ClusterNode, pid int) {
					defer wg.Done()
					runNodePublisher(config, metrics, n, pid, &totalPublished)
				}(node, pubIdx)
			}
		}
	}

	wg.Wait()
	close(done)

	metrics.EndTime = time.Now()
	return metrics
}

// runBatchPublisher sends events in batches for high throughput
func runBatchPublisher(config ClusterLoadTestConfig, metrics *ClusterMetrics, publisherID int, totalPublished *int64) {
	payload := make([]byte, config.PayloadSize)
	rand.Read(payload)

	nodeCount := len(config.Nodes)
	batchSize := config.BatchSize
	totalEvents := config.EventsPerPub

	for batchStart := 0; batchStart < totalEvents; batchStart += batchSize {
		// Select node in round-robin fashion based on batch
		node := config.Nodes[(publisherID+batchStart/batchSize)%nodeCount]

		// Build batch
		batchEnd := batchStart + batchSize
		if batchEnd > totalEvents {
			batchEnd = totalEvents
		}

		events := make([]*types.Event, 0, batchEnd-batchStart)
		scheduleTime := time.Now().Add(config.ScheduleDelay)

		for i := batchStart; i < batchEnd; i++ {
			eventKey := fmt.Sprintf("pub-%d-event-%d", publisherID, i)
			event := &types.Event{
				MessageId:  fmt.Sprintf("batch-%d-%d-%d", publisherID, i, time.Now().UnixNano()),
				ScheduleTs: scheduleTime.UnixMilli(),
				Payload:    payload,
				Topic:      config.Topic,
				Meta: map[string]string{
					"publisher":     fmt.Sprintf("%d", publisherID),
					"sequence":      fmt.Sprintf("%d", i),
					"partition_key": eventKey,
				},
			}
			events = append(events, event)
		}

		req := &types.PublishBatchRequest{
			Events: events,
		}

		// Publish batch with timing
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		resp, err := node.client.PublishBatch(ctx, req)
		cancel()
		latency := time.Since(start)

		eventsInBatch := int64(len(events))

		metrics.mu.Lock()
		if err != nil {
			metrics.NodeErrors[node.ID] += eventsInBatch
			metrics.TotalErrors += eventsInBatch
			// Log first gRPC error for debugging
			log.Printf("gRPC ERROR %s: %v (errors so far: %d)", node.ID, err, metrics.NodeErrors[node.ID])
		} else if resp != nil {
			metrics.NodePublished[node.ID] += int64(resp.PublishedCount)
			metrics.TotalPublished += int64(resp.PublishedCount)
			metrics.TotalErrors += int64(resp.ErrorCount)
			// Log first handler-level error
			if resp.ErrorCount > 0 {
				log.Printf("Handler ERROR %s: resp.ErrorCount=%d (dup=%d, published=%d)",
					node.ID, resp.ErrorCount, resp.DuplicateCount, resp.PublishedCount)
			}
			// Record latency per event (amortized)
			perEventLatency := latency / time.Duration(len(events))
			for range events {
				metrics.NodeLatencies[node.ID] = append(metrics.NodeLatencies[node.ID], perEventLatency)
				metrics.AllLatencies = append(metrics.AllLatencies, perEventLatency)
			}
		}
		metrics.mu.Unlock()

		atomic.AddInt64(totalPublished, eventsInBatch)
	}
}

// runRoundRobinPublisher sends events across all nodes in rotation
func runRoundRobinPublisher(config ClusterLoadTestConfig, metrics *ClusterMetrics, publisherID int, totalPublished *int64) {
	payload := make([]byte, config.PayloadSize)
	rand.Read(payload)

	nodeCount := len(config.Nodes)

	for i := 0; i < config.EventsPerPub; i++ {
		// Select node in round-robin fashion
		node := config.Nodes[(publisherID+i)%nodeCount]

		// Create event with unique key for partition distribution
		eventKey := fmt.Sprintf("pub-%d-event-%d", publisherID, i)
		scheduleTime := time.Now().Add(config.ScheduleDelay)

		event := &types.Event{
			MessageId:  fmt.Sprintf("cluster-test-%d-%d-%d", publisherID, i, time.Now().UnixNano()),
			ScheduleTs: scheduleTime.UnixMilli(),
			Payload:    payload,
			Topic:      config.Topic,
			Meta: map[string]string{
				"publisher":   fmt.Sprintf("%d", publisherID),
				"sequence":    fmt.Sprintf("%d", i),
				"target_node": node.ID,
				"event_key":   eventKey,
			},
		}

		req := &types.PublishRequest{
			Event: event,
		}

		// Publish with timing
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := node.client.Publish(ctx, req)
		cancel()
		latency := time.Since(start)

		metrics.mu.Lock()
		if err != nil {
			metrics.NodeErrors[node.ID]++
			metrics.TotalErrors++
		} else {
			metrics.NodePublished[node.ID]++
			metrics.TotalPublished++
			metrics.NodeLatencies[node.ID] = append(metrics.NodeLatencies[node.ID], latency)
			metrics.AllLatencies = append(metrics.AllLatencies, latency)
			if resp != nil {
				metrics.PartitionCounts[resp.PartitionId]++
			}
		}
		metrics.mu.Unlock()

		atomic.AddInt64(totalPublished, 1)
	}
}

// runNodePublisher sends all events to a specific node
func runNodePublisher(config ClusterLoadTestConfig, metrics *ClusterMetrics, node ClusterNode, publisherID int, totalPublished *int64) {
	payload := make([]byte, config.PayloadSize)
	rand.Read(payload)

	for i := 0; i < config.EventsPerPub; i++ {
		eventKey := fmt.Sprintf("%s-pub-%d-event-%d", node.ID, publisherID, i)
		scheduleTime := time.Now().Add(config.ScheduleDelay)

		event := &types.Event{
			MessageId:  fmt.Sprintf("cluster-test-%s-%d-%d-%d", node.ID, publisherID, i, time.Now().UnixNano()),
			ScheduleTs: scheduleTime.UnixMilli(),
			Payload:    payload,
			Topic:      config.Topic,
			Meta: map[string]string{
				"publisher":   fmt.Sprintf("%d", publisherID),
				"sequence":    fmt.Sprintf("%d", i),
				"target_node": node.ID,
				"event_key":   eventKey,
			},
		}

		req := &types.PublishRequest{
			Event: event,
		}

		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := node.client.Publish(ctx, req)
		cancel()
		latency := time.Since(start)

		metrics.mu.Lock()
		if err != nil {
			metrics.NodeErrors[node.ID]++
			metrics.TotalErrors++
		} else {
			metrics.NodePublished[node.ID]++
			metrics.TotalPublished++
			metrics.NodeLatencies[node.ID] = append(metrics.NodeLatencies[node.ID], latency)
			metrics.AllLatencies = append(metrics.AllLatencies, latency)
			if resp != nil {
				metrics.PartitionCounts[resp.PartitionId]++
			}
		}
		metrics.mu.Unlock()

		atomic.AddInt64(totalPublished, 1)
	}
}

// printClusterResults prints comprehensive test results
func printClusterResults(config ClusterLoadTestConfig, metrics *ClusterMetrics) {
	duration := metrics.EndTime.Sub(metrics.StartTime)
	totalEvents := config.NumPublishers * config.EventsPerPub * len(config.Nodes)

	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    CLUSTER LOAD TEST RESULTS                  ║")
	fmt.Println("╠═══════════════════════════════════════════════════════════════╣")

	// Overall stats
	successRate := float64(metrics.TotalPublished) / float64(totalEvents) * 100
	throughput := float64(metrics.TotalPublished) / duration.Seconds()

	fmt.Printf("║ Duration:           %-42v ║\n", duration.Round(time.Millisecond))
	fmt.Printf("║ Total Published:    %-42d ║\n", metrics.TotalPublished)
	fmt.Printf("║ Total Errors:       %-42d ║\n", metrics.TotalErrors)
	fmt.Printf("║ Success Rate:       %-42.2f%% ║\n", successRate)
	fmt.Printf("║ Throughput:         %-42.0f events/sec ║\n", throughput)

	// Per-node breakdown
	fmt.Println("╠═══════════════════════════════════════════════════════════════╣")
	fmt.Println("║                      PER-NODE BREAKDOWN                       ║")
	fmt.Println("╠═══════════════════════════════════════════════════════════════╣")

	for _, node := range config.Nodes {
		published := metrics.NodePublished[node.ID]
		errors := metrics.NodeErrors[node.ID]
		latencies := metrics.NodeLatencies[node.ID]
		nodeRate := float64(published) / duration.Seconds()

		fmt.Printf("║ %s:                                                          ║\n", node.ID)
		fmt.Printf("║   Published:        %-42d ║\n", published)
		fmt.Printf("║   Errors:           %-42d ║\n", errors)
		fmt.Printf("║   Rate:             %-42.0f events/sec ║\n", nodeRate)

		if len(latencies) > 0 {
			p50, p95, p99 := calculatePercentiles(latencies)
			fmt.Printf("║   Latency P50:      %-42v ║\n", p50.Round(time.Microsecond))
			fmt.Printf("║   Latency P95:      %-42v ║\n", p95.Round(time.Microsecond))
			fmt.Printf("║   Latency P99:      %-42v ║\n", p99.Round(time.Microsecond))
		}
	}

	// Partition distribution
	fmt.Println("╠═══════════════════════════════════════════════════════════════╣")
	fmt.Println("║                   PARTITION DISTRIBUTION                      ║")
	fmt.Println("╠═══════════════════════════════════════════════════════════════╣")

	for partID, count := range metrics.PartitionCounts {
		pct := float64(count) / float64(metrics.TotalPublished) * 100
		fmt.Printf("║ Partition %-3d:      %-30d (%.1f%%) ║\n", partID, count, pct)
	}

	// Overall latency stats
	fmt.Println("╠═══════════════════════════════════════════════════════════════╣")
	fmt.Println("║                    OVERALL LATENCY STATS                      ║")
	fmt.Println("╠═══════════════════════════════════════════════════════════════╣")

	if len(metrics.AllLatencies) > 0 {
		p50, p95, p99 := calculatePercentiles(metrics.AllLatencies)
		min, max, avg := calculateMinMaxAvg(metrics.AllLatencies)

		fmt.Printf("║ Min:                %-42v ║\n", min.Round(time.Microsecond))
		fmt.Printf("║ Max:                %-42v ║\n", max.Round(time.Microsecond))
		fmt.Printf("║ Avg:                %-42v ║\n", avg.Round(time.Microsecond))
		fmt.Printf("║ P50:                %-42v ║\n", p50.Round(time.Microsecond))
		fmt.Printf("║ P95:                %-42v ║\n", p95.Round(time.Microsecond))
		fmt.Printf("║ P99:                %-42v ║\n", p99.Round(time.Microsecond))
	}

	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")

	// Summary
	fmt.Println()
	if successRate >= 99.9 {
		fmt.Println("✅ EXCELLENT: Cluster handled load with 99.9%+ success rate!")
	} else if successRate >= 99.0 {
		fmt.Println("✅ GOOD: Cluster handled load with 99%+ success rate")
	} else if successRate >= 95.0 {
		fmt.Println("⚠️  WARNING: Some events failed, success rate below 99%")
	} else {
		fmt.Println("❌ FAILURE: Significant event loss, check cluster health")
	}

	// Performance assessment
	eventsPerNode := throughput / float64(len(config.Nodes))
	fmt.Printf("\n📈 Performance: %.0f events/sec total (%.0f per node)\n", throughput, eventsPerNode)
}

func calculatePercentiles(latencies []time.Duration) (p50, p95, p99 time.Duration) {
	if len(latencies) == 0 {
		return 0, 0, 0
	}

	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	p50 = sorted[len(sorted)*50/100]
	p95 = sorted[len(sorted)*95/100]
	p99 = sorted[len(sorted)*99/100]

	return p50, p95, p99
}

func calculateMinMaxAvg(latencies []time.Duration) (min, max, avg time.Duration) {
	if len(latencies) == 0 {
		return 0, 0, 0
	}

	min = latencies[0]
	max = latencies[0]
	var total time.Duration

	for _, l := range latencies {
		if l < min {
			min = l
		}
		if l > max {
			max = l
		}
		total += l
	}

	avg = total / time.Duration(len(latencies))
	return min, max, avg
}

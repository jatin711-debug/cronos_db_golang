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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"cronos_db/pkg/utils"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"cronos_db/pkg/types"
)

const connsPerNode = 4 // Multiple gRPC connections per node for better throughput

// ClusterNode represents a node in the cluster
type ClusterNode struct {
	ID       string
	GRPCAddr string
	HTTPAddr string
	conn     *grpc.ClientConn
	conns    []*grpc.ClientConn           // Multiple connections for throughput
	clients  []types.EventServiceClient   // Round-robin clients
	client   types.EventServiceClient     // Legacy single client (for probing)
	pclient  types.PartitionServiceClient // Partition metadata client
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
	PartitionCount int           // Total cluster partitions (for route discovery)
}

// ClusterMetrics holds cluster-wide metrics with sharded collection to reduce lock contention
type ClusterMetrics struct {
	mu sync.Mutex

	// Per-node metrics
	NodePublished map[string]*atomic.Int64
	NodeErrors    map[string]*atomic.Int64

	// Sharded latency collection - each publisher writes to its own shard
	latencyShards []*latencyShard

	// Aggregate metrics (atomic for lock-free updates)
	TotalPublished atomic.Int64
	TotalErrors    atomic.Int64
	StartTime      time.Time
	EndTime        time.Time

	// Partition distribution
	PartitionCounts map[int32]int64
}

// latencyShard holds latencies for a subset of publishers to avoid contention
type latencyShard struct {
	mu        sync.Mutex
	nodeID    string
	latencies []time.Duration
}

func newClusterMetrics(nodes []ClusterNode, numShards int) *ClusterMetrics {
	m := &ClusterMetrics{
		NodePublished:   make(map[string]*atomic.Int64),
		NodeErrors:      make(map[string]*atomic.Int64),
		latencyShards:   make([]*latencyShard, numShards),
		PartitionCounts: make(map[int32]int64),
		StartTime:       time.Now(),
	}
	for _, n := range nodes {
		m.NodePublished[n.ID] = &atomic.Int64{}
		m.NodeErrors[n.ID] = &atomic.Int64{}
	}
	for i := range m.latencyShards {
		m.latencyShards[i] = &latencyShard{
			latencies: make([]time.Duration, 0, 1024),
		}
	}
	return m
}

func (m *ClusterMetrics) recordLatency(shardID int, nodeID string, latency time.Duration, count int) {
	shard := m.latencyShards[shardID%len(m.latencyShards)]
	shard.mu.Lock()
	shard.nodeID = nodeID
	for i := 0; i < count; i++ {
		shard.latencies = append(shard.latencies, latency)
	}
	shard.mu.Unlock()
}

// collectLatencies gathers all latencies from shards (call after test completes)
func (m *ClusterMetrics) collectLatencies() (map[string][]time.Duration, []time.Duration) {
	nodeLatencies := make(map[string][]time.Duration)
	var allLatencies []time.Duration

	for _, shard := range m.latencyShards {
		shard.mu.Lock()
		if len(shard.latencies) > 0 {
			allLatencies = append(allLatencies, shard.latencies...)
			nodeLatencies[shard.nodeID] = append(nodeLatencies[shard.nodeID], shard.latencies...)
		}
		shard.mu.Unlock()
	}
	return nodeLatencies, allLatencies
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
	scheduleDelay := flag.Duration("delay", 0, "Schedule delay from now (0 = immediate, for pure throughput tests)")
	topic := flag.String("topic", "cluster-loadtest", "Topic name")
	roundRobin := flag.Bool("round-robin", true, "Distribute events round-robin across nodes")
	testFailover := flag.Bool("failover", false, "Test failover scenario")
	failoverAfter := flag.Duration("failover-after", 10*time.Second, "When to simulate failover")
	batchMode := flag.Bool("batch", false, "Use batch publish API for higher throughput")
	batchSize := flag.Int("batch-size", 100, "Events per batch when using batch mode")
	partitionCount := flag.Int("partition-count", 16, "Total number of partitions in the cluster")

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
		PartitionCount: *partitionCount,
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
	totalPublishers := config.NumPublishers * len(config.Nodes)
	metrics := newClusterMetrics(config.Nodes, totalPublishers)

	// Connect to all nodes with multiple connections per node
	for i := range config.Nodes {
		// Create multiple connections for throughput
		config.Nodes[i].conns = make([]*grpc.ClientConn, connsPerNode)
		config.Nodes[i].clients = make([]types.EventServiceClient, connsPerNode)

		for c := 0; c < connsPerNode; c++ {
			conn, err := grpc.Dial(config.Nodes[i].GRPCAddr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithInitialWindowSize(16*1024*1024),     // 16MB stream window
				grpc.WithInitialConnWindowSize(32*1024*1024), // 32MB connection window
				grpc.WithWriteBufferSize(4*1024*1024),        // 4MB write buffer
				grpc.WithReadBufferSize(4*1024*1024),         // 4MB read buffer
				grpc.WithDefaultCallOptions(
					grpc.MaxCallRecvMsgSize(16*1024*1024),
					grpc.MaxCallSendMsgSize(16*1024*1024),
				),
			)
			if err != nil {
				log.Fatalf("Failed to connect to %s (conn %d): %v", config.Nodes[i].ID, c, err)
			}
			config.Nodes[i].conns[c] = conn
			config.Nodes[i].clients[c] = types.NewEventServiceClient(conn)
			defer conn.Close()
		}

		// Keep legacy single client for probing
		config.Nodes[i].conn = config.Nodes[i].conns[0]
		config.Nodes[i].client = config.Nodes[i].clients[0]
		config.Nodes[i].pclient = types.NewPartitionServiceClient(config.Nodes[i].conns[0])
	}

	// Progress tracking
	totalEvents := int64(config.NumPublishers * config.EventsPerPub * len(config.Nodes))

	leadersByPartition, err := discoverPartitionLeaders(config)
	if err != nil {
		log.Fatalf("Failed to discover partition leaders: %v", err)
	}
	logPartitionLeaders(config, leadersByPartition)

	// Start progress reporter
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(config.ReportInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				published := metrics.TotalPublished.Load()
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
		for pubIdx := 0; pubIdx < totalPublishers; pubIdx++ {
			wg.Add(1)
			go func(publisherID int) {
				defer wg.Done()
				runBatchPublisher(config, metrics, publisherID, leadersByPartition)
			}(pubIdx)
		}
	} else if config.RoundRobin {
		// Round-robin: each publisher sends to all nodes in rotation
		for pubIdx := 0; pubIdx < totalPublishers; pubIdx++ {
			wg.Add(1)
			go func(publisherID int) {
				defer wg.Done()
				runRoundRobinPublisher(config, metrics, publisherID, leadersByPartition)
			}(pubIdx)
		}
	} else {
		// Per-node: publishers are dedicated to specific nodes
		for _, node := range config.Nodes {
			for pubIdx := 0; pubIdx < config.NumPublishers; pubIdx++ {
				wg.Add(1)
				go func(n ClusterNode, pid int) {
					defer wg.Done()
					runNodePublisher(config, metrics, n, pid)
				}(node, pubIdx)
			}
		}
	}

	wg.Wait()
	close(done)

	metrics.EndTime = time.Now()
	return metrics
}

func buildProbeKeys(partitionCount int) (map[int32]string, error) {
	if partitionCount <= 0 {
		return nil, fmt.Errorf("invalid partition-count: %d", partitionCount)
	}

	keys := make(map[int32]string, partitionCount)
	maxAttempts := partitionCount * 50000

	for i := 0; i < maxAttempts && len(keys) < partitionCount; i++ {
		key := fmt.Sprintf("route-probe-key-%d", i)
		pid := utils.HashToPartitionID(key, partitionCount)
		if _, exists := keys[pid]; !exists {
			keys[pid] = key
		}
	}

	if len(keys) != partitionCount {
		return nil, fmt.Errorf("could not generate probe keys for all partitions (got %d/%d)", len(keys), partitionCount)
	}

	return keys, nil
}

func discoverPartitionLeaders(config ClusterLoadTestConfig) (map[int32]int, error) {
	nodeIndexByID := make(map[string]int, len(config.Nodes))
	for idx, node := range config.Nodes {
		nodeIndexByID[node.ID] = idx
	}

	for _, node := range config.Nodes {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		resp, err := node.pclient.ListPartitions(ctx, &types.ListPartitionsRequest{})
		cancel()
		if err != nil {
			log.Printf("  DEBUG: metadata request to %s failed: %v", node.ID, err)
			continue
		}
		if resp == nil {
			log.Printf("  DEBUG: metadata response from %s is nil", node.ID)
			continue
		}

		leaders := make(map[int32]int, config.PartitionCount)
		for _, partition := range resp.GetPartitions() {
			pid := partition.GetPartitionId()
			if pid < 0 || int(pid) >= config.PartitionCount {
				continue
			}

			leaderID := partition.GetLeaderId()
			leaderNodeIdx, ok := nodeIndexByID[leaderID]
			if !ok {
				continue
			}
			leaders[pid] = leaderNodeIdx
		}

		if len(leaders) == config.PartitionCount {
			return leaders, nil
		}
		log.Printf("  DEBUG: metadata from %s returned leaders for %d/%d partitions", node.ID, len(leaders), config.PartitionCount)
	}

	return nil, fmt.Errorf("could not discover leaders for all %d partitions from metadata API", config.PartitionCount)
}

func logPartitionLeaders(config ClusterLoadTestConfig, leadersByPartition map[int32]int) {
	counts := make(map[string]int)
	for _, idx := range leadersByPartition {
		nodeID := config.Nodes[idx].ID
		counts[nodeID]++
	}

	for _, n := range config.Nodes {
		log.Printf("Leader partitions on %s: %d", n.ID, counts[n.ID])
	}
}

// runBatchPublisher sends events in batches for high throughput
func runBatchPublisher(config ClusterLoadTestConfig, metrics *ClusterMetrics, publisherID int, leadersByPartition map[int32]int) {
	payload := make([]byte, config.PayloadSize)
	rand.Read(payload)

	batchSize := config.BatchSize
	totalEvents := config.EventsPerPub

	// Pre-compute publisher ID string to avoid repeated fmt.Sprintf
	pubIDStr := strconv.Itoa(publisherID)

	for batchStart := 0; batchStart < totalEvents; batchStart += batchSize {
		// Build batch
		batchEnd := batchStart + batchSize
		if batchEnd > totalEvents {
			batchEnd = totalEvents
		}

		eventsByNode := make(map[int][]*types.Event)
		scheduleTime := time.Now().Add(config.ScheduleDelay)
		scheduleMs := scheduleTime.UnixMilli()
		nowNano := time.Now().UnixNano()

		for i := batchStart; i < batchEnd; i++ {
			eventKey := fmt.Sprintf("pub-%d-event-%d", publisherID, i)
			partitionID := utils.HashToPartitionID(eventKey, config.PartitionCount)
			nodeIdx, ok := leadersByPartition[partitionID]
			if !ok {
				nodeIdx = (publisherID + i) % len(config.Nodes)
			}

			event := &types.Event{
				MessageId:  fmt.Sprintf("batch-%d-%d-%d", publisherID, i, nowNano),
				ScheduleTs: scheduleMs,
				Payload:    payload,
				Topic:      config.Topic,
				Meta: map[string]string{
					"publisher":     pubIDStr,
					"sequence":      strconv.Itoa(i),
					"partition_key": eventKey,
				},
			}
			eventsByNode[nodeIdx] = append(eventsByNode[nodeIdx], event)
		}

		eventsInBatch := int64(batchEnd - batchStart)

		// Send to different nodes IN PARALLEL
		var batchWg sync.WaitGroup
		for nodeIdx, events := range eventsByNode {
			batchWg.Add(1)
			go func(nIdx int, evts []*types.Event) {
				defer batchWg.Done()
				node := config.Nodes[nIdx]
				// Round-robin across connections for this node
				client := node.clients[publisherID%connsPerNode]
				req := &types.PublishBatchRequest{Events: evts, AllowDuplicate: true}

				start := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				resp, err := client.PublishBatch(ctx, req)
				cancel()
				latency := time.Since(start)

				if err != nil {
					n := int64(len(evts))
					metrics.NodeErrors[node.ID].Add(n)
					metrics.TotalErrors.Add(n)
					log.Printf("gRPC ERROR %s: %v (errors so far: %d)", node.ID, err, metrics.NodeErrors[node.ID].Load())
				} else if resp != nil {
					metrics.NodePublished[node.ID].Add(int64(resp.PublishedCount))
					metrics.TotalPublished.Add(int64(resp.PublishedCount))
					if resp.ErrorCount > 0 {
						metrics.TotalErrors.Add(int64(resp.ErrorCount))
						log.Printf("Handler ERROR %s: resp.ErrorCount=%d (dup=%d, published=%d, reqSize=%d, err=%q)",
							node.ID, resp.ErrorCount, resp.DuplicateCount, resp.PublishedCount, len(evts), resp.GetError())
					}
					// Record per-event latency to shard (no global lock)
					perEventLatency := latency / time.Duration(len(evts))
					metrics.recordLatency(publisherID, node.ID, perEventLatency, len(evts))
				}
			}(nodeIdx, events)
		}
		batchWg.Wait()

		_ = eventsInBatch // Progress tracked via TotalPublished
	}
}

// runRoundRobinPublisher sends events across all nodes in rotation
func runRoundRobinPublisher(config ClusterLoadTestConfig, metrics *ClusterMetrics, publisherID int, leadersByPartition map[int32]int) {
	payload := make([]byte, config.PayloadSize)
	rand.Read(payload)

	pubIDStr := strconv.Itoa(publisherID)

	for i := 0; i < config.EventsPerPub; i++ {
		// Create event with unique key for partition distribution
		eventKey := fmt.Sprintf("pub-%d-event-%d", publisherID, i)
		partitionID := utils.HashToPartitionID(eventKey, config.PartitionCount)
		nodeIdx, ok := leadersByPartition[partitionID]
		if !ok {
			nodeIdx = (publisherID + i) % len(config.Nodes)
		}
		node := config.Nodes[nodeIdx]
		client := node.clients[publisherID%connsPerNode]
		scheduleTime := time.Now().Add(config.ScheduleDelay)

		event := &types.Event{
			MessageId:  fmt.Sprintf("cluster-test-%d-%d-%d", publisherID, i, time.Now().UnixNano()),
			ScheduleTs: scheduleTime.UnixMilli(),
			Payload:    payload,
			Topic:      config.Topic,
			Meta: map[string]string{
				"publisher":     pubIDStr,
				"sequence":      strconv.Itoa(i),
				"target_node":   node.ID,
				"partition_key": eventKey,
			},
		}

		req := &types.PublishRequest{
			Event: event,
		}

		// Publish with timing
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := client.Publish(ctx, req)
		cancel()
		latency := time.Since(start)

		if err != nil {
			metrics.NodeErrors[node.ID].Add(1)
			metrics.TotalErrors.Add(1)
		} else {
			metrics.NodePublished[node.ID].Add(1)
			metrics.TotalPublished.Add(1)
			metrics.recordLatency(publisherID, node.ID, latency, 1)
		}
	}
}

// runNodePublisher sends all events to a specific node
func runNodePublisher(config ClusterLoadTestConfig, metrics *ClusterMetrics, node ClusterNode, publisherID int) {
	payload := make([]byte, config.PayloadSize)
	rand.Read(payload)

	pubIDStr := strconv.Itoa(publisherID)
	client := node.clients[publisherID%connsPerNode]

	for i := 0; i < config.EventsPerPub; i++ {
		eventKey := fmt.Sprintf("%s-pub-%d-event-%d", node.ID, publisherID, i)
		scheduleTime := time.Now().Add(config.ScheduleDelay)

		event := &types.Event{
			MessageId:  fmt.Sprintf("cluster-test-%s-%d-%d-%d", node.ID, publisherID, i, time.Now().UnixNano()),
			ScheduleTs: scheduleTime.UnixMilli(),
			Payload:    payload,
			Topic:      config.Topic,
			Meta: map[string]string{
				"publisher":     pubIDStr,
				"sequence":      strconv.Itoa(i),
				"target_node":   node.ID,
				"partition_key": eventKey,
			},
		}

		req := &types.PublishRequest{
			Event: event,
		}

		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := client.Publish(ctx, req)
		cancel()
		latency := time.Since(start)

		if err != nil {
			metrics.NodeErrors[node.ID].Add(1)
			metrics.TotalErrors.Add(1)
		} else {
			metrics.NodePublished[node.ID].Add(1)
			metrics.TotalPublished.Add(1)
			metrics.recordLatency(publisherID, node.ID, latency, 1)
		}
	}
}

// printClusterResults prints comprehensive test results
func printClusterResults(config ClusterLoadTestConfig, metrics *ClusterMetrics) {
	duration := metrics.EndTime.Sub(metrics.StartTime)
	totalEvents := config.NumPublishers * config.EventsPerPub * len(config.Nodes)

	// Collect latencies from shards
	nodeLatencies, allLatencies := metrics.collectLatencies()

	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    CLUSTER LOAD TEST RESULTS                  ║")
	fmt.Println("╠═══════════════════════════════════════════════════════════════╣")

	// Overall stats
	totalPublished := metrics.TotalPublished.Load()
	totalErrors := metrics.TotalErrors.Load()
	successRate := float64(totalPublished) / float64(totalEvents) * 100
	throughput := float64(totalPublished) / duration.Seconds()

	fmt.Printf("║ Duration:           %-42v ║\n", duration.Round(time.Millisecond))
	fmt.Printf("║ Total Published:    %-42d ║\n", totalPublished)
	fmt.Printf("║ Total Errors:       %-42d ║\n", totalErrors)
	fmt.Printf("║ Success Rate:       %-42.2f%% ║\n", successRate)
	fmt.Printf("║ Throughput:         %-42.0f events/sec ║\n", throughput)

	// Per-node breakdown
	fmt.Println("╠═══════════════════════════════════════════════════════════════╣")
	fmt.Println("║                      PER-NODE BREAKDOWN                       ║")
	fmt.Println("╠═══════════════════════════════════════════════════════════════╣")

	for _, node := range config.Nodes {
		published := metrics.NodePublished[node.ID].Load()
		errors := metrics.NodeErrors[node.ID].Load()
		latencies := nodeLatencies[node.ID]
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
		pct := float64(count) / float64(totalPublished) * 100
		fmt.Printf("║ Partition %-3d:      %-30d (%.1f%%) ║\n", partID, count, pct)
	}

	// Overall latency stats
	fmt.Println("╠═══════════════════════════════════════════════════════════════╣")
	fmt.Println("║                    OVERALL LATENCY STATS                      ║")
	fmt.Println("╠═══════════════════════════════════════════════════════════════╣")

	if len(allLatencies) > 0 {
		p50, p95, p99 := calculatePercentiles(allLatencies)
		min, max, avg := calculateMinMaxAvg(allLatencies)

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

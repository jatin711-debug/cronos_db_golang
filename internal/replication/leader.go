// Package replication implements leader-to-follower WAL streaming, bulk snapshot
// install for catch-up, optional mTLS between nodes, and async cross-region
// event fan-out for CronosDB.
package replication

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

var replicationLag = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "cronos_replication_lag",
		Help: "Replication lag per follower in events",
	},
	[]string{"follower", "partition"},
)

// Leader replicates a partition's WAL to its followers over gRPC
// (ReplicationService.Append). It sends synchronously and waits for a quorum of
// in-sync replicas (including itself) before returning success, so a publish
// that requires min-insync-replicas durability blocks until that many replicas
// have the data.
type Leader struct {
	mu                sync.RWMutex
	partitionID       int32
	nodeID            string // identity of this leader node
	epoch             int64
	followers         map[string]*FollowerInfo
	batchSize         int32         // catch-up chunk size
	flushInterval     time.Duration // reconnect/maintenance tick
	replicateTimeout  time.Duration
	minInSyncReplicas int // minimum ISR size (including leader) required to ack a write
	quit              chan struct{}
	quitOnce          sync.Once
	wal               *storage.WAL
	tlsConfig         *MTLSConfig // optional mTLS for replication connections
}

// FollowerInfo tracks a follower replica and its gRPC replication client state.
type FollowerInfo struct {
	mu sync.Mutex
	// ID is the follower's node identifier.
	ID string
	// Address is the follower's cluster gRPC address (serves ReplicationService).
	Address string
	// NextOffset is the next WAL offset the leader will send to this follower.
	NextOffset int64
	// LastAckTS is the unix timestamp of the last successful ack from the follower.
	LastAckTS int64
	// HighWatermark is the highest offset the follower has acknowledged.
	HighWatermark int64
	// Connected is true when a gRPC client is currently configured for the follower.
	Connected bool
	// LastError is the most recent replication error for this follower, if any.
	LastError error
	// InSync is true when the follower has acknowledged the leader's latest sends.
	InSync bool
	conn   *grpc.ClientConn
	client types.ReplicationServiceClient
}

// NewLeader creates a new leader. minInSyncReplicas is the minimum ISR size
// (including the leader itself) required to acknowledge a write as durable;
// 0 is treated as 1 (single-replica mode). nodeID is this leader's identity;
// tlsConfig may be nil for plaintext dev mode.
func NewLeader(partitionID int32, batchSize int32, flushInterval time.Duration, wal *storage.WAL, minInSyncReplicas int, nodeID string, tlsConfig *MTLSConfig) *Leader {
	if minInSyncReplicas <= 0 {
		minInSyncReplicas = 1
	}
	if batchSize <= 0 {
		batchSize = 500
	}
	if flushInterval <= 0 {
		flushInterval = 500 * time.Millisecond
	}
	if nodeID == "" {
		nodeID = "leader"
	}
	if wal != nil {
		wal.SetCurrentTerm(1)
	}
	return &Leader{
		partitionID:       partitionID,
		nodeID:            nodeID,
		epoch:             1,
		followers:         make(map[string]*FollowerInfo),
		batchSize:         batchSize,
		flushInterval:     flushInterval,
		replicateTimeout:  10 * time.Second,
		minInSyncReplicas: minInSyncReplicas,
		quit:              make(chan struct{}),
		wal:               wal,
		tlsConfig:         tlsConfig,
	}
}

// dialCredentials returns the gRPC transport credentials for replication:
// mTLS when configured, otherwise insecure (dev mode).
func (l *Leader) dialCredentials() (credentials.TransportCredentials, error) {
	if l.tlsConfig != nil && l.tlsConfig.Enabled {
		tlsCfg, err := BuildClientTLSConfig(l.tlsConfig)
		if err != nil {
			return nil, err
		}
		return credentials.NewTLS(tlsCfg), nil
	}
	return insecure.NewCredentials(), nil
}

// AddFollower registers a follower and establishes its gRPC client. address is
// the follower's cluster gRPC address (where ReplicationService is served).
func (l *Leader) AddFollower(id, address string) error {
	l.mu.Lock()
	if existing, ok := l.followers[id]; ok {
		// Update the address if it changed; otherwise idempotent no-op so the
		// cluster reconcile loop can call this repeatedly.
		if existing.Address == address {
			l.mu.Unlock()
			return nil
		}
		existing.Address = address
		l.mu.Unlock()
		l.connectFollower(id, address)
		return nil
	}

	nextOffset := int64(0)
	if l.wal != nil {
		nextOffset = l.wal.GetNextOffset()
	}
	l.followers[id] = &FollowerInfo{
		ID:         id,
		Address:    address,
		NextOffset: nextOffset,
		Connected:  false,
		InSync:     false,
	}
	l.mu.Unlock()

	l.connectFollower(id, address)
	return nil
}

// connectFollower creates (or recreates) the gRPC client for a follower. The
// gRPC client dials lazily, so connectivity is confirmed on the first Append.
func (l *Leader) connectFollower(id, address string) {
	creds, err := l.dialCredentials()
	if err != nil {
		log.Printf("[LEADER] Failed to build replication credentials for follower %s: %v", id, err)
		return
	}

	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(64*1024*1024),
			grpc.MaxCallSendMsgSize(64*1024*1024),
		),
	)
	if err != nil {
		log.Printf("[LEADER] Failed to create replication client to follower %s at %s: %v", id, address, err)
		return
	}
	client := types.NewReplicationServiceClient(conn)

	l.mu.RLock()
	f, ok := l.followers[id]
	l.mu.RUnlock()
	if !ok {
		_ = conn.Close()
		return
	}

	f.mu.Lock()
	if f.conn != nil {
		_ = f.conn.Close()
	}
	f.conn = conn
	f.client = client
	f.Connected = true
	f.mu.Unlock()

	log.Printf("[LEADER] Connected to follower %s at %s (gRPC replication)", id, address)
}

// RemoveFollower removes a follower and closes its connection.
func (l *Leader) RemoveFollower(id string) error {
	l.mu.Lock()
	follower, exists := l.followers[id]
	if !exists {
		l.mu.Unlock()
		return fmt.Errorf("follower %s not found", id)
	}
	delete(l.followers, id)
	l.mu.Unlock()

	follower.mu.Lock()
	if follower.conn != nil {
		_ = follower.conn.Close()
		follower.conn = nil
		follower.client = nil
	}
	follower.Connected = false
	follower.mu.Unlock()
	return nil
}

// Replicate sends an event batch to all connected followers and returns success
// once a quorum (min-insync-replicas, counting the leader) has durably acked.
// events must be contiguous and start at the offset the followers expect; the
// caller (publish path) serializes appends+replication per partition to keep
// this ordering. RF=1 (no followers, minISR<=1) is a no-op.
func (l *Leader) Replicate(events []*types.Event) error {
	if len(events) == 0 {
		return nil
	}

	l.mu.RLock()
	followers := make([]*FollowerInfo, 0, len(l.followers))
	for _, f := range l.followers {
		followers = append(followers, f)
	}
	minISR := l.minInSyncReplicas
	l.mu.RUnlock()
	if minISR <= 0 {
		minISR = 1
	}

	// Connected followers are those with a live gRPC client.
	connected := make([]*FollowerInfo, 0, len(followers))
	for _, f := range followers {
		f.mu.Lock()
		ok := f.client != nil
		f.mu.Unlock()
		if ok {
			connected = append(connected, f)
		}
	}

	if len(connected) == 0 {
		// No followers to replicate to. Only acceptable when the leader alone
		// satisfies the durability requirement (RF=1 / minISR<=1).
		if minISR > 1 {
			return fmt.Errorf("not enough replicas: min-insync-replicas=%d but no connected followers for partition %d", minISR, l.partitionID)
		}
		return nil
	}

	// Send to all connected followers concurrently.
	var wg sync.WaitGroup
	var ackMu sync.Mutex
	acks := 1 // the leader itself already has the data durably in its WAL
	for _, f := range connected {
		wg.Add(1)
		go func(f *FollowerInfo) {
			defer wg.Done()
			if err := l.sendToFollower(f, events); err != nil {
				log.Printf("[LEADER] Replicate to follower %s failed (partition %d): %v", f.ID, l.partitionID, err)
				return
			}
			ackMu.Lock()
			acks++
			ackMu.Unlock()
		}(f)
	}
	wg.Wait()

	if acks < minISR {
		return fmt.Errorf("replication quorum not met: %d of %d required in-sync replicas acked (partition %d, offset %d)",
			acks, minISR, l.partitionID, events[len(events)-1].Offset)
	}
	return nil
}

// sendToFollower delivers events to one follower, catching it up first if it is
// behind the batch's starting offset (e.g. a follower that joined after the
// leader already had data).
func (l *Leader) sendToFollower(f *FollowerInfo, events []*types.Event) error {
	startOffset := events[0].Offset

	// Fast path: try a direct append at the expected offset.
	err := l.appendToFollower(f, events)
	if err == nil {
		return nil
	}

	// If the follower is behind (its next offset < this batch's start), ship the
	// missing range from the leader WAL, then retry the batch once.
	f.mu.Lock()
	next := f.NextOffset
	client := f.client
	f.mu.Unlock()
	if client != nil && next >= 0 && next < startOffset {
		if cuErr := l.catchUpFollower(f, next, startOffset); cuErr != nil {
			return cuErr
		}
		return l.appendToFollower(f, events)
	}
	return err
}

// catchUpFollower ships events in [from, to) from the leader's WAL to the
// follower in chunks, advancing on each ack.
func (l *Leader) catchUpFollower(f *FollowerInfo, from, to int64) error {
	if l.wal == nil {
		return fmt.Errorf("no WAL available for catch-up")
	}
	chunk := int64(l.batchSize)
	if chunk <= 0 {
		chunk = 500
	}
	for from < to {
		end := from + chunk
		if end > to {
			end = to
		}
		events, err := l.wal.ReadEvents(from, end-1)
		if err != nil {
			return fmt.Errorf("catch-up read [%d,%d) for follower %s: %w", from, end, f.ID, err)
		}
		if len(events) == 0 {
			break
		}
		if err := l.appendToFollower(f, events); err != nil {
			return err
		}
		f.mu.Lock()
		from = f.NextOffset
		f.mu.Unlock()
	}
	return nil
}

// appendToFollower performs one ReplicationService.Append RPC for a contiguous
// batch and updates the follower's tracked offsets/in-sync state from the reply.
func (l *Leader) appendToFollower(f *FollowerInfo, events []*types.Event) error {
	f.mu.Lock()
	client := f.client
	f.mu.Unlock()
	if client == nil {
		return fmt.Errorf("follower %s has no replication client", f.ID)
	}

	req := &types.ReplicationAppendRequest{
		PartitionId:        l.partitionID,
		Events:             events,
		ExpectedNextOffset: events[0].Offset,
		Term:               atomic.LoadInt64(&l.epoch),
		PrevLogTerm:        l.getPrevLogTerm(events[0].Offset - 1),
		Checksum:           computeBatchChecksum(events),
	}

	ctx, cancel := context.WithTimeout(context.Background(), l.replicateTimeout)
	defer cancel()
	resp, err := client.Append(ctx, req)
	if err != nil {
		f.mu.Lock()
		f.Connected = false
		f.InSync = false
		f.LastError = err
		f.mu.Unlock()
		return fmt.Errorf("append RPC to follower %s: %w", f.ID, err)
	}
	if !resp.GetSuccess() {
		f.mu.Lock()
		if resp.GetNextOffset() > 0 {
			f.NextOffset = resp.GetNextOffset()
		}
		f.InSync = false
		f.LastError = fmt.Errorf("%s", resp.GetError())
		f.mu.Unlock()
		return fmt.Errorf("follower %s rejected append: %s", f.ID, resp.GetError())
	}

	f.mu.Lock()
	f.HighWatermark = resp.GetLastOffset()
	f.NextOffset = resp.GetNextOffset()
	f.InSync = true
	f.Connected = true
	f.LastAckTS = time.Now().UnixMilli()
	f.LastError = nil
	f.mu.Unlock()

	if l.wal != nil {
		lag := l.wal.GetHighWatermark() - resp.GetLastOffset()
		if lag < 0 {
			lag = 0
		}
		replicationLag.WithLabelValues(f.ID, fmt.Sprintf("%d", l.partitionID)).Set(float64(lag))
	}
	return nil
}

// getPrevLogTerm returns the term of the log entry at the given offset, read
// from the WAL when possible; otherwise the leader's current epoch.
func (l *Leader) getPrevLogTerm(offset int64) int64 {
	if offset < 0 {
		return 0
	}
	if l.wal != nil {
		if term, err := l.wal.GetTermForOffset(offset); err == nil && term > 0 {
			return term
		}
	}
	return atomic.LoadInt64(&l.epoch)
}

// GetHighWatermark returns the minimum high watermark across in-sync followers.
func (l *Leader) GetHighWatermark() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.getHighWatermarkLocked()
}

func (l *Leader) getHighWatermarkLocked() int64 {
	if len(l.followers) == 0 {
		return 0
	}
	minWatermark := int64(-1)
	for _, follower := range l.followers {
		follower.mu.Lock()
		inSync := follower.InSync
		hw := follower.HighWatermark
		follower.mu.Unlock()
		if !inSync {
			continue
		}
		if minWatermark == -1 || hw < minWatermark {
			minWatermark = hw
		}
	}
	if minWatermark == -1 {
		return 0
	}
	return minWatermark
}

// GetInSyncReplicas returns the IDs of followers currently in sync and connected.
func (l *Leader) GetInSyncReplicas() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var isr []string
	for id, follower := range l.followers {
		follower.mu.Lock()
		ok := follower.InSync && follower.Connected
		follower.mu.Unlock()
		if ok {
			isr = append(isr, id)
		}
	}
	return isr
}

// GetEpoch returns the current epoch.
func (l *Leader) GetEpoch() int64 {
	return atomic.LoadInt64(&l.epoch)
}

// SetEpoch sets the epoch (used during leader election) and propagates the term
// to the WAL so new records carry it. epoch is stored atomically to match the
// atomic reads in appendToFollower/GetTerm (mixing a mutex write with atomic
// reads is a data race).
func (l *Leader) SetEpoch(epoch int64) {
	atomic.StoreInt64(&l.epoch, epoch)
	if l.wal != nil {
		l.wal.SetCurrentTerm(epoch)
	}
}

// GetFollowerOffsets returns the high watermark offset for each follower.
func (l *Leader) GetFollowerOffsets() map[string]int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	offsets := make(map[string]int64, len(l.followers))
	for id, follower := range l.followers {
		follower.mu.Lock()
		offsets[id] = follower.HighWatermark
		follower.mu.Unlock()
	}
	return offsets
}

// Start begins the leader maintenance loop (reconnecting dead follower clients).
func (l *Leader) Start() {
	utils.GoSafe("leader-replication", l.maintenanceLoop)
}

func (l *Leader) maintenanceLoop() {
	ticker := time.NewTicker(l.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			l.reconnectDeadFollowers()
		case <-l.quit:
			return
		}
	}
}

// reconnectDeadFollowers re-establishes gRPC clients for followers whose client
// was torn down (e.g. after a transport error).
func (l *Leader) reconnectDeadFollowers() {
	l.mu.RLock()
	type reconn struct{ id, addr string }
	var dead []reconn
	for id, f := range l.followers {
		f.mu.Lock()
		if f.client == nil {
			dead = append(dead, reconn{id: id, addr: f.Address})
		}
		f.mu.Unlock()
	}
	l.mu.RUnlock()
	for _, r := range dead {
		l.connectFollower(r.id, r.addr)
	}
}

// Stop stops the leader and closes all follower connections. Safe to call
// multiple times.
func (l *Leader) Stop() {
	l.quitOnce.Do(func() { close(l.quit) })

	l.mu.Lock()
	defer l.mu.Unlock()
	for _, f := range l.followers {
		f.mu.Lock()
		if f.conn != nil {
			_ = f.conn.Close()
			f.conn = nil
			f.client = nil
		}
		f.Connected = false
		f.mu.Unlock()
	}
}

// GetStats returns leader statistics.
func (l *Leader) GetStats() *LeaderStats {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return &LeaderStats{
		Followers:          int64(len(l.followers)),
		ConnectedFollowers: l.countConnectedFollowers(),
		InSyncFollowers:    l.countInSyncFollowers(),
		HighWatermark:      l.getHighWatermarkLocked(),
		Epoch:              l.epoch,
	}
}

func (l *Leader) countConnectedFollowers() int64 {
	var count int64
	for _, follower := range l.followers {
		follower.mu.Lock()
		ok := follower.Connected
		follower.mu.Unlock()
		if ok {
			count++
		}
	}
	return count
}

func (l *Leader) countInSyncFollowers() int64 {
	var count int64
	for _, follower := range l.followers {
		follower.mu.Lock()
		ok := follower.InSync
		follower.mu.Unlock()
		if ok {
			count++
		}
	}
	return count
}

// computeBatchChecksum returns a CRC32 over the event payloads, offsets, and
// terms in a replication batch. It also ensures each event has a per-payload
// checksum populated.
func computeBatchChecksum(events []*types.Event) uint32 {
	h := crc32.NewIEEE()
	buf := make([]byte, 20)
	for _, e := range events {
		if e.Checksum == 0 {
			e.Checksum = crc32.ChecksumIEEE(e.Payload)
		}
		binary.BigEndian.PutUint64(buf[0:8], uint64(e.Offset))
		binary.BigEndian.PutUint64(buf[8:16], uint64(e.Term))
		binary.BigEndian.PutUint32(buf[16:20], e.Checksum)
		h.Write(buf)
		h.Write(e.Payload)
	}
	return h.Sum32()
}

// LeaderStats is a snapshot of replication leader health for a partition.
type LeaderStats struct {
	// Followers is the number of registered followers.
	Followers int64
	// ConnectedFollowers is how many followers currently have a live client connection.
	ConnectedFollowers int64
	// InSyncFollowers is how many followers are in the in-sync replica set.
	InSyncFollowers int64
	// HighWatermark is the quorum-committed high-watermark offset.
	HighWatermark int64
	// Epoch is the current leadership epoch for the partition.
	Epoch int64
}

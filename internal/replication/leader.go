package replication

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var replicationLag = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "cronos_replication_lag",
		Help: "Replication lag per follower in events",
	},
	[]string{"follower", "partition"},
)

// Leader manages replication to followers
type Leader struct {
	mu                sync.RWMutex
	partitionID       int32
	nodeID            string // identity of this leader node used in replication handshakes
	epoch             int64
	followers         map[string]*FollowerInfo
	batchSize         int32
	flushInterval     time.Duration
	replicateTimeout  time.Duration
	minInSyncReplicas int // minimum ISR size (including leader) required to ack a write
	quit              chan struct{}
	quitOnce          sync.Once
	wal               *storage.WAL
	tlsConfig         *MTLSConfig // optional mTLS for replication connections
}

// FollowerInfo represents metadata about a follower replica
type FollowerInfo struct {
	mu            sync.Mutex // Protects mutable fields below from concurrent goroutine access
	ID            string
	Address       string
	NextOffset    int64
	LastAckTS     int64
	HighWatermark int64
	Connected     bool
	Buffer        []*types.Event
	LastError     error
	InSync        bool // Whether follower is in-sync with leader
	transport     *Transport
}

// NewLeader creates a new leader. minInSyncReplicas is the minimum ISR size
// (including the leader itself) required to acknowledge a write as durable;
// 0 is treated as 1 (single-replica mode). nodeID is advertised to followers
// during the replication handshake; tlsConfig may be nil for plaintext dev mode.
func NewLeader(partitionID int32, batchSize int32, flushInterval time.Duration, wal *storage.WAL, minInSyncReplicas int, nodeID string, tlsConfig *MTLSConfig) *Leader {
	if minInSyncReplicas <= 0 {
		minInSyncReplicas = 1
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

// AddFollower adds a follower
func (l *Leader) AddFollower(id, address string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, exists := l.followers[id]; exists {
		return fmt.Errorf("follower %s already exists", id)
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

	// Establish connection
	utils.GoSafe("leader-connect", func() { l.connectFollower(id, address) })

	return nil
}

// connectFollower establishes connection to follower. If mTLS is configured,
// the connection is upgraded to TLS before the handshake.
func (l *Leader) connectFollower(id, address string) {
	var conn net.Conn
	var err error

	if l.tlsConfig != nil && l.tlsConfig.Enabled {
		tlsCfg, err := BuildClientTLSConfig(l.tlsConfig)
		if err != nil {
			log.Printf("[LEADER] Failed to build TLS config for follower %s: %v", id, err)
			return
		}
		conn, err = tls.Dial("tcp", address, tlsCfg)
	} else {
		conn, err = net.DialTimeout("tcp", address, 5*time.Second)
	}
	if err != nil {
		log.Printf("[LEADER] Failed to connect to follower %s at %s: %v", id, address, err)
		return
	}

	transport := NewTransport(conn)

	// Perform handshake using the real leader node identity.
	handshake := &HandshakeMessage{NodeID: l.nodeID}
	payload, err := handshake.Encode()
	if err != nil {
		log.Printf("[LEADER] Handshake encode failed to %s: %v", id, err)
		transport.Close()
		return
	}
	if err := transport.WriteMessage(MsgTypeHandshake, payload); err != nil {
		log.Printf("[LEADER] Handshake failed to %s: %v", id, err)
		transport.Close()
		return
	}

	l.mu.Lock()
	if follower, exists := l.followers[id]; exists {
		follower.transport = transport
		follower.Connected = true
	}
	l.mu.Unlock()

	log.Printf("[LEADER] Connected to follower %s at %s (binary protocol)", id, address)

	// Start reader loop for acks
	utils.GoSafe("leader-read", func() { l.readLoop(id, transport) })
}

// readLoop reads messages from follower
func (l *Leader) readLoop(id string, t *Transport) {
	for {
		msgType, payload, err := t.ReadMessage()
		if err != nil {
			log.Printf("[LEADER] Connection lost to %s: %v", id, err)
			l.mu.Lock()
			if f, ok := l.followers[id]; ok {
				f.mu.Lock()
				f.Connected = false
				f.transport = nil
				f.mu.Unlock()
			}
			l.mu.Unlock()
			return
		}

		if msgType == MsgTypeAppendAck {
			ack := &AppendAckMessage{}
			if err := ack.Decode(payload); err == nil {
				l.handleAck(id, ack)
			}
		}
	}
}

// handleAck handles acknowledgment. ACKs from a different term are ignored.
func (l *Leader) handleAck(id string, ack *AppendAckMessage) {
	l.mu.Lock()
	defer l.mu.Unlock()

	follower, ok := l.followers[id]
	if !ok {
		return
	}

	// Ignore stale-term ACKs.
	if ack.Term != l.epoch {
		log.Printf("[LEADER] ignoring ack from follower %s with stale term %d (current %d)", id, ack.Term, l.epoch)
		return
	}

	if ack.Success {
		if ack.Offset > follower.HighWatermark {
			follower.HighWatermark = ack.Offset
		}
		// Advance NextOffset to the next expected entry. Prefer the follower's
		// reported NextOffset; fall back to HighWatermark+1 for older followers.
		if ack.NextOffset > follower.NextOffset {
			follower.NextOffset = ack.NextOffset
		} else if ack.Offset+1 > follower.NextOffset {
			follower.NextOffset = ack.Offset + 1
		}
		follower.InSync = true
	} else {
		follower.InSync = false
	}
	// Compute and expose replication lag using WAL high watermark
	var lag int64
	if l.wal != nil {
		lag = l.wal.GetHighWatermark() - follower.HighWatermark
	}
	if lag < 0 {
		lag = 0
	}
	replicationLag.WithLabelValues(id, fmt.Sprintf("%d", l.partitionID)).Set(float64(lag))
}

// RemoveFollower removes a follower
func (l *Leader) RemoveFollower(id string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	follower, exists := l.followers[id]
	if !exists {
		return fmt.Errorf("follower %s not found", id)
	}

	// Close connection
	follower.mu.Lock()
	if follower.transport != nil {
		follower.transport.Close()
		follower.transport = nil
	}
	follower.Connected = false
	follower.mu.Unlock()

	delete(l.followers, id)
	return nil
}

// Replicate replicates an event batch to followers and waits for quorum ACKs.
// It buffers events, flushes all followers, then waits for a majority of in-sync
// followers to acknowledge the last offset before returning.
func (l *Leader) Replicate(events []*types.Event) error {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.followers) == 0 {
		minISR := l.minInSyncReplicas
		if minISR <= 0 {
			minISR = 1
		}
		if minISR > 1 {
			return fmt.Errorf("not enough replicas: min-insync-replicas=%d but no followers are configured", minISR)
		}
		return nil // Single-replica (RF=1) mode: leader-only is sufficient.
	}

	isr := l.inSyncReplicasLocked()

	// minInSyncReplicas is the total ISR size including the leader. We need at
	// least (minInSyncReplicas - 1) follower acks in addition to the leader's
	// own local write. If there are fewer followers in ISR than required, the
	// write is rejected rather than silently returning with zero durability.
	minISR := l.minInSyncReplicas
	if minISR <= 0 {
		minISR = 1
	}
	if len(isr)+1 < minISR {
		return fmt.Errorf("not enough replicas: min-insync-replicas=%d but only %d ISR members (incl. leader)", minISR, len(isr)+1)
	}

	neededAcks := (len(isr) + 1) / 2
	if minISR-1 > neededAcks {
		neededAcks = minISR - 1
	}
	if neededAcks == 0 {
		// We already validated that there is at least one follower above; with
		// minISR=1 and a single ISR follower we still need one follower ack to
		// be safe (leader + follower = RF=2 durability).
		neededAcks = 1
	}

	activeISR := 0
	for _, id := range isr {
		if f := l.followers[id]; f != nil && f.Connected {
			activeISR++
		}
	}
	if activeISR < neededAcks {
		return fmt.Errorf("replication quorum failed: only %d of %d required ISR followers connected", activeISR, neededAcks)
	}

	lastOffset := events[len(events)-1].Offset

	// Phase 1: Buffer events to all active ISR followers
	sendChan := make(chan error, activeISR)
	for _, id := range isr {
		follower := l.followers[id]
		if follower == nil || !follower.Connected {
			continue
		}
		utils.GoSafe("leader-send-batch", func() {
			f := follower
			err := l.sendBatch(f, events)
			if err != nil {
				log.Printf("[LEADER] Failed to buffer for %s: %v", f.ID, err)
			}
			sendChan <- err
		})
	}

	for i := 0; i < activeISR; i++ {
		if err := <-sendChan; err != nil {
			return fmt.Errorf("replication buffer failed: %w", err)
		}
	}

	// Phase 2: Flush all active ISR followers so data is on the wire
	flushChan := make(chan error, activeISR)
	for _, id := range isr {
		follower := l.followers[id]
		if follower == nil || !follower.Connected {
			continue
		}
		utils.GoSafe("leader-flush", func() {
			f := follower
			err := l.flushFollower(f)
			if err != nil {
				f.mu.Lock()
				f.LastError = err
				f.InSync = false
				f.mu.Unlock()
			} else {
				f.mu.Lock()
				f.LastError = nil
				f.LastAckTS = time.Now().UnixMilli()
				f.mu.Unlock()
			}
			flushChan <- err
		})
	}

	successes := 0
	failures := 0
	maxFailures := activeISR - neededAcks
	for i := 0; i < activeISR; i++ {
		err := <-flushChan
		if err == nil {
			successes++
			if successes >= neededAcks {
				break // Enough followers flushed, wait for acks
			}
		} else {
			failures++
			if failures > maxFailures {
				return fmt.Errorf("replication quorum failed: too many follower flush errors: %w", err)
			}
		}
	}

	// Phase 3: Wait for quorum of ISR followers to ack the last offset
	return l.waitForQuorumAcks(lastOffset, neededAcks, l.replicateTimeout)
}

// inSyncReplicasLocked returns the list of follower IDs that are currently in ISR.
func (l *Leader) inSyncReplicasLocked() []string {
	var isr []string
	for id, f := range l.followers {
		if f.InSync && f.Connected {
			isr = append(isr, id)
		}
	}
	return isr
}

// waitForQuorumAcks blocks until neededAcks in-sync followers have acked an offset
// >= targetOffset, or until timeout.
func (l *Leader) waitForQuorumAcks(targetOffset int64, neededAcks int, timeout time.Duration) error {
	if neededAcks <= 0 {
		return nil
	}
	deadline := time.Now().Add(timeout)
	pollInterval := 10 * time.Millisecond

	for time.Now().Before(deadline) {
		l.mu.RLock()
		acks := 0
		for _, f := range l.followers {
			if f.InSync && f.Connected && f.HighWatermark >= targetOffset {
				acks++
			}
		}
		l.mu.RUnlock()

		if acks >= neededAcks {
			return nil
		}

		time.Sleep(pollInterval)
	}

	return fmt.Errorf("replication quorum timeout: not enough ISR followers acked offset %d within %v", targetOffset, timeout)
}

// sendBatch sends a batch to a follower (public, acquires f.mu)
func (l *Leader) sendBatch(follower *FollowerInfo, events []*types.Event) error {
	follower.mu.Lock()
	defer follower.mu.Unlock()
	return l.sendBatchLocked(follower, events)
}

// sendBatchLocked sends a batch to a follower (caller must hold f.mu)
func (l *Leader) sendBatchLocked(follower *FollowerInfo, events []*types.Event) error {
	if len(events) == 0 {
		return nil
	}

	// Verify the buffer is contiguous with the follower's next expected offset.
	if events[0].Offset != follower.NextOffset {
		return fmt.Errorf("gap detected for follower %s: expected offset %d, got %d", follower.ID, follower.NextOffset, events[0].Offset)
	}
	for i := 1; i < len(events); i++ {
		if events[i].Offset != events[i-1].Offset+1 {
			return fmt.Errorf("non-contiguous offsets for follower %s: %d followed by %d", follower.ID, events[i-1].Offset, events[i].Offset)
		}
	}

	// Add to follower's buffer
	follower.Buffer = append(follower.Buffer, events...)

	// Send batch if buffer is full
	if int32(len(follower.Buffer)) >= l.batchSize {
		return l.flushFollower(follower)
	}

	return nil
}

// flushFollower flushes a follower's buffer
func (l *Leader) flushFollower(follower *FollowerInfo) error {
	follower.mu.Lock()
	if len(follower.Buffer) == 0 {
		follower.mu.Unlock()
		return nil
	}
	events := make([]*types.Event, len(follower.Buffer))
	copy(events, follower.Buffer)
	trans := follower.transport
	nextOffset := follower.NextOffset
	follower.mu.Unlock()

	if trans == nil {
		return fmt.Errorf("no connection to follower %s", follower.ID)
	}

	prevLogIndex := nextOffset - 1
	prevLogTerm := l.getPrevLogTerm(prevLogIndex)

	// Populate per-event checksums and compute a batch-level checksum.
	batchChecksum := computeBatchChecksum(events)

	req := &types.ReplicationAppendRequest{
		PartitionId:        l.partitionID,
		Events:             events,
		ExpectedNextOffset: nextOffset,
		Term:               atomic.LoadInt64(&l.epoch),
		PrevLogTerm:        prevLogTerm,
		Checksum:           batchChecksum,
	}

	if err := trans.WriteProtoMessage(MsgTypeAppendEntries, req); err != nil {
		return err
	}

	log.Printf("[LEADER] Replicating %d events to %s (binary)", len(events), follower.ID)

	// Only advance NextOffset and remove sent events after a successful write.
	follower.mu.Lock()
	if len(events) > 0 {
		lastSentOffset := events[len(events)-1].Offset
		if lastSentOffset+1 > follower.NextOffset {
			follower.NextOffset = lastSentOffset + 1
		}
		// Remove the exact prefix we just sent from the buffer.
		if len(follower.Buffer) >= len(events) {
			follower.Buffer = follower.Buffer[len(events):]
		} else {
			follower.Buffer = follower.Buffer[:0]
		}
	}
	follower.mu.Unlock()

	return nil
}

// getPrevLogTerm returns the term of the log entry at the given offset.
// It reads the term from the WAL record when possible; otherwise it falls back
// to the leader's current epoch.
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

// GetHighWatermark returns the minimum high watermark across followers
func (l *Leader) GetHighWatermark() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.getHighWatermarkLocked()
}

// getHighWatermarkLocked returns the min high watermark (caller must hold RLock)
func (l *Leader) getHighWatermarkLocked() int64 {
	if len(l.followers) == 0 {
		return 0
	}

	minWatermark := int64(-1)
	for _, follower := range l.followers {
		if !follower.InSync {
			continue
		}
		if minWatermark == -1 || follower.HighWatermark < minWatermark {
			minWatermark = follower.HighWatermark
		}
	}

	if minWatermark == -1 {
		return 0
	}
	return minWatermark
}

// GetInSyncReplicas returns the list of in-sync replica IDs
func (l *Leader) GetInSyncReplicas() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var isr []string
	for id, follower := range l.followers {
		if follower.InSync && follower.Connected {
			isr = append(isr, id)
		}
	}
	return isr
}

// GetEpoch returns the current epoch
func (l *Leader) GetEpoch() int64 {
	return atomic.LoadInt64(&l.epoch)
}

// SetEpoch sets the epoch (used during leader election)
func (l *Leader) SetEpoch(epoch int64) {
	l.mu.Lock()
	l.epoch = epoch
	l.mu.Unlock()
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
		offsets[id] = follower.HighWatermark
	}
	return offsets
}

// Start starts the leader replication
func (l *Leader) Start() {
	utils.GoSafe("leader-replication", l.replicationLoop)
}

// replicationLoop is the leader replication loop
func (l *Leader) replicationLoop() {
	ticker := time.NewTicker(l.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			l.flushAllFollowers()
		case <-l.quit:
			return
		}
	}
}

// flushAllFollowers flushes all follower buffers
func (l *Leader) flushAllFollowers() {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, follower := range l.followers {
		if len(follower.Buffer) > 0 && follower.Connected {
			if err := l.flushFollower(follower); err != nil {
				log.Printf("[LEADER] Failed to flush to %s: %v", follower.ID, err)
			}
		}
	}
}

// Stop stops the leader. Safe to call multiple times.
func (l *Leader) Stop() {
	l.quitOnce.Do(func() { close(l.quit) })

	// Close all connections
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, f := range l.followers {
		f.mu.Lock()
		if f.transport != nil {
			f.transport.Close()
			f.transport = nil
		}
		f.Connected = false
		f.mu.Unlock()
	}
}

// GetStats returns leader statistics
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

// countConnectedFollowers counts connected followers
func (l *Leader) countConnectedFollowers() int64 {
	var count int64
	for _, follower := range l.followers {
		if follower.Connected {
			count++
		}
	}
	return count
}

// countInSyncFollowers counts in-sync followers
func (l *Leader) countInSyncFollowers() int64 {
	var count int64
	for _, follower := range l.followers {
		if follower.InSync {
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

type LeaderStats struct {
	Followers          int64
	ConnectedFollowers int64
	InSyncFollowers    int64
	HighWatermark      int64
	Epoch              int64
}

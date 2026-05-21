package replication

import (
	"fmt"
	"log"
	"sync"
	"time"

	"cronos_db/internal/storage"
	"cronos_db/pkg/types"

	"net"
)

// Leader manages replication to followers
type Leader struct {
	mu            sync.RWMutex
	partitionID   int32
	epoch         int64
	followers     map[string]*FollowerInfo
	batchSize     int32
	flushInterval time.Duration
	quit          chan struct{}
	transports    map[string]*Transport
	wal           *storage.WAL
}

// FollowerInfo represents metadata about a follower replica
type FollowerInfo struct {
	mu            sync.Mutex     // Protects mutable fields below from concurrent goroutine access
	ID            string
	Address       string
	NextOffset    int64
	LastAckTS     int64
	HighWatermark int64
	Connected     bool
	Buffer        []*types.Event
	LastError     error
	InSync        bool // Whether follower is in-sync with leader
}

// NewLeader creates a new leader
func NewLeader(partitionID int32, batchSize int32, flushInterval time.Duration, wal *storage.WAL) *Leader {
	return &Leader{
		partitionID:   partitionID,
		epoch:         1,
		followers:     make(map[string]*FollowerInfo),
		batchSize:     batchSize,
		flushInterval: flushInterval,
		quit:          make(chan struct{}),
		transports:    make(map[string]*Transport),
		wal:           wal,
	}
}

// AddFollower adds a follower
func (l *Leader) AddFollower(id, address string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, exists := l.followers[id]; exists {
		return fmt.Errorf("follower %s already exists", id)
	}

	l.followers[id] = &FollowerInfo{
		ID:         id,
		Address:    address,
		NextOffset: 0,
		Connected:  false,
		InSync:     false,
	}

	// Establish connection
	go l.connectFollower(id, address)

	return nil
}

// connectFollower establishes connection to follower
func (l *Leader) connectFollower(id, address string) {
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		log.Printf("[LEADER] Failed to connect to follower %s at %s: %v", id, address, err)
		return
	}

	transport := NewTransport(conn)

	// Perform handshake
	handshake := &HandshakeMessage{NodeID: "leader"} // In real system, pass actual ID
	payload, _ := handshake.Encode()
	if err := transport.WriteMessage(MsgTypeHandshake, payload); err != nil {
		log.Printf("[LEADER] Handshake failed to %s: %v", id, err)
		transport.Close()
		return
	}

	l.mu.Lock()
	l.transports[id] = transport
	if follower, exists := l.followers[id]; exists {
		follower.Connected = true
	}
	l.mu.Unlock()

	log.Printf("[LEADER] Connected to follower %s at %s (binary protocol)", id, address)

	// Start reader loop for acks
	go l.readLoop(id, transport)
}

// readLoop reads messages from follower
func (l *Leader) readLoop(id string, t *Transport) {
	for {
		msgType, payload, err := t.ReadMessage()
		if err != nil {
			log.Printf("[LEADER] Connection lost to %s: %v", id, err)
			l.mu.Lock()
			if f, ok := l.followers[id]; ok {
				f.Connected = false
			}
			delete(l.transports, id)
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

// handleAck handles acknowledgment
func (l *Leader) handleAck(id string, ack *AppendAckMessage) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if follower, ok := l.followers[id]; ok {
		if ack.Success {
			if ack.Offset > follower.HighWatermark {
				follower.HighWatermark = ack.Offset
			}
			follower.InSync = true
		}
	}
}

// RemoveFollower removes a follower
func (l *Leader) RemoveFollower(id string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, exists := l.followers[id]; !exists {
		return fmt.Errorf("follower %s not found", id)
	}

	// Close connection
	if trans, exists := l.transports[id]; exists {
		trans.Close()
		delete(l.transports, id)
	}

	delete(l.followers, id)
	return nil
}

// Replicate replicates an event batch to followers
func (l *Leader) Replicate(events []*types.Event) error {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.followers) == 0 {
		return nil // No followers to replicate to
	}

	var activeCount int
	for _, follower := range l.followers {
		if follower.Connected {
			activeCount++
		}
	}

	neededAcks := (len(l.followers) + 1) / 2
	if activeCount < neededAcks {
		return fmt.Errorf("replication quorum failed: only %d of %d followers connected", activeCount, len(l.followers))
	}

	ackChan := make(chan error, activeCount)
	for _, follower := range l.followers {
		if !follower.Connected {
			continue
		}

		go func(f *FollowerInfo) {
			f.mu.Lock()
			err := l.sendBatchLocked(f, events)
			if err != nil {
				f.LastError = err
				f.InSync = false
				log.Printf("[LEADER] Failed to replicate to %s: %v", f.ID, err)
				f.mu.Unlock()
				ackChan <- err
			} else {
				f.LastError = nil
				f.LastAckTS = time.Now().UnixMilli()
				f.mu.Unlock()
				ackChan <- nil
			}
		}(follower)
	}

	successes := 0
	failures := 0
	maxFailures := activeCount - neededAcks

	for i := 0; i < activeCount; i++ {
		err := <-ackChan
		if err == nil {
			successes++
			if successes >= neededAcks {
				return nil // Quorum achieved! Return early.
			}
		} else {
			failures++
			if failures > maxFailures {
				// We can no longer achieve quorum
				return fmt.Errorf("replication quorum failed: too many follower replication errors: %w", err)
			}
		}
	}

	return nil
}

// sendBatch sends a batch to a follower (public, acquires f.mu)
func (l *Leader) sendBatch(follower *FollowerInfo, events []*types.Event) error {
	follower.mu.Lock()
	defer follower.mu.Unlock()
	return l.sendBatchLocked(follower, events)
}

// sendBatchLocked sends a batch to a follower (caller must hold f.mu)
func (l *Leader) sendBatchLocked(follower *FollowerInfo, events []*types.Event) error {
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
	if len(follower.Buffer) == 0 {
		return nil
	}

	trans, exists := l.transports[follower.ID]
	if !exists || trans == nil {
		return fmt.Errorf("no connection to follower %s", follower.ID)
	}

	req := &types.ReplicationAppendRequest{
		PartitionId:        l.partitionID,
		Events:             follower.Buffer,
		ExpectedNextOffset: follower.NextOffset,
		Term:               l.epoch,
	}

	if err := trans.WriteProtoMessage(MsgTypeAppendEntries, req); err != nil {
		return err
	}

	log.Printf("[LEADER] Replicating %d events to %s (binary)", len(follower.Buffer), follower.ID)

	// Assume success for now, real ack comes async
	// Update next offset
	if len(follower.Buffer) > 0 {
		lastEvent := follower.Buffer[len(follower.Buffer)-1]
		follower.NextOffset = lastEvent.Offset + 1
	}

	// Clear buffer
	follower.Buffer = follower.Buffer[:0]

	return nil
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
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.epoch
}

// SetEpoch sets the epoch (used during leader election)
func (l *Leader) SetEpoch(epoch int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.epoch = epoch
}

// Start starts the leader replication
func (l *Leader) Start() {
	go l.replicationLoop()
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

// Stop stops the leader
func (l *Leader) Stop() {
	close(l.quit)

	// Close all connections
	l.mu.Lock()
	defer l.mu.Unlock()

	for id, trans := range l.transports {
		trans.Close()
		delete(l.transports, id)
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

// LeaderStats represents leader statistics
type LeaderStats struct {
	Followers          int64
	ConnectedFollowers int64
	InSyncFollowers    int64
	HighWatermark      int64
	Epoch              int64
}

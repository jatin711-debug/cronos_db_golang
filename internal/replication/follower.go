package replication

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"cronos_db/internal/storage"
	"cronos_db/pkg/types"
)

// Follower handles replication from leader
type Follower struct {
	mu           sync.RWMutex
	partitionID  int32
	epoch        int64
	leaderID     string
	leaderAddr   string
	nextOffset   int64
	wal          *storage.WAL
	quit         chan struct{}
	active       bool
	lastSyncTime time.Time
	syncInterval time.Duration
	catchupMode  bool // True when catching up with leader
	listener     net.Listener
	nodeID       string
}

// NewFollower creates a new follower
func NewFollower(partitionID int32, wal *storage.WAL, nodeID string) *Follower {
	return &Follower{
		partitionID:  partitionID,
		wal:          wal,
		nextOffset:   wal.GetNextOffset(),
		quit:         make(chan struct{}),
		nodeID:       nodeID,
		syncInterval: 1 * time.Second,
		catchupMode:  false,
	}
}

// Start starts the follower replication server
func (f *Follower) Start(port int) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.active {
		return nil
	}

	// Listen for connections from leader
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	f.listener = listener
	f.active = true

	log.Printf("[FOLLOWER] Started replication server on port %d", port)

	go f.acceptLoop()
	return nil
}

// acceptLoop accepts connections
func (f *Follower) acceptLoop() {
	for {
		conn, err := f.listener.Accept()
		if err != nil {
			select {
			case <-f.quit:
				return
			default:
				log.Printf("[FOLLOWER] Accept error: %v", err)
				continue
			}
		}

		go f.handleConnection(conn)
	}
}

// handleConnection handles leader connection
func (f *Follower) handleConnection(conn net.Conn) {
	defer conn.Close()

	transport := NewTransport(conn)
	log.Printf("[FOLLOWER] Accepted connection from %s", conn.RemoteAddr())

	for {
		msgType, payload, err := transport.ReadMessage()
		if err != nil {
			log.Printf("[FOLLOWER] Connection error: %v", err)
			return
		}

		if err := f.handleMessage(transport, msgType, payload); err != nil {
			log.Printf("[FOLLOWER] Handle message error: %v", err)
			return
		}
	}
}

// handleMessage handles a protocol message
func (f *Follower) handleMessage(t *Transport, msgType uint8, payload []byte) error {
	switch msgType {
	case MsgTypeHandshake:
		hs := &HandshakeMessage{}
		if err := hs.Decode(payload); err != nil {
			return err
		}
		log.Printf("[FOLLOWER] Handshake from node %s", hs.NodeID)
		// Ack? For now assume OK if connection accepted.

	case MsgTypeAppendEntries:
		msg := &AppendEntriesMessage{}
		if err := msg.Decode(payload); err != nil {
			return err
		}

		// Write to WAL
		// Use AppendBatch
		if f.wal != nil {
			if err := f.wal.AppendBatch(msg.Events); err != nil {
				log.Printf("[FOLLOWER] Failed to append batch: %v", err)
				// Send failure ack
				ack := &AppendAckMessage{
					Term:    msg.Term,
					Success: false,
					Offset:  f.nextOffset,
				}
				pl, _ := ack.Encode()
				return t.WriteMessage(MsgTypeAppendAck, pl)
			}
		}

		// Update offset
		if len(msg.Events) > 0 {
			f.nextOffset = msg.Events[len(msg.Events)-1].Offset + 1
		}

		// Send success ack
		ack := &AppendAckMessage{
			Term:    msg.Term,
			Success: true,
			Offset:  f.nextOffset - 1,
		}
		pl, _ := ack.Encode()
		return t.WriteMessage(MsgTypeAppendAck, pl)

	case MsgTypeHeartbeat:
		// Respond to heartbeat
		ack := &AppendAckMessage{
			Term:    0,
			Success: true,
			Offset:  f.nextOffset - 1,
		}
		pl, _ := ack.Encode()
		return t.WriteMessage(MsgTypeHeartbeatAck, pl)
	}

	return nil
}

// AppendEvents appends replicated events to WAL (Logic moved to handleMessage)
func (f *Follower) AppendEvents(events []*types.Event, leaderEpoch int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// ... (Existing logic can be kept if needed for other paths, but we are using binary protocol now)
	return nil
}

// GetNextOffset returns next offset to replicate
func (f *Follower) GetNextOffset() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.nextOffset
}

// GetEpoch returns the follower's epoch
func (f *Follower) GetEpoch() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.epoch
}

// SetEpoch sets the epoch (used during leader failover)
func (f *Follower) SetEpoch(epoch int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.epoch = epoch
}

// PromoteToLeader promotes this follower to become the leader
func (f *Follower) PromoteToLeader() (*Leader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.active {
		return nil, fmt.Errorf("follower not active")
	}

	// Stop follower replication
	f.active = false
	if f.listener != nil {
		f.listener.Close()
	}

	// Create new leader
	leader := NewLeader(f.partitionID, 100, 100*time.Millisecond)
	leader.SetEpoch(f.epoch + 1)

	log.Printf("[FOLLOWER] Promoted to leader for partition %d (new epoch: %d)", f.partitionID, f.epoch+1)
	return leader, nil
}

// Stop stops the follower
func (f *Follower) Stop() {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.active {
		return
	}

	f.active = false
	close(f.quit)

	if f.listener != nil {
		f.listener.Close()
	}

	log.Printf("[FOLLOWER] Stopped replication for partition %d", f.partitionID)
}

// SetWAL sets the WAL for this follower
func (f *Follower) SetWAL(wal *storage.WAL) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.wal = wal
	f.nextOffset = wal.GetNextOffset()
}

// SetLeader updates the leader information
func (f *Follower) SetLeader(leaderID, leaderAddr string, epoch int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.leaderID = leaderID
	f.leaderAddr = leaderAddr
	f.epoch = epoch

	log.Printf("[FOLLOWER] Updated leader info to %s (epoch: %d)", leaderID, epoch)
	return nil
}

// IsCatchingUp returns true if follower is catching up
func (f *Follower) IsCatchingUp() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.catchupMode
}

// GetStats returns follower statistics
func (f *Follower) GetStats() *FollowerStats {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return &FollowerStats{
		PartitionID:  f.partitionID,
		LeaderID:     f.leaderID,
		Epoch:        f.epoch,
		NextOffset:   f.nextOffset,
		Active:       f.active,
		CatchingUp:   f.catchupMode,
		LastSyncTime: f.lastSyncTime,
	}
}

// FollowerStats represents follower statistics
type FollowerStats struct {
	PartitionID  int32
	LeaderID     string
	Epoch        int64
	NextOffset   int64
	Active       bool
	CatchingUp   bool
	LastSyncTime time.Time
}

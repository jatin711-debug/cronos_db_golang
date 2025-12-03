package replication

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"cronos_db/internal/storage"
	"cronos_db/pkg/types"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	conn         *grpc.ClientConn
	lastSyncTime time.Time
	syncInterval time.Duration
	catchupMode  bool // True when catching up with leader
}

// NewFollower creates a new follower
func NewFollower(partitionID int32, leaderID, leaderAddr string) *Follower {
	return &Follower{
		partitionID:  partitionID,
		epoch:        0,
		leaderID:     leaderID,
		leaderAddr:   leaderAddr,
		nextOffset:   0,
		quit:         make(chan struct{}),
		active:       false,
		syncInterval: 1 * time.Second,
		catchupMode:  false,
	}
}

// Start starts the follower
func (f *Follower) Start() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.active {
		return nil
	}

	// Connect to leader
	if err := f.connectToLeader(); err != nil {
		return fmt.Errorf("connect to leader: %w", err)
	}

	f.active = true
	go f.replicationLoop()

	log.Printf("[FOLLOWER] Started replication from leader %s for partition %d", f.leaderID, f.partitionID)
	return nil
}

// connectToLeader establishes connection to leader
func (f *Follower) connectToLeader() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, f.leaderAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return err
	}

	f.conn = conn
	return nil
}

// replicationLoop is the follower replication loop
func (f *Follower) replicationLoop() {
	ticker := time.NewTicker(f.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			f.mu.RLock()
			active := f.active
			f.mu.RUnlock()

			if !active {
				return
			}

			if err := f.syncFromLeader(); err != nil {
				log.Printf("[FOLLOWER] Sync error from leader %s: %v", f.leaderID, err)
			}
		case <-f.quit:
			return
		}
	}
}

// syncFromLeader syncs from leader
func (f *Follower) syncFromLeader() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.conn == nil {
		if err := f.connectToLeader(); err != nil {
			return fmt.Errorf("reconnect: %w", err)
		}
	}

	// In production, this would call leader's ReplicationService.Sync()
	// Request: SyncRequest{PartitionID, Epoch, FromOffset}
	// Response: SyncResponse{Events[], LeaderEpoch, HighWatermark}

	f.lastSyncTime = time.Now()
	return nil
}

// AppendEvents appends replicated events to WAL
func (f *Follower) AppendEvents(events []*types.Event, leaderEpoch int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Check epoch - reject if stale
	if leaderEpoch < f.epoch {
		return fmt.Errorf("stale epoch: got %d, have %d", leaderEpoch, f.epoch)
	}

	// Update epoch if newer
	if leaderEpoch > f.epoch {
		f.epoch = leaderEpoch
		log.Printf("[FOLLOWER] Updated epoch to %d", f.epoch)
	}

	if f.wal == nil {
		return fmt.Errorf("WAL not set")
	}

	for _, event := range events {
		// Validate offset sequence
		if event.Offset != f.nextOffset {
			f.catchupMode = true
			return fmt.Errorf("offset gap: expected %d, got %d", f.nextOffset, event.Offset)
		}

		if err := f.wal.AppendEvent(event); err != nil {
			return fmt.Errorf("append event: %w", err)
		}
		f.nextOffset = event.Offset + 1
	}

	f.catchupMode = false
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

	if f.conn != nil {
		f.conn.Close()
		f.conn = nil
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

	// Close old connection if leader changed
	if f.leaderID != leaderID && f.conn != nil {
		f.conn.Close()
		f.conn = nil
	}

	f.leaderID = leaderID
	f.leaderAddr = leaderAddr
	f.epoch = epoch

	log.Printf("[FOLLOWER] Updated leader to %s (epoch: %d)", leaderID, epoch)
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

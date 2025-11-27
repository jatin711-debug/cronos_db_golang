package replication

import (
	"fmt"
	"sync"
	"time"

	"cronos_db/internal/storage"
	"cronos_db/pkg/types"
)

// Follower handles replication from leader
type Follower struct {
	mu           sync.RWMutex
	partitionID  int32
	leaderID     string
	leaderAddr   string
	nextOffset   int64
	wal          *storage.WAL
	quit         chan struct{}
	active       bool
}

// NewFollower creates a new follower
func NewFollower(partitionID int32, leaderID, leaderAddr string) *Follower {
	return &Follower{
		partitionID: partitionID,
		leaderID:    leaderID,
		leaderAddr:  leaderAddr,
		nextOffset:  0,
		quit:        make(chan struct{}),
		active:      false,
	}
}

// Start starts the follower
func (f *Follower) Start() {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.active {
		return
	}

	f.active = true
	go f.replicationLoop()
}

// replicationLoop is the follower replication loop
func (f *Follower) replicationLoop() {
	ticker := time.NewTicker(1 * time.Second) // Sync every second
	defer ticker.Stop()

	for f.active {
		select {
		case <-ticker.C:
			if err := f.syncFromLeader(); err != nil {
				// Log error
			}
		case <-f.quit:
			return
		}
	}
}

// syncFromLeader syncs from leader
func (f *Follower) syncFromLeader() error {
	// TODO: Call leader's ReplicationService.Sync() to get missing events
	// This would send ReplicationSyncRequest and receive events

	return nil
}

// AppendEvents appends replicated events to WAL
func (f *Follower) AppendEvents(events []*types.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, event := range events {
		if err := f.wal.AppendEvent(event); err != nil {
			return fmt.Errorf("append event: %w", err)
		}
		f.nextOffset = event.Offset + 1
	}

	return nil
}

// GetNextOffset returns next offset to replicate
func (f *Follower) GetNextOffset() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.nextOffset
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
}

// SetWAL sets the WAL for this follower
func (f *Follower) SetWAL(wal *storage.WAL) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.wal = wal
	f.nextOffset = wal.GetNextOffset()
}

// GetStats returns follower statistics
func (f *Follower) GetStats() *FollowerStats {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return &FollowerStats{
		PartitionID: f.partitionID,
		LeaderID:    f.leaderID,
		NextOffset:  f.nextOffset,
		Active:      f.active,
	}
}

// FollowerStats represents follower statistics
type FollowerStats struct {
	PartitionID int32
	LeaderID    string
	NextOffset  int64
	Active      bool
}

package replication

import (
	"fmt"
	"sync"
	"time"

	"cronos_db/pkg/types"
)

// Leader manages replication to followers
type Leader struct {
	mu           sync.RWMutex
	partitionID  int32
	followers    map[string]*FollowerInfo
	batchSize    int32
	flushInterval time.Duration
	quit         chan struct{}
}

// FollowerInfo represents metadata about a follower replica
type FollowerInfo struct {
	ID          string
	Address     string
	NextOffset  int64
	LastAckTS   int64
	HighWatermark int64
	Connected   bool
	Buffer      []*types.Event
	LastError   error
}

// NewLeader creates a new leader
func NewLeader(partitionID int32, batchSize int32, flushInterval time.Duration) *Leader {
	return &Leader{
		partitionID: partitionID,
		followers:   make(map[string]*FollowerInfo),
		batchSize:   batchSize,
		flushInterval: flushInterval,
		quit:        make(chan struct{}),
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
		Connected:  true,
	}

	return nil
}

// RemoveFollower removes a follower
func (l *Leader) RemoveFollower(id string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, exists := l.followers[id]; !exists {
		return fmt.Errorf("follower %s not found", id)
	}

	delete(l.followers, id)
	return nil
}

// Replicate replicates an event batch to followers
func (l *Leader) Replicate(events []*types.Event) error {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Send to all followers
	for _, follower := range l.followers {
		if !follower.Connected {
			continue
		}

		if err := l.sendBatch(follower, events); err != nil {
			follower.LastError = err
			// Log error
			continue
		}

		follower.LastError = nil
		follower.LastAckTS = time.Now().UnixMilli()
	}

	return nil
}

// sendBatch sends a batch to a follower
func (l *Leader) sendBatch(follower *FollowerInfo, events []*types.Event) error {
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

	// TODO: Send batch via gRPC to follower
	// This would use ReplicationService.Append()

	// Clear buffer
	follower.Buffer = follower.Buffer[:0]

	return nil
}

// GetHighWatermark returns the minimum high watermark across followers
func (l *Leader) GetHighWatermark() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.followers) == 0 {
		return 0
	}

	minWatermark := int64(-1)
	for _, follower := range l.followers {
		if minWatermark == -1 || follower.HighWatermark < minWatermark {
			minWatermark = follower.HighWatermark
		}
	}

	return minWatermark
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
	l.mu.RLock()
	defer l.mu.RUnlock()

	for _, follower := range l.followers {
		if len(follower.Buffer) > 0 {
			l.flushFollower(follower)
		}
	}
}

// Stop stops the leader
func (l *Leader) Stop() {
	close(l.quit)
}

// GetStats returns leader statistics
func (l *Leader) GetStats() *LeaderStats {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return &LeaderStats{
		Followers:      int64(len(l.followers)),
		ConnectedFollowers: l.countConnectedFollowers(),
		HighWatermark:  l.GetHighWatermark(),
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

// LeaderStats represents leader statistics
type LeaderStats struct {
	Followers           int64
	ConnectedFollowers  int64
	HighWatermark       int64
}

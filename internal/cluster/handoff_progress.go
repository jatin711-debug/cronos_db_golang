package cluster

import (
	"context"
	"log"
	"slices"
	"time"
)

// A handoff moves a partition's leadership to another replica once that
// replica's log ends where the leader's does. The log is not all a leader
// needs: it also has to know what each consumer group has finished, or it
// delivers that again. The leader copies this to its followers separately
// from the log, and a follower that has just come back needs the whole copy.
//
// Up to v0.14.1 a handoff did not wait for it. A node that returned after
// five minutes away was leading its partitions again twelve seconds later,
// with its log caught up and its record of the groups' progress where it had
// been when the node stopped, and delivered everything since a second time:
// 64,000 events per group in the soak that found it.

// leaderProgressReporter is implemented by a partition accessor that can ask
// a partition's leader which of its followers hold its consumer progress
// ("" is this node). The partition manager implements it.
type leaderProgressReporter interface {
	LeaderProgressFollowers(ctx context.Context, addr string, partitionID int32) (reported bool, followers []string, err error)
}

// progressWaitLogEvery is how often a handoff that waits for the target to
// hold the consumer progress says so.
const progressWaitLogEvery = time.Minute

// targetHoldsProgress reports whether the target of a handoff holds the
// leader's consumer progress, as the leader tells it. It is also true when
// the leader cannot tell: a build that does not report it, or a partition
// that replicates no consumer progress. The handoff then goes ahead as it
// did before. It is false when the leader does not answer.
func (m *Manager) targetHoldsProgress(partitionID int32, leader, target string, alive map[string]string) bool {
	m.mu.RLock()
	reporter, ok := m.positions.(leaderProgressReporter)
	m.mu.RUnlock()
	if !ok {
		return true
	}
	addr := alive[leader]
	if leader == m.config.NodeID {
		addr = ""
	} else if addr == "" {
		return false
	}
	ctx, cancel := context.WithTimeout(m.ctx, positionQueryTimeout)
	reported, followers, err := reporter.LeaderProgressFollowers(ctx, addr, partitionID)
	cancel()
	if err != nil {
		return false
	}
	if !reported || slices.Contains(followers, target) {
		return true
	}
	m.mu.Lock()
	if m.progressWaitLogged == nil {
		m.progressWaitLogged = make(map[int32]time.Time)
	}
	logIt := time.Since(m.progressWaitLogged[partitionID]) >= progressWaitLogEvery
	if logIt {
		m.progressWaitLogged[partitionID] = time.Now()
	}
	m.mu.Unlock()
	if logIt {
		log.Printf("[CLUSTER] Partition %d: handoff %s -> %s waits until %s holds the consumer groups' progress", partitionID, leader, target, target)
	}
	return false
}

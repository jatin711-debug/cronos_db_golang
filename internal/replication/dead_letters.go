package replication

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func (l *Leader) SetDeadLetterSource(version func() uint64, page func(types.DeadLetterCursor) ([]*types.DeadLetterRecord, types.DeadLetterCursor, bool, error)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.deadLetterVersion, l.deadLetterPage = version, page
}

// DeadLettersReplicated deliberately requires every assigned follower. A
// disconnected/old follower pins retention until it catches up or is removed.
func (l *Leader) DeadLettersReplicated(version uint64) bool {
	if version == 0 {
		return true
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, f := range l.followers {
		f.mu.Lock()
		ready := f.client != nil && f.deadLetterSent >= version && !f.deadLetterSentAt.IsZero()
		f.mu.Unlock()
		if !ready {
			return false
		}
	}
	return true
}

// shipDeadLetters sends journal pages before the corresponding completion
// snapshot. The explicit acknowledgement prevents an older server silently
// ignoring the additive protocol fields from releasing the retention gate.
func (l *Leader) shipDeadLetters(f *FollowerInfo) bool {
	l.mu.RLock()
	versionSource, source := l.deadLetterVersion, l.deadLetterPage
	l.mu.RUnlock()
	if source == nil {
		return true
	}
	version := versionSource()
	if version == 0 {
		return true
	}
	f.mu.Lock()
	if f.deadLetterSent == version && time.Since(f.deadLetterSentAt) < progressResendInterval && !f.deadLetterActive {
		f.mu.Unlock()
		return true
	}
	if f.client == nil || f.progressSending {
		f.mu.Unlock()
		return false
	}
	if !f.deadLetterActive {
		f.deadLetterCursor = types.DeadLetterCursor{}
		f.deadLetterRoundVersion = version
		f.deadLetterActive = true
	}
	cursor, roundVersion, client := f.deadLetterCursor, f.deadLetterRoundVersion, f.client
	f.progressSending = true
	f.mu.Unlock()
	go func() {
		records, next, done, err := source(cursor)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), l.getReplicateTimeout())
			resp, callErr := client.SyncConsumerProgress(ctx, &types.ReplicationProgressRequest{PartitionId: l.partitionID, Term: atomic.LoadInt64(&l.epoch), LeaderId: l.nodeID, DeadLetters: records, RequireDeadLetterAck: true})
			cancel()
			err = callErr
			if err == nil && (!resp.GetSuccess() || !resp.GetDeadLettersDurable()) {
				err = fmt.Errorf("dead-letter durable acknowledgement missing: %s", resp.GetError())
			}
		}
		f.mu.Lock()
		f.progressSending = false
		if err == nil && f.client == client {
			f.deadLetterCursor = next
			if done {
				f.deadLetterActive = false
				f.deadLetterSent = roundVersion
				f.deadLetterSentAt = time.Now()
			}
		}
		firstFailure := err != nil && !f.progressFailing
		f.progressFailing = err != nil
		f.mu.Unlock()
		if firstFailure {
			log.Printf("[LEADER] Dead-letter replication to %s failed: %v", f.ID, err)
		}
	}()
	return false
}

func (l *Leader) followerHasDeadLetters(f *FollowerInfo) bool {
	l.mu.RLock()
	source := l.deadLetterVersion
	l.mu.RUnlock()
	if source == nil {
		return true
	}
	version := source()
	f.mu.Lock()
	defer f.mu.Unlock()
	return version == 0 || (f.deadLetterSent >= version && !f.deadLetterSentAt.IsZero())
}

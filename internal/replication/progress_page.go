package replication

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func (l *Leader) SetProgressPageSource(version func() uint64, page func(types.ProgressCursor) ([]*types.ConsumerGroupProgress, types.ProgressCursor, bool, error)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.progressVersion, l.progressPage = version, page
}

func (l *Leader) shipPagedConsumerProgress() {
	l.mu.RLock()
	versionSource, source, feedSource := l.progressVersion, l.progressPage, l.changeFeedSource
	followers := make([]*FollowerInfo, 0, len(l.followers))
	for _, f := range l.followers {
		followers = append(followers, f)
	}
	l.mu.RUnlock()
	version := versionSource()
	var feedOffset int64
	var feedKnown bool
	if feedSource != nil {
		feedOffset, feedKnown = feedSource()
	}
	for _, f := range followers {
		if !l.shipDeadLetters(f) {
			continue
		}
		f.mu.Lock()
		client := f.client
		logEnd := f.NextOffset
		if f.LastAckTS == 0 {
			logEnd = -1
		}
		tookAll := f.progressSentLogEnd >= f.progressSentReach || logEnd < f.progressSentReach
		current := f.progressSent == version && time.Since(f.progressSentAt) < progressResendInterval &&
			tookAll && f.feedSentKnown == feedKnown && (!feedKnown || f.feedSent == feedOffset)
		if client == nil || f.progressSending || (!f.progressRoundActive && current) {
			f.mu.Unlock()
			continue
		}
		if !f.progressRoundActive {
			f.progressCursor = types.ProgressCursor{}
			f.progressRoundVersion = version
			f.progressRoundActive = true
			f.progressRoundLogEnd = logEnd
			f.progressRoundReach = 0
		}
		f.progressRoundLogEnd = min(f.progressRoundLogEnd, logEnd)
		cursor, roundVersion := f.progressCursor, f.progressRoundVersion
		f.progressSending = true
		f.mu.Unlock()
		go func(f *FollowerInfo) {
			groups, next, done, err := source(cursor)
			if err == nil && !l.followerHasDeadLetters(f) {
				err = fmt.Errorf("new dead letters await replication")
			}
			if err == nil {
				ctx, cancel := context.WithTimeout(context.Background(), l.getReplicateTimeout())
				resp, callErr := client.SyncConsumerProgress(ctx, &types.ReplicationProgressRequest{PartitionId: l.partitionID, Term: atomic.LoadInt64(&l.epoch), LeaderId: l.nodeID, Groups: groups, HasChangeFeedOffset: feedKnown, ChangeFeedOffset: feedOffset})
				cancel()
				err = callErr
				if err == nil && !resp.GetSuccess() {
					err = fmt.Errorf("%s", resp.GetError())
				}
			}
			f.mu.Lock()
			f.progressSending = false
			if err == nil && f.client == client {
				f.progressCursor = next
				f.progressRoundReach = max(f.progressRoundReach, progressReach(groups))
				if done {
					f.progressRoundActive = false
					f.progressSent, f.progressSentAt, f.progressSentLogEnd = roundVersion, time.Now(), f.progressRoundLogEnd
					f.progressSentReach = f.progressRoundReach
					f.feedSent, f.feedSentKnown = feedOffset, feedKnown
				}
			}
			firstFailure := err != nil && !f.progressFailing
			f.progressFailing = err != nil
			f.mu.Unlock()
			if firstFailure {
				log.Printf("[LEADER] Consumer progress page to %s failed: %v", f.ID, err)
			}
		}(f)
	}
}

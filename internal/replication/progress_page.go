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

// SetProgressDeltaSource lets followers that hold the consumer progress up to
// a version be sent only what changed after it. delta returns every group
// with its floor and the completions recorded after since, and the version
// that brings the follower to; ok=false means the follower needs the full copy.
func (l *Leader) SetProgressDeltaSource(delta func(since uint64) (groups []*types.ConsumerGroupProgress, version uint64, ok bool)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.progressDelta = delta
}

func (l *Leader) shipPagedConsumerProgress() {
	l.mu.RLock()
	versionSource, source, deltaSource, feedSource := l.progressVersion, l.progressPage, l.progressDelta, l.changeFeedSource
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
		// Completions left for later go once the follower's log has grown.
		carryDue := len(f.progressCarry) > 0 && logEnd > f.progressSentLogEnd
		current := f.progressSent == version && time.Since(f.progressSentAt) < progressResendInterval &&
			tookAll && !carryDue && f.feedSentKnown == feedKnown && (!feedKnown || f.feedSent == feedOffset)
		if client == nil || f.progressSending || (!f.progressRoundActive && current) {
			f.mu.Unlock()
			continue
		}
		// A follower that took the full copy lately, took all of what it was
		// sent since, and whose log end is known, is sent only what changed.
		if !f.progressRoundActive && deltaSource != nil && tookAll && logEnd >= 0 &&
			!f.progressFullAt.IsZero() && time.Since(f.progressFullAt) < progressFullInterval {
			since, carry := f.progressSent, f.progressCarry
			f.progressSending = true
			f.mu.Unlock()
			go l.shipProgressDelta(f, client, deltaSource, since, carry, logEnd, feedOffset, feedKnown)
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
				err = l.sendProgress(client, groups, feedOffset, feedKnown)
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
					f.progressFullAt = time.Now()
					f.progressCarry = nil
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

// shipProgressDelta sends a follower what changed in the consumer progress
// after the version it holds, with the completions an earlier delta sent
// beyond its log end. The follower takes progress only as far as its own log
// reaches, so whatever this delta names at or beyond logEnd is carried to the
// next one; floors need no carrying, every delta has them. When no delta can
// be built, the follower is sent the full copy next.
func (l *Leader) shipProgressDelta(f *FollowerInfo, client types.ReplicationServiceClient,
	deltaSource func(uint64) ([]*types.ConsumerGroupProgress, uint64, bool), since uint64, carry map[string][]int64,
	logEnd, feedOffset int64, feedKnown bool) {
	groups, version, ok := deltaSource(since)
	var err error
	if ok {
		mergeCarry(groups, carry)
		if !l.followerHasDeadLetters(f) {
			err = fmt.Errorf("new dead letters await replication")
		}
	}
	if ok && err == nil {
		err = l.sendProgress(client, groups, feedOffset, feedKnown)
	}
	f.mu.Lock()
	f.progressSending = false
	switch {
	case !ok:
		f.progressFullAt = time.Time{}
	case err == nil && f.client == client:
		var later map[string][]int64
		for _, group := range groups {
			completed := group.GetCompletedOffsets()
			// Ascending: everything from the first at or beyond logEnd.
			for i, offset := range completed {
				if offset >= logEnd {
					if later == nil {
						later = make(map[string][]int64)
					}
					later[group.GetGroupId()] = append([]int64(nil), completed[i:]...)
					break
				}
			}
		}
		f.progressSent, f.progressSentAt = version, time.Now()
		f.progressSentLogEnd, f.progressSentReach = logEnd, min(progressReach(groups), logEnd)
		f.progressCarry = later
		f.feedSent, f.feedSentKnown = feedOffset, feedKnown
	}
	firstFailure := err != nil && !f.progressFailing
	f.progressFailing = err != nil
	f.mu.Unlock()
	if firstFailure {
		log.Printf("[LEADER] Consumer progress to %s failed: %v", f.ID, err)
	}
}

// mergeCarry adds carried completions to a delta's groups, keeping each
// group's offsets ascending and without repeats. Carried offsets below the
// group's floor are dropped: the floor covers them now.
func mergeCarry(groups []*types.ConsumerGroupProgress, carry map[string][]int64) {
	if len(carry) == 0 {
		return
	}
	for _, group := range groups {
		carried := carry[group.GetGroupId()]
		if len(carried) == 0 {
			continue
		}
		merged := make([]int64, 0, len(group.CompletedOffsets)+len(carried))
		a, b := group.CompletedOffsets, carried
		for len(a) > 0 || len(b) > 0 {
			var next int64
			switch {
			case len(b) == 0 || (len(a) > 0 && a[0] < b[0]):
				next, a = a[0], a[1:]
			case len(a) == 0 || b[0] < a[0]:
				next, b = b[0], b[1:]
			default: // equal
				next, a, b = a[0], a[1:], b[1:]
			}
			if next >= group.GetCommittedOffset() {
				merged = append(merged, next)
			}
		}
		group.CompletedOffsets = merged
	}
}

// sendProgress sends one consumer-progress request to a follower.
func (l *Leader) sendProgress(client types.ReplicationServiceClient, groups []*types.ConsumerGroupProgress, feedOffset int64, feedKnown bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), l.getReplicateTimeout())
	defer cancel()
	resp, err := client.SyncConsumerProgress(ctx, &types.ReplicationProgressRequest{PartitionId: l.partitionID, Term: atomic.LoadInt64(&l.epoch), LeaderId: l.nodeID, Groups: groups, HasChangeFeedOffset: feedKnown, ChangeFeedOffset: feedOffset})
	if err != nil {
		return err
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("%s", resp.GetError())
	}
	return nil
}

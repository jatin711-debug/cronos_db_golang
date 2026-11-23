package replication

import (
	"context"
	"fmt"
	"log"
	"sort"
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
			f.progressRoundCarry, f.progressRoundCarried, f.progressRoundShort = nil, 0, false
		}
		f.progressRoundLogEnd = min(f.progressRoundLogEnd, logEnd)
		cursor, roundVersion := f.progressCursor, f.progressRoundVersion
		f.progressSending = true
		f.mu.Unlock()
		go l.shipProgressRound(f, client, source, feedSource, cursor, roundVersion)
	}
}

// shipProgressRound sends a follower the full copy of the consumer progress
// from cursor on, one page after another, until it has all of it.
//
// A page went with each maintenance pass, 4,096 completions every half
// second. The copy holds every completion above each group's floor, and an
// event scheduled hours ahead holds the floor for those hours: with half a
// million completions per group, a follower that had been away, or whose
// connection had been made again, waited minutes for the copy. If it came to
// lead before the end it delivered again whatever the copy had not reached,
// and what was finished while the copy ran reached it only with the delta
// after.
//
// A failed page ends the pass; the next one continues from the page that
// failed. So does a dead letter recorded meanwhile, which has to reach the
// follower before the completion that goes with it.
//
// A follower takes progress only as far as its own log reaches. What a page
// names beyond that is judged against where the follower's log ended as that
// page was sent: a few completions there are kept and go with the next delta
// (progressRoundCarry); many, or a floor, mean a follower that is behind, and
// it is sent the copy again once its log has grown.
//
// This was judged for the whole copy, against where the log had ended when
// the copy began. By the time a copy of many pages ended, consumers had
// finished events appended since; the copy counted as cut short, and the next
// pass began it again instead of sending what had changed. A partition with
// enough completions above its floors was sent the full copy without end,
// and its followers were always a copy's length behind.
func (l *Leader) shipProgressRound(f *FollowerInfo, client types.ReplicationServiceClient,
	source func(types.ProgressCursor) ([]*types.ConsumerGroupProgress, types.ProgressCursor, bool, error),
	feedSource func() (int64, bool), cursor types.ProgressCursor, roundVersion uint64) {
	for {
		var feedOffset int64
		var feedKnown bool
		if feedSource != nil {
			feedOffset, feedKnown = feedSource()
		}
		groups, next, done, err := source(cursor)
		if err == nil && !l.followerHasDeadLetters(f) {
			err = fmt.Errorf("new dead letters await replication")
		}
		// Where the follower's log ends as this page goes: it only grows, so
		// the follower takes at least the completions below it.
		f.mu.Lock()
		logEnd := f.NextOffset
		if f.LastAckTS == 0 {
			logEnd = -1
		}
		f.mu.Unlock()
		if err == nil {
			err = l.sendProgress(client, groups, feedOffset, feedKnown)
		}
		f.mu.Lock()
		more := err == nil && f.client == client && !done
		if err == nil && f.client == client {
			f.progressCursor = next
			f.progressRoundReach = max(f.progressRoundReach, progressReach(groups))
			f.noteLeftByRound(groups, logEnd)
			if done {
				f.progressRoundActive = false
				f.progressSent, f.progressSentAt = roundVersion, time.Now()
				if f.progressRoundShort {
					// As sent to the log the follower had when the copy
					// began: it goes again once that log reaches the copy.
					f.progressSentLogEnd, f.progressSentReach = f.progressRoundLogEnd, f.progressRoundReach
					f.progressCarry = nil
				} else {
					f.progressSentLogEnd, f.progressSentReach = logEnd, min(f.progressRoundReach, logEnd)
					f.progressCarry = f.progressRoundCarry
				}
				f.progressRoundCarry = nil
				f.feedSent, f.feedSentKnown = feedOffset, feedKnown
				f.progressFullAt = time.Now()
			}
		}
		if more {
			select {
			case <-l.quit:
				more = false // this node no longer leads
			default:
			}
		}
		if !more {
			f.progressSending = false
		}
		firstFailure := err != nil && !f.progressFailing
		f.progressFailing = err != nil
		f.mu.Unlock()
		if firstFailure {
			log.Printf("[LEADER] Consumer progress page to %s failed: %v", f.ID, err)
		}
		if !more {
			return
		}
		cursor = next
	}
}

// maxRoundCarry is how many completions of a full copy may wait for the
// follower's log to grow and go with the next delta. A follower that left
// more than this is behind, and gets the copy again when it has caught up.
const maxRoundCarry = 4096

// noteLeftByRound records what a page of the full copy named that a follower
// whose log ended at logEnd did not take: the completions at or beyond it, in
// progressRoundCarry, or progressRoundShort when they are too many or a floor
// lies beyond it. The caller holds f.mu.
func (f *FollowerInfo) noteLeftByRound(groups []*types.ConsumerGroupProgress, logEnd int64) {
	for _, group := range groups {
		if group.GetCommittedOffset() > logEnd {
			f.progressRoundShort = true
		}
		completed := group.GetCompletedOffsets()
		// Ascending: everything from the first at or beyond logEnd.
		first := sort.Search(len(completed), func(i int) bool { return completed[i] >= logEnd })
		if first == len(completed) || f.progressRoundShort {
			continue
		}
		f.progressRoundCarried += len(completed) - first
		if f.progressRoundCarried > maxRoundCarry {
			f.progressRoundShort = true
			continue
		}
		if f.progressRoundCarry == nil {
			f.progressRoundCarry = make(map[string][]int64)
		}
		f.progressRoundCarry[group.GetGroupId()] = append(f.progressRoundCarry[group.GetGroupId()], completed[first:]...)
	}
	if f.progressRoundShort {
		f.progressRoundCarry = nil
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

// ProgressFollowers returns the followers that hold this leader's consumer
// progress, by ID. Such a follower has taken the full copy since it was last
// connected and is not being sent it again; it took all of what it was sent,
// and nothing waits for its log to grow; and it holds the current version, or
// acknowledged what had changed within the last few maintenance passes, which
// is as near as a follower gets while consumers go on acknowledging.
//
// A replica that is not among them would, if it came to lead, deliver again
// what it has not been told was finished. reported is false when this leader
// does not replicate consumer progress in pages and so cannot say.
func (l *Leader) ProgressFollowers() (followers []string, reported bool) {
	l.mu.RLock()
	versionSource, paged := l.progressVersion, l.progressPage != nil
	all := make([]*FollowerInfo, 0, len(l.followers))
	for _, f := range l.followers {
		all = append(all, f)
	}
	fresh := 3*l.flushInterval + time.Second
	l.mu.RUnlock()
	if !paged || versionSource == nil {
		return nil, false
	}
	version := versionSource()
	for _, f := range all {
		f.mu.Lock()
		holds := !f.progressFullAt.IsZero() && !f.progressRoundActive && len(f.progressCarry) == 0 &&
			f.progressSentLogEnd >= f.progressSentReach &&
			(f.progressSent == version || time.Since(f.progressSentAt) < fresh)
		f.mu.Unlock()
		if holds {
			followers = append(followers, f.ID)
		}
	}
	sort.Strings(followers)
	return followers, true
}

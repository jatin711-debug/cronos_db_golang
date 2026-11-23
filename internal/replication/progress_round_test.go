package replication

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
)

// pagedProgressClient is a follower that keeps the completions of every page
// of consumer progress it takes. It refuses the page numbered failAt, once.
type pagedProgressClient struct {
	types.ReplicationServiceClient
	failAt int
	// afterPage, if set, is called with the number of pages taken so far.
	afterPage func(taken int)

	mu     sync.Mutex
	failed bool
	pages  [][]int64
}

func (c *pagedProgressClient) SyncConsumerProgress(_ context.Context, r *types.ReplicationProgressRequest, _ ...grpc.CallOption) (*types.ReplicationProgressResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failAt > 0 && len(c.pages)+1 == c.failAt && !c.failed {
		c.failed = true
		return nil, errors.New("connection lost")
	}
	for _, group := range r.GetGroups() {
		c.pages = append(c.pages, slices.Clone(group.GetCompletedOffsets()))
	}
	if c.afterPage != nil {
		c.afterPage(len(c.pages))
	}
	return &types.ReplicationProgressResponse{Success: true}, nil
}

func (c *pagedProgressClient) taken() [][]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.pages)
}

// pagedProgress is a leader whose one group has finished offsets 0 to
// pages*perPage-1 above its floor, handed out perPage at a time.
func pagedProgress(pages, perPage int) func(types.ProgressCursor) ([]*types.ConsumerGroupProgress, types.ProgressCursor, bool, error) {
	total := int64(pages * perPage)
	return func(cursor types.ProgressCursor) ([]*types.ConsumerGroupProgress, types.ProgressCursor, bool, error) {
		from := max(cursor.NextOffset, 0)
		offsets := make([]int64, 0, perPage)
		for offset := from; offset < from+int64(perPage) && offset < total; offset++ {
			offsets = append(offsets, offset)
		}
		next := types.ProgressCursor{Group: "g", NextOffset: from + int64(perPage)}
		return []*types.ConsumerGroupProgress{{GroupId: "g", Topic: "t", CompletedOffsets: offsets}}, next, next.NextOffset >= total, nil
	}
}

func idle(f *FollowerInfo) func() bool {
	return func() bool { f.mu.Lock(); defer f.mu.Unlock(); return !f.progressSending }
}

// The full copy of the consumer progress goes to a follower in one pass, page
// after page. It went a page per maintenance pass, so a follower that came to
// lead meanwhile delivered again what the copy had not reached yet.
func TestLeader_TheFullCopyOfProgressGoesInOnePass(t *testing.T) {
	const pages, perPage = 25, 100
	c := &pagedProgressClient{}
	f := &FollowerInfo{ID: "f", client: c, Connected: true, InSync: true, LastAckTS: time.Now().UnixMilli(), NextOffset: pages * perPage}
	l := makeLeaderForTest(2, map[string]*FollowerInfo{"f": f})
	l.SetProgressPageSource(func() uint64 { return 1 }, pagedProgress(pages, perPage))

	l.shipPagedConsumerProgress()
	waitFor(t, "the pass to end", idle(f))

	taken := c.taken()
	if len(taken) != pages {
		t.Fatalf("one pass sent %d pages of %d", len(taken), pages)
	}
	for i, page := range taken {
		if len(page) != perPage || page[0] != int64(i*perPage) {
			t.Fatalf("page %d holds %d offsets from %d, want %d from %d", i, len(page), page[0], perPage, i*perPage)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.progressRoundActive || f.progressFullAt.IsZero() || f.progressSent != 1 {
		t.Fatalf("after the last page: round active=%v, full copy at %v, version %d; want the copy recorded as taken at version 1",
			f.progressRoundActive, f.progressFullAt, f.progressSent)
	}
}

// A page that fails ends the pass, and the next pass goes on from that page:
// the follower is sent it again and nothing before it.
func TestLeader_AFailedPageOfProgressIsWhereTheNextPassStarts(t *testing.T) {
	const pages, perPage = 25, 100
	c := &pagedProgressClient{failAt: 10}
	f := &FollowerInfo{ID: "f", client: c, Connected: true, InSync: true, LastAckTS: time.Now().UnixMilli(), NextOffset: pages * perPage}
	l := makeLeaderForTest(2, map[string]*FollowerInfo{"f": f})
	l.SetProgressPageSource(func() uint64 { return 1 }, pagedProgress(pages, perPage))

	l.shipPagedConsumerProgress()
	waitFor(t, "the first pass to end", idle(f))
	if got := len(c.taken()); got != 9 {
		t.Fatalf("the first pass delivered %d pages, want the 9 before the one that failed", got)
	}
	f.mu.Lock()
	active, full := f.progressRoundActive, f.progressFullAt
	f.mu.Unlock()
	if !active || !full.IsZero() {
		t.Fatalf("after a failed page: round active=%v, full copy at %v; want the copy still under way", active, full)
	}

	l.shipPagedConsumerProgress()
	waitFor(t, "the second pass to end", idle(f))
	taken := c.taken()
	if len(taken) != pages {
		t.Fatalf("two passes delivered %d pages, want %d", len(taken), pages)
	}
	for i, page := range taken {
		if page[0] != int64(i*perPage) {
			t.Fatalf("page %d starts at offset %d, want %d: a page was skipped or sent twice", i, page[0], i*perPage)
		}
	}
}

// A follower holds its leader's consumer progress once it has taken the full
// copy, and for as long as what changes keeps reaching it. A handoff waits
// for its target to be among them.
func TestLeader_ProgressFollowers(t *testing.T) {
	const pages, perPage = 25, 100
	c := &pagedProgressClient{}
	f := &FollowerInfo{ID: "f", client: c, Connected: true, InSync: true, LastAckTS: time.Now().UnixMilli(), NextOffset: pages * perPage}
	l := makeLeaderForTest(2, map[string]*FollowerInfo{"f": f})
	if _, reported := l.ProgressFollowers(); reported {
		t.Fatal("a leader that replicates no consumer progress reported who holds it")
	}

	var version atomic.Uint64
	version.Store(1)
	l.SetProgressPageSource(version.Load, pagedProgress(pages, perPage))
	holds := func() bool {
		followers, reported := l.ProgressFollowers()
		if !reported {
			t.Fatal("a leader that replicates consumer progress did not report who holds it")
		}
		return slices.Contains(followers, "f")
	}
	set := func(change func()) {
		f.mu.Lock()
		change()
		f.mu.Unlock()
	}

	if holds() {
		t.Fatal("a follower that was sent nothing yet holds the progress")
	}
	l.shipPagedConsumerProgress()
	waitFor(t, "the full copy", idle(f))
	if !holds() {
		t.Fatal("a follower that took the full copy does not hold the progress")
	}

	version.Store(2)
	set(func() { f.progressSentAt = time.Now().Add(-time.Minute) })
	if holds() {
		t.Fatal("a follower that was last sent the progress a minute and a version ago holds it")
	}
	set(func() { f.progressSentAt = time.Now() })
	if !holds() {
		t.Fatal("a follower that acknowledged the progress a moment ago does not hold it")
	}

	set(func() { f.progressCarry = map[string][]int64{"g": {9}} })
	if holds() {
		t.Fatal("a follower with completions waiting for its log to grow holds the progress")
	}
	set(func() { f.progressCarry = nil; f.progressRoundActive = true })
	if holds() {
		t.Fatal("a follower that is being sent the full copy again holds the progress")
	}
	set(func() { f.progressRoundActive = false; f.progressFullAt = time.Time{} })
	if holds() {
		t.Fatal("a follower whose connection was made again holds the progress before it took the full copy")
	}
}

// A full copy of many pages takes a while, and consumers go on finishing
// events appended meanwhile: its last pages name completions beyond where the
// follower's log ended when the copy began. The follower holds those entries
// by the time the pages arrive and takes them. The copy then counts as taken,
// and the follower is sent what changed next, not the full copy again. It
// counted as cut short, and a partition with many completions above its
// floors was sent the full copy without end.
func TestLeader_ACopyTheLogGrewDuringIsNotBegunAgain(t *testing.T) {
	const pages, perPage = 25, 100
	c := &pagedProgressClient{}
	// The follower's log ends where the first page does, and grows by a page's
	// worth of entries with each page it takes.
	f := &FollowerInfo{ID: "f", client: c, Connected: true, InSync: true, LastAckTS: time.Now().UnixMilli(), NextOffset: perPage}
	c.afterPage = func(taken int) {
		f.mu.Lock()
		f.NextOffset = int64((taken + 1) * perPage)
		f.mu.Unlock()
	}
	l := makeLeaderForTest(2, map[string]*FollowerInfo{"f": f})

	var version atomic.Uint64
	version.Store(1)
	var full, deltas atomic.Int32
	page := pagedProgress(pages, perPage)
	l.SetProgressPageSource(version.Load, func(cursor types.ProgressCursor) ([]*types.ConsumerGroupProgress, types.ProgressCursor, bool, error) {
		full.Add(1)
		return page(cursor)
	})
	l.SetProgressDeltaSource(func(uint64) ([]*types.ConsumerGroupProgress, uint64, bool) {
		deltas.Add(1)
		return []*types.ConsumerGroupProgress{{GroupId: "g", Topic: "t"}}, version.Load(), true
	})

	l.shipPagedConsumerProgress()
	waitFor(t, "the full copy", idle(f))
	if got := full.Load(); got != pages {
		t.Fatalf("the full copy took %d pages, want %d", got, pages)
	}

	version.Store(2)
	l.shipPagedConsumerProgress()
	waitFor(t, "what changed to be sent", idle(f))
	if full.Load() != pages || deltas.Load() != 1 {
		t.Fatalf("after a copy the follower took in full, %d more pages of the full copy and %d deltas were sent; want none and 1",
			full.Load()-pages, deltas.Load())
	}
}

// A few completions of a copy can lie beyond the follower's log, which is a
// batch behind its leader's at any moment. The follower leaves those; they
// are kept and go with the next delta once its log has grown, and the copy
// is not sent again for them.
func TestLeader_AFewCompletionsBeyondTheFollowersLogGoWithTheNextDelta(t *testing.T) {
	const pages, perPage = 25, 100
	c := &pagedProgressClient{}
	// The follower lacks the last hundred entries the copy speaks of.
	f := &FollowerInfo{ID: "f", client: c, Connected: true, InSync: true, LastAckTS: time.Now().UnixMilli(), NextOffset: (pages - 1) * perPage}
	l := makeLeaderForTest(2, map[string]*FollowerInfo{"f": f})
	var full, deltas atomic.Int32
	page := pagedProgress(pages, perPage)
	l.SetProgressPageSource(func() uint64 { return 1 }, func(cursor types.ProgressCursor) ([]*types.ConsumerGroupProgress, types.ProgressCursor, bool, error) {
		full.Add(1)
		return page(cursor)
	})
	l.SetProgressDeltaSource(func(uint64) ([]*types.ConsumerGroupProgress, uint64, bool) {
		deltas.Add(1)
		return []*types.ConsumerGroupProgress{{GroupId: "g", Topic: "t"}}, 1, true
	})

	l.shipPagedConsumerProgress()
	waitFor(t, "the full copy", idle(f))
	if followers, _ := l.ProgressFollowers(); len(followers) != 0 {
		t.Fatalf("a follower that left the last completions of the copy holds the progress: %v", followers)
	}
	l.shipPagedConsumerProgress()
	waitFor(t, "the pass to end", idle(f))
	if full.Load() != pages || deltas.Load() != 0 {
		t.Fatalf("%d pages and %d deltas went to a follower whose log has not grown; want the %d pages of one copy and nothing more", full.Load(), deltas.Load(), pages)
	}

	f.mu.Lock()
	f.NextOffset = pages * perPage
	f.mu.Unlock()
	l.shipPagedConsumerProgress()
	waitFor(t, "the delta", idle(f))
	if full.Load() != pages || deltas.Load() != 1 {
		t.Fatalf("once the follower's log had grown, %d more pages of the full copy and %d deltas were sent; want none and 1", full.Load()-pages, deltas.Load())
	}
	taken := c.taken()
	last := taken[len(taken)-1]
	if len(last) != perPage || last[0] != (pages-1)*perPage || last[perPage-1] != pages*perPage-1 {
		t.Fatalf("the delta named %d completions, want the %d from offset %d that the follower had left", len(last), perPage, (pages-1)*perPage)
	}
	if followers, _ := l.ProgressFollowers(); !slices.Contains(followers, "f") {
		t.Fatal("a follower that took the copy and what it had left does not hold the progress")
	}
}

// A follower that is far behind leaves most of a copy, more than a delta
// should carry. It is sent the copy again once its log has grown.
func TestLeader_ACopyFarBeyondTheFollowersLogGoesAgain(t *testing.T) {
	const pages, perPage = 60, 100
	c := &pagedProgressClient{}
	f := &FollowerInfo{ID: "f", client: c, Connected: true, InSync: true, LastAckTS: time.Now().UnixMilli(), NextOffset: 10 * perPage}
	l := makeLeaderForTest(2, map[string]*FollowerInfo{"f": f})
	var full atomic.Int32
	page := pagedProgress(pages, perPage)
	l.SetProgressPageSource(func() uint64 { return 1 }, func(cursor types.ProgressCursor) ([]*types.ConsumerGroupProgress, types.ProgressCursor, bool, error) {
		full.Add(1)
		return page(cursor)
	})
	l.SetProgressDeltaSource(func(uint64) ([]*types.ConsumerGroupProgress, uint64, bool) {
		t.Error("a delta went to a follower that has to be sent the full copy again")
		return nil, 1, false
	})

	l.shipPagedConsumerProgress()
	waitFor(t, "the full copy", idle(f))
	l.shipPagedConsumerProgress()
	waitFor(t, "the pass to end", idle(f))
	if got := full.Load(); got != pages {
		t.Fatalf("%d pages went to a follower whose log is still short of the copy, want the %d of one copy", got, pages)
	}
	if followers, _ := l.ProgressFollowers(); len(followers) != 0 {
		t.Fatalf("a follower that left most of the copy holds the progress: %v", followers)
	}

	f.mu.Lock()
	f.NextOffset = pages * perPage
	f.mu.Unlock()
	l.shipPagedConsumerProgress()
	waitFor(t, "the copy to go again", idle(f))
	if got := full.Load(); got != 2*pages {
		t.Fatalf("%d pages in all, want %d: the copy again once the follower's log reached it", got, 2*pages)
	}
	if followers, _ := l.ProgressFollowers(); !slices.Contains(followers, "f") {
		t.Fatal("a follower that took the copy in full does not hold the progress")
	}
}

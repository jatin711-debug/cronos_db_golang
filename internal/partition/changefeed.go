package partition

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
)

// A change feed hands the events of a partition to something outside it:
// change data capture sinks and replication to other regions.
//
// It replaces a hook on the log's append path, which had three faults. It
// fired before a publish was accepted, so a consumer could see an event that
// was then refused to its producer and never delivered. It fired on every
// replica, so each event was exported once per replica. And it was attached
// at startup to the partitions that existed then, which in a cluster is one.
//
// The feed pulls instead. One goroutine per partition reads the log behind
// the accepted watermark and hands the events over in log order. It runs on
// the partition's leader only. How far it has got is kept in a small file and
// sent to the followers with the consumer progress, so a restart or a new
// leader carries on from there: a consumer may see an event again, it does
// not miss one. A consumer that fails is offered the same events again later,
// and nothing but the feed waits for it.

// FeedFunc receives accepted events of one partition, in log order. Returning
// an error has the same events offered again; the feed never skips.
type FeedFunc func(ctx context.Context, partitionID int32, events []*types.Event) error

// PublishToken identifies a publish between BeginPublish and EndPublish.
type PublishToken uint64

const (
	feedCursorFile = "changefeed.json"
	// feedBatchEvents bounds how many events are read and handed over at once.
	feedBatchEvents = 500
	// feedIdlePoll is how long the feed sleeps when nothing wakes it. Finished
	// publishes do; the poll covers what advances the watermark silently.
	feedIdlePoll = time.Second
	// feedSaveInterval bounds how often the position is written to disk, and so
	// how much is handed over again after a crash.
	feedSaveInterval = time.Second
	feedDeliverLimit = 30 * time.Second
	feedRetryMin     = 100 * time.Millisecond
	feedRetryMax     = 5 * time.Second
)

type changeFeed struct {
	deliver   FeedFunc
	clustered bool
	path      string
	wake      chan struct{}

	// cursor is the offset of the last event handed over, -1 before the first.
	cursor atomic.Int64

	// open holds, for every publish in flight, the lowest offset it can still
	// append at. Guarded by mu.
	mu        sync.Mutex
	open      map[PublishToken]int64
	nextToken PublishToken

	// The rest belongs to the feed goroutine.
	saved   int64
	savedAt time.Time
	failing bool
}

type feedPosition struct {
	Offset int64 `json:"offset"`
}

// SetChangeFeed has every partition created from now on hand its accepted
// events to deliver. Call it before partitions are created.
func (pm *PartitionManager) SetChangeFeed(deliver FeedFunc) {
	pm.mu.Lock()
	pm.changeFeed = deliver
	pm.mu.Unlock()
}

// openChangeFeed attaches the feed to a partition that is not taking appends
// yet. A partition without a stored position starts at the end of its log:
// switching a feed on does not replay what was written before.
func (p *Partition) openChangeFeed(deliver FeedFunc, clustered bool) error {
	f := &changeFeed{
		deliver:   deliver,
		clustered: clustered,
		path:      filepath.Join(p.DataDir, feedCursorFile),
		wake:      make(chan struct{}, 1),
		open:      make(map[PublishToken]int64),
	}
	data, err := os.ReadFile(f.path)
	var stored feedPosition
	switch {
	case err == nil && json.Unmarshal(data, &stored) == nil && stored.Offset >= -1:
		f.cursor.Store(stored.Offset)
		f.saved = stored.Offset
	case err == nil:
		log.Printf("[Partition %d] Change feed position in %s is unreadable; continuing from the end of the log", p.ID, f.path)
		fallthrough
	case os.IsNotExist(err):
		f.cursor.Store(p.Wal.GetLastOffset())
		// Written now, not with the first events: a crash before the first
		// save would otherwise look like a feed that was just switched on.
		if err := f.write(f.cursor.Load()); err != nil {
			return fmt.Errorf("store change feed position: %w", err)
		}
	default:
		return fmt.Errorf("read change feed position: %w", err)
	}
	p.feed = f
	return nil
}

func (f *changeFeed) write(offset int64) error {
	data, err := json.Marshal(feedPosition{Offset: offset})
	if err != nil {
		return err
	}
	if err := utils.AtomicWriteFile(f.path, data, 0600); err != nil {
		return err
	}
	f.saved, f.savedAt = offset, time.Now()
	return nil
}

// advance moves the position forward to offset; it never moves it back.
func (f *changeFeed) advance(offset int64) {
	for {
		current := f.cursor.Load()
		if offset <= current || f.cursor.CompareAndSwap(current, offset) {
			return
		}
	}
}

// BeginPublish marks a publish to this partition as in flight. Callers check
// that the partition is writable only after calling it, and pass the token to
// EndPublish when the publish has finished, whatever its outcome.
//
// It serves two readers. A leadership handoff needs to know when the log can
// no longer grow: once the partition is not writable and nothing is in
// flight, no publish that could still append exists. The change feed needs to
// know which entries are not decided yet: everything a publish in flight has
// appended, or may still append.
func (p *Partition) BeginPublish() PublishToken {
	p.publishing.Add(1)
	f := p.feed
	if f == nil {
		return 0
	}
	// Whatever this publish appends gets an offset at or after this one.
	low := p.Wal.GetNextOffset()
	f.mu.Lock()
	f.nextToken++
	token := f.nextToken
	f.open[token] = low
	f.mu.Unlock()
	return token
}

// EndPublish ends a publish started with BeginPublish.
func (p *Partition) EndPublish(token PublishToken) {
	if f := p.feed; f != nil && token != 0 {
		f.mu.Lock()
		delete(f.open, token)
		f.mu.Unlock()
		p.wakeFeed()
	}
	p.publishing.Add(-1)
}

// AcceptedThrough returns the offset up to which every log entry belongs to
// an accepted publish: it has been acknowledged to its producer, or it will
// be delivered regardless. -1 means none.
//
// An entry is not accepted while its publish is in flight, while it is held
// after a publish that failed past the append, or, on a replicated partition,
// while it is not known to be on the required replicas. That last rule also
// makes the answer survive a failover: what a quorum holds is in the log of
// whichever replica leads next.
func (p *Partition) AcceptedThrough() int64 {
	// The order of these reads matters. A publish that has appended at or
	// below this log end is either still open, or it ended; one that ended
	// without being accepted was held before it ended. Reading the open
	// publishes before the held ranges therefore cannot miss it.
	through := p.Wal.GetLastOffset()

	if f := p.feed; f != nil {
		f.mu.Lock()
		for _, low := range f.open {
			through = min(through, low-1)
		}
		f.mu.Unlock()
	}

	p.heldMu.Lock()
	if len(p.held) > 0 {
		through = min(through, p.held[0].from-1)
	}
	p.heldMu.Unlock()

	if leader := p.replQuorum.Load(); leader != nil {
		through = min(through, leader.QuorumOffset())
	}
	return max(through, -1)
}

// wakeFeed tells the change feed that the accepted watermark may have moved.
func (p *Partition) wakeFeed() {
	if f := p.feed; f != nil {
		select {
		case f.wake <- struct{}{}:
		default:
		}
	}
}

// ChangeFeedPosition returns the offset of the last event the feed has handed
// over. known is false when the partition has no feed.
func (p *Partition) ChangeFeedPosition() (offset int64, known bool) {
	if p.feed == nil {
		return 0, false
	}
	return p.feed.cursor.Load(), true
}

// NoteLeaderFeedPosition records how far the leader's change feed has got, so
// that this replica continues from there if it takes over. A leader without a
// feed is exporting nothing; a replica that takes over from it starts at the
// end of the log, as if the feed had just been switched on.
func (p *Partition) NoteLeaderFeedPosition(offset int64, known bool) {
	f := p.feed
	if f == nil {
		return
	}
	if !known {
		offset = p.Wal.GetLastOffset()
	}
	f.advance(offset)
}

// runChangeFeed is the feed's goroutine. It ends when the partition stops.
func (p *Partition) runChangeFeed() {
	f := p.feed
	defer func() {
		if cursor := f.cursor.Load(); cursor != f.saved {
			if err := f.write(cursor); err != nil {
				log.Printf("[Partition %d] Storing the change feed position failed: %v", p.ID, err)
			}
		}
	}()

	timer := time.NewTimer(0)
	defer timer.Stop()
	var retry time.Duration
	var retryAt time.Time
	for {
		select {
		case <-p.deliveryQuit:
			return
		case <-f.wake:
			if time.Now().Before(retryAt) {
				continue // the consumer is failing; the timer decides when to try again
			}
		case <-timer.C:
		}

		wait := feedIdlePoll
		if err := p.drainFeed(); err != nil {
			retry = min(max(2*retry, feedRetryMin), feedRetryMax)
			wait, retryAt = retry, time.Now().Add(retry)
			if !f.failing {
				f.failing = true
				log.Printf("[Partition %d] Change feed is waiting at offset %d and will keep retrying: %v", p.ID, f.cursor.Load(), err)
			}
		} else {
			retry, retryAt = 0, time.Time{}
			if f.failing {
				f.failing = false
				log.Printf("[Partition %d] Change feed continues, now at offset %d", p.ID, f.cursor.Load())
			}
		}

		if cursor := f.cursor.Load(); cursor != f.saved && time.Since(f.savedAt) >= feedSaveInterval {
			if err := f.write(cursor); err != nil {
				log.Printf("[Partition %d] Storing the change feed position failed: %v", p.ID, err)
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(wait)
	}
}

// drainFeed hands over everything between the feed's position and the
// accepted watermark. It returns the consumer's error, or a read error, with
// the position left where the failure was.
func (p *Partition) drainFeed() error {
	f := p.feed
	for {
		select {
		case <-p.deliveryQuit:
			return nil
		default:
		}
		// Only the leader exports: its followers hold the same events.
		if f.clustered && !p.IsLeader() {
			return nil
		}
		cursor := f.cursor.Load()
		through := p.AcceptedThrough()
		// A demotion clears what the watermark is computed from, after it has
		// cleared the leader flag. Looking at the flag again here means a
		// watermark that was read during a demotion is not used.
		if f.clustered && !p.IsLeader() {
			return nil
		}
		if cursor >= through {
			return nil
		}
		from := cursor + 1
		to := min(through, cursor+feedBatchEvents)
		events, err := p.Wal.ReadEvents(from, to)
		if err != nil {
			return fmt.Errorf("read log at offsets %d-%d: %w", from, to, err)
		}
		if len(events) == 0 || events[0].Offset != from {
			// Retention removed entries the feed had not reached.
			next := to + 1
			if len(events) > 0 {
				next = events[0].Offset
			}
			log.Printf("[Partition %d] Change feed skips offsets %d-%d: they are no longer in the log", p.ID, from, next-1)
			if len(events) == 0 {
				f.advance(to)
				continue
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), feedDeliverLimit)
		err = f.deliver(ctx, p.ID, events)
		cancel()
		if err != nil {
			return err
		}
		f.advance(events[len(events)-1].Offset)
	}
}

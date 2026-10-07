package partition

import (
	"log"
	"sort"

	"github.com/jatin711-debug/cronos_db_golang/internal/dedup"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// A publish appends its events to the log before it knows whether they will be
// accepted: replication to the required replicas, a requested fsync, or
// scheduling can still fail. The log cannot take the events back, so the
// partition holds them instead. Held events are not scheduled, and their
// message IDs are recorded as appended rather than accepted, until one of
// these confirms that the log is replicated far enough:
//
//   - a retry of the publish, which is finished instead of appended again;
//   - a later publish that succeeds, since followers hold a prefix of the log;
//   - the replication leader's maintenance loop, once catch-up gets there.
//
// A restart or a promotion schedules everything in the log, so both start
// with nothing held.

// heldRange is a run of consecutive log offsets whose publish was not accepted.
type heldRange struct {
	from, to int64
	// claimed is true when the publish claimed the message IDs for
	// deduplication, false when it allowed duplicates.
	claimed bool
}

// acceptChunk bounds how many held events are read from the log at a time.
const acceptChunk = 10_000

// HoldUnaccepted records that events are in the log but that their publish
// failed before it was accepted. events must be consecutive; claimed says
// whether the publish claimed their message IDs.
func (p *Partition) HoldUnaccepted(events []*types.Event, claimed bool) {
	if len(events) == 0 {
		return
	}
	p.heldMu.Lock()
	defer p.heldMu.Unlock()

	p.insertHeldLocked(heldRange{from: events[0].Offset, to: events[len(events)-1].Offset, claimed: claimed})
	if !claimed || p.DedupStore == nil {
		return
	}
	ids := make([]string, len(events))
	stored := make([]int64, len(events))
	createdTS := make([]int64, len(events))
	for i, event := range events {
		ids[i], stored[i], createdTS[i] = event.MessageId, dedup.AppendedAt(event.Offset), event.CreatedTs
	}
	if err := p.DedupStore.PutBatch(ids, stored, createdTS); err != nil {
		// The IDs stay claimed, so retries are refused until the next restart
		// rebuilds these records from the log.
		log.Printf("[Partition %d] Recording %d unaccepted events at offsets %d-%d failed: %v",
			p.ID, len(events), events[0].Offset, events[len(events)-1].Offset, err)
	}
}

// insertHeldLocked adds a range, keeping held ordered by offset and joining it
// to a neighbour it continues. A partition that cannot reach its replicas
// fails every publish, and all of them collapse into one range.
func (p *Partition) insertHeldLocked(r heldRange) {
	i := sort.Search(len(p.held), func(i int) bool { return p.held[i].from > r.from })
	if i > 0 && p.held[i-1].to+1 == r.from && p.held[i-1].claimed == r.claimed {
		p.held[i-1].to = r.to
	} else {
		p.held = append(p.held, heldRange{})
		copy(p.held[i+1:], p.held[i:])
		p.held[i] = r
		i++
	}
	if i < len(p.held) && p.held[i-1].to+1 == p.held[i].from && p.held[i-1].claimed == p.held[i].claimed {
		p.held[i-1].to = p.held[i].to
		p.held = append(p.held[:i], p.held[i+1:]...)
	}
	p.heldCount.Store(int32(len(p.held)))
}

// HasUnaccepted reports whether any publish is held. It is one atomic load, so
// the publish path can ask on every request.
func (p *Partition) HasUnaccepted() bool { return p.heldCount.Load() > 0 }

// AcceptThrough accepts the held publishes whose events are at or below
// offset: it schedules the events and records their message IDs as accepted.
// The caller must know the log to be replicated as required up to offset.
func (p *Partition) AcceptThrough(offset int64) error {
	if !p.HasUnaccepted() {
		return nil
	}
	p.heldMu.Lock()
	defer p.heldMu.Unlock()
	defer func() { p.heldCount.Store(int32(len(p.held))) }()

	for len(p.held) > 0 && p.held[0].from <= offset {
		r := &p.held[0]
		for through := min(r.to, offset); r.from <= through; {
			end := min(r.from+acceptChunk-1, through)
			if err := p.acceptRangeLocked(r.from, end, r.claimed); err != nil {
				return err
			}
			r.from = end + 1
		}
		if r.from > r.to {
			p.held = p.held[1:]
		}
	}
	return nil
}

// acceptRangeLocked schedules the log entries in [from, to] and, when their
// publish claimed them, records the message IDs as accepted.
func (p *Partition) acceptRangeLocked(from, to int64, claimed bool) error {
	events, err := p.Wal.ReadEvents(from, to)
	if err != nil {
		return err
	}
	if err := p.Scheduler.ScheduleBatch(events); err != nil {
		return err
	}
	if claimed {
		p.recordAccepted(events)
	}
	return nil
}

// recordAccepted moves message IDs from appended to accepted. An ID whose
// record no longer says "appended at this offset" is left alone: it was
// released, or belongs to a different publish by now.
func (p *Partition) recordAccepted(events []*types.Event) {
	if p.DedupStore == nil {
		return
	}
	ids := make([]string, 0, len(events))
	offsets := make([]int64, 0, len(events))
	createdTS := make([]int64, 0, len(events))
	for _, event := range events {
		stored, found, err := p.DedupStore.GetOffset(event.MessageId)
		if err != nil || !found || stored != dedup.AppendedAt(event.Offset) {
			continue
		}
		ids = append(ids, event.MessageId)
		offsets = append(offsets, event.Offset)
		createdTS = append(createdTS, event.CreatedTs)
	}
	if err := p.DedupStore.PutBatch(ids, offsets, createdTS); err != nil {
		// The events are scheduled. Their IDs stay appended, which a retry
		// resolves without scheduling them again.
		log.Printf("[Partition %d] Recording %d accepted publishes failed: %v", p.ID, len(ids), err)
	}
}

// AcceptAppended finishes earlier publishes on a retry. logged are their
// events as read from the log, which the caller knows to be replicated as
// required up to the last of them. Publishes still held are scheduled and
// accepted with everything before them. The rest were found in the log after
// a restart or a promotion and are scheduled already, so they are only
// recorded as accepted.
func (p *Partition) AcceptAppended(logged []*types.Event) error {
	last := int64(-1)
	for _, event := range logged {
		last = max(last, event.Offset)
	}
	if err := p.AcceptThrough(last); err != nil {
		return err
	}
	p.heldMu.Lock()
	defer p.heldMu.Unlock()
	p.recordAccepted(logged)
	return nil
}

// LoggedEvent returns the log entry at offset if it is the event with
// messageID. inLog is false when the log does not hold that event there: it
// ends earlier, the entry was replaced by a newer leader's, or it was removed
// by retention. An error means the log could not be read.
func (p *Partition) LoggedEvent(messageID string, offset int64) (event *types.Event, inLog bool, err error) {
	if offset < 0 || offset > p.Wal.GetLastOffset() {
		return nil, false, nil
	}
	events, err := p.Wal.ReadEvents(offset, offset)
	if err != nil {
		return nil, false, err
	}
	if len(events) != 1 || events[0].Offset != offset || events[0].MessageId != messageID {
		return nil, false, nil
	}
	return events[0], true, nil
}

// dropHeld forgets held publishes. It is called when the whole log is about to
// be scheduled, and when this node stops leading and can no longer accept them.
func (p *Partition) dropHeld() {
	p.heldMu.Lock()
	p.held = nil
	p.heldCount.Store(0)
	p.heldMu.Unlock()
}

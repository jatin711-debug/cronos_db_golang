package consumer

import (
	"sort"
	"sync"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// A leader copies its partitions' consumer progress to their followers. The
// full copy is every completion record above each group's floor, and a floor
// cannot pass an event that is not due yet: one event scheduled a day ahead
// keeps every completion after it on record for that day. Sent in full each
// time it changed, the copy grew with everything delivered since the oldest
// pending event; in the soak it reached about a million offsets per group, and
// each follower was sent and rewrote all of them over and over.
//
// So the leader also keeps the completions it recorded recently, each with
// the progress version it was recorded under. A follower known to hold the
// progress up to some version is sent the floors and what was recorded after
// that version, and nothing else (ExportProgressDelta). A follower that has
// no such version, or one the journal no longer reaches back to, gets the
// full copy as before.

const (
	// maxJournalOffsets bounds the completions a partition's journal keeps.
	// A follower further behind than this gets the full copy.
	maxJournalOffsets = 1 << 18
	// maxDeltaOffsets is about how many completions one delta carries. What
	// was recorded after that goes with the next one.
	maxDeltaOffsets = 1 << 16
)

type journalEntry struct {
	version uint64
	group   string
	offsets []int64
}

// progressJournal holds one partition's recent completions, oldest first.
type progressJournal struct {
	// entries[head:] are the entries held. Those before head were dropped
	// and are cleared; see dropOldest.
	entries []journalEntry
	head    int
	offsets int
	// horizon is the latest version whose completions may be missing from
	// entries: a delta can be built only from a version at or after it.
	horizon uint64
}

// journalCompactAt is the least number of dropped entries at the front of a
// journal that is worth moving the held ones over.
const journalCompactAt = 1024

// live returns the entries held, oldest first.
func (journal *progressJournal) live() []journalEntry {
	return journal.entries[journal.head:]
}

// dropOldest forgets the oldest entries until the journal is within its
// bound.
//
// The entries dropped stay in the slice, cleared, until they are at least as
// many as the ones held, and the held ones are moved to the front then: each
// move costs no more than the drops since the last one. They used to be moved
// on every drop. A journal at its bound drops with every ack, so every ack
// copied the whole journal, megabytes of it, under the journal's lock; in the
// soak that was a fifth of a leader's CPU.
func (journal *progressJournal) dropOldest() {
	for journal.offsets > maxJournalOffsets && journal.head < len(journal.entries) {
		oldest := &journal.entries[journal.head]
		journal.offsets -= len(oldest.offsets)
		journal.horizon = max(journal.horizon, oldest.version)
		*oldest = journalEntry{} // lets go of its offsets
		journal.head++
	}
	if journal.head >= journalCompactAt && 2*journal.head >= len(journal.entries) {
		held := copy(journal.entries, journal.entries[journal.head:])
		clear(journal.entries[held:])
		journal.entries = journal.entries[:held]
		journal.head = 0
	}
}

type progressJournals struct {
	mu       sync.Mutex
	journals map[int32]*progressJournal
}

// recordLocked adds completions recorded under version. The caller holds mu.
func (j *progressJournals) recordLocked(partitionID int32, version uint64, added map[string][]int64) {
	journal := j.journals[partitionID]
	if journal == nil {
		if j.journals == nil {
			j.journals = make(map[int32]*progressJournal)
		}
		// Whatever was recorded before now is not in it.
		journal = &progressJournal{horizon: version - 1}
		j.journals[partitionID] = journal
	}
	for group, offsets := range added {
		if len(offsets) == 0 {
			continue
		}
		journal.entries = append(journal.entries, journalEntry{version: version, group: group, offsets: offsets})
		journal.offsets += len(offsets)
	}
	journal.dropOldest()
}

// reset empties a partition's journal: no delta reaches back past version.
func (j *progressJournals) reset(partitionID int32, version uint64) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.journals == nil {
		j.journals = make(map[int32]*progressJournal)
	}
	j.journals[partitionID] = &progressJournal{horizon: version}
}

// sinceLocked returns, per group, the completions recorded after version, and
// whether the journal reaches back that far. Once it has gathered limit
// completions it stops at the end of the version it is in: upTo is then that
// version, the one the completions returned bring a follower to, and what was
// recorded later is left for the next call. upTo is 0 when nothing was left.
// The caller holds mu.
func (j *progressJournals) sinceLocked(partitionID int32, version uint64, limit int) (added map[string][]int64, upTo uint64, ok bool) {
	journal := j.journals[partitionID]
	if journal == nil || version < journal.horizon {
		return nil, 0, false
	}
	entries := journal.live()
	first := sort.Search(len(entries), func(i int) bool { return entries[i].version > version })
	added = make(map[string][]int64)
	total := 0
	for i, entry := range entries[first:] {
		// The entries of one version are next to each other, one per group.
		if total >= limit && entry.version != entries[first+i-1].version {
			return added, entries[first+i-1].version, true
		}
		added[entry.group] = append(added[entry.group], entry.offsets...)
		total += len(entry.offsets)
	}
	return added, 0, true
}

// noteCompletions bumps the progress version for completions just recorded
// on a partition and adds them to its journal under that version. Both happen
// under the journal's lock, so a delta built for a version has every
// completion recorded under it.
func (g *GroupManager) noteCompletions(partitionID int32, added map[string][]int64) {
	g.journal.mu.Lock()
	version := g.progressVersion.Add(1)
	g.journal.recordLocked(partitionID, version, added)
	g.journal.mu.Unlock()
}

// ResetProgressJournal forgets a partition's recent completions, so that each
// follower is next sent the full copy. A node that starts leading calls it:
// what was recorded while it did not lead is not in its journal.
func (g *GroupManager) ResetProgressJournal(partitionID int32) {
	g.journal.reset(partitionID, g.progressVersion.Load())
}

// ExportProgressDelta returns a partition's consumer progress for a follower
// that holds it up to version since: every tracked group with its current
// floor, and the completions above that floor recorded after since. ok is
// false when the journal does not reach back to since; the follower then
// needs the full copy. version is the progress version the delta brings the
// follower up to.
//
// A delta carries about maxDeltaOffsets completions. When more were recorded
// after since, version is short of the current one, and the delta from it
// carries the next ones. Such a follower used to be given no delta at all and
// was sent the full copy instead. That copy is every completion above each
// floor, so sending it could take longer than the leader took to record
// another delta's worth: the follower then got one full copy after another
// and was never nearer than a copy's length to what its leader knew.
func (g *GroupManager) ExportProgressDelta(partitionID int32, since uint64) (groups []*types.ConsumerGroupProgress, version uint64, ok bool) {
	g.journal.mu.Lock()
	version = g.progressVersion.Load()
	added, upTo, ok := g.journal.sinceLocked(partitionID, since, maxDeltaOffsets)
	g.journal.mu.Unlock()
	if !ok {
		return nil, version, false
	}
	if upTo != 0 {
		version = upTo
	}

	g.mu.RLock()
	defer g.mu.RUnlock()
	for id, group := range g.groups {
		if _, tracked := group.CommittedOffsets[partitionID]; !tracked {
			continue
		}
		floor := g.floorLocked(id, partitionID)
		var offsets []int64
		for _, offset := range added[id] {
			if offset >= floor {
				offsets = append(offsets, offset)
			}
		}
		sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
		offsets = compactSorted(offsets)
		groups = append(groups, &types.ConsumerGroupProgress{GroupId: id, Topic: group.Topic, CommittedOffset: floor, CompletedOffsets: offsets})
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].GroupId < groups[j].GroupId })
	return groups, version, true
}

// compactSorted removes repeats from a sorted slice in place.
func compactSorted(offsets []int64) []int64 {
	if len(offsets) < 2 {
		return offsets
	}
	out := offsets[:1]
	for _, offset := range offsets[1:] {
		if offset != out[len(out)-1] {
			out = append(out, offset)
		}
	}
	return out
}

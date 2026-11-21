package consumer

import (
	"math/rand"
	"slices"
	"testing"
)

// modelJournal is the journal as it is easiest to get right: one slice,
// cut from its front whenever it is over its bound.
type modelJournal struct {
	entries []journalEntry
	offsets int
	horizon uint64
}

func (m *modelJournal) record(version uint64, group string, offsets []int64) {
	if len(offsets) == 0 {
		return
	}
	m.entries = append(m.entries, journalEntry{version: version, group: group, offsets: offsets})
	m.offsets += len(offsets)
	for m.offsets > maxJournalOffsets && len(m.entries) > 0 {
		m.offsets -= len(m.entries[0].offsets)
		m.horizon = max(m.horizon, m.entries[0].version)
		m.entries = m.entries[1:]
	}
}

func (m *modelJournal) since(version uint64) (map[string][]int64, int, bool) {
	if version < m.horizon {
		return nil, 0, false
	}
	added := make(map[string][]int64)
	total := 0
	for _, entry := range m.entries {
		if entry.version > version {
			added[entry.group] = append(added[entry.group], entry.offsets...)
			total += len(entry.offsets)
		}
	}
	return added, total, true
}

// The journal drops its oldest entries without moving the others each time.
// Through several times its bound in completions, it answers every question
// as the plain model does, and holds no more than about twice what it keeps.
func TestProgressJournalMatchesAModel(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	journals := &progressJournals{}
	model := &modelJournal{}
	groups := []string{"a", "b", "c"}

	var version uint64
	var versions []uint64
	next := int64(0)
	check := func(since uint64) {
		t.Helper()
		got, gotTotal, gotOK := journals.sinceLocked(0, since)
		want, wantTotal, wantOK := model.since(since)
		if gotOK != wantOK || gotTotal != wantTotal {
			t.Fatalf("since version %d (of %d): ok=%v total=%d, want ok=%v total=%d", since, version, gotOK, gotTotal, wantOK, wantTotal)
		}
		if !gotOK {
			return
		}
		if len(got) != len(want) {
			t.Fatalf("since version %d: %d groups, want %d", since, len(got), len(want))
		}
		for group, offsets := range want {
			if !slices.Equal(got[group], offsets) {
				t.Fatalf("since version %d: group %s has %d offsets, want %d", since, group, len(got[group]), len(offsets))
			}
		}
	}

	// Mostly acks of one or a few events, which is what fills a journal with
	// entries, and now and then a large one.
	for recorded := 0; recorded < 5*maxJournalOffsets; {
		size := 1 + rng.Intn(4)
		if rng.Intn(500) == 0 {
			size = 1 + rng.Intn(20000)
		}
		batch := make([]int64, size)
		for i := range batch {
			batch[i] = next
			next++
		}
		recorded += size
		group := groups[rng.Intn(len(groups))]

		version++
		journals.recordLocked(0, version, map[string][]int64{group: batch})
		if version == 1 {
			// A journal begins with the version it is first recorded under:
			// whatever came before is not in it.
			model.horizon = 0
		}
		model.record(version, group, batch)
		versions = append(versions, version)

		journal := journals.journals[0]
		if journal.offsets != model.offsets || journal.horizon != model.horizon {
			t.Fatalf("version %d: %d offsets from horizon %d, want %d from %d", version, journal.offsets, journal.horizon, model.offsets, model.horizon)
		}
		if limit := 2*len(model.entries) + 2*journalCompactAt; len(journal.entries) > limit {
			t.Fatalf("version %d: the journal holds %d entries for %d kept; dropped ones are not let go", version, len(journal.entries), len(model.entries))
		}
		if version%4099 == 0 {
			check(version)
			check(model.horizon)
			if model.horizon > 0 {
				check(model.horizon - 1)
			}
			check(versions[rng.Intn(len(versions))])
		}
	}
	check(0)
	check(model.horizon)
	check(version - 1)
}

// An ack recorded in a journal at its bound drops the oldest entry. It should
// cost the same whatever the journal holds.
func BenchmarkProgressJournalAtItsBound(b *testing.B) {
	journals := &progressJournals{}
	var version uint64
	offset := int64(0)
	record := func() {
		version++
		journals.recordLocked(0, version, map[string][]int64{"a": {offset}})
		offset++
	}
	for i := 0; i < maxJournalOffsets+1; i++ {
		record()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record()
	}
}

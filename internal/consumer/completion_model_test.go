package consumer

import (
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// completionModel is what a group's completion state means, kept the plain
// way: a floor, the log start, and the set of completed offsets above them.
type completionModel struct {
	floor, logStart int64
	done            map[int64]bool
}

func (m *completionModel) effective() int64 { return max(m.floor, m.logStart) }

func (m *completionModel) complete(offsets []int64) {
	for _, o := range offsets {
		if o >= m.effective() {
			m.done[o] = true
		}
	}
	for m.done[m.effective()] {
		delete(m.done, m.effective())
		m.floor = m.effective() + 1
	}
	m.trim()
}

func (m *completionModel) trim() {
	for o := range m.done {
		if o < m.effective() {
			delete(m.done, o)
		}
	}
}

func (m *completionModel) setLogStart(o int64) {
	if o <= m.logStart || o <= 0 {
		return
	}
	m.logStart = o
	if m.floor < o {
		m.floor = o
	}
	m.trim()
}

func (m *completionModel) forget(o int64) {
	o = max(o, 0)
	for d := range m.done {
		if d >= o {
			delete(m.done, d)
		}
	}
	if m.floor > o {
		m.floor = o
	}
}

func (m *completionModel) isCompleted(o int64) bool { return o < m.effective() || m.done[o] }

// replicated takes a leader's progress as a follower does: the floor rises to
// the leader's, as far as the local log reaches, and completions are added
// below the log's end; nothing advances the floor further.
func (m *completionModel) replicated(floor, logEnd int64, offsets []int64) {
	if f := min(floor, logEnd); f > m.floor {
		m.floor = f
	}
	m.trim()
	for _, o := range offsets {
		if o >= m.effective() && o < logEnd {
			m.done[o] = true
		}
	}
}

// The bitmap completion state answers every question as the plain model does,
// through acks in any order, raises of the log start, cuts of the log's end,
// and restarts, in memory and with a store. Offsets span several blocks, so
// floors cross block boundaries and blocks are dropped and trimmed.
func TestCompletionStateMatchesTheModel(t *testing.T) {
	for _, withStore := range []bool{false, true} {
		for seed := int64(1); seed <= seeds(); seed++ {
			name := "memory"
			if withStore {
				name = "store"
			}
			t.Run(name, func(t *testing.T) {
				checkAgainstModel(t, seed, withStore)
			})
		}
	}
}

func checkAgainstModel(t *testing.T, seed int64, withStore bool) {
	rng := rand.New(rand.NewSource(seed))
	const span = 5*completionBlockBits + 300
	dir := t.TempDir()
	open := func() *GroupManager {
		if withStore {
			return newStoreBackedManager(t, dir)
		}
		return NewGroupManager()
	}
	g := open()
	defer func() { _ = g.Close() }()
	if err := g.CreateGroup("g", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	m := &completionModel{done: make(map[int64]bool)}
	logStart := int64(0)
	var ops []string

	check := func(step int, what string) {
		t.Helper()
		if got, want := g.CompletionFloor("g", 0), m.effective(); got != want {
			t.Fatalf("seed %d step %d (%s): floor %d, model %d", seed, step, what, got, want)
		}
		for o := int64(0); o < span; o++ {
			if got, want := g.IsCompleted("g", 0, o), m.isCompleted(o); got != want {
				g.mu.RLock()
				inSet := g.sets[completionPrefix("g", 0)].has(o)
				g.mu.RUnlock()
				if len(ops) > 12 {
					ops = ops[len(ops)-12:]
				}
				t.Fatalf("seed %d step %d (%s): IsCompleted(%d) = %v, model %v (floor %d, cached highest %d, in set %v); last ops: %v",
					seed, step, what, o, got, want, m.effective(), g.highestCompleted("g", 0), inSet, ops)
			}
		}
	}

	for step := 0; step < 400; step++ {
		var what string
		switch r := rng.Intn(100); {
		case r < 70: // an ack of a few events, near the floor or anywhere
			n := 1 + rng.Intn(60)
			base := m.effective() + int64(rng.Intn(3*completionBlockBits))
			if rng.Intn(3) == 0 {
				base = int64(rng.Intn(span))
			}
			var offsets []int64
			for i := 0; i < n; i++ {
				o := base + int64(rng.Intn(200))
				if o < span {
					offsets = append(offsets, o)
				}
			}
			if len(offsets) == 0 {
				continue
			}
			events := make([]*types.Event, len(offsets))
			for i, o := range offsets {
				events[i] = &types.Event{Topic: "orders", PartitionId: 0, Offset: o}
			}
			if err := g.CommitDelivery("g", 0, events); err != nil {
				t.Fatal(err)
			}
			m.complete(offsets)
			what = "ack"
			ops = append(ops, fmt.Sprintf("ack %d..%d(n=%d)", slicesMin(offsets), slicesMax(offsets), len(offsets)))
		case r < 80: // the log's start moves up, as retention removes segments
			to := m.effective() + int64(rng.Intn(completionBlockBits+500))
			if to > logStart {
				logStart = to
			}
			if err := g.SetLogStart(0, to); err != nil {
				t.Fatal(err)
			}
			m.setLogStart(to)
			what = "log start"
			ops = append(ops, fmt.Sprintf("logstart %d", to))
		case r < 80+4: // a leader's progress, as a follower takes it
			floor := m.effective() + int64(rng.Intn(completionBlockBits))
			logEnd := m.effective() + int64(rng.Intn(3*completionBlockBits))
			var offsets []int64
			for i := 0; i < rng.Intn(80); i++ {
				offsets = append(offsets, m.effective()+int64(rng.Intn(3*completionBlockBits)))
			}
			err := g.ApplyReplicatedProgress(0, logEnd, []*types.ConsumerGroupProgress{{GroupId: "g", Topic: "orders", CommittedOffset: floor, CompletedOffsets: offsets}})
			if err != nil {
				t.Fatal(err)
			}
			m.replicated(floor, logEnd, offsets)
			what = "replicated"
			ops = append(ops, fmt.Sprintf("replicated floor %d end %d n=%d", floor, logEnd, len(offsets)))
		case r < 90: // the log's end is cut
			at := int64(rng.Intn(span))
			if _, err := g.ForgetCompletionsFrom(0, at); err != nil {
				t.Fatal(err)
			}
			m.forget(at)
			what = "cut"
			ops = append(ops, fmt.Sprintf("cut %d", at))
		case r < 93 && withStore: // restart
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			g = open()
			if logStart > 0 {
				if err := g.SetLogStart(0, logStart); err != nil {
					t.Fatal(err)
				}
			}
			what = "restart"
			ops = append(ops, "restart")
		default:
			continue
		}
		check(step, what)
	}
}

func slicesMin(s []int64) int64 { return slices.Min(s) }
func slicesMax(s []int64) int64 { return slices.Max(s) }

// seeds is how many random histories TestCompletionStateMatchesTheModel
// checks: CRONOS_MODEL_SEEDS, or 8.
func seeds() int64 {
	if n, err := strconv.ParseInt(os.Getenv("CRONOS_MODEL_SEEDS"), 10, 64); err == nil && n > 0 {
		return n
	}
	return 8
}

// A block past the one the floor is in keeps every bit: none of its offsets
// is below the floor. Up to the fix, the bits below the floor's place in its
// own block were cleared in every later block a write touched.
func TestClearBelowLeavesLaterBlocks(t *testing.T) {
	var b completionBlock
	for _, o := range []int64{2*completionBlockBits + 1, 2*completionBlockBits + 100, 2*completionBlockBits + 4000} {
		b.set(o)
	}
	before := b
	b.clearBelow(completionBlockBits+500, 2) // the floor in block 1
	if b != before {
		t.Fatal("bits of block 2 were cleared by a floor in block 1")
	}
	b.clearBelow(2*completionBlockBits+100, 2) // the floor in block 2 itself
	if b.has(2*completionBlockBits+1) || !b.has(2*completionBlockBits+100) || !b.has(2*completionBlockBits+4000) {
		t.Fatal("a floor in the block cleared the wrong bits")
	}
	b.clearBelow(3*completionBlockBits, 2) // the floor past block 2
	if !b.empty() {
		t.Fatal("a floor past the block left bits")
	}
}

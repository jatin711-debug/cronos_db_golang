package consumer

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"sort"
	"strconv"

	"github.com/cockroachdb/pebble"
)

// Completions above a group's floor are held as a bit per offset, in blocks
// of completionBlockBits consecutive offsets, and each block is stored as one
// record. Up to v0.13.1 every completion was a record of its own, and a floor
// held back by an event scheduled hours ahead kept one record per event
// delivered since: hundreds of thousands per group, each written, looked up
// in the store by every completion check, and deleted again when the floor
// passed it. Catching up a group far behind spent most of a node's CPU on
// those records and the compactions they caused (see docs/RELEASE_V0.13.1.md).
//
// A block costs one bit per offset, held in memory and stored, and an ack
// rewrites the block it falls in. Bits below the floor are redundant: they
// are cleared when the floor passes them, and a block left empty is removed,
// so a group whose floor keeps up with its acks stores no blocks at all.
// Every completion check is answered from memory.
//
// A block is stored under the group's completion prefix, like the records of
// earlier releases, with the block's last offset as 20 digits and a "b"
// after them. An earlier release that runs on the store after a rollback
// does not read the blocks, and delivers again what they held, but its range
// deletions remove them as they removed its own records: when its log is cut
// at an offset it deletes every block that holds that offset or a later one,
// and when its floor rises, every block wholly below the floor. A block that
// an earlier release left behind is therefore never about entries that have
// since been replaced.

const (
	// completionBlockBits is how many consecutive offsets a block covers.
	completionBlockBits  = 4096
	completionBlockWords = completionBlockBits / 64

	// completionKeyPrefix starts every completion prefix, and with it every
	// stored block and every per-offset record of earlier releases.
	completionKeyPrefix = "done:"
	// blockKeySuffix ends a block's key and tells it from a per-offset record
	// (up to v0.13.1), whose key ends with the offset's digits.
	blockKeySuffix = 'b'
)

// completionBlock holds the completions of completionBlockBits consecutive
// offsets, starting at a multiple of completionBlockBits.
type completionBlock [completionBlockWords]uint64

func blockOf(offset int64) int64 { return offset / completionBlockBits }

func (b *completionBlock) has(offset int64) bool {
	bit := offset % completionBlockBits
	return b[bit/64]&(1<<uint(bit%64)) != 0
}

func (b *completionBlock) set(offset int64) {
	bit := offset % completionBlockBits
	b[bit/64] |= 1 << uint(bit%64)
}

// clearBelow clears the bits of offsets below offset, which lies in this
// block or past it.
func (b *completionBlock) clearBelow(offset int64, block int64) {
	if blockOf(offset) > block {
		*b = completionBlock{}
		return
	}
	bit := int(offset % completionBlockBits)
	for w := 0; w < bit/64; w++ {
		b[w] = 0
	}
	b[bit/64] &^= (1 << uint(bit%64)) - 1
}

// clearFrom clears the bits of offsets at or above offset, which lies in
// this block.
func (b *completionBlock) clearFrom(offset int64) {
	bit := int(offset % completionBlockBits)
	b[bit/64] &= (1 << uint(bit%64)) - 1
	for w := bit/64 + 1; w < completionBlockWords; w++ {
		b[w] = 0
	}
}

func (b *completionBlock) empty() bool {
	for _, w := range b {
		if w != 0 {
			return false
		}
	}
	return true
}

// highest returns the highest offset set in the block, or -1.
func (b *completionBlock) highest(block int64) int64 {
	for w := completionBlockWords - 1; w >= 0; w-- {
		if b[w] != 0 {
			return block*completionBlockBits + int64(w*64+63-bits.LeadingZeros64(b[w]))
		}
	}
	return -1
}

// encode stores the block's words little-endian, without the trailing words
// that are zero: the block an ack has just reached is mostly empty.
func (b *completionBlock) encode() []byte {
	n := completionBlockWords
	for n > 0 && b[n-1] == 0 {
		n--
	}
	out := make([]byte, n*8)
	for w := 0; w < n; w++ {
		binary.LittleEndian.PutUint64(out[w*8:], b[w])
	}
	return out
}

func decodeBlock(value []byte) (*completionBlock, error) {
	if len(value)%8 != 0 || len(value) > completionBlockWords*8 {
		return nil, fmt.Errorf("completion block of %d bytes", len(value))
	}
	b := new(completionBlock)
	for w := 0; w < len(value)/8; w++ {
		b[w] = binary.LittleEndian.Uint64(value[w*8:])
	}
	return b, nil
}

// completionSet holds a group's completions above its floor on one
// partition. It is changed only by a writer that holds the manager's
// commitMu and mu, and read under mu.
type completionSet struct {
	blocks map[int64]*completionBlock
}

func (s *completionSet) has(offset int64) bool {
	if s == nil || offset < 0 {
		return false
	}
	b := s.blocks[blockOf(offset)]
	return b != nil && b.has(offset)
}

// highest returns the highest offset in the set, or -1.
func (s *completionSet) highest() int64 {
	if s == nil {
		return -1
	}
	top := int64(-1)
	for block, b := range s.blocks {
		if block*completionBlockBits+completionBlockBits-1 <= top {
			continue
		}
		top = max(top, b.highest(block))
	}
	return top
}

// ascend lists the offsets in the set at or above from, in ascending order,
// at most limit of them (limit <= 0: all).
func (s *completionSet) ascend(from int64, limit int) []int64 {
	if s == nil {
		return nil
	}
	from = max(from, 0)
	numbers := make([]int64, 0, len(s.blocks))
	for block := range s.blocks {
		if block >= blockOf(from) {
			numbers = append(numbers, block)
		}
	}
	sort.Slice(numbers, func(i, j int) bool { return numbers[i] < numbers[j] })
	var offsets []int64
	for _, block := range numbers {
		b := s.blocks[block]
		for w, word := range b {
			for word != 0 {
				offset := block*completionBlockBits + int64(w*64+bits.TrailingZeros64(word))
				word &= word - 1
				if offset < from {
					continue
				}
				offsets = append(offsets, offset)
				if limit > 0 && len(offsets) == limit {
					return offsets
				}
			}
		}
	}
	return offsets
}

// appendBlockKey appends the stored key of a block: the completion prefix,
// the block's last offset as 20 digits, and blockKeySuffix. A group's blocks
// sort by number, between prefix and prefix+":".
func appendBlockKey(dst []byte, prefix string, block int64) []byte {
	dst = append(dst, prefix...)
	var scratch [20]byte
	digits := strconv.AppendInt(scratch[:0], block*completionBlockBits+completionBlockBits-1, 10)
	for i := len(digits); i < len(scratch); i++ {
		dst = append(dst, '0')
	}
	dst = append(dst, digits...)
	return append(dst, blockKeySuffix)
}

// parseBlockKey returns the completion prefix and block number of a stored
// block key; ok is false for any other key, such as a per-offset record of
// an earlier release.
func parseBlockKey(key string) (prefix string, block int64, ok bool) {
	if len(key) < len(completionKeyPrefix)+21 || key[len(key)-1] != blockKeySuffix {
		return "", 0, false
	}
	last, err := strconv.ParseInt(key[len(key)-21:len(key)-1], 10, 64)
	if err != nil || last < 0 || last%completionBlockBits != completionBlockBits-1 {
		return "", 0, false
	}
	return key[:len(key)-21], last / completionBlockBits, true
}

// completionPlan is one progress write to a group's completion set on a
// partition: the floor it leaves and the blocks it changes. Plans are made
// under commitMu, stored in one batch, and then installed in memory.
type completionPlan struct {
	group  string
	prefix string
	// before is the floor the plan starts from, floor the one it leaves.
	before, floor int64
	// put holds the new version of every block the plan changes and keeps;
	// drop lists the stored blocks it removes.
	put  map[int64]*completionBlock
	drop []int64
}

// raised reports whether the plan moves the floor up.
func (p *completionPlan) raised() bool { return p.floor > p.before }

// planLocked makes the plan that adds offsets to a group's set, raises its
// floor to at least raiseTo, and, with advance, moves the floor on over every
// completed offset that follows it. Offsets below the resulting floor are not
// added, nor are those at or above limit (limit < 0: none is too high). The
// caller holds commitMu and at least a read lock on mu.
func (g *GroupManager) planLocked(group string, partition int32, add []int64, raiseTo, limit int64, advance bool) *completionPlan {
	prefix := completionPrefix(group, partition)
	before := g.floorLocked(group, partition)
	set := g.sets[prefix]
	p := &completionPlan{group: group, prefix: prefix, before: before, floor: max(before, raiseTo), put: make(map[int64]*completionBlock)}
	for _, offset := range add {
		if offset < p.floor || (limit >= 0 && offset >= limit) {
			continue
		}
		block := blockOf(offset)
		b := p.put[block]
		if b == nil {
			b = new(completionBlock)
			if held := set.blocksAt(block); held != nil {
				*b = *held
			}
			p.put[block] = b
		}
		b.set(offset)
	}
	if advance {
		p.advance(set)
	}
	// Bits below the floor are redundant: clear them, and remove the blocks
	// that are left empty or lie wholly below it.
	staged := make(map[int64]bool, len(p.put))
	for block, b := range p.put {
		staged[block] = true
		b.clearBelow(p.floor, block)
		held := set.blocksAt(block)
		switch {
		case b.empty():
			delete(p.put, block)
			if held != nil {
				p.drop = append(p.drop, block)
			}
		case held != nil && *held == *b:
			// Nothing new: a follower is sent completions it holds again
			// and again.
			delete(p.put, block)
		}
	}
	if set != nil && p.floor > before {
		// Only blocks from the old floor's to the new floor's change.
		trim := func(block int64, held *completionBlock) {
			if staged[block] || block*completionBlockBits >= p.floor {
				return
			}
			trimmed := *held
			trimmed.clearBelow(p.floor, block)
			switch {
			case trimmed.empty():
				p.drop = append(p.drop, block)
			case trimmed != *held:
				p.put[block] = &trimmed
			}
		}
		lo, hi := blockOf(max(before, 0)), blockOf(p.floor)
		if hi-lo+1 <= int64(len(set.blocks)) {
			for block := lo; block <= hi; block++ {
				if held := set.blocks[block]; held != nil {
					trim(block, held)
				}
			}
		} else {
			for block, held := range set.blocks {
				trim(block, held)
			}
		}
	}
	return p
}

// advance moves the plan's floor on over the completed offsets that follow
// it, a word of the block at a time.
func (p *completionPlan) advance(set *completionSet) {
	for p.floor >= 0 {
		block := blockOf(p.floor)
		b := p.put[block]
		if b == nil {
			b = set.blocksAt(block)
		}
		if b == nil {
			return
		}
		bit := p.floor % completionBlockBits
		// The bits from the floor on, shifted down; what is shifted in is 0.
		run := bits.TrailingZeros64(^(b[bit/64] >> uint(bit%64)))
		if run == 0 {
			return
		}
		p.floor += int64(run)
	}
}

// has reports whether offset is complete once the plan is applied to set.
func (p *completionPlan) has(set *completionSet, offset int64) bool {
	if b, ok := p.put[blockOf(offset)]; ok {
		return b.has(offset)
	}
	return set.has(offset)
}

func (s *completionSet) blocksAt(block int64) *completionBlock {
	if s == nil {
		return nil
	}
	return s.blocks[block]
}

// writePlan adds a plan's changes to a batch: its blocks and, when it moves
// the floor, the floor.
func writePlan(batch *pebble.Batch, partition int32, p *completionPlan) error {
	var key []byte
	for block, b := range p.put {
		key = appendBlockKey(key[:0], p.prefix, block)
		if err := batch.Set(key, b.encode(), nil); err != nil {
			return err
		}
	}
	for _, block := range p.drop {
		key = appendBlockKey(key[:0], p.prefix, block)
		if err := batch.Delete(key, nil); err != nil {
			return err
		}
	}
	if p.floor != p.before {
		return setFloorInBatch(batch, p.group, partition, p.floor)
	}
	return nil
}

// installPlanLocked applies a stored plan to memory: the blocks and the
// floor. The caller holds commitMu and mu.
func (g *GroupManager) installPlanLocked(partition int32, p *completionPlan) {
	set := g.sets[p.prefix]
	if set == nil && len(p.put) > 0 {
		set = &completionSet{blocks: make(map[int64]*completionBlock)}
		if g.sets == nil {
			g.sets = make(map[string]*completionSet)
		}
		g.sets[p.prefix] = set
	}
	if set != nil {
		for block, b := range p.put {
			set.blocks[block] = b
		}
		for _, block := range p.drop {
			delete(set.blocks, block)
		}
		if len(set.blocks) == 0 {
			delete(g.sets, p.prefix)
		}
	}
	if p.floor != p.before {
		g.setFloorLocked(p.group, partition, p.floor)
	}
}

// loadCompletionSets reads every stored block, and converts the per-offset
// records of earlier releases into blocks. Bits below a group's floor are
// left out. A block that does not decode is dropped: the offsets it held are
// delivered again, which at-least-once delivery allows.
func (s *OffsetStore) loadCompletionSets(floors map[string]int64) (map[string]*completionSet, error) {
	sets := make(map[string]*completionSet)
	block := func(prefix string, number int64) *completionBlock {
		set := sets[prefix]
		if set == nil {
			set = &completionSet{blocks: make(map[int64]*completionBlock)}
			sets[prefix] = set
		}
		b := set.blocks[number]
		if b == nil {
			b = new(completionBlock)
			set.blocks[number] = b
		}
		return b
	}
	err := s.withDB(func(db *pebble.DB) error {
		iter, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte(completionKeyPrefix), UpperBound: []byte("done;")})
		if err != nil {
			return err
		}
		var legacy [][]byte                          // per-offset records, removed once converted
		converted := make(map[string]map[int64]bool) // blocks that took some of them
		for iter.First(); iter.Valid(); iter.Next() {
			key := string(iter.Key())
			if prefix, number, ok := parseBlockKey(key); ok {
				value, err := iter.ValueAndErr()
				if err != nil {
					continue
				}
				stored, err := decodeBlock(value)
				if err != nil {
					continue
				}
				b := block(prefix, number)
				for w := range b {
					b[w] |= stored[w]
				}
				continue
			}
			// A record of an earlier release: the prefix, then the offset
			// as 20 digits.
			legacy = append(legacy, append([]byte(nil), iter.Key()...))
			if len(key) < len(completionKeyPrefix)+21 {
				continue
			}
			prefix := key[:len(key)-20]
			offset, err := strconv.ParseInt(key[len(key)-20:], 10, 64)
			if err != nil || offset < 0 || offset < floors[prefix] {
				continue
			}
			block(prefix, blockOf(offset)).set(offset)
			if converted[prefix] == nil {
				converted[prefix] = make(map[int64]bool)
			}
			converted[prefix][blockOf(offset)] = true
		}
		if err := iter.Close(); err != nil {
			return err
		}
		// Bits below the floor are redundant.
		for prefix, set := range sets {
			for number, b := range set.blocks {
				b.clearBelow(floors[prefix], number)
				if b.empty() {
					delete(set.blocks, number)
				}
			}
			if len(set.blocks) == 0 {
				delete(sets, prefix)
			}
		}
		if len(legacy) == 0 {
			return nil
		}
		batch := db.NewBatch()
		defer batch.Close()
		var key []byte
		for prefix, numbers := range converted {
			for number := range numbers {
				if b := sets[prefix].blocksAt(number); b != nil {
					key = appendBlockKey(key[:0], prefix, number)
					if err := batch.Set(key, b.encode(), nil); err != nil {
						return err
					}
				}
			}
		}
		for _, old := range legacy {
			if err := batch.Delete(old, nil); err != nil {
				return err
			}
		}
		return batch.Commit(pebble.Sync)
	})
	if err != nil {
		return nil, fmt.Errorf("load consumer completions: %w", err)
	}
	return sets, nil
}

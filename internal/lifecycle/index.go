package lifecycle

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

type Log interface {
	GetFirstOffset() int64
	GetLastOffset() int64
	ReadEventsWithin(int64, int64, int64) ([]*types.Event, error)
}

// Index stores payload-free WAL history. The head includes tentative records;
// LatestThrough and TerminalThrough use only records at an accepted boundary.
// Compound reads and rebuild/cleanup are atomic to each other. The owner must
// still serialize WAL changes with Apply/Rebuild and choose the correct boundary.
type Index struct {
	mu          sync.RWMutex
	db          *pebble.DB
	start       int64
	unavailable error
}

// ErrIncompleteIndex means no lookup is safe until a complete WAL rebuild.
var ErrIncompleteIndex = errors.New("lifecycle index requires WAL reconstruction")

// Open never trusts the persisted cache. Rebuild must succeed before use.
func Open(dir string, cache *pebble.Cache) (*Index, error) {
	opts := &pebble.Options{Cache: cache, MemTableSize: 4 << 20, DisableWAL: true}
	db, err := pebble.Open(filepath.Join(dir, "event_lifecycle"), opts)
	if err != nil {
		return nil, err
	}
	return &Index{db: db, unavailable: ErrIncompleteIndex}, nil
}

func (i *Index) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.db.Close()
}

func rootKey(r *types.EventReference) []byte {
	b, _ := json.Marshal(r)
	return append([]byte{'h'}, b...)
}

func offsetKey(offset int64) []byte {
	b := make([]byte, 9)
	b[0] = 'r'
	binary.BigEndian.PutUint64(b[1:], uint64(offset))
	return b
}

func encodeOffset(offset int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(offset))
	return b
}

func decodeOffset(b []byte) (int64, error) {
	if len(b) != 8 || b[0]&0x80 != 0 {
		return 0, fmt.Errorf("invalid lifecycle index offset")
	}
	return int64(binary.BigEndian.Uint64(b)), nil
}

// Length-prefix the serialized reference so one root cannot share another's
// history prefix, even when message IDs contain quotes or embedded nulls.
func historyPrefix(head []byte) []byte {
	prefix := make([]byte, 5+len(head)-1)
	prefix[0] = 'v'
	binary.BigEndian.PutUint32(prefix[1:5], uint32(len(head)-1))
	copy(prefix[5:], head[1:])
	return prefix
}

func historyKey(head []byte, offset int64) []byte {
	return append(historyPrefix(head), encodeOffset(offset)...)
}

func prefixEnd(prefix []byte) []byte {
	end := bytes.Clone(prefix)
	for n := len(end) - 1; n >= 0; n-- {
		if end[n] != 0xff {
			end[n]++
			return end[:n+1]
		}
	}
	return nil
}

func (i *Index) readOffset(key []byte) (int64, bool, error) {
	v, c, err := i.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	defer c.Close()
	offset, err := decodeOffset(v)
	return offset, err == nil, err
}

func (i *Index) Latest(r *types.EventReference) (int64, bool, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if i.unavailable != nil {
		return 0, false, i.unavailable
	}
	offset, found, err := i.readOffset(rootKey(r))
	return offset, found && offset >= i.start, err
}

// LatestThrough finds the latest retained mutation at or below through. No
// mutation means the original record is the accepted state, if still retained.
// One bounded iterator seek suffices; payloads and older history are not scanned.
func (i *Index) LatestThrough(r *types.EventReference, through int64) (int64, bool, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if i.unavailable != nil {
		return 0, false, i.unavailable
	}
	return i.latestThrough(rootKey(r), through)
}

func (i *Index) latestThrough(head []byte, through int64) (int64, bool, error) {
	if through < i.start {
		return 0, false, nil
	}
	prefix := historyPrefix(head)
	upper := prefixEnd(prefix)
	iter, err := i.db.NewIter(&pebble.IterOptions{LowerBound: historyKey(head, i.start), UpperBound: upper})
	if err != nil {
		return 0, false, err
	}
	defer iter.Close()
	seek := upper
	if through < math.MaxInt64 {
		seek = historyKey(head, through+1)
	}
	if !iter.SeekLT(seek) {
		return 0, false, iter.Error()
	}
	key := iter.Key()
	if len(key) != len(prefix)+8 || !bytes.HasPrefix(key, prefix) {
		return 0, false, fmt.Errorf("invalid lifecycle history key")
	}
	offset, err := decodeOffset(key[len(prefix):])
	if err != nil {
		return 0, false, err
	}
	if _, err = decodeOffset(iter.Value()); err != nil {
		return 0, false, err
	}
	return offset, true, nil
}

func (i *Index) Apply(e *types.Event) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.unavailable != nil {
		return i.unavailable
	}
	r, err := Decode(e)
	if err == nil && r != nil {
		err = i.apply(e, r)
	}
	if err != nil {
		i.unavailable = fmt.Errorf("%w: %w", ErrIncompleteIndex, err)
	}
	return err
}

func (i *Index) apply(e *types.Event, r *Record) error {
	if e.Offset < i.start {
		return nil
	} // a late observation cannot resurrect a pruned copy
	head := rootKey(r.Root)
	version := historyKey(head, e.Offset)
	if term, exists, err := i.readOffset(version); err != nil {
		return err
	} else if exists && term != e.Term {
		return fmt.Errorf("lifecycle offset %d changed term without rebuilding the index", e.Offset)
	}
	if previous, c, err := i.db.Get(offsetKey(e.Offset)); err == nil {
		same := bytes.Equal(previous, head)
		c.Close()
		if !same {
			return fmt.Errorf("lifecycle offset %d changed identity without rebuilding the index", e.Offset)
		}
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	latest, found, err := i.readOffset(head)
	if err != nil {
		return err
	}
	b := i.db.NewBatch()
	defer b.Close()
	if !found || e.Offset > latest {
		if err = b.Set(head, encodeOffset(e.Offset), nil); err != nil {
			return err
		}
	}
	if err = b.Set(version, encodeOffset(e.Term), nil); err != nil {
		return err
	}
	if err = b.Set(offsetKey(e.Offset), head, nil); err != nil {
		return err
	}
	return b.Commit(pebble.NoSync)
}

// Active identifies the latest local copy for timer reconstruction. It does not
// prove quorum acceptance. DeliverableThrough adds that separate requirement.
func (i *Index) Active(e *types.Event) (bool, error) {
	return i.DeliverableThrough(e, math.MaxInt64)
}

func (i *Index) sameMutation(e *types.Event, r *Record) (bool, error) {
	term, found, err := i.readOffset(historyKey(rootKey(r.Root), e.Offset))
	if err != nil {
		return false, err
	}
	if !found {
		return false, ErrIncompleteIndex
	}
	return term == e.Term, nil
}

// DeliverableThrough freezes an original while a staged successor is unresolved.
// Neither that original nor its unaccepted successor may be delivered. Cached
// records below the retained prefix stay inactive after their head is collected.
func (i *Index) DeliverableThrough(e *types.Event, through int64) (bool, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if i.unavailable != nil {
		return false, i.unavailable
	}
	if e == nil || e.Offset < i.start || e.Offset > through {
		return false, nil
	}
	r, err := Decode(e)
	if err != nil {
		return false, err
	}
	if r != nil && r.Kind == Cancel {
		return false, nil
	}
	root := Reference(e)
	if r != nil {
		root = r.Root
		if same, err := i.sameMutation(e, r); err != nil || !same {
			if errors.Is(err, ErrIncompleteIndex) {
				// Workers can retain a copy removed by a suffix cut. The
				// authoritative retention path still requires indexed history.
				return false, nil
			}
			return false, err // a worker may still hold a record from a cut suffix
		}
	}
	latest, found, err := i.readOffset(rootKey(root))
	if err == nil && r != nil && !found {
		return false, ErrIncompleteIndex
	}
	return err == nil && (!found || e.Offset == latest), err
}

// TerminalThrough is a retention proof, unlike !Active: a tentative successor
// suppresses delivery but does not make its predecessor removable. An accepted
// cancel record itself is terminal; an active replacement still needs consumers.
func (i *Index) TerminalThrough(e *types.Event, through int64) (bool, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if i.unavailable != nil {
		return false, i.unavailable
	}
	if e == nil || e.Offset < i.start || e.Offset > through {
		return false, nil
	}
	r, err := Decode(e)
	if err != nil {
		return false, err
	}
	root := Reference(e)
	if r != nil {
		root = r.Root
		if same, err := i.sameMutation(e, r); err != nil || !same {
			return false, err
		}
		if r.Kind == Cancel {
			return true, nil
		}
	}
	latest, found, err := i.latestThrough(rootKey(root), through)
	return err == nil && found && latest > e.Offset, err
}

// Rebuild never trusts an earlier cache generation after crash, restore, or
// suffix replacement. Readers see one complete generation, not a partial scan.
func (i *Index) Rebuild(ctx context.Context, wal Log) (rebuildErr error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	defer func() {
		if rebuildErr != nil {
			i.unavailable = fmt.Errorf("%w: %w", ErrIncompleteIndex, rebuildErr)
		} else {
			i.unavailable = nil
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := i.db.DeleteRange([]byte{'h'}, []byte{'w'}, pebble.NoSync); err != nil {
		return err
	}
	i.start = wal.GetFirstOffset()
	end := wal.GetLastOffset()
	for from := i.start; from <= end; {
		if err := ctx.Err(); err != nil {
			return err
		}
		to := min(from+1023, end)
		events, err := wal.ReadEventsWithin(from, to, 4<<20)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			from = to + 1
			continue
		}
		from = events[len(events)-1].Offset + 1
		for _, e := range events {
			r, err := Decode(e)
			if err != nil {
				return err
			}
			if r != nil {
				if err = i.apply(e, r); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// DropBefore forgets history only after the WAL prefix was removed. Its floor
// also rejects old replay/worker references after the last head is collected.
// Each batch is bounded by 128 payload-free records.
func (i *Index) DropBefore(start int64) (cleanupErr error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.unavailable != nil {
		return i.unavailable
	}
	if start < i.start {
		return nil
	}
	defer func() {
		if cleanupErr != nil {
			i.unavailable = fmt.Errorf("%w: %w", ErrIncompleteIndex, cleanupErr)
		}
	}()
	iter, err := i.db.NewIter(&pebble.IterOptions{LowerBound: offsetKey(i.start), UpperBound: offsetKey(start)})
	if err != nil {
		return err
	}
	defer iter.Close()
	b := i.db.NewBatch()
	defer func() { b.Close() }()
	pending := 0
	for iter.First(); iter.Valid(); iter.Next() {
		key, head := iter.Key(), iter.Value()
		if len(key) != 9 || len(head) < 2 || head[0] != 'h' {
			return fmt.Errorf("invalid lifecycle cleanup key")
		}
		offset, err := decodeOffset(key[1:])
		if err != nil {
			return err
		}
		current, found, err := i.readOffset(head)
		if err != nil {
			return err
		}
		if found && current < start {
			if err = b.Delete(head, nil); err != nil {
				return err
			}
		}
		if err = b.Delete(historyKey(head, offset), nil); err != nil {
			return err
		}
		if err = b.Delete(key, nil); err != nil {
			return err
		}
		pending++
		if pending == 128 {
			if err = b.Commit(pebble.NoSync); err != nil {
				return err
			}
			b.Close()
			b = i.db.NewBatch()
			pending = 0
		}
	}
	if err = iter.Error(); err != nil {
		return err
	}
	if err = b.Commit(pebble.NoSync); err != nil {
		return err
	}
	i.start = start
	return nil
}

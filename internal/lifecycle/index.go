package lifecycle

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

type Log interface {
	GetFirstOffset() int64
	GetLastOffset() int64
	ReadEventsWithin(int64, int64, int64) ([]*types.Event, error)
}

type Index struct{ db *pebble.DB }

func Open(dir string, cache *pebble.Cache) (*Index, error) {
	opts := &pebble.Options{Cache: cache, MemTableSize: 4 << 20, DisableWAL: true}
	db, err := pebble.Open(filepath.Join(dir, "event_lifecycle"), opts)
	if err != nil {
		return nil, err
	}
	return &Index{db: db}, nil
}
func (i *Index) Close() error { return i.db.Close() }
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

func (i *Index) Latest(r *types.EventReference) (int64, bool, error) {
	v, c, err := i.db.Get(rootKey(r))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	defer c.Close()
	if len(v) != 8 {
		return 0, false, fmt.Errorf("invalid lifecycle index offset")
	}
	return int64(binary.BigEndian.Uint64(v)), true, nil
}

func (i *Index) Apply(e *types.Event) error {
	r, err := Decode(e)
	if err != nil || r == nil {
		return err
	}
	k := rootKey(r.Root)
	b := i.db.NewBatch()
	defer b.Close()
	if err = b.Set(k, encodeOffset(e.Offset), nil); err != nil {
		return err
	}
	if err = b.Set(offsetKey(e.Offset), k, nil); err != nil {
		return err
	}
	return b.Commit(pebble.NoSync)
}

func (i *Index) Active(e *types.Event) (bool, error) {
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
	}
	offset, found, err := i.Latest(root)
	return err == nil && (!found || offset == e.Offset), err
}

// Rebuild never trusts a previous index generation, including after a crash,
// restore, or WAL suffix replacement. It holds one bounded WAL chunk at a time.
func (i *Index) Rebuild(ctx context.Context, wal Log) error {
	if err := i.db.DeleteRange([]byte{'h'}, []byte{'s'}, pebble.NoSync); err != nil {
		return err
	}
	end := wal.GetLastOffset()
	for from := wal.GetFirstOffset(); from <= end; {
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
			if err := i.Apply(e); err != nil {
				return err
			}
		}
	}
	return nil
}

// DropBefore forgets derived keys only after the prefix was removed. The
// iterator and each batch have a constant payload-free bound.
func (i *Index) DropBefore(start int64) error {
	it, err := i.db.NewIter(&pebble.IterOptions{LowerBound: offsetKey(0), UpperBound: offsetKey(start)})
	if err != nil {
		return err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		key := it.Value()
		value, c, err := i.db.Get(key)
		if err == nil {
			if len(value) != 8 {
				c.Close()
				return fmt.Errorf("invalid lifecycle index offset")
			}
			current := int64(binary.BigEndian.Uint64(value))
			c.Close()
			if current < start {
				if err = i.db.Delete(key, pebble.NoSync); err != nil {
					return err
				}
			}
		} else if !errors.Is(err, pebble.ErrNotFound) {
			return err
		}
		if err = i.db.Delete(it.Key(), pebble.NoSync); err != nil {
			return err
		}
	}
	return it.Error()
}

package delivery

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// Version changes only after a journal record is durable. It is local to this
// queue instance; leaders reset follower cursors when they start or reconnect.
func (d *DeadLetterQueue) Version() uint64 { d.mu.RLock(); defer d.mu.RUnlock(); return d.version }

// ExportPage walks the durable journal, including entries evicted from the UI
// cache and removal tombstones. One oversized record may exceed the page budget.
func (d *DeadLetterQueue) ExportPage(cursor types.DeadLetterCursor) ([]*types.DeadLetterRecord, types.DeadLetterCursor, bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	records, next, done, err := d.writer.readPage(cursor)
	if err != nil {
		return nil, cursor, false, err
	}
	out := make([]*types.DeadLetterRecord, 0, len(records))
	for _, raw := range records {
		data, err := d.unseal(raw)
		if err != nil {
			return nil, cursor, false, err
		}
		var e DLQEntry
		if err := json.Unmarshal(data, &e); err != nil {
			return nil, cursor, false, err
		}
		out = append(out, &types.DeadLetterRecord{Event: e.Event, DeliveryId: e.DeliveryID, Attempts: e.Attempts, LastError: e.LastError, FailedAt: e.FailedAt, Subscriber: e.Subscriber, PartitionId: e.PartitionID, Tombstone: e.Tombstone})
	}
	return out, next, done, nil
}

// ApplyReplica fsyncs before acknowledging, and is idempotent across retries
// and restarts. Each node encrypts its own copy with its own configured key.
func (d *DeadLetterQueue) ApplyReplica(partition int32, records []*types.DeadLetterRecord) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, r := range records {
		if r == nil || r.DeliveryId == "" || (!r.Tombstone && (r.Event == nil || r.PartitionId != partition || r.Event.PartitionId != partition)) {
			return fmt.Errorf("invalid dead-letter record")
		}
		e := &DLQEntry{Event: r.Event, DeliveryID: r.DeliveryId, Attempts: r.Attempts, LastError: r.LastError, FailedAt: r.FailedAt, Subscriber: r.Subscriber, PartitionID: r.PartitionId, Tombstone: r.Tombstone}
		if err := d.appendEntry(e); err != nil {
			return err
		}
	}
	return nil
}

// appendEntry is called with mu held. Visibility always follows durable write.
func (d *DeadLetterQueue) appendEntry(e *DLQEntry) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	key := sha256.Sum256(data)
	if _, ok := d.seen[key]; ok {
		return nil
	}
	sealed, err := d.seal(data)
	if err != nil {
		return err
	}
	if err := d.writer.WriteEntry(sealed); err != nil {
		return err
	}
	d.remember(e, key)
	return nil
}

func (d *DeadLetterQueue) remember(e *DLQEntry, key [32]byte) {
	if d.seen == nil {
		d.seen = make(map[[32]byte]struct{})
		d.removed = make(map[string]bool)
	}
	if _, ok := d.seen[key]; ok {
		return
	}
	d.seen[key] = struct{}{}
	d.version++
	if e.Tombstone {
		key := e.DeliveryID
		if e.Event != nil {
			key = deadLetterIdentity(e)
		}
		d.removed[key] = true
		kept := d.entries[:0]
		for _, old := range d.entries {
			if old.DeliveryID != e.DeliveryID || (e.Event != nil && deadLetterIdentity(old) != deadLetterIdentity(e)) {
				kept = append(kept, old)
			}
		}
		clear(d.entries[len(kept):])
		d.entries = kept
	} else if !d.removed[e.DeliveryID] && !d.removed[deadLetterIdentity(e)] {
		if len(d.entries) >= d.maxSize {
			d.entries[0] = nil
			d.entries = d.entries[1:]
		}
		d.entries = append(d.entries, e)
	}
}

func (sw *DLQSegmentWriter) readPage(cursor types.DeadLetterCursor) ([][]byte, types.DeadLetterCursor, bool, error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	files, err := os.ReadDir(sw.dataDir)
	if err != nil {
		return nil, cursor, false, err
	}
	var records [][]byte
	size := 0
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".dlq") {
			continue
		}
		seq, err := strconv.ParseInt(strings.TrimSuffix(file.Name(), ".dlq"), 10, 64)
		if err != nil || seq < cursor.Segment {
			continue
		}
		pos := int64(dlqHeaderSize)
		if seq == cursor.Segment {
			pos = max(pos, cursor.Position)
		}
		f, err := os.Open(filepath.Join(sw.dataDir, file.Name()))
		if err != nil {
			return nil, cursor, false, err
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, cursor, false, err
		}
		for pos < info.Size() {
			var header [8]byte
			if _, err := f.ReadAt(header[:], pos); err != nil {
				f.Close()
				return nil, cursor, false, err
			}
			n := int64(binary.BigEndian.Uint32(header[:4])) - 4
			if n < 0 || n > 64<<20 || pos+8+n > info.Size() {
				f.Close()
				return nil, cursor, false, fmt.Errorf("invalid DLQ record at %s:%d", file.Name(), pos)
			}
			data := make([]byte, int(n))
			if _, err := f.ReadAt(data, pos+8); err != nil && err != io.EOF {
				f.Close()
				return nil, cursor, false, err
			}
			if crc32.ChecksumIEEE(data) != binary.BigEndian.Uint32(header[4:]) {
				f.Close()
				return nil, cursor, false, fmt.Errorf("DLQ checksum mismatch")
			}
			records = append(records, data)
			size += len(data)
			pos += 8 + n
			cursor = types.DeadLetterCursor{Segment: seq, Position: pos}
			if size >= 512<<10 || len(records) >= 256 {
				f.Close()
				return records, cursor, false, nil
			}
		}
		f.Close()
		cursor = types.DeadLetterCursor{Segment: seq, Position: pos}
	}
	return records, cursor, true, nil
}

// A batched delivery has several events sharing one delivery ID. New tombstones
// remove one event; legacy tombstones without an event retain their old scope.
func deadLetterIdentity(e *DLQEntry) string {
	return fmt.Sprintf("%s\x00%d\x00%s", e.DeliveryID, e.Event.GetOffset(), e.Event.GetMessageId())
}

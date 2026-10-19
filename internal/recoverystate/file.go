// Package recoverystate binds bounded recovery pages to one WAL generation.
package recoverystate

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/protobuf/proto"
)

const Format uint32 = 1
const Filename = "recovery-state.snapshot"
const maxPage = storage.MaxRecordLength + (4 << 20)

type Manifest struct {
	Version                             uint32
	Partition                           int32
	Epoch, First, Last, AcceptedThrough int64
	Encrypted                           bool
}

func frame(w io.Writer, b []byte) error {
	if len(b) > maxPage {
		return fmt.Errorf("recovery page exceeds limit")
	}
	var h [8]byte
	binary.BigEndian.PutUint32(h[:4], uint32(len(b)))
	binary.BigEndian.PutUint32(h[4:], crc32.ChecksumIEEE(b))
	if _, err := w.Write(h[:]); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func readFrame(r io.Reader) ([]byte, error) {
	var h [8]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(h[:4])
	if n > maxPage {
		return nil, fmt.Errorf("recovery page exceeds limit")
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(b) != binary.BigEndian.Uint32(h[4:]) {
		return nil, fmt.Errorf("recovery page checksum mismatch")
	}
	return b, nil
}

func Write(path string, m Manifest, cipher *storage.SegmentCipher, pages func(func(*types.ReplicationProgressRequest) error) error) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	m.Version, m.Encrypted = Format, cipher != nil
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err := frame(f, data); err != nil {
		return err
	}
	if err := pages(func(page *types.ReplicationProgressRequest) error {
		data, err := proto.Marshal(page)
		if err != nil {
			return err
		}
		if cipher != nil {
			data, err = cipher.Encrypt(data)
		}
		if err != nil {
			return err
		}
		return frame(f, data)
	}); err != nil {
		return err
	}
	if err := frame(f, nil); err != nil {
		return err
	}
	return f.Sync()
}

// Read validates the complete stream, including its explicit footer. Visitors
// must be repeatable: installation recovery can replay after a crash.
func Read(path string, cipher *storage.SegmentCipher, visit func(*types.ReplicationProgressRequest) error) (Manifest, error) {
	var m Manifest
	f, err := os.Open(path)
	if err != nil {
		return m, err
	}
	defer f.Close()
	b, err := readFrame(f)
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return m, err
	}
	if m.Version != Format || m.Partition < 0 || m.Epoch < 0 || m.First < 0 || m.Last < m.First-1 || m.AcceptedThrough < -1 || m.AcceptedThrough > m.Last || (m.Encrypted && cipher == nil) {
		return m, fmt.Errorf("invalid recovery manifest")
	}
	for {
		b, err := readFrame(f)
		if err != nil {
			return m, err
		}
		if len(b) == 0 {
			var tail [1]byte
			if n, err := f.Read(tail[:]); n != 0 || err != io.EOF {
				return m, fmt.Errorf("data after recovery footer")
			}
			return m, nil
		}
		if m.Encrypted {
			b, err = cipher.Decrypt(b)
			if err != nil {
				return m, err
			}
		}
		page := &types.ReplicationProgressRequest{}
		if err := proto.Unmarshal(b, page); err != nil {
			return m, err
		}
		if page.PartitionId != m.Partition || page.Term != m.Epoch {
			return m, fmt.Errorf("recovery page belongs to another generation")
		}
		for _, g := range page.Groups {
			if g.GroupId == "" || g.CommittedOffset < 0 || g.CommittedOffset > m.AcceptedThrough+1 {
				return m, fmt.Errorf("invalid recovery progress")
			}
			for _, at := range g.CompletedOffsets {
				if at < 0 || at > m.AcceptedThrough {
					return m, fmt.Errorf("completion outside accepted snapshot prefix")
				}
			}
		}
		for _, d := range page.DeadLetters {
			if d.DeliveryId == "" || (!d.Tombstone && (d.Event == nil || d.PartitionId != m.Partition || d.Event.PartitionId != m.Partition)) {
				return m, fmt.Errorf("invalid recovery dead letter")
			}
		}
		if visit != nil {
			if err := visit(page); err != nil {
				return m, err
			}
		}
	}
}

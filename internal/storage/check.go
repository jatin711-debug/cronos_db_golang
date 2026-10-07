package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// LogDamage is a place in a log where the bytes are not valid records.
type LogDamage struct {
	// Segment is the file, Position the byte in it where the valid records
	// stop, and Found what is there instead.
	Segment  string
	Position int64
	Found    string
	// AfterOffset is the last offset that can be read before the damage.
	AfterOffset int64
	// NextOffset is the first offset that can be read after it. The events
	// from AfterOffset+1 to NextOffset-1 cannot be read.
	NextOffset int64
	// EndOfLog is true when nothing follows: the log ends before these bytes,
	// as it does after a write that a crash interrupted. A node that opens
	// the log cuts them off by itself.
	EndOfLog bool
	// Repaired is true when the damaged part was cut out.
	Repaired bool
}

// LogCheck is what CheckLog found in one log.
type LogCheck struct {
	// Segments is the number of segment files, Events the number of events in
	// them, and FirstOffset and LastOffset the range they span (-1 for a log
	// that holds none).
	Segments    int
	Events      int64
	FirstOffset int64
	LastOffset  int64
	Damage      []LogDamage
}

// CheckLog reads the log in a partition directory of a stopped node, every
// record of every segment, and reports what it holds and where it is damaged.
//
// With repair set it cuts each damaged segment after its last valid record,
// which gives up the events behind it in that segment and leaves a hole in the
// log where they were. A log with a hole opens and serves what it holds, in a
// partition without another replica. It is the wrong repair for a replica of
// a cluster: the other replicas still have those events, and a replica is
// repaired by removing its partition directory, after which the leader fills
// it again.
func CheckLog(dataDir string, cipher *SegmentCipher, repair bool) (*LogCheck, error) {
	entries, err := os.ReadDir(filepath.Join(dataDir, "segments"))
	if os.IsNotExist(err) {
		return &LogCheck{FirstOffset: -1, LastOffset: -1}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read segments: %w", err)
	}
	var names []string
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".log" {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)

	segments := make([]*Segment, 0, len(names))
	defer func() {
		for _, segment := range segments {
			_ = segment.Close()
		}
	}()
	check := &LogCheck{Segments: len(names), FirstOffset: -1, LastOffset: -1}
	end := -1 // index of the last segment that holds records
	for _, name := range names {
		segment, err := OpenSegment(dataDir, name, cipher)
		if err != nil {
			return nil, fmt.Errorf("open segment %s: %w", name, err)
		}
		segments = append(segments, segment)
		if segment.GetLastOffset() < segment.GetFirstOffset() {
			continue
		}
		end = len(segments) - 1
		if check.FirstOffset < 0 {
			check.FirstOffset = segment.GetFirstOffset()
		}
		check.LastOffset = segment.GetLastOffset()
		check.Events += segment.GetLastOffset() - segment.GetFirstOffset() + 1
	}

	for i, segment := range segments {
		invalid, position, found := segment.InvalidTail()
		if !invalid {
			continue
		}
		damage := LogDamage{
			Segment:     segment.GetFilename(),
			Position:    position,
			Found:       found,
			AfterOffset: segment.GetLastOffset(),
			NextOffset:  -1,
			EndOfLog:    i >= end,
		}
		if !damage.EndOfLog {
			damage.NextOffset = segments[i+1].GetFirstOffset()
			if repair {
				if err := segment.clearInvalidTail(); err != nil {
					return check, fmt.Errorf("repair segment %s: %w", segment.GetFilename(), err)
				}
				damage.Repaired = true
			}
		}
		check.Damage = append(check.Damage, damage)
	}
	return check, nil
}

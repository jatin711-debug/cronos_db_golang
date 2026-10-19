// Package lifecycle describes WAL-ordered changes to future events. The WAL is
// authoritative; Index is only a payload-free, reconstructible lookup table.
package lifecycle

import (
	"encoding/json"
	"fmt"
	"maps"
	"strings"

	"github.com/jatin711-debug/cronos_db_golang/internal/logcontrol"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

const Prefix = "cronos.internal."
const Key = Prefix + "lifecycle"
const Marker = "lifecycle.version"
const Cancel = "cancel"
const Reschedule = "reschedule"
const Relocate = "relocate"

type Record struct {
	Version        int                   `json:"version"`
	Kind           string                `json:"kind"`
	Root           *types.EventReference `json:"root"`
	Previous       int64                 `json:"previous"`
	RequestID      string                `json:"request_id,omitempty"`
	RequestKind    string                `json:"request_kind,omitempty"`
	ExpectedOffset int64                 `json:"expected_offset"`
	ExpectedTerm   int64                 `json:"expected_term"`
	RevisionOffset int64                 `json:"revision_offset"`
	RevisionTerm   int64                 `json:"revision_term"`
}

func HasReservedMeta(e *types.Event) bool {
	for key := range e.GetMeta() {
		if strings.HasPrefix(key, Prefix) {
			return true
		}
	}
	return false
}

func Reference(e *types.Event) *types.EventReference {
	return &types.EventReference{PartitionId: e.PartitionId, Offset: e.Offset, Term: e.Term, MessageId: e.MessageId, Topic: e.Topic}
}

func ValidReference(r *types.EventReference) bool {
	return r != nil && r.PartitionId >= 0 && r.Offset >= 0 && r.Term >= 0 && r.MessageId != "" && len(r.MessageId) <= 128 && len(r.Topic) <= 255
}

func SameReference(a, b *types.EventReference) bool {
	return a != nil && b != nil && a.PartitionId == b.PartitionId && a.Offset == b.Offset && a.Term == b.Term && a.MessageId == b.MessageId && a.Topic == b.Topic
}

// Decode rejects unknown or malformed reserved metadata rather than silently
// interpreting a future record version as an ordinary event.
func Decode(e *types.Event) (*Record, error) {
	if logcontrol.IsTermStart(e) {
		return nil, logcontrol.ValidateTermStart(e)
	}
	if !HasReservedMeta(e) {
		return nil, nil
	}
	for key := range e.Meta {
		if strings.HasPrefix(key, Prefix) && key != Key {
			return nil, fmt.Errorf("unknown internal event metadata %q", key)
		}
	}
	var r Record
	if err := json.Unmarshal([]byte(e.Meta[Key]), &r); err != nil {
		return nil, fmt.Errorf("invalid lifecycle record: %w", err)
	}
	if r.Version != 1 || (r.Kind != Cancel && r.Kind != Reschedule && r.Kind != Relocate) || !ValidReference(r.Root) || r.Previous < r.Root.Offset || r.Previous >= e.Offset || r.Root.PartitionId != e.PartitionId || r.Root.MessageId != e.MessageId || r.Root.Topic != e.Topic || r.RevisionOffset < r.Root.Offset || r.RevisionOffset > r.Previous || r.RevisionTerm < r.Root.Term || r.RevisionTerm > e.Term {
		return nil, fmt.Errorf("invalid lifecycle record at offset %d", e.Offset)
	}
	if (r.Kind == Cancel && (len(e.Payload) != 0 || e.ScheduleTs != 0)) || (r.Kind != Cancel && (len(e.Payload) == 0 || e.ScheduleTs <= 0)) || (r.Kind != Relocate && r.RequestID == "") {
		return nil, fmt.Errorf("invalid lifecycle event body at offset %d", e.Offset)
	}
	if len(r.RequestID) > 128 || (r.Kind != Relocate && (r.RequestKind != r.Kind || r.ExpectedOffset != r.RevisionOffset || r.ExpectedTerm != r.RevisionTerm)) || (r.Kind == Relocate && ((r.RequestID == "") != (r.RequestKind == "") || (r.RequestKind != "" && r.RequestKind != Reschedule))) {
		return nil, fmt.Errorf("invalid lifecycle request history at offset %d", e.Offset)
	}
	return &r, nil
}

func Root(e *types.Event) (*types.EventReference, error) {
	r, err := Decode(e)
	if err != nil {
		return nil, err
	}
	if r != nil {
		return r.Root, nil
	}
	return Reference(e), nil
}

func Revision(e *types.Event, r *Record) (int64, int64) {
	if r != nil && r.Kind == Relocate {
		return r.RevisionOffset, r.RevisionTerm
	}
	return e.Offset, e.Term
}

func Replacement(e *types.Event, kind, requestID string, at int64) (*types.Event, error) {
	root, err := Root(e)
	if err != nil {
		return nil, err
	}
	// Validate before appending anything. Unsupported legacy events must pin
	// retention, never create an unreadable control record during evacuation.
	if !ValidReference(root) || (kind != Cancel && kind != Reschedule && kind != Relocate) || (kind != Cancel && (len(e.Payload) == 0 || at <= 0)) || (kind != Relocate && (requestID == "" || len(requestID) > 128)) {
		return nil, fmt.Errorf("event cannot be represented by a valid lifecycle replacement")
	}
	old, err := Decode(e)
	if err != nil {
		return nil, err
	}
	if old != nil && old.Kind == Cancel {
		return nil, fmt.Errorf("canceled event cannot be replaced")
	}
	rev, term := Revision(e, old)
	r := Record{Version: 1, Kind: kind, Root: root, Previous: e.Offset, RequestID: requestID, RequestKind: kind, ExpectedOffset: rev, ExpectedTerm: term, RevisionOffset: rev, RevisionTerm: term}
	if kind == Relocate {
		r.RequestKind = ""
		if old != nil {
			r.RequestID = old.RequestID
			r.RequestKind = old.RequestKind
			r.ExpectedOffset = old.ExpectedOffset
			r.ExpectedTerm = old.ExpectedTerm
		}
	}
	meta := maps.Clone(e.Meta)
	if meta == nil {
		meta = make(map[string]string)
	}
	data, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	meta[Key] = string(data)
	payload := e.Payload
	if kind == Cancel {
		payload = nil
		at = 0
	}
	return &types.Event{MessageId: e.MessageId, Topic: e.Topic, PartitionId: e.PartitionId, CreatedTs: e.CreatedTs, ScheduleTs: at, Payload: payload, Meta: meta}, nil
}

// Public removes only the internal envelope; caller metadata is preserved.
func Public(e *types.Event) *types.Event {
	if !HasReservedMeta(e) {
		return e
	}
	meta := maps.Clone(e.Meta)
	delete(meta, Key)
	return &types.Event{MessageId: e.MessageId, Topic: e.Topic, PartitionId: e.PartitionId, CreatedTs: e.CreatedTs, ScheduleTs: e.ScheduleTs, Payload: e.Payload, Meta: meta, Offset: e.Offset, Term: e.Term, Checksum: e.Checksum}
}

package storage

import "sort"

// The term of a log entry is asked for on every replicated append: the leader
// sends the term of the entry before the batch, and the follower compares it
// with its own. Reading it from the segment means finding the record from the
// nearest index entry, up to an index interval of records each read and, with
// encryption, decrypted on the way. That was most of what a replicated publish
// cost. The WAL therefore remembers the terms of the entries appended since it
// was opened, as runs: a term changes only when leadership does, so there are
// few of them.

// maxTermRuns bounds the remembered runs. A run is added only when the term
// changes; when there are more, the older half is forgotten and those entries
// are read from disk again.
const maxTermRuns = 1024

// termRun is a run of consecutive log entries written in one term.
type termRun struct {
	first, term int64
}

// resetTermsLocked forgets every remembered term. The caller holds w.mu, or
// has the WAL to itself.
func (w *WAL) resetTermsLocked() {
	w.terms = w.terms[:0]
	w.termsFrom, w.termsTo = 0, -1
}

// noteTermsLocked remembers the terms of records just appended, in order.
// The caller holds w.mu.
func (w *WAL) noteTermsLocked(prepared []*PreparedRecord) {
	for _, prep := range prepared {
		offset, term := prep.Event.Offset, prep.Term
		if w.termsTo < w.termsFrom || offset != w.termsTo+1 {
			// Nothing is remembered yet, or the log did not continue from the
			// last remembered entry: start again here.
			w.terms = w.terms[:0]
			w.termsFrom = offset
		}
		if n := len(w.terms); n == 0 || w.terms[n-1].term != term {
			if n >= maxTermRuns {
				w.terms = append(w.terms[:0], w.terms[n/2:]...)
				w.termsFrom = w.terms[0].first
			}
			w.terms = append(w.terms, termRun{first: offset, term: term})
		}
		w.termsTo = offset
	}
}

// dropTermsFromLocked forgets the terms of entries at and after offset, which
// are leaving the log. The caller holds w.mu.
func (w *WAL) dropTermsFromLocked(offset int64) {
	if w.termsTo < offset {
		return
	}
	w.termsTo = offset - 1
	for n := len(w.terms); n > 0 && w.terms[n-1].first > w.termsTo; n = len(w.terms) {
		w.terms = w.terms[:n-1]
	}
	if w.termsTo < w.termsFrom || len(w.terms) == 0 {
		w.resetTermsLocked()
	}
}

// rememberedTerm returns the term of the entry at offset when it was appended
// since the log was opened and is still in the log.
func (w *WAL) rememberedTerm(offset int64) (int64, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if offset < w.termsFrom || offset > w.termsTo || len(w.terms) == 0 {
		return 0, false
	}
	// Retention may have removed the entry; then it has no term, as on disk.
	held := false
	for _, segment := range w.segments {
		if segment.GetFirstOffset() <= offset && offset <= segment.GetLastOffset() {
			held = true
			break
		}
	}
	if !held {
		return 0, false
	}
	i := sort.Search(len(w.terms), func(i int) bool { return w.terms[i].first > offset }) - 1
	if i < 0 {
		return 0, false
	}
	return w.terms[i].term, true
}

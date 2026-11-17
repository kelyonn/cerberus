package raft

import "fmt"

// raftLog holds the replicated log and the three indices that track progress
// through it.
//
// INVARIANT I6 (docs/INVARIANTS.md): every index calculation in the project lives in
// this file. No code outside it indexes entries[] directly. The reason is
// snapshots: once the log is compacted, log index N no longer lives at slice
// position N, and raw indexing scattered across five files breaks everywhere at
// once. The offset exists from day one even though snapshots do not, so that
// adding them later is a change to this file rather than to the whole codebase.
//
// entries[0] is a SENTINEL, not a real entry. It carries the index and term of
// the last entry the most recent snapshot covers. On a fresh log that is
// {Index: 0, Term: 0}. Its purpose is that prevLogTerm lookups at the boundary
// of a compacted log still find a term to compare against, with no special case
// at the call site.
type raftLog struct {
	entries []Entry

	// committed: highest index known to be durably replicated on a majority.
	// applied:   highest index handed to the state machine.
	// stabled:   highest index the driver has fsync'd.
	//
	// committed >= applied always. "committed" and "applied" are different
	// things and the project's glossary is strict about it: commit precedes
	// apply, and an entry can be committed on a node that has not yet applied it.
	committed uint64
	applied   uint64
	stabled   uint64
}

func newLog() *raftLog {
	return &raftLog{
		entries: []Entry{{Index: 0, Term: 0}},
	}
}

// offset is the index of the sentinel: the last index NOT present as a real
// entry. Everything below is either snapshotted or never existed.
func (l *raftLog) offset() uint64 { return l.entries[0].Index }

// firstIndex is the lowest real entry index the log still holds.
func (l *raftLog) firstIndex() uint64 { return l.offset() + 1 }

// lastIndex is the highest entry index the log holds, or offset() if the log
// holds no real entries.
func (l *raftLog) lastIndex() uint64 {
	return l.offset() + uint64(len(l.entries)) - 1
}

// lastTerm is the term of the last entry, used for the §5.4.1 up-to-date check.
// On an empty log this is the sentinel's term, which is what we want: a node
// with nothing is never more up-to-date than anyone.
func (l *raftLog) lastTerm() uint64 { return l.entries[len(l.entries)-1].Term }

// pos maps a log index to a slice position. It accepts the sentinel index,
// because term lookups at the compaction boundary are legitimate; at() does not.
func (l *raftLog) pos(i uint64) (int, bool) {
	if i < l.offset() || i > l.lastIndex() {
		return 0, false
	}
	return int(i - l.offset()), true
}

// at returns the real entry at index i. The sentinel is not a real entry, so
// at(offset()) reports false.
func (l *raftLog) at(i uint64) (Entry, bool) {
	p, ok := l.pos(i)
	if !ok || p == 0 {
		return Entry{}, false
	}
	return l.entries[p], true
}

// termAt returns the term of the entry at index i. Unlike at, this succeeds on
// the sentinel: a leader sending entries right after the compaction boundary
// needs prevLogTerm for an index it no longer holds as an entry.
//
// ok == false means "that index has been compacted away or is beyond my log",
// which the caller must treat as "I cannot answer", not as term 0.
func (l *raftLog) termAt(i uint64) (uint64, bool) {
	p, ok := l.pos(i)
	if !ok {
		return 0, false
	}
	return l.entries[p].Term, true
}

// slice returns entries in [lo, hi), clamped to what the log actually holds.
// The result is a copy: callers put these in messages, and the simulated
// network may hold them across truncations.
func (l *raftLog) slice(lo, hi uint64) []Entry {
	if lo < l.firstIndex() {
		lo = l.firstIndex()
	}
	if hi > l.lastIndex()+1 {
		hi = l.lastIndex() + 1
	}
	if lo >= hi {
		return nil
	}
	p, ok := l.pos(lo)
	if !ok {
		return nil
	}
	out := make([]Entry, hi-lo)
	copy(out, l.entries[p:p+int(hi-lo)])
	return out
}

// append adds entries to the end of the log. Entries carry the index the leader
// assigned them, so this verifies contiguity rather than assigning indices —
// a gap here would mean a bug upstream in replication, and it is much cheaper to
// catch it at the seam than to debug the divergence it causes later.
func (l *raftLog) append(ents ...Entry) error {
	for _, e := range ents {
		if want := l.lastIndex() + 1; e.Index != want {
			return fmt.Errorf("raft: non-contiguous append: got index %d, want %d", e.Index, want)
		}
		l.entries = append(l.entries, e)
	}
	return nil
}

// truncateFrom deletes the entry at index i and everything after it.
//
// Deleting a committed entry is never legal — it would mean discarding data the
// cluster has already promised to a client — so this refuses rather than
// corrupting state silently. In practice the §5.3 conflict check upstream should
// make it unreachable; if it ever fires, the bug is there.
func (l *raftLog) truncateFrom(i uint64) error {
	if i <= l.committed {
		return fmt.Errorf("raft: refusing to truncate committed entry: index %d <= commitIndex %d", i, l.committed)
	}
	p, ok := l.pos(i)
	if !ok || p == 0 {
		return nil // nothing to do: already absent, or the sentinel
	}
	l.entries = l.entries[:p]
	if l.stabled >= i {
		l.stabled = i - 1
	}
	return nil
}

// matches reports whether the log contains an entry at the given index whose
// term is the given term. This is the AppendEntries consistency check (§5.3):
// it is what makes the Log Matching Property hold inductively.
func (l *raftLog) matches(index, term uint64) bool {
	t, ok := l.termAt(index)
	return ok && t == term
}

// isUpToDate implements the election restriction, §5.4.1: is the candidate's log
// at least as up-to-date as ours?
//
// A higher last term wins outright. On equal terms the longer log wins. This
// single comparison is what guarantees Leader Completeness — a candidate missing
// a committed entry cannot win, because a majority holds that entry and every
// one of them refuses the vote.
func (l *raftLog) isUpToDate(lastIdx, lastTerm uint64) bool {
	if lastTerm != l.lastTerm() {
		return lastTerm > l.lastTerm()
	}
	return lastIdx >= l.lastIndex()
}

// commitTo advances commitIndex. It never moves backwards: commitIndex is
// knowledge, and knowledge is not retracted.
func (l *raftLog) commitTo(i uint64) {
	if i > l.committed {
		if i > l.lastIndex() {
			i = l.lastIndex()
		}
		l.committed = i
	}
}

// unstableEntries are the entries the driver has not yet fsync'd. They go in
// Ready.Entries, and Advance marks them stable.
func (l *raftLog) unstableEntries() []Entry {
	return l.slice(l.stabled+1, l.lastIndex()+1)
}

// nextCommittedEntries are entries committed but not yet handed to the state
// machine. They go in Ready.CommittedEntries.
func (l *raftLog) nextCommittedEntries() []Entry {
	return l.slice(l.applied+1, l.committed+1)
}

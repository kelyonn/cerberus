package raft

import "testing"

func TestLogIndexArithmetic(t *testing.T) {
	l := newLog()

	if got, want := l.firstIndex(), uint64(1); got != want {
		t.Errorf("firstIndex on empty log = %d, want %d", got, want)
	}
	if got, want := l.lastIndex(), uint64(0); got != want {
		t.Errorf("lastIndex on empty log = %d, want %d", got, want)
	}
	if _, ok := l.at(0); ok {
		t.Error("at(0) returned an entry; index 0 is the sentinel, not a real entry")
	}
	// The sentinel must still answer term lookups: prevLogTerm at the boundary
	// depends on it.
	if term, ok := l.termAt(0); !ok || term != 0 {
		t.Errorf("termAt(0) = %d, %v; want 0, true (the sentinel)", term, ok)
	}

	if err := l.append(Entry{Index: 1, Term: 1}, Entry{Index: 2, Term: 1}, Entry{Index: 3, Term: 2}); err != nil {
		t.Fatalf("append: %v", err)
	}

	tests := []struct {
		name  string
		index uint64
		term  uint64
		ok    bool
	}{
		{"first entry", 1, 1, true},
		{"middle entry", 2, 1, true},
		{"last entry", 3, 2, true},
		{"past the end", 4, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			term, ok := l.termAt(tc.index)
			if ok != tc.ok || (ok && term != tc.term) {
				t.Errorf("termAt(%d) = %d, %v; want %d, %v", tc.index, term, ok, tc.term, tc.ok)
			}
		})
	}

	if got, want := l.lastIndex(), uint64(3); got != want {
		t.Errorf("lastIndex = %d, want %d", got, want)
	}
	if got, want := l.lastTerm(), uint64(2); got != want {
		t.Errorf("lastTerm = %d, want %d", got, want)
	}
}

func TestLogAppendRejectsGaps(t *testing.T) {
	l := newLog()
	if err := l.append(Entry{Index: 2, Term: 1}); err == nil {
		t.Fatal("append accepted index 2 into an empty log; a gap here means a replication bug upstream")
	}
}

func TestLogSliceCopiesEntries(t *testing.T) {
	l := newLog()
	if err := l.append(Entry{Index: 1, Term: 1, Data: []byte("a")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	s := l.slice(1, 2)
	s[0].Term = 99
	if term, _ := l.termAt(1); term != 1 {
		t.Error("slice aliased the log; a message in flight could mutate the log it came from")
	}
}

func TestLogRefusesToTruncateCommittedEntries(t *testing.T) {
	l := newLog()
	if err := l.append(Entry{Index: 1, Term: 1}, Entry{Index: 2, Term: 1}); err != nil {
		t.Fatalf("append: %v", err)
	}
	l.commitTo(2)

	if err := l.truncateFrom(2); err == nil {
		t.Fatal("truncateFrom removed a committed entry; that is data the cluster already promised")
	}
}

// TestIsUpToDate covers the election restriction, §5.4.1. This single comparison
// is what guarantees Leader Completeness, so it is worth pinning precisely.
func TestIsUpToDate(t *testing.T) {
	l := newLog()
	if err := l.append(Entry{Index: 1, Term: 1}, Entry{Index: 2, Term: 2}); err != nil {
		t.Fatalf("append: %v", err)
	}
	// Our log ends at index 2, term 2.

	tests := []struct {
		name             string
		lastIdx, lastTrm uint64
		want             bool
	}{
		{"higher term, shorter log wins", 1, 3, true},
		{"lower term, longer log loses", 9, 1, false},
		{"same term, longer log wins", 3, 2, true},
		{"same term, same length ties and counts as up-to-date", 2, 2, true},
		{"same term, shorter log loses", 1, 2, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := l.isUpToDate(tc.lastIdx, tc.lastTrm); got != tc.want {
				t.Errorf("isUpToDate(%d, %d) = %v, want %v", tc.lastIdx, tc.lastTrm, got, tc.want)
			}
		})
	}
}

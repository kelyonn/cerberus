package raft

import (
	"math/rand"
	"testing"
)

func testNode(t *testing.T, id uint64, peers []uint64) *Node {
	t.Helper()
	n, err := New(Config{
		ID:               id,
		Peers:            peers,
		ElectionTimeout:  10,
		HeartbeatTimeout: 3,
		Rand:             rand.New(rand.NewSource(1)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return n
}

// TestLeaderCannotCommitPreviousTermByCountingReplicas is the Figure 8 rule,
// tested directly.
//
// This is the single subtlest rule in Raft and the one most implementations get
// wrong, so it gets a test that does not depend on getting a whole cluster into
// the right shape by luck.
func TestLeaderCannotCommitPreviousTermByCountingReplicas(t *testing.T) {
	n := testNode(t, 1, []uint64{1, 2, 3})

	// An entry inherited from term 1, now replicated on a majority (nodes 1
	// and 2). We are leader in term 2.
	if err := n.log.append(Entry{Index: 1, Term: 1, Data: []byte("inherited")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	n.currentTerm = 2
	n.role = RoleLeader
	n.matchIndex = map[uint64]uint64{1: 1, 2: 1, 3: 0}

	if n.maybeCommit() {
		t.Fatal("committed an entry from a previous term by counting replicas (Figure 8)")
	}
	if n.log.committed != 0 {
		t.Fatalf("commitIndex = %d, want 0: a majority holding an old entry is not enough", n.log.committed)
	}

	// Append an entry of our OWN term and get it onto a majority. Now the rule
	// is satisfied — and committing index 2 carries index 1 with it, which is
	// exactly how the inherited entry becomes committed safely.
	if err := n.log.append(Entry{Index: 2, Term: 2, Data: nil}); err != nil {
		t.Fatalf("append: %v", err)
	}
	n.matchIndex = map[uint64]uint64{1: 2, 2: 2, 3: 0}

	if !n.maybeCommit() {
		t.Fatal("refused to commit an entry of the current term held by a majority")
	}
	if n.log.committed != 2 {
		t.Fatalf("commitIndex = %d, want 2", n.log.committed)
	}
}

// TestCommitRuleUsesMatchIndexNotNextIndex pins the distinction that
// RAFT_DESIGN.md §1 warns about. nextIndex is a guess and runs ahead of reality;
// committing on it would commit entries no follower actually holds.
func TestCommitRuleUsesMatchIndexNotNextIndex(t *testing.T) {
	n := testNode(t, 1, []uint64{1, 2, 3})
	n.currentTerm = 1
	n.role = RoleLeader
	if err := n.log.append(Entry{Index: 1, Term: 1}); err != nil {
		t.Fatalf("append: %v", err)
	}

	// Optimistic guesses everywhere, but only the leader actually holds it.
	n.nextIndex = map[uint64]uint64{1: 2, 2: 2, 3: 2}
	n.matchIndex = map[uint64]uint64{1: 1, 2: 0, 3: 0}

	if n.maybeCommit() {
		t.Fatal("committed on nextIndex; only the leader actually holds the entry")
	}
}

func TestNewLeaderAppendsNoOp(t *testing.T) {
	n := testNode(t, 1, []uint64{1, 2, 3})
	n.becomeCandidate()
	n.becomeLeader()

	ents := n.LogEntries()
	if len(ents) != 1 {
		t.Fatalf("new leader has %d entries, want 1 (the no-op)", len(ents))
	}
	if ents[0].Data != nil {
		t.Errorf("entry data = %q, want nil (a no-op)", ents[0].Data)
	}
	if ents[0].Term != n.currentTerm {
		t.Errorf("no-op term = %d, want current term %d", ents[0].Term, n.currentTerm)
	}
}

// TestAppendEntriesKeepsEntriesBeyondADuplicateRPC is the truncation trap from
// RAFT_DESIGN.md §4, tested directly.
//
// The follower holds three entries. A stale, delayed AppendEntries arrives
// carrying only the first one — a duplicate of a message already processed.
//
// "Truncate to prevLogIndex, then append" would delete entries 2 and 3, which
// the leader may already have committed. Entry-by-entry conflict detection sees
// index 1 term 1 matching what it already has and changes nothing.
//
// This is the bug that passes every ordinary test: it needs message reordering
// or duplication to appear at all.
func TestAppendEntriesKeepsEntriesBeyondADuplicateRPC(t *testing.T) {
	n := testNode(t, 2, []uint64{1, 2, 3})
	n.currentTerm = 1
	if err := n.log.append(
		Entry{Index: 1, Term: 1, Data: []byte("a")},
		Entry{Index: 2, Term: 1, Data: []byte("b")},
		Entry{Index: 3, Term: 1, Data: []byte("c")},
	); err != nil {
		t.Fatalf("append: %v", err)
	}

	stale := Message{
		Type: MsgApp, From: 1, To: 2, Term: 1,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []Entry{{Index: 1, Term: 1, Data: []byte("a")}},
	}
	if err := n.Step(stale); err != nil {
		t.Fatalf("Step: %v", err)
	}

	if got, want := n.log.lastIndex(), uint64(3); got != want {
		t.Fatalf("log truncated to index %d, want %d retained: a delayed duplicate destroyed committed data", got, want)
	}
}

// TestAppendEntriesTruncatesOnGenuineConflict is the other half: when the terms
// really do differ, the entry and everything after it must go.
func TestAppendEntriesTruncatesOnGenuineConflict(t *testing.T) {
	n := testNode(t, 2, []uint64{1, 2, 3})
	n.currentTerm = 2
	if err := n.log.append(
		Entry{Index: 1, Term: 1},
		Entry{Index: 2, Term: 1}, // this one is wrong; the leader has term 2 here
		Entry{Index: 3, Term: 1}, // and this one must go with it
	); err != nil {
		t.Fatalf("append: %v", err)
	}

	m := Message{
		Type: MsgApp, From: 1, To: 2, Term: 2,
		PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []Entry{{Index: 2, Term: 2}},
	}
	if err := n.Step(m); err != nil {
		t.Fatalf("Step: %v", err)
	}

	if got, want := n.log.lastIndex(), uint64(2); got != want {
		t.Fatalf("lastIndex = %d, want %d", got, want)
	}
	if term, _ := n.log.termAt(2); term != 2 {
		t.Errorf("entry 2 term = %d, want 2: the conflicting entry was not replaced", term)
	}
}

// TestCommitIndexBoundedByEntriesInThisRPC covers the second §4 trap: the bound
// is the last entry in THIS message, not the follower's own last index.
func TestCommitIndexBoundedByEntriesInThisRPC(t *testing.T) {
	n := testNode(t, 2, []uint64{1, 2, 3})
	n.currentTerm = 1
	if err := n.log.append(
		Entry{Index: 1, Term: 1},
		Entry{Index: 2, Term: 1},
		Entry{Index: 3, Term: 1},
	); err != nil {
		t.Fatalf("append: %v", err)
	}

	// A heartbeat that only reaches back to index 1, but claims a high
	// leaderCommit. The follower may only commit up to index 1.
	m := Message{
		Type: MsgApp, From: 1, To: 2, Term: 1,
		PrevLogIndex: 1, PrevLogTerm: 1,
		Entries:      nil,
		LeaderCommit: 3,
	}
	if err := n.Step(m); err != nil {
		t.Fatalf("Step: %v", err)
	}

	if got, want := n.log.committed, uint64(1); got != want {
		t.Fatalf("commitIndex = %d, want %d: bounded by the last entry in this RPC", got, want)
	}
}

// TestElectionTimerResetsOnlyOnGrantedVote covers the §3 trap. Resetting on a
// denial lets a node with a stale log suppress healthy candidates forever, and
// it shows up as an unexplained liveness stall rather than as an error.
func TestElectionTimerResetsOnlyOnGrantedVote(t *testing.T) {
	n := testNode(t, 2, []uint64{1, 2, 3})
	n.currentTerm = 5
	if err := n.log.append(Entry{Index: 1, Term: 5}); err != nil {
		t.Fatalf("append: %v", err)
	}
	n.electionElapsed = 7

	// A candidate whose log is behind ours. The vote must be denied AND the
	// timer must be left alone.
	behind := Message{
		Type: MsgVote, From: 3, To: 2, Term: 5,
		LastLogIndex: 0, LastLogTerm: 0,
	}
	if err := n.Step(behind); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n.electionElapsed != 7 {
		t.Fatalf("electionElapsed = %d, want 7: a denied vote must not reset the timer", n.electionElapsed)
	}

	// A candidate with an up-to-date log gets the vote, and that does reset it.
	ok := Message{
		Type: MsgVote, From: 1, To: 2, Term: 5,
		LastLogIndex: 1, LastLogTerm: 5,
	}
	if err := n.Step(ok); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n.electionElapsed != 0 {
		t.Fatalf("electionElapsed = %d, want 0: a granted vote resets the timer", n.electionElapsed)
	}
	if n.votedFor != 1 {
		t.Fatalf("votedFor = %d, want 1", n.votedFor)
	}
}

// TestHigherTermAlwaysCausesStepDown covers the rule that applies to every role
// and must be checked before anything else.
func TestHigherTermAlwaysCausesStepDown(t *testing.T) {
	n := testNode(t, 1, []uint64{1, 2, 3})
	n.becomeCandidate()
	n.becomeLeader()
	if n.role != RoleLeader {
		t.Fatal("setup: expected leader")
	}

	if err := n.Step(Message{Type: MsgApp, From: 2, To: 1, Term: n.currentTerm + 5}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if n.role != RoleFollower {
		t.Fatalf("role = %v, want follower after seeing a higher term", n.role)
	}
	if n.votedFor != None {
		t.Fatalf("votedFor = %d, want None: a new term invalidates the old vote", n.votedFor)
	}
}

// TestReadyPersistsBeforeSending pins invariant I5 at the contract level: any
// Ready that carries a vote reply must carry the HardState recording that vote,
// so a driver following the documented order cannot send the promise before
// making it durable.
func TestReadyPersistsBeforeSending(t *testing.T) {
	n := testNode(t, 2, []uint64{1, 2, 3})

	if err := n.Step(Message{Type: MsgVote, From: 1, To: 2, Term: 1, LastLogIndex: 0, LastLogTerm: 0}); err != nil {
		t.Fatalf("Step: %v", err)
	}

	rd := n.Ready()
	if len(rd.Messages) != 1 || rd.Messages[0].Type != MsgVoteResp || !rd.Messages[0].Success {
		t.Fatalf("expected one granted vote reply, got %+v", rd.Messages)
	}
	if rd.HardState == nil {
		t.Fatal("Ready carries a granted vote but no HardState to persist first (invariant I5)")
	}
	if rd.HardState.Vote != 1 {
		t.Fatalf("HardState.Vote = %d, want 1", rd.HardState.Vote)
	}
}

func TestSingleNodeClusterElectsItself(t *testing.T) {
	n := testNode(t, 1, []uint64{1})
	for i := 0; i < 100 && n.Status().Role != RoleLeader; i++ {
		n.Tick()
	}
	if got := n.Status().Role; got != RoleLeader {
		t.Fatalf("role = %v, want leader: a single node is its own majority", got)
	}
}

func TestProposeOnFollowerReportsLeader(t *testing.T) {
	n := testNode(t, 2, []uint64{1, 2, 3})
	n.becomeFollower(3, 1)

	err := n.Propose([]byte("x"))
	var nle *NotLeaderError
	if err == nil {
		t.Fatal("Propose on a follower succeeded")
	}
	if !asNotLeader(err, &nle) || nle.Leader != 1 {
		t.Fatalf("err = %v, want NotLeaderError{Leader: 1} so the client can be redirected", err)
	}
}

func asNotLeader(err error, target **NotLeaderError) bool {
	e, ok := err.(*NotLeaderError)
	if ok {
		*target = e
	}
	return ok
}

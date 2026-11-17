package raft

import (
	"fmt"
	"math/rand"
	"slices"
)

// Config describes a node at construction. Everything the node needs is passed
// in; the node reaches for nothing global.
type Config struct {
	// ID of this node. Must be non-zero — zero means None.
	ID uint64

	// Peers is every voting member of the cluster, including ID itself.
	Peers []uint64

	// ElectionTimeout is the minimum number of ticks a follower waits without
	// hearing from a leader before starting an election. The actual timeout is
	// randomized in [ElectionTimeout, 2*ElectionTimeout) — randomization is what
	// breaks split votes, and a fixed value will hang the cluster in a
	// re-election loop (§5.2).
	ElectionTimeout int

	// HeartbeatTimeout is how many ticks a leader waits between heartbeats.
	// Must be well below ElectionTimeout: broadcastTime ≪ electionTimeout.
	HeartbeatTimeout int

	// Rand is the single source of randomness for this node.
	//
	// INVARIANT I2 (docs/INVARIANTS.md): no package-level rand anywhere. One seeded
	// *rand.Rand per run, threaded explicitly, so that a failing run reproduces
	// byte-for-byte from its seed. This field is why election timeouts do not
	// destroy reproducibility.
	Rand *rand.Rand
}

func (c Config) validate() error {
	switch {
	case c.ID == None:
		return fmt.Errorf("raft: node ID must be non-zero")
	case len(c.Peers) == 0:
		return fmt.Errorf("raft: Peers must be non-empty")
	case !slices.Contains(c.Peers, c.ID):
		return fmt.Errorf("raft: Peers must contain the node's own ID %d", c.ID)
	case c.ElectionTimeout <= 0:
		return fmt.Errorf("raft: ElectionTimeout must be positive")
	case c.HeartbeatTimeout <= 0:
		return fmt.Errorf("raft: HeartbeatTimeout must be positive")
	case c.HeartbeatTimeout >= c.ElectionTimeout:
		return fmt.Errorf("raft: HeartbeatTimeout %d must be well below ElectionTimeout %d",
			c.HeartbeatTimeout, c.ElectionTimeout)
	case c.Rand == nil:
		return fmt.Errorf("raft: Rand must be provided (invariant I2: no package-level rand)")
	}
	return nil
}

// Node is one Raft server: a pure state machine driven by Tick and Step.
type Node struct {
	id    uint64
	peers []uint64 // sorted; see invariant I3 below

	// Persistent state (Figure 2). currentTerm and votedFor reach disk via
	// Ready.HardState before any message that depends on them is sent.
	currentTerm uint64
	votedFor    uint64
	log         *raftLog

	role Role
	lead uint64

	// Leader-only, reinitialised on every election.
	//
	// nextIndex is a GUESS, corrected downward when a follower rejects.
	// matchIndex is KNOWLEDGE, and only ever moves up. The commit rule uses
	// matchIndex and never nextIndex — conflating them is the classic early bug
	// that commits entries a follower does not actually hold.
	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64

	// Candidate-only: who has answered this election, and how.
	votes map[uint64]bool

	electionElapsed  int
	heartbeatElapsed int

	electionTimeout           int
	randomizedElectionTimeout int
	heartbeatTimeout          int

	rng *rand.Rand

	// Outbound messages accumulated since the last Advance.
	msgs []Message

	// prevHardState is the last HardState the driver was told to persist, so
	// Ready can report only genuine changes.
	prevHardState HardState

	// pending records what the most recent Ready reported, so Advance consumes
	// exactly that and no more. Without this, a Ready call that is not followed
	// by Advance (or a second Ready before Advance) would double-count entries.
	pending    Ready
	hasPending bool
}

// New creates a node in the follower role at term 0 with an empty log — the
// state every Raft server starts in, and the state a restarted server starts in
// before its persisted HardState and log are loaded back.
func New(c Config) (*Node, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}

	// INVARIANT I3 (docs/INVARIANTS.md): Go randomizes map iteration order
	// deliberately. Peers are held in a sorted slice and always iterated in that
	// order, so no behaviour can depend on hash order. This is the easiest
	// invariant in the project to violate by accident and it silently destroys
	// determinism when you do.
	peers := slices.Clone(c.Peers)
	slices.Sort(peers)

	n := &Node{
		id:               c.ID,
		peers:            peers,
		log:              newLog(),
		role:             RoleFollower,
		votedFor:         None,
		lead:             None,
		nextIndex:        make(map[uint64]uint64, len(peers)),
		matchIndex:       make(map[uint64]uint64, len(peers)),
		votes:            make(map[uint64]bool, len(peers)),
		electionTimeout:  c.ElectionTimeout,
		heartbeatTimeout: c.HeartbeatTimeout,
		rng:              c.Rand,
	}
	n.resetElectionTimer()
	return n, nil
}

// Restart rebuilds a node from state recovered off disk after a crash.
//
// currentTerm and votedFor come back exactly as they were, which is what makes
// the durability rule mean anything: a node that promised a vote before dying
// must still remember that promise, or it can vote twice in one term and elect
// two leaders.
//
// lastApplied deliberately does NOT come back — it restarts at zero and the log
// is replayed into the state machine. That is safe precisely because Apply is
// deterministic (invariant I4): replaying the same entries must produce the same
// state. It is also why a nondeterministic Apply is so dangerous here — the
// divergence it produces appears only after a restart and looks exactly like a
// consensus bug.
func Restart(c Config, hs HardState, entries []Entry) (*Node, error) {
	n, err := New(c)
	if err != nil {
		return nil, err
	}
	n.currentTerm = hs.Term
	n.votedFor = hs.Vote
	n.prevHardState = hs

	if err := n.log.append(entries...); err != nil {
		return nil, err
	}
	n.log.stabled = n.log.lastIndex()
	n.log.commitTo(hs.Commit)
	return n, nil
}

// ID returns this node's ID.
func (n *Node) ID() uint64 { return n.id }

// quorum is a strict majority: ⌊n/2⌋ + 1. Not "more than half the responses",
// not "all but one" — a strict majority of the configured cluster.
func (n *Node) quorum() int { return len(n.peers)/2 + 1 }

// resetElectionTimer picks a fresh randomized timeout from this node's own
// seeded source (invariant I2).
func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	n.randomizedElectionTimeout = n.electionTimeout + n.rng.Intn(n.electionTimeout)
}

// Tick advances the logical clock by one.
//
// This is the only notion of time in the core. There is no time.Now and no
// timer; a simulated hour costs a few hundred thousand cheap calls, which is
// what lets the harness explore rare interleavings (ADR-002).
func (n *Node) Tick() {
	if n.role == RoleLeader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.heartbeatTimeout {
			n.heartbeatElapsed = 0
			n.broadcastAppend()
		}
		return
	}

	n.electionElapsed++
	if n.electionElapsed >= n.randomizedElectionTimeout {
		n.becomeCandidate()
	}
}

// Step feeds one inbound message into the state machine.
func (n *Node) Step(m Message) error {
	// The rule that applies to every role, checked FIRST, before any
	// type-specific handling (RAFT_DESIGN.md §2). Getting this ordering wrong is
	// a rich source of bugs: a stale leader must step down on seeing a higher
	// term even if the message is otherwise something it would ignore.
	switch {
	case m.Term > n.currentTerm:
		lead := m.From
		if m.Type == MsgVote {
			// A vote REQUEST does not tell us who the leader is, and it
			// certainly does not grant the vote. It only moves us to the new
			// term as a follower; the vote decision happens in handleVote,
			// which still has to run the up-to-date check.
			lead = None
		}
		n.becomeFollower(m.Term, lead)

	case m.Term < n.currentTerm:
		// Stale message. Reply so the sender discovers the current term and
		// steps down — dropping it silently would leave a deposed leader
		// retrying forever.
		switch m.Type {
		case MsgVote:
			n.send(Message{Type: MsgVoteResp, To: m.From, Term: n.currentTerm, Success: false})
		case MsgApp:
			n.send(Message{Type: MsgAppResp, To: m.From, Term: n.currentTerm, Success: false})
		}
		return nil
	}

	switch m.Type {
	case MsgVote:
		n.handleVote(m)
	case MsgVoteResp:
		n.handleVoteResp(m)
	case MsgApp:
		return n.handleAppend(m)
	case MsgAppResp:
		n.handleAppendResp(m)
	default:
		return fmt.Errorf("raft: unknown message type %d", m.Type)
	}
	return nil
}

// Propose submits a command for replication. Only a leader can; anyone else
// reports who to redirect to via ErrNotLeader.
func (n *Node) Propose(cmd []byte) error {
	if n.role != RoleLeader {
		return &NotLeaderError{Leader: n.lead}
	}
	return n.appendAndBroadcast(Entry{
		Term:  n.currentTerm,
		Index: n.log.lastIndex() + 1,
		Data:  cmd,
	})
}

// appendAndBroadcast appends one entry to the leader's own log and pushes it out.
func (n *Node) appendAndBroadcast(e Entry) error {
	if err := n.log.append(e); err != nil {
		return err
	}
	// The leader trivially holds what it just wrote. Recording it here keeps the
	// commit rule uniform: it counts matchIndex across all peers including self,
	// with no special case for the leader.
	n.matchIndex[n.id] = n.log.lastIndex()
	n.nextIndex[n.id] = n.log.lastIndex() + 1
	n.broadcastAppend()
	return nil
}

// NotLeaderError is returned by Propose on a non-leader. Leader is the node the
// client should retry against, or None if this node does not know one.
type NotLeaderError struct{ Leader uint64 }

func (e *NotLeaderError) Error() string {
	if e.Leader == None {
		return "raft: not leader, and no leader currently known"
	}
	return fmt.Sprintf("raft: not leader, try node %d", e.Leader)
}

// --- role transitions -------------------------------------------------------

func (n *Node) becomeFollower(term, lead uint64) {
	if term > n.currentTerm {
		n.currentTerm = term
		// A new term means the vote from the old term is meaningless. Clearing
		// it is what allows this node to vote in the new term.
		n.votedFor = None
	}
	n.role = RoleFollower
	n.lead = lead
	n.resetElectionTimer()
}

// becomeCandidate starts an election: §5.2.
func (n *Node) becomeCandidate() {
	n.currentTerm++
	n.role = RoleCandidate
	n.lead = None
	n.votedFor = n.id // a candidate always votes for itself
	n.resetElectionTimer()

	n.votes = map[uint64]bool{n.id: true}

	// A single-node cluster is its own majority and wins immediately.
	if n.quorum() == 1 {
		n.becomeLeader()
		return
	}

	for _, peer := range n.peers { // sorted: invariant I3
		if peer == n.id {
			continue
		}
		n.send(Message{
			Type:         MsgVote,
			To:           peer,
			Term:         n.currentTerm,
			LastLogIndex: n.log.lastIndex(),
			LastLogTerm:  n.log.lastTerm(),
		})
	}
}

// becomeLeader takes leadership and immediately appends a no-op entry.
//
// The no-op is not cosmetic. RAFT_DESIGN.md §5: a leader may not commit an entry
// from a previous term by counting replicas, so a fresh leader can commit
// nothing at all until it has an entry of its own term in the log. Appending one
// on election is what unblocks that, and it advances commitIndex past every
// inherited entry as a side effect — which is also what makes a read servable
// immediately after an election rather than after the next client write.
func (n *Node) becomeLeader() {
	n.role = RoleLeader
	n.lead = n.id
	n.heartbeatElapsed = 0

	// Leader state is rebuilt from scratch every election (Figure 2).
	// nextIndex starts optimistic and is corrected downward by rejections;
	// matchIndex starts at zero because the new leader knows nothing yet.
	for _, peer := range n.peers {
		n.nextIndex[peer] = n.log.lastIndex() + 1
		n.matchIndex[peer] = 0
	}

	// appendAndBroadcast sets our own matchIndex and sends to everyone.
	_ = n.appendAndBroadcast(Entry{
		Term:  n.currentTerm,
		Index: n.log.lastIndex() + 1,
		Data:  nil, // nil Data == no-op
	})
}

// --- outbound ---------------------------------------------------------------

// send queues a message. It does not transmit anything: the core has no network
// (invariant I1). Messages leave in Ready.Messages, after the driver has
// persisted the HardState and entries in that same Ready.
func (n *Node) send(m Message) {
	m.From = n.id
	if m.Term == 0 {
		m.Term = n.currentTerm
	}
	n.msgs = append(n.msgs, m)
}

// --- the Ready contract -----------------------------------------------------

func (n *Node) hardState() HardState {
	return HardState{Term: n.currentTerm, Vote: n.votedFor, Commit: n.log.committed}
}

// Ready returns the work the driver must now perform. It does not mutate
// progress state — the driver has not done the work yet. Calling Ready twice
// without an intervening Advance returns the same work twice, which is
// deliberate: it means a driver that crashes mid-cycle simply redoes it.
func (n *Node) Ready() Ready {
	rd := Ready{
		Entries:          n.log.unstableEntries(),
		CommittedEntries: n.log.nextCommittedEntries(),
		Messages:         slices.Clone(n.msgs),
	}
	if hs := n.hardState(); hs != n.prevHardState {
		rd.HardState = &hs
	}
	n.pending, n.hasPending = rd, true
	return rd
}

// Advance acknowledges that everything in the most recent Ready has been
// persisted, sent, and applied, and moves the node's progress markers up to
// match.
func (n *Node) Advance() {
	if !n.hasPending {
		return
	}
	rd := n.pending

	if len(rd.Entries) > 0 {
		last := rd.Entries[len(rd.Entries)-1].Index
		if last > n.log.stabled {
			n.log.stabled = last
		}
	}
	if len(rd.CommittedEntries) > 0 {
		n.log.applied = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
	}
	if rd.HardState != nil {
		n.prevHardState = *rd.HardState
	}
	// Drop exactly the messages that were handed out. Anything queued since the
	// Ready call stays put for the next cycle.
	n.msgs = n.msgs[len(rd.Messages):]

	n.pending, n.hasPending = Ready{}, false
}

// --- observability ----------------------------------------------------------
//
// ARCHITECTURE.md §9: build the state dump in week 1, you will use it
// constantly. The invariant checkers in the harness read the cluster through
// these, which is also why they are read-only copies.

// Status is a snapshot of the node's state for logging and assertions.
type Status struct {
	ID        uint64
	Role      Role
	Term      uint64
	Vote      uint64
	Lead      uint64
	Commit    uint64
	Applied   uint64
	LastIndex uint64
}

func (n *Node) Status() Status {
	return Status{
		ID:        n.id,
		Role:      n.role,
		Term:      n.currentTerm,
		Vote:      n.votedFor,
		Lead:      n.lead,
		Commit:    n.log.committed,
		Applied:   n.log.applied,
		LastIndex: n.log.lastIndex(),
	}
}

// LogEntries returns a copy of every real entry in the log, for invariant
// checking. A copy, so a checker cannot perturb the run it is observing.
func (n *Node) LogEntries() []Entry {
	return n.log.slice(n.log.firstIndex(), n.log.lastIndex()+1)
}

// TermAt exposes the log's term lookup to the harness's S3 checker.
func (n *Node) TermAt(i uint64) (uint64, bool) { return n.log.termAt(i) }

// Package raft implements the Raft consensus algorithm as a pure, deterministic
// state machine.
//
// INVARIANT I1 (docs/INVARIANTS.md): nothing in this package starts a goroutine, calls
// time.Now, sleeps, touches the network, or touches the disk. The core counts
// Tick() calls and returns a Ready describing work for an outer driver loop to
// perform. See docs/DECISIONS.md ADR-002 for why.
package raft

// None is the zero node ID: "no vote cast", "no leader known". Real node IDs
// start at 1 so that the zero value of a uint64 is always unambiguous.
const None uint64 = 0

// Role is the node's current role. §5.1.
type Role uint8

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader
)

func (r Role) String() string {
	switch r {
	case RoleFollower:
		return "follower"
	case RoleCandidate:
		return "candidate"
	case RoleLeader:
		return "leader"
	}
	return "unknown"
}

// Entry is one log entry. Index starts at 1; index 0 is reserved for the log
// sentinel (see log.go).
//
// Data == nil marks a no-op entry, which a leader appends immediately on
// election so that it has an entry of its own term to commit. RAFT_DESIGN.md §5
// explains why that is load-bearing rather than cosmetic.
type Entry struct {
	Term  uint64
	Index uint64
	Data  []byte
}

// HardState is the state that must survive a crash. Figure 2, "Persistent state
// on all servers".
//
// INVARIANT I5: the driver fsyncs this before sending any message from the same
// Ready. The Ready contract is what makes that ordering structural rather than a
// rule someone has to remember.
type HardState struct {
	Term   uint64
	Vote   uint64
	Commit uint64
}

// MsgType identifies a message. Only the four messages needed for elections and
// replication exist at this milestone; InstallSnapshot arrives with step 7 of
// RAFT_DESIGN.md §10.
type MsgType uint8

const (
	MsgVote MsgType = iota
	MsgVoteResp
	MsgApp
	MsgAppResp
)

func (t MsgType) String() string {
	switch t {
	case MsgVote:
		return "MsgVote"
	case MsgVoteResp:
		return "MsgVoteResp"
	case MsgApp:
		return "MsgApp"
	case MsgAppResp:
		return "MsgAppResp"
	}
	return "unknown"
}

// Message is the union of every RPC in the protocol.
//
// A single flat struct rather than a type per RPC: it keeps Step a single
// switch, and it makes the simulated network trivial because every message has
// the same shape. Fields are documented with the message types that use them.
type Message struct {
	Type MsgType
	From uint64
	To   uint64
	Term uint64

	// MsgVote — the candidate's last log entry, for the §5.4.1 up-to-date check.
	LastLogIndex uint64
	LastLogTerm  uint64

	// MsgApp — the entry immediately preceding Entries, which the receiver uses
	// to enforce the Log Matching Property inductively (§5.3).
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64

	// MsgVoteResp: was the vote granted.
	// MsgAppResp:  did the append succeed.
	Success bool

	// MsgAppResp, on success only: the highest index the follower now holds that
	// is known to agree with the leader. This becomes the leader's matchIndex
	// for that follower — knowledge, never a guess. See RAFT_DESIGN.md §1.
	MatchIndex uint64
}

// Ready describes everything the outside world must do on the node's behalf.
// The core never performs any of it.
//
// The driver must handle the fields in exactly this order:
//
//  1. persist HardState        (fsync)
//  2. append and persist Entries (fsync)
//  3. send Messages
//  4. apply CommittedEntries to the state machine
//  5. call Advance()
//
// Steps 1 and 2 before step 3 is invariant I5. Doing it backwards lets a node
// promise something it then forgets across a crash, which breaks Election Safety.
type Ready struct {
	HardState        *HardState
	Entries          []Entry
	Messages         []Message
	CommittedEntries []Entry
}

// IsEmpty reports whether there is no work to do, letting a driver skip the
// whole persist/send/apply cycle.
func (r Ready) IsEmpty() bool {
	return r.HardState == nil &&
		len(r.Entries) == 0 &&
		len(r.Messages) == 0 &&
		len(r.CommittedEntries) == 0
}

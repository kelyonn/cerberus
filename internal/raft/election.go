package raft

// Leader election: §5.2, and the election restriction of §5.4.1.

// handleVote processes a RequestVote from a candidate.
//
// By the time this runs, Step has already reconciled terms, so m.Term ==
// n.currentTerm. Everything left is the two-part decision from Figure 2.
func (n *Node) handleVote(m Message) {
	// Part one: have we already promised this term to someone else? A repeat
	// request from the same candidate is fine and must be granted again —
	// the original reply may simply have been dropped, and refusing it would
	// stall an election that should succeed.
	canVote := n.votedFor == None || n.votedFor == m.From

	// Part two: the election restriction, §5.4.1. This is the check that
	// guarantees Leader Completeness. A candidate whose log is missing a
	// committed entry cannot win, because a majority of the cluster holds that
	// entry and every one of those nodes fails this comparison.
	upToDate := n.log.isUpToDate(m.LastLogIndex, m.LastLogTerm)

	granted := canVote && upToDate

	if granted {
		n.votedFor = m.From

		// TRAP (RAFT_DESIGN.md §3): reset the election timer ONLY when the vote
		// is actually granted, never when it is denied.
		//
		// Resetting on denial means a node with a stale log — one that keeps
		// asking and keeps being refused — silently keeps every healthy follower
		// from ever timing out and standing for election itself. The cluster
		// stays leaderless with no error anywhere. It looks like a liveness
		// mystery, not a bug in vote handling.
		n.resetElectionTimer()
	}

	// votedFor is now set, and this reply is queued behind it in the same Ready.
	// The driver fsyncs HardState before sending messages, so the promise is
	// durable before it is made (invariant I5).
	n.send(Message{
		Type:    MsgVoteResp,
		To:      m.From,
		Term:    n.currentTerm,
		Success: granted,
	})
}

// handleVoteResp counts a reply to our own RequestVote.
func (n *Node) handleVoteResp(m Message) {
	// Late replies from an election we have already left. Step has confirmed
	// m.Term == n.currentTerm, so this only filters out replies that arrive
	// after we won, or after we stepped down within the same term.
	if n.role != RoleCandidate {
		return
	}

	// First answer from this peer wins; a duplicate must not be counted twice.
	// The simulated network duplicates messages on purpose, so this matters.
	if _, seen := n.votes[m.From]; !seen {
		n.votes[m.From] = m.Success
	}

	granted, denied := 0, 0
	for _, peer := range n.peers { // sorted: invariant I3
		v, voted := n.votes[peer]
		switch {
		case !voted:
		case v:
			granted++
		default:
			denied++
		}
	}

	switch {
	case granted >= n.quorum():
		n.becomeLeader()
	case denied >= n.quorum():
		// The election is unwinnable: a majority has refused. Step down and wait
		// for the next timeout rather than sitting as a candidate until it
		// expires — the wait would be pure dead air.
		n.becomeFollower(n.currentTerm, None)
	}
}

package raft

import "slices"

// Log replication (§5.3) and the commit rule (§5.4.2).

// broadcastAppend sends an AppendEntries to every peer. With no entries to send
// this is a heartbeat, but it is otherwise an ordinary AppendEntries and the
// receiver runs the full consistency check on it — which is how a heartbeat
// carries commitIndex forward and how a lagging follower gets discovered.
func (n *Node) broadcastAppend() {
	for _, peer := range n.peers { // sorted: invariant I3
		if peer == n.id {
			continue
		}
		n.sendAppend(peer)
	}
}

// sendAppend sends one follower everything the leader thinks it is missing.
func (n *Node) sendAppend(to uint64) {
	next := n.nextIndex[to]
	if next < 1 {
		next = 1
	}
	prevIndex := next - 1

	prevTerm, ok := n.log.termAt(prevIndex)
	if !ok {
		// The entry this follower needs has already been compacted away, so we
		// cannot describe it. The fix is InstallSnapshot, which arrives with
		// step 7 of RAFT_DESIGN.md §10. Until then there are no snapshots, so
		// this branch is unreachable; skipping the peer keeps it harmless rather
		// than sending a message with a fabricated prevLogTerm.
		return
	}

	n.send(Message{
		Type:         MsgApp,
		To:           to,
		Term:         n.currentTerm,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      n.log.slice(next, n.log.lastIndex()+1),
		LeaderCommit: n.log.committed,
	})
}

// handleAppend processes an AppendEntries from a leader. Figure 2, receiver
// implementation, steps 1–5. Step already handled step 1 (stale term).
func (n *Node) handleAppend(m Message) error {
	// A candidate that hears from a leader of the current term has lost the
	// election and returns to being a follower.
	if n.role == RoleCandidate {
		n.becomeFollower(m.Term, m.From)
	}
	n.lead = m.From

	// We heard from the current leader, so the election timer restarts. Note the
	// asymmetry with handleVote, which resets only on a granted vote: here the
	// message is proof a leader exists, which is exactly what the timer is
	// waiting to hear.
	n.resetElectionTimer()

	// Step 2: the consistency check. Do we hold prevLogIndex at prevLogTerm?
	//
	// This is the Log Matching Property being maintained inductively: if our log
	// agrees with the leader's at prevLogIndex, then by induction it agrees on
	// everything before it too, so appending what follows keeps the logs
	// identical. If it fails, the leader will retry with a lower nextIndex until
	// it finds the point where the two logs converge.
	if !n.log.matches(m.PrevLogIndex, m.PrevLogTerm) {
		n.send(Message{Type: MsgAppResp, To: m.From, Term: n.currentTerm, Success: false})
		return nil
	}

	// Steps 3 and 4, entry by entry.
	//
	// TRAP (RAFT_DESIGN.md §4) — the truncation bug. Figure 2 says delete on
	// CONFLICT. It does NOT say "truncate to prevLogIndex, then append".
	//
	// The two behave identically until a delayed or duplicated AppendEntries
	// shows up carrying entries we already have. Blind truncation then throws
	// away everything past prevLogIndex — including entries a later RPC already
	// delivered and that the leader has since committed. The result is data
	// loss that appears only under reordering, which means it passes every
	// happy-path test and surfaces once in a few thousand randomized runs.
	for i, e := range m.Entries {
		if e.Index <= n.log.offset() {
			continue // already covered by a snapshot; nothing to check or store
		}

		term, ok := n.log.termAt(e.Index)
		if !ok {
			// Past the end of our log: everything from here is genuinely new.
			return n.appendAndReply(m, m.Entries[i:])
		}
		if term != e.Term {
			// A real conflict: same index, different term. Delete this entry and
			// every entry after it, then take the leader's version.
			if err := n.log.truncateFrom(e.Index); err != nil {
				return err
			}
			return n.appendAndReply(m, m.Entries[i:])
		}
		// Same index, same term. By Log Matching this IS the leader's entry;
		// leave it alone. This is the case blind truncation gets wrong.
	}

	return n.appendAndReply(m, nil)
}

// appendAndReply completes steps 4 and 5 and answers the leader.
func (n *Node) appendAndReply(m Message, toAppend []Entry) error {
	if len(toAppend) > 0 {
		if err := n.log.append(toAppend...); err != nil {
			return err
		}
	}

	// Step 5: advance commitIndex.
	//
	// TRAP (RAFT_DESIGN.md §4): the bound is the index of the last entry IN THIS
	// RPC, not our log's last index. Using lastIndex() lets a follower that is
	// ahead of this particular message — because a later AppendEntries already
	// landed — advance commitIndex over entries this leader has not actually
	// committed.
	lastNewIndex := m.PrevLogIndex + uint64(len(m.Entries))
	if m.LeaderCommit > n.log.committed {
		n.log.commitTo(min(m.LeaderCommit, lastNewIndex))
	}

	// The entries are queued for fsync in the same Ready as this reply, and the
	// driver persists before it sends. So by the time the leader sees
	// success=true, the follower's copy is durable (invariant I5) — which is
	// what makes the leader's replica count mean something.
	n.send(Message{
		Type:       MsgAppResp,
		To:         m.From,
		Term:       n.currentTerm,
		Success:    true,
		MatchIndex: lastNewIndex,
	})
	return nil
}

// handleAppendResp processes a follower's reply on the leader.
func (n *Node) handleAppendResp(m Message) {
	if n.role != RoleLeader {
		return
	}

	if !m.Success {
		// The follower's log diverges from where we guessed. Back nextIndex off
		// by one and try again.
		//
		// One index per round trip is O(log length) RPCs to resynchronize a
		// badly diverged follower. RAFT_DESIGN.md §4.1 describes the fast-backup
		// optimization that skips a whole term per round trip; it is step 10 of
		// the implementation order, deliberately after correctness.
		if n.nextIndex[m.From] > 1 {
			n.nextIndex[m.From]--
		}
		n.sendAppend(m.From)
		return
	}

	// matchIndex is knowledge and only ever moves up. A stale reply that arrives
	// out of order carries an older MatchIndex, and letting it move matchIndex
	// backwards would un-commit an entry.
	if m.MatchIndex > n.matchIndex[m.From] {
		n.matchIndex[m.From] = m.MatchIndex
	}
	n.nextIndex[m.From] = n.matchIndex[m.From] + 1

	if n.maybeCommit() {
		// Tell the followers about the new commitIndex promptly rather than
		// waiting for the next heartbeat, so they can apply.
		n.broadcastAppend()
	}
}

// maybeCommit advances commitIndex if the commit rule allows it, and reports
// whether it moved.
//
// The rule has TWO conditions (RAFT_DESIGN.md §5):
//
//  1. a majority of matchIndex >= N, and
//  2. log[N].Term == currentTerm.
//
// Condition 2 is Figure 8, and it is the subtlest thing in Raft. Without it the
// system loses committed data:
//
// An old leader replicated entry E to a majority but died before committing it.
// A new leader sees E on a majority and — counting replicas alone — concludes it
// is committed. But nothing yet stops a different node, whose log lacks E, from
// winning a later election: the §5.4.1 up-to-date check only compares the LAST
// entry, and that node's last entry can be newer. It then overwrites E on
// everyone. An entry that was reported committed is gone.
//
// Counting replicas is only safe for entries of the leader's OWN term, because
// an entry of the current term on a majority means a majority now has a log the
// up-to-date check will prefer, so no candidate lacking it can ever win.
//
// The consequence is that a fresh leader can commit nothing until it appends an
// entry of its own — which is exactly why becomeLeader appends a no-op.
func (n *Node) maybeCommit() bool {
	matches := make([]uint64, 0, len(n.peers))
	for _, peer := range n.peers { // sorted: invariant I3
		matches = append(matches, n.matchIndex[peer])
	}
	slices.Sort(matches)

	// With matches ascending, the element at len-quorum is the largest N for
	// which a majority reports matchIndex >= N. For 3 nodes that is the median.
	//
	// This uses matchIndex, never nextIndex. nextIndex is a guess that runs
	// ahead of what the follower actually holds; committing on it would commit
	// entries nobody has.
	n0 := matches[len(matches)-n.quorum()]
	if n0 <= n.log.committed {
		return false
	}

	// Condition 2. See the comment above — this is not an optimization.
	if term, ok := n.log.termAt(n0); !ok || term != n.currentTerm {
		return false
	}

	n.log.commitTo(n0)
	return true
}

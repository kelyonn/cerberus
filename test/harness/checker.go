package harness

import (
	"fmt"
	"slices"

	"github.com/kelyonn/cerberus/internal/raft"
)

// Checker asserts the five Raft safety properties from RAFT_DESIGN.md §7 after
// every single step of every run.
//
// Checking all five rather than only the user-visible one is deliberate. S5 is
// what a user would notice, but a violation of S5 typically surfaces thousands
// of steps after the mistake that caused it. S1–S4 fail close to the cause, so
// the trace you get points near the bug instead of near the symptom.
type Checker struct {
	// S1: term -> the node that was observed as leader in it.
	leaderInTerm map[uint64]uint64

	// S2: node -> the log it had while last observed as leader, and in which term.
	leaderLog  map[uint64][]raft.Entry
	leaderTerm map[uint64]uint64

	// S4: index -> what we know about the entry committed at that index.
	committed map[uint64]committedEntry

	tick uint64

	// S5: index -> entry, for every entry any node has actually applied.
	applied map[uint64]raft.Entry
}

// committedEntry is an entry some node has reported as committed, together with
// the one piece of context S4 cannot be checked without.
type committedEntry struct {
	entry raft.Entry

	// commitTerm is an upper bound on the term in which this entry BECAME
	// committed — the lowest term of any node we have seen holding it below its
	// commitIndex.
	//
	// This is not the same as entry.Term, and conflating the two produces false
	// S4 failures. An entry can be appended in term 1 and only become committed
	// in term 10: the Figure 8 rule forbids a leader from committing an
	// inherited entry directly, so old entries get committed later, as a side
	// effect of committing an entry of the current term. Leader Completeness
	// constrains leaders of terms after the entry was COMMITTED; a leader
	// elected in term 6, before the entry committed in term 10, is entitled to
	// know nothing about it.
	commitTerm uint64

	// Where the claim came from, for reading failures.
	node uint64
	tick uint64
}

func (c committedEntry) String() string {
	return fmt.Sprintf("entry %d@%d, observed committed by tick %d on node %d, committed no later than term %d",
		c.entry.Index, c.entry.Term, c.tick, c.node, c.commitTerm)
}

func NewChecker() *Checker {
	return &Checker{
		leaderInTerm: make(map[uint64]uint64),
		leaderLog:    make(map[uint64][]raft.Entry),
		leaderTerm:   make(map[uint64]uint64),
		committed:    make(map[uint64]committedEntry),
		applied:      make(map[uint64]raft.Entry),
	}
}

// SetTick tells the checker which tick it is observing, for provenance.
func (c *Checker) SetTick(t uint64) { c.tick = t }

// Violation describes a broken safety property.
type Violation struct {
	Property string
	Detail   string
}

func (v *Violation) Error() string { return v.Property + ": " + v.Detail }

// Check runs all five properties against the current cluster state. ids must be
// sorted so that the property that fails first is a function of the seed and
// not of map order (invariant I3).
func (c *Checker) Check(ids []uint64, nodes map[uint64]*raft.Node) error {
	for _, check := range []func([]uint64, map[uint64]*raft.Node) error{
		c.checkS1ElectionSafety,
		c.checkS2LeaderAppendOnly,
		c.checkS3LogMatching,
		c.checkS4LeaderCompleteness,
	} {
		if err := check(ids, nodes); err != nil {
			return err
		}
	}
	return nil
}

// S1 — Election Safety: at most one leader per term.
//
// This is the property that persistence exists to protect. If votedFor reaches
// disk after the vote reply rather than before, a node that crashes in between
// can vote twice in one term, and two candidates can both reach a majority.
func (c *Checker) checkS1ElectionSafety(ids []uint64, nodes map[uint64]*raft.Node) error {
	for _, id := range ids {
		st := nodes[id].Status()
		if st.Role != raft.RoleLeader {
			continue
		}
		if prev, seen := c.leaderInTerm[st.Term]; seen && prev != id {
			return &Violation{
				Property: "S1 Election Safety",
				Detail:   fmt.Sprintf("two leaders in term %d: node %d and node %d", st.Term, prev, id),
			}
		}
		c.leaderInTerm[st.Term] = id
	}
	return nil
}

// S2 — Leader Append-Only: a leader never overwrites or deletes entries in its
// own log; it only appends.
func (c *Checker) checkS2LeaderAppendOnly(ids []uint64, nodes map[uint64]*raft.Node) error {
	for _, id := range ids {
		st := nodes[id].Status()
		if st.Role != raft.RoleLeader {
			continue
		}
		now := nodes[id].LogEntries()

		// Only comparable across observations within one leadership term.
		if prev, ok := c.leaderLog[id]; ok && c.leaderTerm[id] == st.Term {
			if len(now) < len(prev) {
				return &Violation{
					Property: "S2 Leader Append-Only",
					Detail: fmt.Sprintf("node %d (leader, term %d) log shrank from %d to %d entries",
						id, st.Term, len(prev), len(now)),
				}
			}
			for i := range prev {
				if now[i].Index != prev[i].Index || now[i].Term != prev[i].Term {
					return &Violation{
						Property: "S2 Leader Append-Only",
						Detail: fmt.Sprintf("node %d (leader, term %d) rewrote entry at position %d: was %d@%d, now %d@%d",
							id, st.Term, i, prev[i].Index, prev[i].Term, now[i].Index, now[i].Term),
					}
				}
			}
		}
		c.leaderLog[id] = now
		c.leaderTerm[id] = st.Term
	}
	return nil
}

// S3 — Log Matching: if two logs contain an entry with the same index and term,
// the logs are identical in every entry up through that index.
//
// This is the property the prevLogIndex/prevLogTerm consistency check maintains
// inductively, and it is the one the truncation bug breaks.
func (c *Checker) checkS3LogMatching(ids []uint64, nodes map[uint64]*raft.Node) error {
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			a, b := nodes[ids[i]], nodes[ids[j]]
			la, lb := a.LogEntries(), b.LogEntries()

			maxIdx := uint64(len(la))
			if uint64(len(lb)) < maxIdx {
				maxIdx = uint64(len(lb))
			}

			// Walk down from the highest shared index looking for agreement;
			// everything below the first agreeing entry must then match too.
			for idx := maxIdx; idx >= 1; idx-- {
				ta, oka := a.TermAt(idx)
				tb, okb := b.TermAt(idx)
				if !oka || !okb || ta != tb {
					continue
				}
				for k := uint64(1); k <= idx; k++ {
					ea, _ := a.TermAt(k)
					eb, _ := b.TermAt(k)
					if ea != eb {
						return &Violation{
							Property: "S3 Log Matching",
							Detail: fmt.Sprintf("nodes %d and %d agree at index %d (term %d) but differ at index %d (term %d vs %d)",
								ids[i], ids[j], idx, ta, k, ea, eb),
						}
					}
				}
				break
			}
		}
	}
	return nil
}

// S4 — Leader Completeness: an entry committed in term T is present in the log
// of every leader of every term greater than T.
//
// This is the property the §5.4.1 up-to-date check buys, and the one the Figure
// 8 commit rule protects. A leader that is missing a committed entry is the
// precise moment data loss becomes inevitable, and catching it here is much
// better than catching the resulting divergence later.
func (c *Checker) checkS4LeaderCompleteness(ids []uint64, nodes map[uint64]*raft.Node) error {
	// Record everything currently committed anywhere, tracking the tightest
	// (lowest) term in which we have seen each entry committed.
	for _, id := range ids {
		st := nodes[id].Status()
		for _, e := range nodes[id].LogEntries() {
			if e.Index > st.Commit {
				continue
			}
			prev, seen := c.committed[e.Index]
			if seen && prev.commitTerm <= st.Term {
				continue
			}
			c.committed[e.Index] = committedEntry{
				entry: e, commitTerm: st.Term, node: id, tick: c.tick,
			}
		}
	}

	// Check in index order so that a failure reports the lowest offending index
	// rather than whichever the map happened to yield first (invariant I3).
	indices := make([]uint64, 0, len(c.committed))
	for idx := range c.committed {
		indices = append(indices, idx)
	}
	slices.Sort(indices)

	for _, id := range ids {
		st := nodes[id].Status()
		if st.Role != raft.RoleLeader {
			continue
		}
		for _, idx := range indices {
			want := c.committed[idx]

			// Only leaders of terms AFTER the entry became committed are
			// constrained. A leader from an earlier term — typically a stale one
			// sitting in a minority partition — is entitled to be missing it, and
			// will have it overwritten when the partition heals.
			if st.Term <= want.commitTerm {
				continue
			}

			got, ok := nodes[id].TermAt(idx)
			if !ok {
				return &Violation{
					Property: "S4 Leader Completeness",
					Detail: fmt.Sprintf("node %d is leader in term %d but is missing index %d\n    %s",
						id, st.Term, idx, want),
				}
			}
			if got != want.entry.Term {
				return &Violation{
					Property: "S4 Leader Completeness",
					Detail: fmt.Sprintf("node %d is leader in term %d but holds %d@%d\n    %s",
						id, st.Term, idx, got, want),
				}
			}
		}
	}
	return nil
}

// ObserveApply records an entry being handed to a state machine and enforces
// S5 — State Machine Safety: if a server has applied an entry at a given index,
// no server ever applies a different entry at that index.
//
// S5 is the user-visible property. Everything else exists to guarantee it.
func (c *Checker) ObserveApply(node uint64, e raft.Entry) error {
	if prev, seen := c.applied[e.Index]; seen {
		if prev.Term != e.Term {
			return &Violation{
				Property: "S5 State Machine Safety",
				Detail: fmt.Sprintf("index %d applied as term %d elsewhere, but node %d applied term %d",
					e.Index, prev.Term, node, e.Term),
			}
		}
	}
	c.applied[e.Index] = e
	return nil
}

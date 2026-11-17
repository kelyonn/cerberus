# Cerberus — Glossary

Precise definitions. Sloppy terminology in a consensus system causes real
confusion — several of the pairs below get conflated constantly, and each
conflation maps to a specific class of bug.

---

## Raft mechanics

**Term** — A logical clock. A monotonically increasing integer, not a period of
wall-clock time. Each term begins with an election and has at most one leader.
Terms let nodes detect stale information: any message carrying a term higher than
your own means you are behind, and you become a follower immediately.

**Index** — Position of an entry in the log. Starts at 1. Index 0 is a sentinel
holding the snapshot boundary once compaction has happened.

**Entry** — `{Term, Index, Data}`. The unit of replication.

**Committed** — An entry is committed when it is durably replicated on a majority
*and* the leader has established it will survive all future leaders. It is
guaranteed never to be lost or reordered. **Not the same as applied.**

**Applied** — An entry has been handed to the state machine and mutated it. Commit
always precedes apply. `lastApplied <= commitIndex` always.

> Conflating *committed* and *applied* is the most common vocabulary error here.
> An entry can be committed on a node that hasn't applied it yet — that gap is
> exactly what ReadIndex has to wait out before serving a linearizable read.

**`commitIndex`** — Highest index known to be committed. Volatile; recomputed
after restart.

**`lastApplied`** — Highest index applied to the state machine. Volatile.

**`matchIndex[peer]`** — **Knowledge.** Highest index the leader *knows* is
replicated on that peer. Only ever increases. The commit rule uses this.

**`nextIndex[peer]`** — **A guess.** The next index the leader intends to send.
Initialised optimistically to `lastLogIndex + 1` and corrected *downward* when the
follower rejects. Never use this for the commit rule.

**Quorum / majority** — A strict majority, `⌊n/2⌋ + 1`. For 3 nodes that's 2; for
5 it's 3. Any two quorums of the same cluster necessarily overlap in at least one
node, and that overlap is the entire basis of Raft's safety.

**Heartbeat** — An `AppendEntries` with no entries. Suppresses follower election
timeouts and (with a majority response) proves current leadership.

**No-op entry** — An empty entry a new leader appends immediately on election. Its
purpose is to get an entry *of the current term* into the log, which is what lets
the commit rule advance past entries inherited from previous terms.

---

## Roles

**Follower** — Passive. Responds to RPCs, never initiates. Becomes a candidate on
election timeout.

**Candidate** — Standing for election in a term it incremented itself. Becomes
leader on a majority of votes, follower on discovering a current leader or higher
term.

**Leader** — Handles all client requests, replicates entries, advances
`commitIndex`. Exactly one per term, at most.

**Learner / non-voting member** — A node that receives replication but does not
vote and does not count toward quorum. New nodes join as learners so they can
catch up on a large log without stalling elections, then get promoted.

---

## Storage

**Snapshot** — A serialized state machine at a particular index, plus
`lastIncludedIndex`, `lastIncludedTerm`, and the cluster configuration at that
point. Replaces the log prefix it covers. **Includes the session table.**

**Log compaction** — Discarding the log prefix a snapshot covers. Introduces an
index offset, which is why all index arithmetic lives behind methods.

**`HardState`** — The state that must survive a crash: `currentTerm`, `votedFor`,
and the log. `fsync`'d before any dependent RPC is answered.

**Torn write** — A record partially written when the process died mid-`fsync`.
Detected by CRC on recovery and truncated. Safe to discard, because it was never
acknowledged to a client.

---

## Correctness

**Linearizability** — A specific formal property, not a synonym for "consistent."
Every operation appears to take effect instantaneously at some single point
between its invocation and its response, and that ordering is consistent with real
time. This is the strongest single-object consistency model.

**Stale read** — A read returning a value older than the most recent completed
write. The reason a partitioned ex-leader must not serve reads from local state
without first confirming leadership.

**ReadIndex** — The protocol for linearizable reads without a log append: record
`commitIndex`, confirm leadership with a heartbeat round to a majority, wait for
`lastApplied >= readIndex`, then read locally.

**Session table** — Per-client `(clientID, seqNum) → result` map inside the state
machine. Suppresses duplicate application of retried requests. Part of the
snapshot.

**Split vote** — Multiple candidates in the same term, none reaching a majority.
Resolved by randomized election timeouts, which is why the timeout must never be a
fixed value.

**Split brain** — Two nodes both believing they are leader and both serving. Raft
prevents the *harmful* form via quorum overlap: an ex-leader may briefly still
think it leads, but it cannot commit anything, and ReadIndex stops it serving
stale reads.

---

## Testing

**Deterministic simulation** — Running the whole cluster in one goroutine with a
simulated clock and a seeded network, so any execution reproduces exactly from its
seed. The premise of this project's entire verification strategy.

**Seed** — The integer that determines a whole run: timeouts, delays, drops,
workload. A bug report here is a seed number.

**Fault injection** — Programmatically dropping, delaying, reordering, or
duplicating messages, and crashing/partitioning nodes.

**Soak test** — Long randomized run (10k seeds × 50k steps) to surface rare
interleavings.

**Invariant checker** — Code asserting S1–S5 after every step, so a violation is
caught where it happens rather than a thousand steps later when a client sees a
bad read.

**Porcupine** — The Go linearizability checker used here. Takes a concurrent
operation history and a sequential model, searches for a valid linearization.

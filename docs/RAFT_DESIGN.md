# Cerberus — Raft Implementation Spec

Reference: *In Search of an Understandable Consensus Algorithm (Extended Version)*,
Ongaro & Ousterhout. Section numbers below refer to that paper. Read §5 in full
before writing code; it is about 9 pages and it is the whole algorithm.

This document is the spec you implement against. It states the rules, then the
traps — the traps are where the time goes.

---

## 1. State

### Persistent on every server
Must survive a crash. `fsync` before responding to any RPC that depends on it.

| Field | Meaning |
|---|---|
| `currentTerm` | Latest term this server has seen. Starts at 0, increases monotonically. |
| `votedFor` | Candidate that received this server's vote in `currentTerm`, or none. |
| `log[]` | Entries, each `{Term, Index, Data}`. First index is 1. |

### Volatile on every server

| Field | Meaning |
|---|---|
| `commitIndex` | Highest index known committed. Initialised 0. |
| `lastApplied` | Highest index applied to the state machine. Initialised 0. |

### Volatile on leaders — reinitialised after every election

| Field | Meaning |
|---|---|
| `nextIndex[peer]` | Next index to send to that peer. Init to `lastLogIndex + 1`. |
| `matchIndex[peer]` | Highest index known replicated on that peer. Init to 0. |

`nextIndex` is a *guess* that gets corrected downward on rejection. `matchIndex`
is *knowledge* and only ever moves up. Conflating them is a common early bug: the
commit rule must use `matchIndex`, never `nextIndex`.

---

## 2. The three roles

```
                  times out, starts election
      ┌────────┐ ──────────────────────────► ┌───────────┐
      │Follower│                             │ Candidate │
      └────────┘ ◄────────────────────────── └───────────┘
           ▲       discovers current leader        │
           │       or higher term                  │ wins majority
           │                                       ▼
           │        discovers higher term    ┌──────────┐
           └─────────────────────────────────│  Leader  │
                                             └──────────┘
```

**Rule that applies to all roles, always:** if any RPC request or response
carries a term `T > currentTerm`, set `currentTerm = T`, clear `votedFor`, and
convert to follower. Check this *first*, before any other handling of the message.

---

## 3. RequestVote RPC

**Arguments:** `term`, `candidateID`, `lastLogIndex`, `lastLogTerm`
**Results:** `term`, `voteGranted`

Receiver logic:

1. If `term < currentTerm`, reply `voteGranted = false`.
2. If (`votedFor` is none or equals `candidateID`) **and** the candidate's log is
   at least as up-to-date as ours, grant the vote and reset the election timer.

"At least as up-to-date" (§5.4.1) compares the *last entry*:
- Higher `lastLogTerm` wins.
- Equal terms: longer log (higher `lastLogIndex`) wins.

This check is what guarantees the Leader Completeness Property. A candidate
missing a committed entry cannot win, because a majority of nodes hold that entry
and every one of them will refuse the vote.

> **Trap.** Reset the election timer *only* when you actually grant a vote — not
> when you deny one. Resetting on denial lets a node with a stale log repeatedly
> suppress healthy candidates and stall elections indefinitely.

> **Trap.** `votedFor` must be `fsync`'d *before* the `voteGranted: true` reply
> goes out. If the node crashes after replying but before persisting, it can vote
> for a different candidate in the same term after restart, producing two leaders
> in one term.

---

## 4. AppendEntries RPC

**Arguments:** `term`, `leaderID`, `prevLogIndex`, `prevLogTerm`, `entries[]`, `leaderCommit`
**Results:** `term`, `success`

Receiver logic:

1. Reply `false` if `term < currentTerm`.
2. Reply `false` if the log has no entry at `prevLogIndex` whose term equals
   `prevLogTerm`. *(This is the Log Matching Property doing its work.)*
3. If an existing entry conflicts with a new one (same index, different term),
   delete that entry **and everything after it**.
4. Append any new entries not already present.
5. If `leaderCommit > commitIndex`, set `commitIndex = min(leaderCommit, index of last new entry)`.

An `entries` array of length zero is a heartbeat, but it is otherwise an ordinary
`AppendEntries` and steps 1–5 apply to it unchanged.

> **Trap — the truncation bug.** Step 3 says delete on *conflict*. It does not say
> "truncate to `prevLogIndex` then append." Those differ when a delayed or
> duplicated RPC arrives carrying entries you already have plus some you already
> have *beyond* them. Blind truncation throws away committed entries and is one of
> the nastiest bugs in this project — it surfaces as data loss under reordering,
> not under normal operation. Compare entry-by-entry and truncate only from the
> first genuine mismatch.

> **Trap.** In step 5, it is `min(leaderCommit, index of last new entry)` — the
> last entry *in this RPC*, not your log's last index. Using the latter lets a
> lagging follower advance `commitIndex` past what it actually holds.

### 4.1 Fast backup (optimization, do it in week 3)

Decrementing `nextIndex` by one per round trip is `O(log length)` RPCs to
resynchronize a badly diverged follower. The paper's §5.3 footnote suggests the
rejection reply carry `conflictTerm` and `conflictIndex` so the leader can skip a
whole term at a time. Worth doing, but only once the basic path is correct.

---

## 5. The commit rule, and the trap in it

A leader advances `commitIndex` to `N` when **both** hold:

1. A majority of `matchIndex[i] >= N`, **and**
2. `log[N].Term == currentTerm`.

Condition 2 is not an optimization. Without it the system loses committed data.

This is **Figure 8** in the paper and it is the subtlest thing in Raft. The
scenario: a leader from an old term replicated an entry to a majority but died
before committing it. A new leader sees that entry replicated on a majority and
concludes it is committed — but a *different* node can still win a later election
with a log that lacks the entry, and overwrite it. Counting replicas is only safe
for entries in your own term.

The consequence: a freshly elected leader cannot commit anything until it appends
an entry of its own. Standard practice is to append a **no-op entry** immediately
on election. Doing so also advances `commitIndex` past any inherited entries,
which is what makes ReadIndex usable right after an election.

**Implement the no-op-on-election from the start.** Adding it later means
re-testing everything.

---

## 6. Snapshots and log compaction

The log cannot grow forever. When it exceeds a threshold, the state machine
serializes itself and the log is truncated.

A snapshot contains:
- `lastIncludedIndex`, `lastIncludedTerm` — the entry it replaces
- the cluster configuration at that point
- the serialized state machine, **including the session table**

After truncation, `log[0]` is a sentinel holding `lastIncludedIndex/Term`, so that
`prevLogTerm` lookups at the boundary still work. Every index calculation in the
codebase now needs an offset. Get this wrong and you get off-by-one panics
everywhere.

> **Strong recommendation.** Put *all* index arithmetic behind methods on the log
> type — `log.at(i)`, `log.lastIndex()`, `log.termAt(i)`, `log.slice(lo, hi)` —
> from day one, before snapshots exist. Retrofitting an offset into raw slice
> indexing scattered across five files is genuinely miserable, and it is the most
> predictable time-sink in this project.

### InstallSnapshot RPC

Sent when a follower needs an entry the leader has already discarded.

**Arguments:** `term`, `leaderID`, `lastIncludedIndex`, `lastIncludedTerm`, `offset`, `data[]`, `done`

Receiver: reply immediately if `term < currentTerm`. Otherwise write the chunk;
when `done`, discard any log prefix covered by the snapshot, restore the state
machine from it, and set `lastApplied = commitIndex = lastIncludedIndex`.

> **Trap.** If the follower's log already contains `lastIncludedIndex` with a
> matching term, keep the entries *after* it rather than wiping the whole log. A
> follower that is only slightly behind should not be reset to the snapshot point.

---

## 7. Safety properties — assert these in tests

Each becomes a checker in the harness (`TESTING.md` §4). Assert them after every
step of every randomized run.

| # | Property | Assertion |
|---|---|---|
| S1 | **Election Safety** | At most one leader per term. |
| S2 | **Leader Append-Only** | A leader never overwrites or deletes entries in its own log. |
| S3 | **Log Matching** | If two logs share an entry with the same index and term, they are identical up through that index. |
| S4 | **Leader Completeness** | An entry committed in term T appears in the log of every leader of every term > T. |
| S5 | **State Machine Safety** | If a server applied an entry at index i, no server ever applies a different entry at index i. |

S5 is the user-visible one; S1–S4 exist to guarantee it. Checking all five gives
you failures that point near their cause instead of failures that surface a
thousand steps downstream.

---

## 8. Membership changes

Cerberus uses **single-server changes** (Ongaro's thesis §4.1), not joint
consensus. One node added or removed at a time, so old and new majorities always
overlap and no two-phase protocol is needed.

Rules:

- The change is a special log entry. A server adopts a new configuration **as
  soon as it appends the entry**, not when it commits — using the committed
  config would deadlock the change.
- Only one uncommitted configuration change in flight at a time.
- New nodes join as **non-voting learners** first and catch up before being
  promoted, so an empty node cannot stall elections while it replicates a
  large log.
- A leader that removes itself steps down once the removal entry commits.

> **Trap.** A removed server that hasn't learned it was removed will time out and
> start elections, disrupting the cluster with higher terms. Mitigation: servers
> ignore `RequestVote` received within the minimum election timeout of hearing
> from a current leader (paper §6, "disruptive servers").

---

## 9. Timing

```
broadcastTime  ≪  electionTimeout  ≪  MTBF
```

| Parameter | Value | Notes |
|---|---|---|
| Heartbeat interval | 50 ms | |
| Election timeout | randomized 150–300 ms | Randomization is what breaks split votes — do not use a fixed value. |
| Tick interval | 10 ms | The core counts ticks, never wall-clock. |

The Raft core must **never** call `time.Now()`. It counts `Tick()` calls. That is
what lets the harness run a simulated hour in a millisecond.

---

## 10. Implementation order

Follow this. Each step is independently testable and each depends on the last.

| # | Step | Done when |
|---|---|---|
| 1 | State types, log with all index math behind methods | Unit tests on the log type pass |
| 2 | `Tick`/`Step`/`Ready`/`Advance` skeleton, no logic | Compiles, does nothing, cleanly |
| 3 | Leader election | 3 nodes elect one leader; re-elect after killing it |
| 4 | Log replication + commit rule (with no-op on election) | Entries replicate and commit on a majority |
| 5 | Persistence + crash recovery | Restarted node rejoins with intact state |
| 6 | KV state machine + session dedup | `Put`/`Get` work; retries don't double-apply |
| 7 | Snapshotting + `InstallSnapshot` | Lagging node catches up after compaction |
| 8 | Linearizable reads (ReadIndex) | Stale reads gone from a partitioned old leader |
| 9 | Membership changes | Add and remove a node under live load |
| 10 | Fast backup, batching, pipelining | Only after 1–9 are green |

Do not start step N+1 while step N has a known failing test. In a system this
stateful, bugs compound multiplicatively — two unfixed bugs are far more than
twice as hard to isolate as one.

# Cerberus — Testing & Verification Strategy

This is the document that makes Cerberus worth building. A Raft implementation
that passes happy-path tests is a weekend project. One that is *demonstrably*
linearizable under randomized fault injection is not.

Build this infrastructure in **week 1**, before the hard bugs exist. It is the
single highest-leverage decision in the project, and building it late means
debugging the hard weeks without it.

---

## 1. The problem it solves

The characteristic Raft bug looks like this:

> A test passes 399 times. On run 400 a committed entry disappears.

With sleeps, real timers, and real sockets, you cannot reproduce that. You get
one corrupted log, no idea what sequence produced it, and no way to tell whether
your fix worked or just made the window smaller. Making a bug rarer instead of
fixing it is the standard failure mode of AI-assisted work on this project, and
it is worse than not fixing it — now it fails on run 4000.

The fix is determinism. Convert "1 in 400" into "seed 8823 fails every time."
Then it's a normal bug.

---

## 2. Determinism requirements

Four rules. Break any one and the whole thing stops working.

**D1 — No wall-clock time in the core.** The Raft core counts `Tick()` calls. The
harness controls the clock and advances it explicitly. No `time.Now()`, no
`time.Sleep`, no `time.After` anywhere under `internal/raft`.

**D2 — No goroutines in the core.** `Tick`/`Step`/`Ready`/`Advance` only. All
concurrency lives in the driver loop, which the harness replaces.

**D3 — One seeded RNG, threaded explicitly.** Election timeouts, message delays,
drop decisions, and workload generation all draw from a `*rand.Rand` created from
the run's seed. No package-level `rand`. No `map` iteration influencing behaviour
— Go randomizes map order deliberately, and it will silently destroy determinism.

**D4 — Total message ordering under harness control.** The simulated transport
holds an ordered queue. The harness decides delivery order. Nothing is delivered
by a background goroutine.

Write a test that asserts D1–D4 hold: run the same seed twice, hash the full
cluster state after every step, compare. If the hashes ever diverge, determinism
has broken and every other test in this document is now lying to you. Run it in
CI.

---

## 3. The harness

`test/harness/` — a whole cluster in one goroutine.

```go
type Cluster struct {
    nodes   map[uint64]*Node
    net     *SimNetwork
    rng     *rand.Rand
    seed    int64
    steps   int
}

func NewCluster(seed int64, n int) *Cluster

func (c *Cluster) Step()                              // one unit of simulated time
func (c *Cluster) Partition(groups ...[]uint64)       // split the network
func (c *Cluster) Heal()                              // remove all partitions
func (c *Cluster) Crash(id uint64)                    // lose volatile state
func (c *Cluster) Restart(id uint64)                  // recover from disk
func (c *Cluster) Isolate(id uint64)                  // one-node partition
func (c *Cluster) SetLatency(min, max int)            // in ticks
func (c *Cluster) SetDropRate(p float64)
func (c *Cluster) Check() error                       // assert S1–S5
```

`Step()` does, in order: deliver whatever the network schedules for this tick,
call `Tick()` on every live node, drain each node's `Ready()` and apply its side
effects through the simulated storage and transport, then run the invariant
checks.

A 10,000-step run — simulating ~100 seconds of cluster time, several elections,
and dozens of partitions — should complete in well under a second. If it's slow,
something is doing real I/O or real sleeping.

### 3.1 SimNetwork

Programmable faults:

| Fault | Meaning |
|---|---|
| Drop | Message discarded. Raft must tolerate this. |
| Delay | Delivered N ticks later, drawn from the seeded RNG. |
| Reorder | A natural consequence of per-message delays. |
| Duplicate | Same message delivered twice. Raft must be idempotent. |
| Partition | Message dropped iff endpoints are in different groups. |

Duplication matters more than it looks — it is what surfaces the truncation bug
in `RAFT_DESIGN.md` §4.

---

## 4. Invariant checking

After **every** step of **every** run, assert the five safety properties from
`RAFT_DESIGN.md` §7.

Cheapest and most valuable of the five to implement first:

```go
// S5 — State Machine Safety.
// The harness keeps a global map: index → the entry applied there, by anyone.
// If a node applies a different entry at an index already recorded, the run
// aborts immediately with the full history.
func (c *Cluster) checkAppliedConsistency(nodeID uint64, e Entry) error {
    if prev, ok := c.applied[e.Index]; ok && !prev.Equal(e) {
        return fmt.Errorf(
            "S5 violated at index %d: node %d applied %v, but %v was applied earlier",
            e.Index, nodeID, e, prev)
    }
    c.applied[e.Index] = e
    return nil
}
```

Failing at the step where the invariant breaks — rather than a thousand steps
later when a client sees a bad read — is the difference between a two-hour bug
and a two-day one.

---

## 5. Linearizability checking

Invariants catch *internal* corruption. Linearizability checking catches
*externally visible* wrongness — including bugs that leave every Raft invariant
intact, like a broken session table or a stale ReadIndex.

Use **Porcupine** (`github.com/anishathalye/porcupine`).

Record a history of client operations, each with invocation time, response time,
input, and output:

```go
type Op struct {
    ClientID int
    Kind     OpKind   // Put | Get | Delete | CAS
    Key      string
    Value    string
    Output   string
    Call     int64    // simulated tick at invocation
    Return   int64    // simulated tick at response
}
```

Then define the sequential model — what a correct single-threaded KV store would
do — and Porcupine searches for a valid linearization of the concurrent history.
If none exists, the implementation is not linearizable and you get the offending
history back.

**Operations that time out must stay in the history as unknown-outcome.** They
may or may not have taken effect, and the checker handles that ambiguity
correctly. Dropping them hides exactly the bugs you are hunting.

---

## 6. Test tiers

| Tier | What | When |
|---|---|---|
| **Unit** | Log index math, term comparison, up-to-date check, session table | Every save |
| **Deterministic scenarios** | Hand-written: split vote, partitioned leader, log divergence, snapshot catch-up | Every save |
| **Randomized (short)** | 500 seeds × 5k steps, random faults, invariants + linearizability | Every commit |
| **Soak** | 10,000 seeds × 50k steps | Nightly CI |
| **Real-network integration** | 3 real processes, real gRPC, `SIGKILL` restarts | Before calling it done |

The last tier matters: it is what catches bugs in the parts the simulation
*replaces* — real `fsync` behaviour, torn writes, actual socket semantics. The
simulation proves the algorithm; the integration tests prove the plumbing.

---

## 7. Scenarios that must be tested explicitly

Randomized testing finds these eventually. Write them by hand anyway, because a
named failing test is worth ten times a seed number when you're debugging.

1. **Split vote** — two candidates, same term, no majority. Resolves on the next
   randomized timeout.
2. **Partitioned leader** — isolate the leader; a new one is elected; heal; the
   old leader steps down and its uncommitted entries are correctly overwritten.
3. **Figure 8** — the commit rule scenario from `RAFT_DESIGN.md` §5. Construct it
   deliberately and verify the entry from the previous term is *not* committed by
   replica count alone.
4. **Log divergence** — a follower with a long conflicting suffix converges.
5. **Snapshot catch-up** — compact the leader's log past a lagging follower's
   `nextIndex`, verify `InstallSnapshot` fires and the follower converges.
6. **Duplicate client request** — same `(clientID, seqNum)` twice, verify one
   effect and identical responses.
7. **Stale read from a deposed leader** — partition the leader, write via the new
   leader, read from the old one. Must not return stale data.
8. **Crash mid-`fsync`** — truncated final record, verify CRC detection and
   correct recovery.
9. **Restart the entire cluster** — all nodes down, all back up. State intact,
   new leader elected.
10. **Membership change under load** — add and remove a node while writes are
    in flight. No lost writes.

---

## 8. CI

```yaml
on: [push]
jobs:
  test:
    - go vet ./...
    - go test -race ./...
    - go test -run TestDeterminism ./test/harness    # D1–D4
    - go test -run TestRandomized -seeds=500 ./test/chaos
    - go test -run TestLinearizability ./test/linz
```

`-race` is not optional. It catches the class of bug that determinism testing
cannot, because it lives in the driver loop rather than the core.

---

## 9. When you find a bug

Every real bug the harness catches goes in `DESIGN.md`:

1. **Symptom** — what failed, which invariant, which seed.
2. **Trace** — the relevant slice of the history.
3. **Root cause** — which rule was violated and why the code violated it.
4. **Fix** — the change, and the regression test that now pins it.

This file is the most valuable output of the project. It is direct evidence that
you understand the algorithm rather than having transcribed it, and it is what
turns an interview question into a story you actually have.

Aim for three or four entries. You will not have to manufacture them.

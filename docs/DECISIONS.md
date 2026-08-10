# Cerberus — Architecture Decisions

Append-only. Each entry records what was decided, what was rejected, and why.
Never rewrite history here — if a decision is reversed, add a new entry that
supersedes the old one and mark the old one.

Format: context → decision → alternatives rejected → consequences.

---

## ADR-001 — Implement Raft from the paper, not a library

**Status:** Accepted · 2026-08-06

**Context.** The project needs a consensus layer. `hashicorp/raft` and
`etcd/raft` are both mature and would work.

**Decision.** Implement Raft from the paper.

**Rejected.**
- *`hashicorp/raft`* — reduces the project to configuring a library. The
  understanding this project exists to produce would not happen.
- *Paxos / Viewstamped Replication* — worse specified, far worse documented, and
  the learning-per-hour is lower. Multi-Paxos in particular has no canonical
  description precise enough to implement against.
- *A homegrown protocol* — unverifiable, and nobody should trust it.

**Consequences.** Longest path to a working system, and the debugging is the bulk
of the schedule. Accepted deliberately: that debugging *is* the deliverable.

---

## ADR-002 — The Raft core is a pure, deterministic state machine

**Status:** Accepted · 2026-08-06

**Context.** The natural Go design is a goroutine per peer with timers and
channels. It is also the design under which a bug that appears once in 400 runs
can never be reproduced.

**Decision.** The core exposes `Tick`/`Step`/`Ready`/`Advance` and contains no
goroutines, no timers, and no I/O. It returns descriptions of work; an outer
driver loop performs the side effects. This follows `etcd/raft`.

**Rejected.**
- *Goroutine-per-peer with real timers* — simpler to write, impossible to debug.
  Failures are irreproducible, and "fixes" that merely narrow the race are
  indistinguishable from real ones.
- *Goroutines with a mockable clock interface* — better, but scheduling
  nondeterminism remains. Doesn't get to reproducibility.

**Consequences.**
- A whole cluster runs in one goroutine under a simulated clock; runs reproduce
  byte-for-byte from a seed. This is what makes `docs/TESTING.md` possible.
- The `Ready` contract structurally enforces persist-before-send (I5).
- Cost: the driver loop is fiddlier, and the discipline must hold everywhere. One
  `time.Now()` in the core silently destroys it.

---

## ADR-003 — Build the deterministic harness before the algorithm

**Status:** Accepted · 2026-08-06

**Context.** The instinct is to get a leader elected first and add tests after.

**Decision.** The simulated network, clock, and cluster harness are built on Day
2, against a stub node, before any Raft logic exists.

**Rejected.** *Harness after a working happy path* — this is the standard order
and it means doing the hardest debugging of the project without the tool that
makes it tractable. By the time it hurts enough to build the harness, there are
several interacting bugs.

**Consequences.** Two days before anything visibly "works." Every bug after that
is reproducible from a seed.

---

## ADR-004 — Single-server membership changes, not joint consensus

**Status:** Accepted · 2026-08-06

**Context.** The paper describes joint consensus (§6); Ongaro's thesis (§4.1)
describes the simpler single-server approach.

**Decision.** Single-server changes. One node added or removed at a time, so old
and new majorities always overlap.

**Rejected.** *Joint consensus* — handles simultaneous multi-node changes, needs a
two-phase `C_old,new` configuration, materially more complex, and this project has
no use case requiring it.

**Consequences.** Cannot swap multiple nodes atomically; changes are serialized.
Requires learner state so a new empty node doesn't stall elections while it
catches up.

---

## ADR-005 — ReadIndex for linearizable reads, not lease reads

**Status:** Accepted · 2026-08-06

**Context.** Serving reads from the leader's memory is not linearizable — a
partitioned ex-leader will serve stale data. Two standard fixes exist.

**Decision.** ReadIndex: confirm leadership via a heartbeat round to a majority,
wait for `lastApplied >= readIndex`, then read locally.

**Rejected.** *Lease reads* — faster (no heartbeat round) but correctness depends
on bounded clock drift between nodes. Trading a safety assumption for latency is
not a trade worth making before the safe version demonstrably works. Listed as a
stretch goal in `docs/ROADMAP.md`.

**Consequences.** One heartbeat round per read batch, amortizable across
concurrent reads. No log append, so still much cheaper than routing reads through
consensus.

---

## ADR-006 — Append-only log + in-memory map, no custom storage engine

**Status:** Accepted · 2026-08-06

**Context.** A real KV store needs a real storage engine — an LSM tree or B-tree
with its own compaction and cache management.

**Decision.** Append-only log with `[len][crc32][payload]` framing, in-memory map
for the state machine. Snapshots serialize the map.

**Rejected.**
- *Custom LSM tree* — a genuinely interesting project, and a separate one. Would
  consume the whole schedule and add nothing to the consensus work.
- *Embedding Pebble or BoltDB* — reasonable, but adds a large dependency and
  operational surface for no learning benefit at this scale.

**Consequences.** Dataset must fit in memory. Fine — `docs/PRD.md` explicitly
scopes this out, and every hour saved goes to correctness work.

---

## ADR-007 — Verification by deterministic fault injection + linearizability checking

**Status:** Accepted · 2026-08-06

**Context.** How do we know it's correct? Most portfolio Raft implementations
answer "the tests pass," where the tests exercise the happy path.

**Decision.** Three layers:
1. Invariant checkers for the five Raft safety properties, asserted after every
   step of every run.
2. Randomized fault injection — partitions, crashes, delays, drops, duplicates —
   all seeded.
3. Machine-checked linearizability (Porcupine) over the resulting client
   histories.

**Rejected.**
- *Happy-path integration tests only* — the common approach; catches almost none
  of the bugs that matter.
- *TLA+ specification* — genuinely rigorous, and verifies the *design* rather than
  the *implementation*. The Raft design is already verified in the paper; our bugs
  will be in the code.
- *Jepsen* — the right tool for a production system, heavyweight for this scale,
  and it can't reach the in-process determinism that makes bugs reproducible here.

**Consequences.** This is what makes the project uncommon. Layers 1 and 2 need the
harness from ADR-003. Expect the first randomized run to fail immediately — that's
it working.

---

## ADR-008 — Kalyan owns the consensus core; Claude owns everything around it

**Status:** Accepted · 2026-08-06

**Context.** This project is built with AI assistance, under real time pressure
(placements). The obvious use of that assistance — have Claude write the Raft
implementation — would produce working code in about a day.

**Decision.** `internal/raft/**` and `internal/store`'s `Apply` logic are written
by Kalyan. Claude writes the test harness, simulated network, invariant checkers,
storage plumbing, transport, tooling, benchmarks, CI, and docs, and acts as
reviewer and trace-reader for the core.

**Rejected.**
- *Claude writes everything* — fastest path to a repository, and it destroys the
  asset. The project exists to be defensible in an interview; the understanding
  that makes it defensible is manufactured by the debugging, which is exactly what
  would be handed away.
- *Kalyan writes everything* — the harness, Porcupine wiring, gRPC, and CI are
  ~40 hours of work that teach nothing about consensus. Spending scarce hours
  there rather than on the algorithm is a bad trade.

**Consequences.**
- Total time roughly halves versus solo (~145h → ~60h of Kalyan's time), because
  plumbing compresses ~4x while consensus logic compresses ~1.2x.
- The verification infrastructure — the part that makes this project uncommon —
  exists from week one rather than never.
- Requires discipline in both directions. The boundary is restated in `CLAUDE.md`
  §1 so it survives context loss between sessions.

---

## Template

```markdown
## ADR-NNN — <short imperative title>

**Status:** Proposed | Accepted | Superseded by ADR-NNN · YYYY-MM-DD

**Context.** What forced a decision.

**Decision.** What was chosen.

**Rejected.** What else was considered, and the specific reason each lost.

**Consequences.** What this makes easy, what it makes hard, what it rules out.
```

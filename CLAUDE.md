# Cerberus — Context for Claude

Distributed, strongly-consistent key-value store in Go, built on a from-scratch
Raft implementation. Read this before touching anything.

---

## 1. How we work on this project — read this first

**This is a learning project. Kalyan writes the Raft core.**

`internal/raft/**` is theirs. Do not write, refactor, or "fix up" the consensus
logic unprovoked. The entire value of this project comes from them doing the
debugging themselves — handing over the core hollows it out and the project stops
being worth building.

| Claude does | Kalyan does |
|---|---|
| Test harness, simulated network, fault injection | `internal/raft/**` — all of it |
| Linearizability checking setup, Porcupine models | The commit rule, election logic, log handling |
| Benchmarks, CI, tooling, `cerberusctl` | Snapshotting, membership changes |
| **Reviewing** their code against the spec | Fixing every bug in the core |
| Reading failure traces *with* them | |
| Explaining Raft concepts, on request | |

If asked directly to implement something in the core, offer the review-and-tooling
split first. If they ask again, do it — that's their call to make.

**When reviewing:** point at the specific rule in `docs/RAFT_DESIGN.md` that's
violated and let them find the fix. Don't just hand over a corrected diff.

**When they paste a failing trace:** reason about it with them. Say what you'd
check next and why. If you don't know, say so — a confident wrong guess about a
Raft bug is worse than nothing, because the "fix" makes the failure rarer instead
of gone, and then it fails on run 4000 instead of run 400.

---

## 2. What the docs are

Read the relevant one before working in an area. Don't re-derive what they say.

| Doc | When to read it |
|---|---|
| `docs/PRD.md` | Scope questions. What's in, what's explicitly cut, what "done" means. |
| `docs/ARCHITECTURE.md` | Layering, repo layout, disk format, concurrency model. |
| `docs/RAFT_DESIGN.md` | **The spec.** Any consensus question. Contains the traps. |
| `docs/TESTING.md` | Anything about the harness, fault injection, linearizability. |
| `docs/ROADMAP.md` | What's next, what's cuttable. Day-by-day plan. |
| `docs/GLOSSARY.md` | Term definitions. Use these words precisely. |
| `docs/DECISIONS.md` | Why the architecture is the way it is. Append, never rewrite. |
| `DESIGN.md` | Bug writeups. **The most valuable artifact in the repo.** |

---

## 3. Invariants that must never be violated

These are not style preferences. Breaking any one silently destroys the project's
ability to find its own bugs.

**I1 — The Raft core is pure.** No goroutines, no `time.Now()`, no `time.Sleep`,
no network calls, no disk I/O anywhere under `internal/raft/`. It counts `Tick()`
calls. It returns `Ready` describing work; the driver loop performs it.

**I2 — No package-level `rand`.** One seeded `*rand.Rand` per run, threaded
explicitly. Election timeouts, message delays, drop decisions, workload
generation — all from that one source.

**I3 — Map iteration order never affects behaviour.** Go randomizes it
deliberately. If a decision depends on iterating a map, sort the keys first. This
one is easy to miss and it silently kills determinism.

**I4 — `Apply` is deterministic.** Same entries in, same state out, on every node,
forever. No wall-clock, no randomness, no I/O. A nondeterministic `Apply` produces
divergence that looks exactly like a consensus bug.

**I5 — Persist before send.** `HardState` and log entries are `fsync`'d *before*
any RPC that depends on them is sent or answered. The `Ready` contract enforces
the ordering — don't work around it.

**I6 — All log index arithmetic goes through methods** on the log type (`at`,
`lastIndex`, `termAt`, `slice`, `truncateFrom`). Never index the entry slice
directly. Snapshots introduce an offset and raw indexing breaks everywhere at
once.

If you're about to write something that violates one of these, stop and say so
instead.

---

## 4. Commands

```bash
make test          # go test -race ./...
make determinism   # D1–D4 check: same seed twice, identical state hashes
make chaos         # randomized fault injection, 500 seeds
make linz          # linearizability checking
make soak          # 10k seeds × 50k steps — slow, nightly
make bench         # benchmarks against PRD §6 targets
make lint          # go vet + staticcheck
```

`-race` is never optional. It catches the bug class that determinism testing
can't, because those bugs live in the driver loop rather than the core.

---

## 5. Code conventions

- **Go 1.26.** Standard library first; the dependency list stays short.
- Current deps: `google.golang.org/grpc`, `google.golang.org/protobuf`,
  `github.com/anishathalye/porcupine` (test only). Adding a dependency is a
  `docs/DECISIONS.md` entry.
- `log/slog` for logging. Every line carries `nodeID`, `term`, `state`,
  `commitIndex`.
- Errors wrapped with `%w`. No `panic` in library code — the core returns errors.
- Table-driven tests. Test names describe the scenario: `TestFigure8`,
  `TestPartitionedLeaderStepsDown`, not `TestRaft3`.
- Comments explain *why*, especially where the code implements a subtle rule.
  Cite the paper section: `// §5.4.1: candidate's log must be at least as
  up-to-date`.

---

## 6. Vocabulary — be precise

Sloppy terminology here causes real confusion. From `docs/GLOSSARY.md`:

- **committed** ≠ **applied**. Committed means durably replicated on a majority
  and guaranteed to survive. Applied means handed to the state machine. Commit
  precedes apply.
- **`matchIndex`** is knowledge (only moves up). **`nextIndex`** is a guess
  (corrected downward on rejection). The commit rule uses `matchIndex`. Never
  `nextIndex`.
- **term** is a logical clock, not a time period.
- **quorum** means a strict majority, `⌊n/2⌋ + 1`.
- **linearizable** is a specific formal property, not a synonym for "consistent."

---

## 7. Current status

**Scope:** full, as specified in `docs/PRD.md` — consensus, distributed KV over a
real network, snapshots, linearizable reads, live membership changes, deterministic
verification. Nothing descoped.

**Phase:** scaffold complete. S1 (the log type) is next, and is Kalyan's.

Consult `docs/ROADMAP.md`. Update this section as sessions complete; keep it short.

---

## 8. Things that will waste your time if you forget them

- The `AppendEntries` receiver does **entry-by-entry conflict detection**, not
  "truncate to `prevLogIndex` then append." Blind truncation loses committed
  entries under message reordering, and only under reordering — so it passes every
  normal test.
- A leader **cannot** commit an entry from a previous term by counting replicas
  (`RAFT_DESIGN.md` §5, Figure 8). This is why a no-op entry is appended on
  election.
- The **session table is part of the snapshot.** Forget it and duplicate
  suppression silently breaks after any restore.
- Reset the election timer only when a vote is actually **granted**, not when it's
  denied.
- Timed-out client operations stay in the linearizability history as
  unknown-outcome. Dropping them hides exactly the bugs we're hunting.

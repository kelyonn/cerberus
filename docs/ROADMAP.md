# Cerberus — Build Plan

**Scope:** full. Raft consensus, distributed KV store over a real network,
snapshots and log compaction, linearizable reads, live membership changes,
deterministic fault injection, and machine-checked linearizability. Everything in
`docs/PRD.md`.

**Budget:** ~60 hours of Kalyan's time, plus the Week 0 learning sprint (7h).

Organised as **sessions**, not days — placements make the calendar lumpy, so work
in whatever blocks appear. Hours listed are Kalyan's; Claude's work happens
between sessions and is marked ⚙.

---

## Ownership

| | Owner |
|---|---|
| `internal/raft/**` — log, node, election, replication, commit rule, snapshots, membership | **Kalyan** |
| `internal/store` — `Apply` logic, session table | **Kalyan** |
| Harness, sim network, invariant checkers, all test scaffolding | **Claude** |
| Storage file format, CRC framing, recovery | **Claude** |
| gRPC transport, CLI, benchmarks, CI, docs | **Claude** |
| Reviewing core code against the spec; reading traces | **Claude** |

Rationale in `CLAUDE.md` §1. If Kalyan asks directly for core code, Claude writes
it — after flagging the tradeoff once.

**Priority markers:** 🔴 load-bearing · 🟡 core · 🟢 polish

---

# Phase 1 — Consensus core · ~14h

### S1 — The log type · **2h** 🔴
- [ ] `Entry`, `HardState`, `Message`, `Config` types
- [ ] Log type with **every index calculation behind a method**: `at`,
      `lastIndex`, `lastTerm`, `termAt`, `slice`, `truncateFrom`, `append`

> No raw slice indexing, even though there's no snapshot offset yet. Snapshots
> arrive in S13 and retrofitting an offset breaks everything at once
> (`CLAUDE.md` I6).

**⚙** Claude: log unit tests, including with an artificial snapshot offset.

---

### S2 ⚙ — The harness · **1h review** 🔴
Claude builds `SimNetwork` (seeded drop / delay / reorder / duplicate /
partition), `Cluster` (`Step`, `Crash`, `Restart`, `Partition`, `Heal`,
`Isolate`), and the determinism test.

Kalyan reviews the interface. Understanding this is what makes every later
session tractable.

**Done when:** `make determinism` passes over 100 seeds against a stub node.

---

### S3 — Node skeleton · **2h** 🔴
- [ ] `Node` struct, role enum, `Tick` / `Step` / `Ready` / `Advance`
- [ ] Role transitions
- [ ] Universal higher-term rule, checked **first** (`RAFT_DESIGN.md` §2)
- [ ] Randomized election timeouts from the seeded RNG

**Read first:** paper §5.1, Figure 2.

---

### S4 — Leader election · **3h** 🔴
- [ ] `RequestVote` send + receive paths
- [ ] "At least as up-to-date" check (§5.4.1)
- [ ] Vote counting, majority, promotion
- [ ] Heartbeats suppressing follower timeouts

> Reset the election timer only when a vote is **granted**, never on denial.

**Read first:** paper §5.2, §5.4.1.

---

### S5 — Log replication · **3h** 🔴
- [ ] `AppendEntries` send path, `nextIndex` / `matchIndex`
- [ ] Receive path with **entry-by-entry conflict detection**
- [ ] `nextIndex` backoff on rejection
- [ ] `Propose()`

**Read first:** paper §5.3, and the Students' Guide.

---

### S6 — Commit rule · **2h** 🔴
- [ ] Majority `matchIndex` **and** `log[N].Term == currentTerm`
- [ ] No-op entry on election
- [ ] `CommittedEntries` in `Ready`, `lastApplied`

**Read first:** paper §5.4.2 + Figure 8, traced on paper.

**⚙** Claude: `TestFigure8`, constructing the scenario deliberately.

---

### S7 — Persistence · **1.5h** 🔴
**⚙** Claude: segmented append-only log, `[len][crc32][payload]` framing,
recovery, CRC truncation of torn writes.

Kalyan wires the `Ready` ordering: `HardState` and entries `fsync`'d **before**
any dependent message is sent.

**🚩 Phase 1 gate:** 3 nodes elect, replicate, commit, and recover from crashes,
deterministically.

---

# Phase 2 — Store, network, verification · ~9h

### S8 — KV state machine · **2h** 🔴
- [ ] `Apply`, `Get`, `Put`, `Delete`, `CAS` — fully deterministic
- [ ] Session table: `(clientID, seqNum)` dedup with cached results

**⚙** Claude: command encoding, `TestDuplicateRequest`.

**Read first:** paper §8.

---

### S9 — Server & real network · **2h** 🟡
Kalyan: the driver loop ordering (`ARCHITECTURE.md` §3.1) — this is I5 and it's
load-bearing.

**⚙** Claude: gRPC transport, RPC surface, leader redirect, client library,
`cerberusctl`.

**Done when:** three real processes on localhost serve `Put`/`Get`.

---

### S10 ⚙ — Invariant checkers · **0.5h review** 🔴
Claude: S1–S5 checkers running after every step, plus the randomized
fault-injection driver (500 seeds × 5k steps).

**Expect red immediately.** That's the harness working.

---

### S11 ⚙ — Linearizability · **0.5h review** 🔴
Claude: history recording, Porcupine model, timed-out ops retained as
unknown-outcome, CI wiring.

---

### S12 — Bug bash #1 · **4h** 🔴
- [ ] Fix everything S10–S11 surfaced
- [ ] Regression test pinning each fix

Claude reads traces with you and points at the violated rule; you write the fix.

> Budget generously. This is the session that produces the project's value.

**🚩 Phase 2 gate:** a working, verified distributed KV store. If everything
stopped here you'd still have a real project.

---

# Phase 3 — Snapshots · ~9h

### S13 — Snapshot & compaction · **4h** 🔴
- [ ] `Store.Snapshot()` / `Restore()` — **including the session table**
- [ ] Log compaction, sentinel at `log[0]`, offset arithmetic throughout
- [ ] Snapshot persistence and load on startup

> The offset is why S1 insisted on methods. If any raw indexing survived, this is
> where it detonates.

**Read first:** paper §7.

---

### S14 — InstallSnapshot · **3h** 🔴
- [ ] Send path, triggered when `nextIndex` drops below the compaction point
- [ ] Receive path, including "keep entries after a matching `lastIncludedIndex`"

**⚙** Claude: `TestSnapshotCatchUp`.

---

### S15 — Bug bash #2 · **2h** 🔴
Snapshots interact with everything. Re-run the full randomized suite and fix what
falls out.

---

# Phase 4 — Reads & membership · ~10h

### S16 — ReadIndex · **3h** 🟡
- [ ] Record `commitIndex`, confirm leadership via heartbeat majority, wait for
      `lastApplied >= readIndex`
- [ ] Batch reads across concurrent requests

**⚙** Claude: `TestStaleRead` — partitioned ex-leader must not serve stale data.

**Read first:** paper §6.4.

---

### S17 — Membership I · **4h** 🟡
- [ ] Configuration as a log entry; adopt on **append**, not commit
- [ ] Single-server add/remove, one change in flight
- [ ] Learner state — catch up before being granted a vote

**Read first:** paper §6, thesis §4.1.

---

### S18 — Membership II · **3h** 🟡
- [ ] Leader steps down after committing its own removal
- [ ] Disruptive-server suppression (`RAFT_DESIGN.md` §8)

**⚙** Claude: `TestMembershipUnderLoad`.

---

# Phase 5 — Hardening & ship · ~12h

### S19 — Real-network chaos · **2h** 🟡
**⚙** Claude: 3-process integration harness, `SIGKILL` restarts, process-level
latency and partition injection.

Kalyan: triage what it finds. This tier catches what the simulator *replaces* —
real `fsync` semantics, torn writes, socket behaviour.

---

### S20 — Soak · **4h** 🔴
- [ ] 10,000 seeds × 50k steps overnight
- [ ] Triage and fix everything it finds

---

### S21 — Performance · **3h** 🟢
- [ ] Batch proposals into single `AppendEntries` rounds
- [ ] Pipeline `AppendEntries` without waiting for responses
- [ ] Fast backup with `conflictTerm` / `conflictIndex`

**⚙** Claude: benchmarks against `docs/PRD.md` §6 targets.

---

### S22 — Write it up · **2h** 🔴
- [ ] `DESIGN.md` entry per real bug: symptom, trace, root cause, fix, and why
      the tests missed it before

---

### S23 — Ship · **1h** 🟡
- [ ] Self-check: answer the `docs/PRD.md` §8 questions out loud, from memory
- [ ] Verify all five `docs/PRD.md` §7 "done" criteria

**⚙** Claude: README, architecture diagram, `docker-compose.yml`, final CI.

---

## Totals

| Phase | Kalyan's hours |
|---|---|
| 1 — Consensus core | ~14 |
| 2 — Store, network, verification | ~9 |
| 3 — Snapshots | ~9 |
| 4 — Reads & membership | ~10 |
| 5 — Hardening & ship | ~12 |
| Slack | ~6 |
| **Build total** | **~60** |
| Week 0 learning | 7 |
| **Total** | **~67** |

**Calendar:** ~10 weeks at 1 hr/day · ~5 weeks at 2 hr/day · faster with weekend
blocks. Phase 2's gate is the natural pause point if placements get loud — the
project is coherent and presentable from there onward.

---

## Order discipline

Do not start a session while the previous one has a known failing test. Bugs in
stateful systems compound multiplicatively — two unfixed bugs are far more than
twice as hard to isolate as one.

If time gets tight, **pause at a phase gate rather than skipping ahead**. A
finished Phase 3 beats a half-finished Phase 5.

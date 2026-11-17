# Cerberus — Product Requirements

## 1. What this is

Cerberus is a distributed, strongly-consistent key-value store written in Go. A
cluster of nodes replicates every write through the Raft consensus algorithm, so
the store keeps serving reads and writes correctly as long as a majority of nodes
are alive and can talk to each other.

The consensus layer is implemented from the Raft paper. Cerberus does not wrap an
existing consensus library.

## 2. Why it exists

This is a learning project with a portfolio second purpose. Both goals point at
the same target, which makes scoping easy:

- **Primary goal:** understand distributed consensus at the level where you can
  reason about failure scenarios from first principles, not recall.
- **Secondary goal:** produce something whose correctness is *demonstrable*, not
  asserted. A Raft implementation that passes happy-path tests is common. One
  with a deterministic fault-injection harness and machine-checked linearizability
  is not.

Every scope decision in this document resolves in favour of **depth over breadth**.

## 3. Non-goals

Explicitly out of scope. These are cut to fund correctness work, not because they
are uninteresting.

| Not building | Why |
|---|---|
| Sharding / multi-Raft-group | Multiplies operational complexity, adds no new consensus insight |
| A custom on-disk storage engine (LSM/B-tree) | Separate project; use an append-only log + in-memory map |
| SQL or a query language | Different domain entirely |
| Authentication, TLS, multi-tenancy | Production concerns, not correctness concerns |
| Web dashboard / GUI | Time sink with no learning payoff |
| Geo-replication, WAN tuning | Requires the above to matter first |

## 4. Functional requirements

### 4.1 Client API

| Operation | Semantics |
|---|---|
| `Put(key, value)` | Replicated through Raft. Returns only after commit + apply. |
| `Get(key)` | Linearizable. Returns the value of the most recent completed `Put`. |
| `Delete(key)` | Replicated through Raft. Idempotent. |
| `CAS(key, expected, new)` | Compare-and-swap. Atomic, replicated. |

All operations are **linearizable**: the system behaves as if each operation took
effect instantaneously at some point between its invocation and its response, and
that ordering is consistent with real time.

### 4.2 Client behaviour

- A client may contact any node. Non-leaders respond with a redirect to the
  current leader rather than serving the request.
- Clients retry on timeout. The system must therefore **detect and suppress
  duplicate requests** — a retried `Put` must not apply twice. This is handled by
  per-client session tracking (`clientID` + monotonic `seqNum`) inside the state
  machine.

### 4.3 Cluster behaviour

- **Fault tolerance:** a cluster of `2f+1` nodes tolerates `f` simultaneous
  failures. A 3-node cluster tolerates 1; a 5-node cluster tolerates 2.
- **Availability:** the cluster serves requests whenever a majority is reachable.
  With no majority, it correctly refuses to serve rather than serving stale data.
- **Crash recovery:** a node that crashes and restarts rejoins with its state
  intact and catches up. No data acknowledged to a client is ever lost, provided
  a majority did not permanently fail.
- **Membership changes:** nodes can be added to and removed from a live cluster
  without downtime, one at a time.
- **Log compaction:** the Raft log does not grow without bound. Snapshots
  truncate it, and lagging or newly-added nodes are caught up by snapshot
  transfer when the entries they need have been discarded.

## 5. Correctness requirements

These are the requirements that actually define the project. Everything above is
table stakes; this section is the deliverable.

**C1 — Linearizability under fault injection.**
Randomized operation histories, generated while the cluster is subjected to
partitions, crashes, restarts, and message delays, must pass a linearizability
checker. Target: 10,000 seeded runs green in CI.

**C2 — Deterministic reproduction.**
Every test run is driven by a seed. A failing run must reproduce identically from
that seed alone. This is non-negotiable and must exist before the hard debugging
starts, not after — see `TESTING.md`.

**C3 — Durability.**
`currentTerm`, `votedFor`, and the log are `fsync`'d before any RPC that depends
on them is answered. A node killed with `SIGKILL` at any point must recover to a
valid state.

**C4 — The five Raft safety properties hold**, and each has a corresponding
assertion in the test harness. See `RAFT_DESIGN.md` §7.

## 6. Non-functional targets

Deliberately modest. These are sanity bounds, not the point of the project.

| Metric | Target | Notes |
|---|---|---|
| Write throughput | ≥ 5k ops/sec, 3 nodes, local | Batching makes this easy to beat |
| Read latency (p99) | < 10 ms local | Via ReadIndex, not log-replicated reads |
| Leader election time | < 1 s after leader failure | Election timeout 150–300 ms |
| Recovery time | < 5 s for a node restarting with a snapshot | |

Performance work is explicitly the *last* thing to do, and only after C1–C4 hold.

## 7. What "done" looks like

Cerberus is finished when all of the following are true:

1. A 3-node and a 5-node cluster both pass the full fault-injection suite.
2. 10,000 seeded randomized runs pass linearizability checking in CI.
3. A node can be added to and removed from a running cluster while the workload
   continues uninterrupted.
4. Snapshots work: a node whose log has been compacted can still bring a lagging
   peer up to date.
5. `DESIGN.md` contains a written walkthrough of at least one real correctness bug
   the harness caught — the symptom, the trace, the root cause, and the fix.

Item 5 is not padding. It is the single highest-value artifact the project
produces, and it is the thing that will be asked about.

## 8. Success criteria for the *learning* goal

You can answer these without notes:

- What happens if a leader commits an entry and dies before informing anyone?
- Why can't a leader commit an entry from a previous term by counting replicas?
- Why does a candidate need the "at least as up-to-date" log check before a
  follower grants it a vote?
- Why is a read served directly from the leader's memory not automatically
  linearizable?
- What breaks if `votedFor` is not persisted before responding to a vote request?

If any of these are still fuzzy at the end, the project is not done regardless of
what the tests say.

# Cerberus

A distributed, strongly-consistent key-value store in Go, built on a from-scratch
implementation of the Raft consensus algorithm.

> **Status:** in development. See [docs/ROADMAP.md](docs/ROADMAP.md).

## What it does

A cluster of `2f+1` nodes replicates every write through Raft, tolerating `f`
simultaneous failures. All operations are linearizable. Nodes can crash, restart,
be partitioned from each other, and be added to or removed from a live cluster,
without violating consistency or losing acknowledged writes.

The consensus layer is implemented from the Raft paper — no consensus library.

## Why it's built the way it is

The Raft core is a **pure, deterministic state machine** — no goroutines, no
timers, no network calls. It is driven by `Tick`/`Step`/`Ready`/`Advance` and
returns descriptions of work for an outer loop to perform.

That design buys the thing this project actually cares about: an entire cluster
runs inside a single goroutine under a simulated clock and a programmable lossy
network, so a failure that would normally appear once in 400 runs instead
reproduces byte-for-byte from its seed. Correctness is verified by machine-checked
linearizability over randomized histories generated under partitions, crashes, and
message reordering — not by asserting the happy path works.

## Documentation

| Document | Contents |
|---|---|
| [PRD.md](docs/PRD.md) | Scope, requirements, non-goals, definition of done |
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | Layering, repo layout, storage format, concurrency model |
| [RAFT_DESIGN.md](docs/RAFT_DESIGN.md) | The algorithm as implemented, and the traps in it |
| [TESTING.md](docs/TESTING.md) | Deterministic simulation, fault injection, linearizability |
| [ROADMAP.md](docs/ROADMAP.md) | Day-by-day build plan |
| [GLOSSARY.md](docs/GLOSSARY.md) | Precise definitions — several of these get conflated constantly |
| [DECISIONS.md](docs/DECISIONS.md) | ADRs: what was chosen, what was rejected, why |
| [DESIGN.md](DESIGN.md) | Writeups of real correctness bugs found and fixed |
| [CLAUDE.md](CLAUDE.md) | Working context for Claude Code |

## Development

```bash
make help      # all targets
make test      # unit + scenario tests, race detector on
make chaos     # randomized fault injection with invariant checking
make linz      # linearizability checking
```

## Reference

- Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm (Extended Version)* — https://raft.github.io/raft.pdf
- Ongaro, *Consensus: Bridging Theory and Practice* (PhD thesis) — membership changes, §4
- Porcupine, a linearizability checker for Go — https://github.com/anishathalye/porcupine

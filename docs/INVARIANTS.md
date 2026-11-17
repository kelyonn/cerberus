# Cerberus — Invariants

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

Any change that would violate one of these is wrong by definition, however
convenient it looks.

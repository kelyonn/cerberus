# Cerberus — Architecture

## 1. Layer model

Cerberus is four layers. Each one knows only about the layer directly beneath it.
Keeping these boundaries clean is what makes the system testable — in particular,
the Raft core must not know that time or the network exist as real things.

```
┌──────────────────────────────────────────────────────────┐
│  Client                                                  │
│  Put / Get / Delete / CAS · leader discovery · retries   │
└───────────────────────────┬──────────────────────────────┘
                            │ RPC
┌───────────────────────────▼──────────────────────────────┐
│  Server                                                  │
│  request routing · leader redirect · session dedup       │
│  waits for the entry it proposed to be applied           │
└───────────────────────────┬──────────────────────────────┘
                            │ Propose(cmd) / applyCh
┌───────────────────────────▼──────────────────────────────┐
│  Raft core                    ← pure, deterministic      │
│  election · replication · commit rule · snapshot mgmt    │
└──────┬──────────────────────────────────┬────────────────┘
       │ Storage iface                    │ Transport iface
┌──────▼───────────────┐          ┌───────▼──────────────────┐
│  Storage             │          │  Transport               │
│  log · hardstate     │          │  RequestVote             │
│  snapshots · fsync   │          │  AppendEntries           │
│                      │          │  InstallSnapshot         │
└──────────────────────┘          └──────────────────────────┘
```

Both `Storage` and `Transport` are interfaces with two implementations: a real one
and a simulated one. The simulated pair is what makes `TESTING.md` possible.

## 2. Repository layout

```
cerberus/
├── cmd/
│   ├── cerberusd/          # server binary
│   └── cerberusctl/        # CLI client
├── internal/
│   ├── raft/               # ← the heart of the project
│   │   ├── raft.go         # Node struct, state transitions, tick()
│   │   ├── election.go     # RequestVote send + handle
│   │   ├── replication.go  # AppendEntries send + handle
│   │   ├── log.go          # log storage, truncation, index math
│   │   ├── snapshot.go     # InstallSnapshot, compaction
│   │   ├── config.go       # membership changes
│   │   └── state.go        # persistent + volatile state types
│   ├── store/              # KV state machine
│   │   ├── store.go        # map + Apply(cmd)
│   │   ├── session.go      # client session table (dedup)
│   │   └── snapshot.go     # serialize / restore
│   ├── server/             # gRPC or net/rpc surface, routing, redirect
│   ├── transport/
│   │   ├── grpc.go         # real network
│   │   └── sim.go          # simulated: drop, delay, reorder, partition
│   └── storage/
│       ├── file.go         # real: append-only log + fsync
│       └── mem.go          # in-memory, for tests
├── test/
│   ├── harness/            # cluster-in-a-process, seeded, simulated clock
│   ├── chaos/              # fault-injection scenarios
│   └── linz/               # Porcupine models + history recording
├── docs/
└── DESIGN.md               # ← bug writeups live here
```

## 3. The single most important design decision

**The Raft core must be a deterministic state machine with no goroutines, no
timers, and no network calls inside it.**

This is the `etcd/raft` design, and it is the difference between a project you can
debug and one you can't.

```go
// The core exposes exactly this shape:
type Node struct { /* ... */ }

func (n *Node) Tick()                              // advance logical clock by 1
func (n *Node) Step(m Message) error               // feed one inbound message
func (n *Node) Propose(cmd []byte) error           // client write
func (n *Node) Ready() Ready                       // what the outside must now do
func (n *Node) Advance()                           // ack that Ready was handled

type Ready struct {
    HardState        *HardState  // persist these before sending Messages
    Entries          []Entry     // append to log, fsync
    Snapshot         *Snapshot   // if non-nil, install it
    Messages         []Message   // send after the above are durable
    CommittedEntries []Entry     // hand to the state machine
}
```

The core never *does* anything. It is asked "given this input, what should
happen?" and returns a description. A driver loop outside the core performs the
side effects.

Why this matters:

- **Determinism.** Same seed + same message order = byte-identical execution.
  Failures reproduce exactly, which is the entire premise of `TESTING.md`.
- **Testability.** A whole 5-node cluster runs inside one goroutine in a unit
  test, with no sleeps and no flakes.
- **Correct ordering falls out for free.** The `Ready` contract *forces* you to
  persist `HardState` and `Entries` before sending `Messages`. Getting this
  backwards is one of the classic Raft bugs, and this design makes it structurally
  hard to write.

The cost is that the driver loop is fiddlier than "just spawn a goroutine per
peer." Pay it. The alternative is chasing a heisenbug for a week in week 3.

### 3.1 The driver loop

Lives in `internal/server`. Roughly:

```go
for {
    select {
    case <-ticker.C:
        node.Tick()

    case m := <-inbound:
        node.Step(m)

    case cmd := <-proposals:
        node.Propose(cmd)
    }

    rd := node.Ready()
    if rd.HardState != nil { storage.SetHardState(rd.HardState) } // fsync
    storage.Append(rd.Entries)                                    // fsync
    for _, m := range rd.Messages { transport.Send(m) }           // only now
    for _, e := range rd.CommittedEntries { stateMachine.Apply(e) }
    node.Advance()
}
```

The ordering in the body is load-bearing. Read §5 of the Raft paper on
persistence before you touch it.

## 4. Storage layout on disk

One directory per node:

```
data/node-1/
├── hardstate            # currentTerm, votedFor — small, rewritten, fsync'd
├── log/
│   ├── 000001.log       # append-only segments of Entry records
│   └── 000002.log
└── snapshots/
    └── snap-000042.bin  # lastIncludedIndex, lastIncludedTerm, config, KV data
```

Entry record framing: `[len:uint32][crc32:uint32][payload:protobuf]`. The CRC is
what lets recovery detect a torn write from a mid-`fsync` crash — on a bad CRC,
truncate the log at that point and treat everything after as lost. It was never
acknowledged to a client, so that's safe.

**Durability rule:** `HardState` and new log entries hit disk with `fsync` *before*
any RPC whose correctness depends on them is sent or answered. Specifically:

- Before responding `voteGranted: true`, `votedFor` must be durable.
- Before responding `success: true` to `AppendEntries`, the entries must be durable.

Violating either lets a node forget a promise across a crash, which breaks
Election Safety. This is `PRD.md` C3.

## 5. The KV state machine

Deliberately simple — the interesting part of this project is above it.

```go
type Store struct {
    mu       sync.RWMutex
    data     map[string][]byte
    sessions map[uint64]sessionEntry   // clientID → last seq + result
}

func (s *Store) Apply(e raft.Entry) any
func (s *Store) Snapshot() ([]byte, error)
func (s *Store) Restore(b []byte) error
```

`Apply` must be **deterministic**: same entry sequence in, same state out, on
every node, forever. No wall-clock time, no map iteration order affecting the
result, no randomness, no I/O. A nondeterministic `Apply` produces divergence that
looks exactly like a consensus bug and will cost you days.

### 5.1 Session table / duplicate suppression

Clients retry. A retried `Put` that gets committed twice is a correctness bug the
consensus layer cannot catch, because both entries are legitimately committed.

Each command carries `(clientID, seqNum)`. On `Apply`:

- If `seqNum <= sessions[clientID].lastSeq`, this is a duplicate: return the
  cached result, do not mutate state.
- Otherwise apply, then record `lastSeq` and the result.

The session table is part of the state machine and therefore part of the
snapshot. If you forget that, dedup silently breaks after any restore. (Raft
paper §6.3, and it is a common miss.)

## 6. Linearizable reads

The naive approach — leader reads its own map and replies — is **not
linearizable**. A partitioned old leader that hasn't yet noticed it was deposed
will happily serve stale data.

Cerberus uses **ReadIndex** (paper §6.4):

1. Leader records `readIndex = commitIndex`.
2. Leader confirms it is still leader by exchanging heartbeats with a majority.
3. Leader waits until `lastApplied >= readIndex`.
4. Only then does it serve the read from local state.

This costs one round of heartbeats per read batch (amortizable across concurrent
reads) but requires no log append. A lease-based optimization exists and is
explicitly a stretch goal — it trades a clock-drift assumption for latency, and
that tradeoff is only worth making once ReadIndex demonstrably works.

## 7. Transport

Interface:

```go
type Transport interface {
    Send(m Message)               // fire-and-forget; Raft tolerates loss
    Recv() <-chan Message
}
```

Note `Send` is one-way with no error return. Raft's design already assumes
messages can be dropped, duplicated, delayed, or reordered — modelling the
transport as lossy rather than as request/response keeps the core honest and
makes the simulated implementation trivial.

- `transport/grpc.go` — real, over gRPC streams.
- `transport/sim.go` — in-process, seeded, with programmable drop rate, latency
  distribution, reordering, and partition sets. This is the fault injector.

## 8. Concurrency model

- **Raft core:** zero goroutines. Single-threaded by construction.
- **Server:** one driver goroutine owns the `Node`. All access to Raft goes
  through channels into that loop. No mutex on Raft state, ever.
- **State machine:** applied from the driver goroutine only; `RWMutex` guards
  concurrent reads from RPC handlers.
- **Transport:** its own goroutines, feeding the driver's inbound channel.

If you find yourself wanting a mutex inside `internal/raft`, that is a signal the
layering broke. Fix the layering.

## 9. Observability

Cheap to add, disproportionately useful during the week-3 bug bash:

- Structured logs (`log/slog`) with `nodeID`, `term`, `state`, `commitIndex` on
  every line.
- A `raftstate` dump endpoint returning full node state as JSON — indispensable
  when a cluster wedges.
- Counters: elections started, votes granted/denied, entries appended, snapshots
  sent, apply lag.

Build the state dump in week 1. You will use it constantly.

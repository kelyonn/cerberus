// Package harness runs a whole Raft cluster inside a single goroutine under a
// simulated clock and a programmable lossy network.
//
// The point of this package is reproducibility. Everything nondeterministic —
// message delay, drops, duplicates, which node times out first — is drawn from
// one seeded *rand.Rand (invariant I2). A run is therefore a pure function of
// its seed, so a failure that shows up once in 400 runs can be replayed exactly
// as many times as it takes to understand it. See docs/DECISIONS.md ADR-003.
package harness

import (
	"slices"

	"math/rand"

	"github.com/kelyonn/cerberus/internal/raft"
)

// inflight is a message in transit, with the tick it should be delivered on.
type inflight struct {
	msg       raft.Message
	deliverAt uint64
	// seq totally orders messages that would otherwise be delivered
	// simultaneously. Without it, ties would resolve by whatever order the
	// slice happened to be in, which is exactly the kind of accidental
	// nondeterminism this harness exists to eliminate.
	seq uint64
}

// NetworkConfig is the fault-injection dial set.
type NetworkConfig struct {
	MinDelay int     // minimum ticks in transit
	MaxDelay int     // maximum ticks in transit
	DropRate float64 // probability a message is lost outright
	DupRate  float64 // probability a message is delivered twice
}

// DefaultNetwork is a well-behaved network: fast, lossless, but still
// reordering, because delays vary.
func DefaultNetwork() NetworkConfig {
	return NetworkConfig{MinDelay: 1, MaxDelay: 3}
}

// Network is the simulated transport. It models what ARCHITECTURE.md §7
// describes: fire-and-forget delivery that may drop, delay, duplicate, and
// reorder. Raft is required to tolerate all four.
type Network struct {
	cfg NetworkConfig
	rng *rand.Rand

	queue []inflight
	seq   uint64

	// partition maps a node to a partition group. Nodes in different groups
	// cannot exchange messages. All nodes start in group 0.
	partition map[uint64]int
}

func NewNetwork(cfg NetworkConfig, rng *rand.Rand) *Network {
	return &Network{cfg: cfg, rng: rng, partition: make(map[uint64]int)}
}

// reachable reports whether two nodes can currently exchange messages.
func (nw *Network) reachable(a, b uint64) bool {
	return nw.partition[a] == nw.partition[b]
}

// Partition places the given nodes into their own group, isolating them from
// every node not listed.
func (nw *Network) Partition(groups ...[]uint64) {
	nw.partition = make(map[uint64]int)
	for g, members := range groups {
		for _, id := range members {
			nw.partition[id] = g + 1
		}
	}
}

// Heal removes all partitions.
func (nw *Network) Heal() { nw.partition = make(map[uint64]int) }

// Send offers a message to the network. It may vanish, arrive late, or arrive
// twice. There is no error return and no delivery guarantee, matching the real
// Transport interface — a core that only works on a reliable network is not a
// Raft implementation.
func (nw *Network) Send(now uint64, m raft.Message) {
	if !nw.reachable(m.From, m.To) {
		return
	}
	if nw.rng.Float64() < nw.cfg.DropRate {
		return
	}

	nw.enqueue(now, m)
	if nw.rng.Float64() < nw.cfg.DupRate {
		// A duplicate with an independently drawn delay, so the copy can
		// overtake the original. Raft must be idempotent under this; the
		// duplicate-vote guard in handleVoteResp is one of the places that
		// matters.
		nw.enqueue(now, m)
	}
}

func (nw *Network) enqueue(now uint64, m raft.Message) {
	spread := nw.cfg.MaxDelay - nw.cfg.MinDelay + 1
	if spread < 1 {
		spread = 1
	}
	delay := nw.cfg.MinDelay + nw.rng.Intn(spread)
	if delay < 1 {
		delay = 1
	}
	nw.seq++
	nw.queue = append(nw.queue, inflight{msg: m, deliverAt: now + uint64(delay), seq: nw.seq})
}

// Deliver returns every message due at or before now, in a total order that
// depends only on the seed, and removes them from the queue.
func (nw *Network) Deliver(now uint64) []raft.Message {
	if len(nw.queue) == 0 {
		return nil
	}

	// Sort by (deliverAt, seq). seq breaks ties deterministically.
	slices.SortFunc(nw.queue, func(a, b inflight) int {
		if a.deliverAt != b.deliverAt {
			return int(a.deliverAt) - int(b.deliverAt)
		}
		return int(a.seq) - int(b.seq)
	})

	var due []raft.Message
	keep := nw.queue[:0]
	for _, f := range nw.queue {
		// A message crossing a partition that went up while it was in flight is
		// dropped at delivery, not at send. That is the honest model: the sender
		// had no way to know.
		if f.deliverAt <= now {
			if nw.reachable(f.msg.From, f.msg.To) {
				due = append(due, f.msg)
			}
			continue
		}
		keep = append(keep, f)
	}
	nw.queue = keep
	return due
}

// InFlight reports how many messages are still in transit. Useful for driving a
// cluster until it goes quiet.
func (nw *Network) InFlight() int { return len(nw.queue) }

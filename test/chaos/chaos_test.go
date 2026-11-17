// Package chaos runs randomized fault injection against the cluster, asserting
// the five Raft safety properties after every step of every run.
//
// This is layer 2 of docs/DECISIONS.md ADR-007. It does not assert liveness:
// under heavy partitioning a cluster is legitimately allowed to make no
// progress. What it asserts is that nothing UNSAFE ever happens, no matter what
// the network does.
//
//	make chaos                    # 500 seeds
//	make chaos SEEDS=5000         # more
//	go test ./test/chaos -run TestRandomized -seeds=1 -seed0=417 -v
package chaos

import (
	"flag"
	"fmt"
	"math/rand"
	"testing"

	"github.com/kelyonn/cerberus/test/harness"
)

var (
	seeds = flag.Int("seeds", 200, "number of seeds to run")
	seed0 = flag.Int64("seed0", 1, "first seed; set with -seeds=1 to replay one failure")
	steps = flag.Int("steps", 1500, "simulated ticks per seed")
	nodes = flag.Int("nodes", 5, "cluster size")
)

// TestRandomized drives a cluster through a randomized fault schedule.
//
// Every seed is an independent, fully reproducible run. When one fails, the
// failure message carries the seed, and re-running that seed alone reproduces it
// exactly — which is the entire reason the harness was built before the
// algorithm (ADR-003).
func TestRandomized(t *testing.T) {
	for i := 0; i < *seeds; i++ {
		seed := *seed0 + int64(i)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runSeed(t, seed, *steps, *nodes)
		})
	}
}

func runSeed(t *testing.T, seed int64, steps, nodes int) {
	t.Helper()

	rng := rand.New(rand.NewSource(seed))

	c := harness.NewCluster(t, harness.ClusterConfig{
		Nodes: nodes,
		Seed:  seed,
		Network: harness.NetworkConfig{
			MinDelay: 1,
			MaxDelay: 1 + rng.Intn(8),
			DropRate: rng.Float64() * 0.20,
			DupRate:  rng.Float64() * 0.10,
		},
	})

	ids := make([]uint64, nodes)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}

	crashed := make(map[uint64]bool)
	proposals := 0

	for step := 0; step < steps; step++ {
		// Roughly one fault every 40 ticks, and a write whenever there is a
		// leader to take it.
		switch {
		case rng.Intn(40) == 0:
			injectFault(c, rng, ids, crashed)
		case rng.Intn(12) == 0:
			if _, ok := c.Leader(); ok {
				c.Propose([]byte(fmt.Sprintf("s%d-op%d", seed, proposals)))
				proposals++
			}
		}
		c.Step()
	}

	// Heal everything and let the cluster settle. Safety was asserted at every
	// step above; this final stretch gives any lingering divergence a chance to
	// surface as an S3 or S5 violation rather than staying hidden.
	c.Heal()
	for id := range crashed {
		if crashed[id] {
			c.Restart(id)
			crashed[id] = false
		}
	}
	c.Run(600)
}

func injectFault(c *harness.Cluster, rng *rand.Rand, ids []uint64, crashed map[uint64]bool) {
	switch rng.Intn(4) {
	case 0: // split the cluster
		var a, b []uint64
		for _, id := range ids {
			if rng.Intn(2) == 0 {
				a = append(a, id)
			} else {
				b = append(b, id)
			}
		}
		if len(a) > 0 && len(b) > 0 {
			c.Partition(a, b)
		}

	case 1: // heal
		c.Heal()

	case 2: // kill a node, but never so many that safety becomes vacuous
		live := 0
		for _, id := range ids {
			if !crashed[id] {
				live++
			}
		}
		if live > len(ids)/2+1 {
			victim := ids[rng.Intn(len(ids))]
			if !crashed[victim] {
				c.Crash(victim)
				crashed[victim] = true
			}
		}

	case 3: // bring one back
		for _, id := range ids {
			if crashed[id] {
				c.Restart(id)
				crashed[id] = false
				break
			}
		}
	}
}

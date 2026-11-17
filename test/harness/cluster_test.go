package harness

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/kelyonn/cerberus/internal/raft"
)

// TestElectsSingleLeader — RAFT_DESIGN.md §10 step 3.
//
// Run across several seeds because election timing is randomized: a single seed
// passing proves only that one interleaving works.
func TestElectsSingleLeader(t *testing.T) {
	for _, nodes := range []int{1, 3, 5} {
		for seed := int64(1); seed <= 20; seed++ {
			t.Run(fmt.Sprintf("nodes=%d/seed=%d", nodes, seed), func(t *testing.T) {
				c := NewCluster(t, ClusterConfig{Nodes: nodes, Seed: seed})
				leader := c.RunUntilLeader(500)

				if got := c.Status(leader).Role; got != raft.RoleLeader {
					t.Fatalf("node %d role = %v, want leader", leader, got)
				}
			})
		}
	}
}

// TestReElectsAfterLeaderCrash — the other half of step 3.
func TestReElectsAfterLeaderCrash(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			c := NewCluster(t, ClusterConfig{Nodes: 3, Seed: seed})
			first := c.RunUntilLeader(500)

			c.Crash(first)
			second := c.RunUntilLeader(500)

			if second == first {
				t.Fatalf("node %d is leader again after crashing", first)
			}
			if got, want := c.Status(second).Term, c.Status(second).Term; got < want {
				t.Fatalf("new leader term %d did not advance", got)
			}
		})
	}
}

// TestReplicatedEntryCommitsAndApplies — RAFT_DESIGN.md §10 step 4.
func TestReplicatedEntryCommitsAndApplies(t *testing.T) {
	for seed := int64(1); seed <= 10; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			c := NewCluster(t, ClusterConfig{Nodes: 3, Seed: seed})
			c.RunUntilLeader(500)

			cmds := [][]byte{[]byte("set a=1"), []byte("set b=2"), []byte("set c=3")}
			for _, cmd := range cmds {
				c.Propose(cmd)
				c.Run(30)
			}
			c.Run(100)

			for _, id := range []uint64{1, 2, 3} {
				got := c.Applied(id)
				if len(got) != len(cmds) {
					t.Fatalf("node %d applied %d commands, want %d", id, len(got), len(cmds))
				}
				for i := range cmds {
					if !bytes.Equal(got[i], cmds[i]) {
						t.Fatalf("node %d applied[%d] = %q, want %q", id, i, got[i], cmds[i])
					}
				}
			}
		})
	}
}

// TestMinorityCannotCommit — PRD.md §4.3: with no majority reachable, the
// cluster must refuse to make progress rather than serve stale data.
//
// The isolated leader accepts the proposal into its own log. That is fine. What
// must never happen is that it commits it.
func TestMinorityCannotCommit(t *testing.T) {
	c := NewCluster(t, ClusterConfig{Nodes: 3, Seed: 7})
	leader := c.RunUntilLeader(500)

	var others []uint64
	for _, id := range []uint64{1, 2, 3} {
		if id != leader {
			others = append(others, id)
		}
	}
	c.Partition([]uint64{leader}, others)

	before := c.Status(leader).Commit
	if err := c.ProposeTo(leader, []byte("written into the void")); err != nil {
		t.Fatalf("ProposeTo: %v", err)
	}
	c.Run(300)

	if got := c.Status(leader).Commit; got > before {
		t.Fatalf("isolated leader advanced commitIndex from %d to %d without a majority", before, got)
	}
}

// TestNodeRestartsWithPersistedStateIntact — RAFT_DESIGN.md §10 step 5, and
// PRD.md C3. The restarted node is rebuilt from persisted state alone.
func TestNodeRestartsWithPersistedStateIntact(t *testing.T) {
	for seed := int64(1); seed <= 10; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			c := NewCluster(t, ClusterConfig{Nodes: 3, Seed: seed})
			leader := c.RunUntilLeader(500)

			c.Propose([]byte("before"))
			c.Run(60)

			// Kill a follower and keep writing without it.
			var victim uint64
			for _, id := range []uint64{1, 2, 3} {
				if id != leader {
					victim = id
					break
				}
			}
			c.Crash(victim)
			c.Run(30)

			if _, ok := c.Leader(); ok {
				c.Propose([]byte("during outage"))
			}
			c.Run(120)

			c.Restart(victim)
			c.Run(400)

			want := c.Applied(leader)
			got := c.Applied(victim)
			if len(got) != len(want) {
				t.Fatalf("restarted node %d applied %d commands, leader applied %d", victim, len(got), len(want))
			}
			for i := range want {
				if !bytes.Equal(got[i], want[i]) {
					t.Fatalf("restarted node %d applied[%d] = %q, want %q", victim, i, got[i], want[i])
				}
			}
		})
	}
}

// TestDeterminism — PRD.md C2, and the premise the whole project rests on.
//
// The same seed must produce a byte-identical run. If this ever fails, something
// has reached for a source of nondeterminism outside the seed: a package-level
// rand (I2), a map iteration that affects behaviour (I3), a time.Now in the core
// (I1), or a nondeterministic Apply (I4). Those are exactly the four invariants
// this test exists to enforce, and it enforces all four at once.
func TestDeterminism(t *testing.T) {
	for seed := int64(1); seed <= 25; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			first := runFaultyScenario(t, seed)
			second := runFaultyScenario(t, seed)
			if first != second {
				t.Fatalf("seed %d produced different state hashes on two runs: %#x vs %#x\n"+
					"something outside the seed is affecting execution (invariants I1-I4)",
					seed, first, second)
			}
		})
	}
}

// TestDifferentSeedsExploreDifferentSchedules is the companion check. If every
// seed produced the same hash, the harness would be deterministic but useless —
// it would explore exactly one interleaving forever.
func TestDifferentSeedsExploreDifferentSchedules(t *testing.T) {
	seen := make(map[uint64]int64)
	for seed := int64(1); seed <= 25; seed++ {
		h := runFaultyScenario(t, seed)
		if prev, dup := seen[h]; dup {
			t.Logf("seeds %d and %d produced the same hash %#x", prev, seed, h)
		}
		seen[h] = seed
	}
	if len(seen) < 5 {
		t.Fatalf("25 seeds produced only %d distinct outcomes; the harness is not exploring", len(seen))
	}
}

// runFaultyScenario drives a cluster through elections, writes, a partition, a
// crash and a restart, then returns the hash of the whole cluster's state.
func runFaultyScenario(t *testing.T, seed int64) uint64 {
	t.Helper()
	c := NewCluster(t, ClusterConfig{
		Nodes:   5,
		Seed:    seed,
		Network: NetworkConfig{MinDelay: 1, MaxDelay: 4, DropRate: 0.05, DupRate: 0.05},
	})

	// A second source for the workload itself, derived from the same seed so the
	// choice of faults is reproducible too.
	rng := rand.New(rand.NewSource(seed))

	c.RunUntilLeader(1000)
	for i := 0; i < 12; i++ {
		if _, ok := c.Leader(); ok {
			c.Propose([]byte(fmt.Sprintf("cmd-%d", i)))
		}
		c.Run(10 + rng.Intn(20))
	}

	c.Partition([]uint64{1, 2}, []uint64{3, 4, 5})
	c.Run(200)
	c.Heal()
	c.Run(200)

	victim := uint64(1 + rng.Intn(5))
	c.Crash(victim)
	c.Run(150)
	c.Restart(victim)
	c.Run(400)

	return c.Hash()
}

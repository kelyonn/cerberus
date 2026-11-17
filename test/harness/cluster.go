package harness

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"slices"

	"github.com/kelyonn/cerberus/internal/raft"
)

// TB is the slice of testing.TB the harness needs. Taking an interface rather
// than testing.TB directly lets the cluster run outside a test — which is what
// cmd/cerberussim does to render a live trace.
type TB interface {
	Helper()
	Fatalf(format string, args ...any)
	Logf(format string, args ...any)
}

// ClusterConfig describes a simulated cluster.
type ClusterConfig struct {
	Nodes            int
	Seed             int64
	Network          NetworkConfig
	ElectionTimeout  int
	HeartbeatTimeout int

	// OnDeliver, if set, is called for every message that actually arrives —
	// after drops and partitions have had their say. This is the observability
	// hook from ARCHITECTURE.md §9: it is how you watch an election happen
	// rather than inferring it from the outcome.
	OnDeliver func(tick uint64, m raft.Message)
}

// WithDefaults fills in the timing from RAFT_DESIGN.md §9. At a 10ms tick,
// 15 ticks is 150ms, so the randomized election timeout lands in the paper's
// 150–300ms band and heartbeats go out every 50ms.
func (c ClusterConfig) WithDefaults() ClusterConfig {
	if c.Nodes == 0 {
		c.Nodes = 3
	}
	if c.ElectionTimeout == 0 {
		c.ElectionTimeout = 15
	}
	if c.HeartbeatTimeout == 0 {
		c.HeartbeatTimeout = 5
	}
	if c.Network == (NetworkConfig{}) {
		c.Network = DefaultNetwork()
	}
	return c
}

// Cluster is a whole Raft cluster in one goroutine, with simulated storage, a
// simulated network, and a simulated clock.
//
// There are no goroutines here and no sleeps anywhere. A test that fails does so
// at a specific tick, from a specific seed, and will do so again every time.
type Cluster struct {
	t   TB
	cfg ClusterConfig

	ids   []uint64 // sorted; invariant I3
	nodes map[uint64]*raft.Node
	rngs  map[uint64]*rand.Rand

	net     *Network
	checker *Checker
	now     uint64

	// Simulated durable storage, one per node. Survives Crash.
	hard   map[uint64]raft.HardState
	stable map[uint64][]raft.Entry

	// Simulated state machine: the entries each node has applied, in order.
	// The real KV store arrives with step 6 of RAFT_DESIGN.md §10; for
	// consensus tests, "what was applied and in what order" is the whole
	// question.
	applied map[uint64][]raft.Entry

	crashed map[uint64]bool
}

// NewCluster builds a cluster of Nodes members with IDs 1..N.
func NewCluster(t TB, cfg ClusterConfig) *Cluster {
	t.Helper()
	cfg = cfg.WithDefaults()

	// One master source per run. Every other source in the system descends from
	// it, so the entire run is a pure function of Seed (invariant I2).
	master := rand.New(rand.NewSource(cfg.Seed))

	c := &Cluster{
		t:       t,
		cfg:     cfg,
		nodes:   make(map[uint64]*raft.Node),
		rngs:    make(map[uint64]*rand.Rand),
		hard:    make(map[uint64]raft.HardState),
		stable:  make(map[uint64][]raft.Entry),
		applied: make(map[uint64][]raft.Entry),
		crashed: make(map[uint64]bool),
		checker: NewChecker(),
		net:     NewNetwork(cfg.Network, rand.New(rand.NewSource(master.Int63()))),
	}

	for i := 1; i <= cfg.Nodes; i++ {
		c.ids = append(c.ids, uint64(i))
	}
	slices.Sort(c.ids)

	for _, id := range c.ids {
		c.rngs[id] = rand.New(rand.NewSource(master.Int63()))
		n, err := raft.New(c.nodeConfig(id))
		if err != nil {
			t.Fatalf("harness: creating node %d: %v", id, err)
		}
		c.nodes[id] = n
	}
	return c
}

func (c *Cluster) nodeConfig(id uint64) raft.Config {
	return raft.Config{
		ID:               id,
		Peers:            slices.Clone(c.ids),
		ElectionTimeout:  c.cfg.ElectionTimeout,
		HeartbeatTimeout: c.cfg.HeartbeatTimeout,
		Rand:             c.rngs[id],
	}
}

// Step advances the cluster by one tick: every live node ticks, due messages are
// delivered, every live node's driver loop runs, and the safety properties are
// asserted.
func (c *Cluster) Step() {
	c.t.Helper()
	c.now++

	for _, id := range c.ids {
		if !c.crashed[id] {
			c.nodes[id].Tick()
		}
	}

	for _, m := range c.net.Deliver(c.now) {
		if c.crashed[m.To] {
			continue // a crashed node is not listening
		}
		if c.cfg.OnDeliver != nil {
			c.cfg.OnDeliver(c.now, m)
		}
		if err := c.nodes[m.To].Step(m); err != nil {
			c.t.Fatalf("harness: tick %d: node %d rejected %s from %d: %v",
				c.now, m.To, m.Type, m.From, err)
		}
	}

	for _, id := range c.ids {
		if !c.crashed[id] {
			c.drive(id)
		}
	}

	c.checker.SetTick(c.now)
	if err := c.checker.Check(c.liveIDs(), c.nodes); err != nil {
		c.t.Fatalf("harness: tick %d, seed %d: %v", c.now, c.cfg.Seed, err)
	}
}

// drive is the driver loop from ARCHITECTURE.md §3.1. The ordering of the body
// is load-bearing: persist, then send, then apply.
func (c *Cluster) drive(id uint64) {
	c.t.Helper()
	n := c.nodes[id]

	rd := n.Ready()
	if rd.IsEmpty() {
		return
	}

	// 1 & 2. Persist. In the real implementation these are fsync'd writes to
	// hardstate and the log segments; here they are just maps, but they survive
	// Crash, which is the property that matters.
	if rd.HardState != nil {
		c.hard[id] = *rd.HardState
	}
	if len(rd.Entries) > 0 {
		c.storeEntries(id, rd.Entries)
	}

	// 3. Only now may messages go out. INVARIANT I5. If this line moved above
	// the two blocks over it, the cluster would still pass every happy-path
	// test and would lose data on crash.
	for _, m := range rd.Messages {
		c.net.Send(c.now, m)
	}

	// 4. Apply.
	for _, e := range rd.CommittedEntries {
		if err := c.checker.ObserveApply(id, e); err != nil {
			c.t.Fatalf("harness: tick %d, seed %d: %v", c.now, c.cfg.Seed, err)
		}
		c.applied[id] = append(c.applied[id], e)
	}

	// 5.
	n.Advance()
}

// storeEntries writes entries to simulated durable storage, overwriting at their
// index rather than blindly appending — a follower that resolved a conflict is
// rewriting an index it already had.
func (c *Cluster) storeEntries(id uint64, ents []raft.Entry) {
	log := c.stable[id]
	for _, e := range ents {
		pos := int(e.Index) - 1
		switch {
		case pos < len(log):
			log = log[:pos]
			log = append(log, e)
		case pos == len(log):
			log = append(log, e)
		default:
			c.t.Fatalf("harness: node %d: gap in persisted log, index %d with %d entries stored",
				id, e.Index, len(log))
		}
	}
	c.stable[id] = log
}

// --- faults -----------------------------------------------------------------

// Crash kills a node. Volatile state is lost; the contents of hard and stable
// survive, exactly as a SIGKILL would leave them.
func (c *Cluster) Crash(id uint64) {
	c.t.Helper()
	c.crashed[id] = true
}

// Restart brings a crashed node back, rebuilding it from persisted state only.
func (c *Cluster) Restart(id uint64) {
	c.t.Helper()
	n, err := raft.Restart(c.nodeConfig(id), c.hard[id], slices.Clone(c.stable[id]))
	if err != nil {
		c.t.Fatalf("harness: restarting node %d: %v", id, err)
	}
	c.nodes[id] = n
	c.applied[id] = nil // the state machine is rebuilt by replaying the log
	c.crashed[id] = false
}

// Partition isolates the listed groups from each other.
func (c *Cluster) Partition(groups ...[]uint64) { c.net.Partition(groups...) }

// Heal removes all partitions.
func (c *Cluster) Heal() { c.net.Heal() }

// --- inspection -------------------------------------------------------------

func (c *Cluster) liveIDs() []uint64 {
	var out []uint64
	for _, id := range c.ids {
		if !c.crashed[id] {
			out = append(out, id)
		}
	}
	return out
}

// Leader returns the current leader if there is exactly one among live nodes.
func (c *Cluster) Leader() (uint64, bool) {
	var found []uint64
	for _, id := range c.liveIDs() {
		if c.nodes[id].Status().Role == raft.RoleLeader {
			found = append(found, id)
		}
	}
	if len(found) == 1 {
		return found[0], true
	}
	return 0, false
}

// RunUntilLeader steps until a single leader exists, or fails the test.
func (c *Cluster) RunUntilLeader(maxSteps int) uint64 {
	c.t.Helper()
	for i := 0; i < maxSteps; i++ {
		c.Step()
		if id, ok := c.Leader(); ok {
			return id
		}
	}
	c.t.Fatalf("harness: seed %d: no leader after %d steps%s", c.cfg.Seed, maxSteps, c.dump())
	return 0
}

// Run steps the cluster n times.
func (c *Cluster) Run(n int) {
	c.t.Helper()
	for i := 0; i < n; i++ {
		c.Step()
	}
}

// Propose submits a command to the current leader, failing if there isn't one.
func (c *Cluster) Propose(cmd []byte) uint64 {
	c.t.Helper()
	id, ok := c.Leader()
	if !ok {
		c.t.Fatalf("harness: seed %d: Propose with no leader%s", c.cfg.Seed, c.dump())
	}
	if err := c.nodes[id].Propose(cmd); err != nil {
		c.t.Fatalf("harness: proposing to node %d: %v", id, err)
	}
	return id
}

// ProposeTo submits a command to a specific node, returning whatever the node
// says. Used to write to a node that believes it is leader but may not be —
// which is the interesting case.
func (c *Cluster) ProposeTo(id uint64, cmd []byte) error {
	return c.nodes[id].Propose(cmd)
}

// Status returns a node's status.
func (c *Cluster) Status(id uint64) raft.Status { return c.nodes[id].Status() }

// Now is the current tick.
func (c *Cluster) Now() uint64 { return c.now }

// IDs returns the cluster's node IDs, sorted.
func (c *Cluster) IDs() []uint64 { return slices.Clone(c.ids) }

// IsCrashed reports whether a node is currently down.
func (c *Cluster) IsCrashed(id uint64) bool { return c.crashed[id] }

// LogOf returns a copy of a node's log, for rendering.
func (c *Cluster) LogOf(id uint64) []raft.Entry { return c.nodes[id].LogEntries() }

// Applied returns the commands a node's state machine has applied, no-op
// entries excluded — those are Raft's business, not the state machine's.
func (c *Cluster) Applied(id uint64) [][]byte {
	var out [][]byte
	for _, e := range c.applied[id] {
		if e.Data != nil {
			out = append(out, e.Data)
		}
	}
	return out
}

// Hash summarises the entire cluster's state in one number.
//
// This is what makes determinism checkable: run a seed twice, hash both, and any
// difference at all — a different election timeout, a different map iteration,
// a stray time.Now — shows up as a different number. Invariants I1 through I4
// are, in the end, enforced by this function.
func (c *Cluster) Hash() uint64 {
	h := fnv.New64a()
	for _, id := range c.ids {
		st := c.nodes[id].Status()
		fmt.Fprintf(h, "n%d r%d t%d v%d c%d a%d|",
			st.ID, st.Role, st.Term, st.Vote, st.Commit, st.Applied)
		for _, e := range c.nodes[id].LogEntries() {
			fmt.Fprintf(h, "%d@%d:%x,", e.Index, e.Term, e.Data)
		}
		h.Write([]byte(";"))
	}
	return h.Sum64()
}

// dump renders the cluster for a failure message. ARCHITECTURE.md §9 promises
// this will get used constantly, which is why it exists before it is needed.
func (c *Cluster) dump() string {
	s := fmt.Sprintf("\n  tick %d, %d in flight", c.now, c.net.InFlight())
	for _, id := range c.ids {
		st := c.nodes[id].Status()
		state := st.Role.String()
		if c.crashed[id] {
			state = "CRASHED"
		}
		s += fmt.Sprintf("\n  n%d %-9s term=%d vote=%d lead=%d commit=%d applied=%d last=%d",
			id, state, st.Term, st.Vote, st.Lead, st.Commit, st.Applied, st.LastIndex)
	}
	return s
}

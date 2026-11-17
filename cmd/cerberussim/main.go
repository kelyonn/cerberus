// Command cerberussim runs a Cerberus cluster under the deterministic harness
// and renders what happens as a readable trace.
//
// There is no network and no disk here — the real transport and storage layers
// are steps 5 and 10 of docs/RAFT_DESIGN.md §10 and are not written yet. What
// this shows is the consensus core doing consensus: elections, replication,
// commit, and how it behaves when the cluster is cut in half or loses a leader.
//
//	go run ./cmd/cerberussim                    # the guided scenario
//	go run ./cmd/cerberussim -seed 42           # a different schedule
//	go run ./cmd/cerberussim -msgs              # every message delivery
//	go run ./cmd/cerberussim -scenario election # just one phase
//
// Every run is a pure function of its seed. Re-run the same seed and you get
// the same trace, tick for tick.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/kelyonn/cerberus/internal/raft"
	"github.com/kelyonn/cerberus/test/harness"
)

var (
	seed     = flag.Int64("seed", 1, "run seed; the same seed always produces the same trace")
	nodes    = flag.Int("nodes", 5, "cluster size")
	showMsgs = flag.Bool("msgs", false, "trace every message delivery, not just state changes")
	scenario = flag.String("scenario", "all", "all | election | replication | partition | crash")
	color    = flag.Bool("color", true, "colourise output")
)

func main() {
	flag.Parse()

	sim := &sim{}
	cfg := harness.ClusterConfig{
		Nodes:   *nodes,
		Seed:    *seed,
		Network: harness.NetworkConfig{MinDelay: 1, MaxDelay: 3},
	}
	if *showMsgs {
		cfg.OnDeliver = func(tick uint64, m raft.Message) { sim.msg(tick, m) }
	}

	c := harness.NewCluster(sim, cfg)
	sim.c = c
	sim.prev = make(map[uint64]raft.Status)

	header(*seed, *nodes)

	switch *scenario {
	case "election":
		sim.election(c)
	case "replication":
		sim.election(c)
		sim.replication(c)
	case "partition":
		sim.election(c)
		sim.replication(c)
		sim.partition(c)
	case "crash":
		sim.election(c)
		sim.replication(c)
		sim.crash(c)
	default:
		sim.election(c)
		sim.replication(c)
		sim.partition(c)
		sim.crash(c)
	}

	sim.table(c, "final state")
	fmt.Println()
	fmt.Println(dim("  Every line above is reproducible: same seed, same trace, same tick numbers."))
	fmt.Printf("%s\n\n", dim(fmt.Sprintf("  Re-run with:  go run ./cmd/cerberussim -seed %d", *seed)))
}

// ---------------------------------------------------------------- scenarios

func (s *sim) election(c *harness.Cluster) {
	phase("1", "Leader election",
		"All nodes start as followers at term 0. The first to time out stands for",
		"election. Timeouts are randomized, which is what stops split votes.")

	leader := s.runUntilLeader(c, 600)
	note(fmt.Sprintf("node %d won. It immediately appended a no-op entry —", leader))
	note("without an entry of its own term it could not commit anything at all.")
	note("That is the Figure 8 rule (docs/RAFT_DESIGN.md §5).")
	s.table(c, "after election")
}

func (s *sim) replication(c *harness.Cluster) {
	phase("2", "Log replication",
		"Three client writes. Each is appended by the leader, replicated, and",
		"committed once a majority reports it — then applied by every node.")

	for i, cmd := range []string{"set user:1 = alice", "set user:2 = bob", "del user:1"} {
		id, _ := c.Leader()
		c.Propose([]byte(cmd))
		event(c.Now(), fmt.Sprintf("client → n%d   %s", id, cmd))
		s.run(c, 40)
		_ = i
	}
	s.run(c, 60)
	s.table(c, "after replication")
}

func (s *sim) partition(c *harness.Cluster) {
	leader, ok := c.Leader()
	if !ok {
		return
	}
	ids := c.IDs()

	// Strand the leader with a minority.
	var minority, majority []uint64
	minority = append(minority, leader)
	for _, id := range ids {
		if id == leader {
			continue
		}
		if len(minority) < len(ids)/2 {
			minority = append(minority, id)
		} else {
			majority = append(majority, id)
		}
	}

	phase("3", "Network partition",
		fmt.Sprintf("Cutting the cluster: {%s} | {%s}.", join(minority), join(majority)),
		"The old leader is stranded in the minority. It does not know that yet.")

	c.Partition(minority, majority)
	before := c.Status(leader).Commit

	if err := c.ProposeTo(leader, []byte("set doomed = yes")); err == nil {
		event(c.Now(), fmt.Sprintf("client → n%d   set doomed = yes  %s", leader, dim("(accepted into its log)")))
	}
	s.run(c, 300)

	after := c.Status(leader).Commit
	if after == before {
		note(fmt.Sprintf("n%d appended the write but never committed it — commitIndex stuck at %d.", leader, after))
		note("No majority is reachable, so it correctly refuses to make progress")
		note("rather than acknowledging a write it cannot guarantee.")
	}
	if nl, ok := c.Leader(); ok && nl != leader {
		note(fmt.Sprintf("Meanwhile the majority side elected n%d at a higher term.", nl))
	}
	s.table(c, "during partition")

	phase("4", "Healing",
		"Reconnecting everything. The stale leader learns of the higher term,",
		"steps down, and has its uncommitted entry overwritten.")
	c.Heal()
	s.run(c, 300)
	note("The uncommitted write is gone. Nothing was ever acknowledged to a client,")
	note("so nothing was lost — that entry was never committed.")
	s.table(c, "after heal")
}

func (s *sim) crash(c *harness.Cluster) {
	leader, ok := c.Leader()
	if !ok {
		return
	}

	phase("5", "Leader crash and recovery",
		fmt.Sprintf("Killing n%d, the current leader. Volatile state is lost;", leader),
		"its persisted HardState and log survive, as a SIGKILL would leave them.")

	c.Crash(leader)
	event(c.Now(), fmt.Sprintf("n%d CRASHED", leader))

	next := s.runUntilLeader(c, 600)
	note(fmt.Sprintf("n%d took over. It holds every committed entry — guaranteed, because", next))
	note("it needed a majority of votes, and any majority overlaps the majority that")
	note("held each committed entry. Those nodes refuse to vote for a stale log.")

	id, _ := c.Leader()
	c.Propose([]byte("set after = crash"))
	event(c.Now(), fmt.Sprintf("client → n%d   set after = crash", id))
	s.run(c, 80)

	event(c.Now(), fmt.Sprintf("n%d RESTARTING from persisted state", leader))
	c.Restart(leader)
	s.run(c, 400)
	note(fmt.Sprintf("n%d rebuilt from HardState + log alone, rejoined, and caught up.", leader))
	s.table(c, "after recovery")
}

// ---------------------------------------------------------------- rendering

type sim struct {
	c    *harness.Cluster
	prev map[uint64]raft.Status
}

// run steps the cluster, reporting every state change as it happens.
func (s *sim) run(c *harness.Cluster, ticks int) {
	for i := 0; i < ticks; i++ {
		c.Step()
		s.diff(c)
	}
}

func (s *sim) runUntilLeader(c *harness.Cluster, max int) uint64 {
	for i := 0; i < max; i++ {
		c.Step()
		s.diff(c)
		if id, ok := c.Leader(); ok {
			return id
		}
	}
	fmt.Println(red("  no leader elected — this should not happen on a healthy cluster"))
	os.Exit(1)
	return 0
}

// diff prints what changed since the last tick. This is the whole trick to
// making a simulation legible: report transitions, not state.
func (s *sim) diff(c *harness.Cluster) {
	for _, id := range c.IDs() {
		if c.IsCrashed(id) {
			continue
		}
		now := c.Status(id)
		was, seen := s.prev[id]
		s.prev[id] = now
		if !seen {
			continue
		}

		switch {
		case now.Role != was.Role:
			line := fmt.Sprintf("n%d  %s → %s", id, was.Role, roleColor(now.Role))
			if now.Term != was.Term {
				line += dim(fmt.Sprintf("  (term %d → %d)", was.Term, now.Term))
			} else {
				line += dim(fmt.Sprintf("  (term %d)", now.Term))
			}
			event(c.Now(), line)

		case now.Term != was.Term:
			// A term bump with no role change: this node learned of a newer term
			// from someone else's message and followed it.
			event(c.Now(), dim(fmt.Sprintf("n%d  term %d → %d", id, was.Term, now.Term)))
		}
		if now.Commit > was.Commit {
			event(c.Now(), dim(fmt.Sprintf("n%d  commitIndex %d → %d", id, was.Commit, now.Commit)))
		}
	}
}

func (s *sim) msg(tick uint64, m raft.Message) {
	var detail string
	switch m.Type {
	case raft.MsgVote:
		detail = fmt.Sprintf("lastLog=%d@%d", m.LastLogIndex, m.LastLogTerm)
	case raft.MsgVoteResp:
		detail = map[bool]string{true: "GRANTED", false: "denied"}[m.Success]
	case raft.MsgApp:
		detail = fmt.Sprintf("prev=%d@%d entries=%d commit=%d",
			m.PrevLogIndex, m.PrevLogTerm, len(m.Entries), m.LeaderCommit)
	case raft.MsgAppResp:
		if m.Success {
			detail = fmt.Sprintf("ok match=%d", m.MatchIndex)
		} else {
			detail = "REJECTED"
		}
	}
	fmt.Printf("  %s  %s\n",
		dim(fmt.Sprintf("tick %5d", tick)),
		dim(fmt.Sprintf("n%d → n%d  %-12s t%-2d  %s", m.From, m.To, m.Type, m.Term, detail)))
}

func (s *sim) table(c *harness.Cluster, title string) {
	fmt.Printf("\n  %s\n", bold(title))
	fmt.Printf("  %s\n", dim("node   state       term  vote  lead  commit  applied  log"))
	for _, id := range c.IDs() {
		if c.IsCrashed(id) {
			fmt.Printf("  n%-4d %s\n", id, red("CRASHED"))
			continue
		}
		st := c.Status(id)
		fmt.Printf("  n%-4d %-11s %4d  %4s  %4s  %6d  %7d  %s\n",
			id, roleColor(st.Role), st.Term,
			idOrDash(st.Vote), idOrDash(st.Lead),
			st.Commit, st.Applied, renderLog(c.LogOf(id), st.Commit))
	}
	fmt.Println()
}

// renderLog draws the log as terms, marking the committed prefix.
func renderLog(ents []raft.Entry, commit uint64) string {
	if len(ents) == 0 {
		return dim("(empty)")
	}
	var b strings.Builder
	for _, e := range ents {
		cell := fmt.Sprintf("%d", e.Term)
		if e.Data == nil {
			cell += "·" // no-op entry
		}
		if e.Index <= commit {
			b.WriteString(green(cell))
		} else {
			b.WriteString(dim(cell))
		}
		b.WriteString(" ")
	}
	return strings.TrimRight(b.String(), " ")
}

// ---------------------------------------------------------------- chrome

func header(seed int64, nodes int) {
	fmt.Println()
	fmt.Println("  " + bold("CERBERUS") + dim("  ·  deterministic cluster simulation"))
	fmt.Printf("  %s\n", dim(fmt.Sprintf(
		"seed %d · %d nodes · election timeout 15-30 ticks · heartbeat 5 ticks · 1 tick = 10ms",
		seed, nodes)))
	fmt.Printf("  %s\n", dim("log column shows entry TERMS; green = committed, · = no-op"))
	fmt.Println()
}

func phase(n, title string, lines ...string) {
	fmt.Printf("\n  %s %s\n", accent("──"+n+"──"), bold(title))
	for _, l := range lines {
		fmt.Printf("  %s\n", dim(l))
	}
	fmt.Println()
}

func event(tick uint64, msg string) {
	fmt.Printf("  %s  %s\n", dim(fmt.Sprintf("tick %5d", tick)), msg)
}

func note(msg string) { fmt.Printf("  %s %s\n", dim("│"), dim(msg)) }

func idOrDash(v uint64) string {
	if v == raft.None {
		return "-"
	}
	return fmt.Sprintf("n%d", v)
}

func join(ids []uint64) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("n%d", id)
	}
	return strings.Join(parts, " ")
}

func roleColor(r raft.Role) string {
	switch r {
	case raft.RoleLeader:
		return accent("LEADER")
	case raft.RoleCandidate:
		return yellow("candidate")
	}
	return "follower"
}

func wrap(code, s string) string {
	if !*color {
		return s
	}
	return "\033[" + code + "m" + s + "\033[0m"
}

func bold(s string) string   { return wrap("1", s) }
func dim(s string) string    { return wrap("2", s) }
func red(s string) string    { return wrap("31", s) }
func green(s string) string  { return wrap("32", s) }
func yellow(s string) string { return wrap("33", s) }
func accent(s string) string { return wrap("36;1", s) }

// ---------------------------------------------------------------- harness.TB
//
// The harness asserts the five safety properties after every step and calls
// Fatalf on a violation. In a test that fails the test; here it stops the
// simulation loudly, which is the behaviour we want either way.

func (s *sim) Helper() {}

func (s *sim) Logf(format string, args ...any) {
	fmt.Printf("  "+format+"\n", args...)
}

func (s *sim) Fatalf(format string, args ...any) {
	fmt.Printf("\n  %s %s\n\n", red("SAFETY VIOLATION"), fmt.Sprintf(format, args...))
	os.Exit(1)
}

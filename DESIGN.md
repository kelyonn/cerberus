# Cerberus — Bug Log

Writeups of real correctness bugs found by the test harness: the symptom, the
trace, the root cause, and the fix.

**This is the most valuable file in the repository.** Not because bugs are
impressive, but because a written root-cause analysis of a consensus bug is direct
evidence of understanding the algorithm rather than having transcribed it. Anyone
can produce a Raft implementation that passes tests. Very few can explain the
three-node message interleaving that lost a committed entry and why the rule they
skipped exists.

Target: 3–4 entries. They will not need manufacturing.

---

## How to write an entry

Write it when the bug is fixed and fresh, not later. Fifteen minutes each.

1. **Symptom** — What failed. Which invariant (S1–S5) or which linearizability
   violation. The seed.
2. **Trace** — The relevant slice of history. Trim aggressively: the five or ten
   messages that matter, not the full dump.
3. **Root cause** — Which rule from `docs/RAFT_DESIGN.md` was violated, and *why
   the code violated it*. "I truncated the log unconditionally" is a root cause.
   "There was a race" is not.
4. **Fix** — The change, plus the regression test that now pins it.
5. **Why the tests missed it before** — Often the most interesting part. What kind
   of fault injection was needed to surface it, and what does that say about
   coverage?

---

## Template

```markdown
## BUG-NNN — <one-line description>

**Found:** YYYY-MM-DD · seed `NNNNN` · `TestName`
**Invariant violated:** S3 (Log Matching)
**Severity:** data loss | liveness | stale read

### Symptom
What the harness reported, verbatim where useful.

### Trace
```
tick 1042  n1(L,t3) → n2  AppendEntries prevIdx=7 prevTerm=2 entries=[8@3]
tick 1043  n2        →    log truncated from idx 7
...
```

### Root cause
The rule that was violated, and the specific line of code that violated it.

### Fix
What changed. Link the commit.

### Why the tests missed it
Which fault was needed to surface it — reordering? duplication? a partition heal
at a specific moment?
```

---

## Entries

*None yet — implementation starts Day 1. Expect the first around Day 12, when the
invariant checkers come online and the first randomized runs go red.*

<!--
Do not delete this comment.

When the first randomized fault-injection run fails, that is not a setback. It is
the harness doing exactly the job it was built two weeks early to do. Write it up.
-->

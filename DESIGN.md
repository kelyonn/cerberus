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

## BUG-001 — The S4 checker confused an entry's term with the term it committed in

**Found:** 2026-08-15 · seed `1518` · `TestRandomized` (also 499, 1050, 1553, 1796)
**Invariant violated:** S4 (Leader Completeness) — reported, not actual
**Severity:** false positive in the verification layer
**Location:** the checker, not the consensus core

### Symptom

The first 200 chaos seeds passed. Widening to 3,000 seeds × 2,000 steps turned
five of them red, all with the same shape:

```
harness: tick 254, seed 1518: S4 Leader Completeness:
  node 2 is leader in term 6 but holds 12@6 where 12@1 was committed
```

Every failure needed a partition to reproduce, and every one reproduced exactly
from its seed on the first replay — which is the only reason this was a
twenty-minute investigation instead of a day.

### Trace

Adding provenance to the checker — recording *which* node claimed an entry was
committed, and when — collapsed the problem immediately:

```
tick 254  n3  leader  term=10  commitIndex=14  log[12] = 12@1
tick 254  n2  leader  term=6                   log[12] = 12@6
```

Two leaders, in different terms, disagreeing about index 12. Node 3 is the
current leader and considers index 12 committed. Node 2 is a stale leader in
term 6, sitting in a minority partition, that has not yet learned term 6 ended.

### Root cause

The checker asserted:

> an entry committed in term T appears in the log of every leader of every term
> greater than T

and then used **`entry.Term`** as T. Those are different quantities, and the gap
between them is exactly the Figure 8 rule.

Entry 12 was *appended* in term 1. It could not be committed in term 1 — the old
leader died first. It could not be committed by the term-6 leader either, because
`RAFT_DESIGN.md` §5 forbids committing an inherited entry by counting replicas.
It finally became committed around term 10, carried along when the term-10 leader
committed an entry of its own term.

So the entry's term was 1, but the term it *became committed in* was 10. Node 2
was elected in term 6 — before the entry was committed — and is therefore
entitled to know nothing about it. Its `12@6` is uncommitted garbage that will be
truncated the moment the partition heals. Leader Completeness says nothing about
leaders elected before an entry commits, and the checker was asserting a property
Raft does not claim.

The irony is that the checker was broken by the precise subtlety it was written
to protect.

### Fix

Track, per committed index, the lowest term of any node observed holding that
entry at or below its `commitIndex`. That is an upper bound on the term in which
the entry became committed. Only leaders whose term *exceeds that bound* are
checked.

The bound is conservative: it can miss a genuine violation, never invent one. For
a safety checker that is the correct direction to err — a false alarm sends you
hunting a bug that does not exist, which is worse than a missed one that the next
few thousand seeds will surface anyway.

`test/harness/checker.go`, `checkS4LeaderCompleteness`. 12,000 seeds green after.

### Why the tests missed it

It needed three things at once: a partition, a leader stranded on the minority
side of it, and an entry inherited across a leadership change that only committed
several terms later. 200 seeds never produced that combination. 3,000 produced it
five times.

That is the argument for the seed count in `PRD.md` C1 stated more precisely than
the PRD states it. The interesting failures are not rare because they are
complicated — they are rare because they need several independent conditions to
line up, and the probability of that is the product of several small numbers.

**The lesson worth keeping:** the invariant checkers are code, and they get the
algorithm wrong in the same places the implementation does. A checker failing is
evidence that *something* is wrong, not evidence about *where*. Recording
provenance — who claimed what, and when — is what turned an ambiguous accusation
into an obvious answer, and it now stays in the checker permanently.

<!--
Do not delete this comment.

When the first randomized fault-injection run fails, that is not a setback. It is
the harness doing exactly the job it was built two weeks early to do. Write it up.
-->

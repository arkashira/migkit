"""Routines held to the source over sequences of calls, not only one call
at a time (leapfrog research P8).

A single call starts from the fixture and is rolled back, so what a call
leaves behind for the next is never seen: a row one procedure writes and
a function reads, a counter a second insert moves further, a session
variable or a temporary table a routine keeps between calls, a package's
state, the second call of a routine that behaves differently once a row
exists. Here the routines that answered every single call the same are
called one after another in one transaction on each side - each call
seeing what the ones before it did, and every table compared after each
call against the fixture - then rolled back, the sessions begun afresh.

Which sequences: every routine twice with the same arguments; every
routine that writes a table before every routine that names it; then
Hypothesis draws sequences of up to `LONGEST` calls for as long as the
routines' own call times allow, and shrinks one that differs to the
fewest, simplest calls that still do. Every sequence that ever told the
two sides apart is kept in the counterexample bank and tried first the
next time.

A routine that commits for itself is left out (it would end the
transaction the others are held in), and so is one that answers from a
random source; both are said.
"""
import time
from dataclasses import dataclass

from . import harness
from .harness import HarnessError, differences
from .routines import Routine

#: seconds the search over sequences may take, per database
SECONDS = 60
#: sequences the search runs at most, however fast the routines are
MOST = 150
#: calls in one drawn sequence, at most
LONGEST = 5
#: argument tuples per routine the drawn sequences choose from
DRAWN = 12


@dataclass
class Step:
    """One routine sequences may call, as each side calls it."""
    r: object                     #: the source's routine
    call: object                  #: what the source's side calls
    t: object                     #: the target's version
    argsets: list
    mode: tuple                   #: `harness.clock_mode`'s answer


@dataclass
class Found:
    """The shortest sequence found to differ."""
    calls: list                   #: [(routine, args)] in order
    at: int                       #: the call whose answer differed
    what: list                    #: `differences`' findings

    def said(self):
        shown = []
        for r, args in self.calls[:self.at + 1]:
            params = [p.name for p in r.params]
            shown.append(f"{r.name}(" + ", ".join(
                f"{n}={a!r}" for n, a in zip(params, args)) + ")")
        before = shown[:-1]
        return ((f"after {', then '.join(before)}, " if before else "")
                + f"{shown[-1]}: {self.what[0]}")


def steps(proof, pairs):
    """([Step], [(routine, why it is left out)])."""
    from .prove import commits_itself
    out, left = [], []
    a, b = proof.a, proof.b
    for r, t in pairs:
        if r.kind not in ("function", "procedure", "event"):
            continue
        if harness.reads_chance(r.body):
            left.append((r, "it answers from a random source"))
            continue
        if commits_itself(a.engine, r) or commits_itself(b.engine, t):
            left.append((r, "it commits for itself, which would end the"
                            " transaction the others are held in"))
            continue
        call = r if r.kind != "event" else Routine(
            kind="procedure", name=f"migkit_event_{r.name}")
        argsets = proof.argsets(a, r, t)[:DRAWN] or [()]
        mode = harness.clock_mode(a, b, r.body or "", t.body or "", False)
        out.append(Step(r, call, t, argsets, mode))
    return out, left


def _names_table(routine, table):
    import re
    leaf = str(table).split(".")[-1]
    return bool(re.search(rf"(?i)(?<![\w$]){re.escape(leaf)}(?![\w$])",
                          routine.body or routine.settings.get("expression")
                          or ""))


def seeds(proof, found_steps):
    """The sequences tried first: each routine twice with the same
    arguments, and each routine that writes a table before each that
    names it."""
    from .prove import written
    out = []
    tables = list(proof.a.watched)
    for i, s in enumerate(found_steps):
        out.append([(i, s.argsets[0]), (i, s.argsets[0])])
    for i, w in enumerate(found_steps):
        writes = written(w.r, proof.src_known, tables) \
            if w.r.kind != "event" else set(tables)
        if not writes:
            continue
        for j, r in enumerate(found_steps):
            if i != j and any(_names_table(r.r, t) for t in writes):
                out.append([(i, w.argsets[0]), (j, r.argsets[0])])
    return out


def _compared(x, y, a, b):
    """The two outcomes as a held call compares them: without the
    counters where either side cannot read one without moving it."""
    if "oracle" in (a.engine, b.engine):
        x.counters, y.counters = {}, {}
    return x, y


def run(proof, found_steps, seq):
    """(the call that differed, what) for one sequence [(step, args)],
    or None where every call answered the same."""
    a, b = proof.a, proof.b
    tables = list(a.watched)
    a.hold()
    b.hold()
    try:
        for k, (i, args) in enumerate(seq):
            s = found_steps[i]
            x, y, clock = proof._both(
                a, b, lambda: proof._render(s.r, a.call(
                    s.call, list(args), tables)),
                lambda: proof._render(s.r, b.call(
                    s.t, list(args), tables)), s.mode)
            x, y = _compared(x, y, a, b)
            got = differences(x, y, clock)
            if got:
                return k, got
            if x.error or y.error:
                # a failure alike on both: what a transaction is left fit
                # for after one is each engine's own (SQL Server's may be
                # doomed), so the sequence ends here
                return None
    finally:
        for side in (a, b):
            try:
                side.release()
            except Exception:  # noqa: BLE001 - opened again all the same
                side.held = False
                side.fresh()
    return None


def prove(proof, pairs):
    """([Found], sequences run, [(routine, why left out)]) over the
    routines of `pairs` - (source routine, target version) each already
    made in the sandbox and each answering every single call the same."""
    found_steps, left = steps(proof, pairs)
    if len(found_steps) == 0:
        return [], 0, left
    by_name = {s.r.name.lower(): i for i, s in enumerate(found_steps)}
    runs, found, seen = 0, [], set()

    def keep(seq, got):
        k, what = got
        calls = [(found_steps[i].r, args) for i, args in seq[:k + 1]]
        last = found_steps[seq[k][0]].r.name
        if last in seen:
            return
        seen.add(last)
        found.append(Found(calls, k, what))
        proof.bank.keep_sequence([(r.name, args) for r, args in calls])

    kept = []
    for one in proof.bank.sequences():
        seq = [(by_name.get(str(n).lower()), args) for n, args in one]
        if all(i is not None for i, _ in seq):
            kept.append(seq)
    began = time.monotonic()
    for seq in kept + seeds(proof, found_steps):
        runs += 1
        got = run(proof, found_steps, seq)
        if got:
            keep(seq, got)
    per = (time.monotonic() - began) / max(1, runs)
    most = int(max(5, min(MOST, SECONDS / max(per, 1e-3))))
    got = _search(proof, found_steps, most)
    runs += got[1]
    if got[0]:
        keep(*got[0])
    return found, runs, left


def _search(proof, found_steps, most):
    """((sequence, (call, what)) or None, sequences run): drawn by
    Hypothesis, shrunk to the fewest and simplest calls that differ."""
    from hypothesis import HealthCheck, Phase, given, settings
    from hypothesis import strategies as st
    found, runs = {}, [0]
    n = len(found_steps)

    def seq_of(drawn):
        return [(i, found_steps[i].argsets[j % len(found_steps[i].argsets)])
                for i, j in drawn]

    @settings(max_examples=most, database=None, deadline=None,
              derandomize=True, suppress_health_check=list(HealthCheck),
              phases=[Phase.generate, Phase.shrink])
    @given(st.lists(st.tuples(st.integers(0, n - 1),
                              st.integers(0, DRAWN - 1)),
                    min_size=2, max_size=LONGEST))
    def probe(drawn):
        runs[0] += 1
        seq = seq_of(drawn)
        got = run(proof, found_steps, seq)
        if got:
            found["seq"], found["got"] = seq, got
            raise AssertionError(got[1][0])
    try:
        probe()
    except HarnessError:
        raise
    except Exception:  # noqa: BLE001 - a difference, or one that did not
        # come back when replayed: the last sequence found to differ is
        # the answer
        if found:
            return (found["seq"], found["got"]), runs[0]
        raise
    return None, runs[0]


def said(found, runs, left):
    """What the database's line says of the sequences, beside its
    routines' counts."""
    out = []
    if runs:
        out.append(f"{runs} sequences of calls held in one transaction"
                   + (" answered the same" if not found else
                      f", {len(found)} of them apart"))
    if left:
        out.append("not called in sequences: " + "; ".join(
            f"{r.name} ({why})" for r, why in left[:4]))
    return out

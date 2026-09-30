"""The smallest part of a routine that still differs (leapfrog research
P4: reduce the input and the program).

The search already shrinks the input that tells a source routine and its
converted version apart. With that input fixed, this shrinks the routine:
statements of the source's body are left out - and the rules convert what
is left again, statement for statement, so the two sides stay aligned -
for as long as the two versions still answer that input differently
(Zeller's delta debugging over the body's statements). What is left is
what a person, a model asked to fill the hole, or a learned rule needs:
a few lines, not the whole routine.

It runs on copies made under a name of its own in both sandboxes, dropped
after; the routines under proof are not touched.
"""
from .harness import differences
from .routines import Routine
from .translate import Refused

#: rounds of delta debugging, at most
ROUNDS = 40


def _statements(body):
    """The source body's statements, as (start, end) offsets, from the
    outermost block - or [] where it is not a block of statements."""
    from . import mysql_pg
    try:
        tree = mysql_pg.read(body)
    except Refused:
        return []
    if tree.kind != "block":
        return []
    return [(s.start, s.end) for s in tree.body]


def _without(body, spans, keep):
    """`body` with only the statements at the indexes in `keep`."""
    out, at = [], 0
    for i, (lo, hi) in enumerate(spans):
        out.append(body[at:lo])
        if i in keep:
            out.append(body[lo:hi])
        else:
            # the statement and the semicolon after it
            j = hi
            while j < len(body) and body[j] in " \t\r\n":
                j += 1
            hi = j + 1 if j < len(body) and body[j] == ";" else hi
        at = hi
    out.append(body[at:])
    return "".join(out)


def smallest(proof, routine, args, convert):
    """(the source's lines that are enough for the two versions to differ
    on `args`, what differed), or None where the routine cannot be cut
    into statements (a MySQL routine converted by the rules only)."""
    if proof.a is None or proof.a.engine != "mysql" or \
            routine.kind not in ("function", "procedure"):
        return None
    body = routine.body or ""
    spans = _statements(body)
    if len(spans) < 2:
        return None
    name = f"migkit_reduce_{routine.name}"[:60]

    def differs(keep):
        cut = _without(body, spans, keep)
        src = Routine(kind=routine.kind, name=name, params=routine.params,
                      returns=routine.returns, body=cut,
                      definition=_definition(routine, name, cut),
                      settings=routine.settings)
        try:
            made = convert(src)
            proof.a.drop(src)
            proof.a.create([src.definition], routine.settings)
            proof.b.drop(Routine(kind=routine.kind, name=name))
            proof.b.create(made.statements)
            t = proof.made(src)
        except Exception:  # noqa: BLE001 - that cut does not stand
            for side in (proof.a, proof.b):
                side.conn.rollback()
            return None
        if t is None:
            return None
        x = proof._render(src, proof.a.call(src, list(args), list(
            proof.a.watched)))
        y = proof._render(src, proof.b.call(t, list(args), list(
            proof.b.watched)))
        return differences(x, y) or None

    whole = set(range(len(spans)))
    found = differs(whole)
    if not found:
        _drop(proof, routine, name)
        return None
    keep, n, rounds = sorted(whole), 2, 0
    while len(keep) >= 2 and rounds < ROUNDS:
        rounds += 1
        chunk = max(1, len(keep) // n)
        parts = [keep[i:i + chunk] for i in range(0, len(keep), chunk)]
        cut = False
        for part in parts:
            rest = [k for k in keep if k not in part]
            got = differs(set(rest)) if rest else None
            if got:
                keep, found, cut = rest, got, True
                n = max(n - 1, 2)
                break
        if not cut:
            if n >= len(keep):
                break
            n = min(len(keep), n * 2)
    _drop(proof, routine, name)
    lines = [body[spans[k][0]:spans[k][1]].strip() for k in keep]
    return lines, found


def _definition(routine, name, body):
    params = ", ".join(f"{'' if p.mode == 'in' else p.mode + ' '}`{p.name}`"
                       f" {p.type}" for p in routine.params)
    if routine.kind == "function":
        return (f"create function `{name}`({params}) returns"
                f" {routine.returns} {body}")
    return f"create procedure `{name}`({params}) {body}"


def _drop(proof, routine, name):
    for side in (proof.a, proof.b):
        try:
            side.drop(Routine(kind=routine.kind, name=name))
        except Exception:  # noqa: BLE001 - nothing to drop
            side.conn.rollback()

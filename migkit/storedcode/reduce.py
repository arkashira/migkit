"""The smallest part of a routine that still differs (leapfrog research
P4: reduce the input and the program).

The search already shrinks the input that tells a source routine and its
converted version apart. With that input fixed, this shrinks the routine:
statements of the source's body are left out - and the same candidate
converts what is left again, so the two sides stay aligned - for as long
as the two versions still answer that input differently (Zeller's delta
debugging over the body's statements). What is left is what a person, a
model asked to fill the hole, or a learned rule needs: a few lines, not
the whole routine.

The body is cut by its own language's reading of it: MySQL's by migkit's
parser of it, T-SQL's by its parser, PL/pgSQL's through PostgreSQL's own
parser (`pg_mysql.spans`), and PL/SQL's by its blocks' words (`BEGIN`,
`IF`, `LOOP`, `CASE` and the `END` that closes each). Only the outermost
block's statements are cut; a statement that holds others goes or stays
whole.

It runs on copies made under a name of its own in both sandboxes, dropped
after; the routines under proof are not touched.
"""
import re

from . import harness
from .harness import differences
from .routines import Routine
from .translate import Refused

#: rounds of delta debugging, at most
ROUNDS = 40


def _mysql_spans(body):
    from . import mysql_pg
    try:
        tree = mysql_pg.read(body)
    except Refused:
        return []
    if tree.kind != "block":
        return []
    return [(s.start, s.end) for s in tree.body]


def _tsql_spans(body):
    """The statements of a T-SQL body - inside its outermost BEGIN ... END
    where it is one - by the offsets of the tokens each spans."""
    from sqlglot.tokens import TokenType

    from . import tsql_pg
    try:
        p = tsql_pg._Parser(body)
        if p.w() == "BEGIN" and p.w(1) not in ("TRY", "TRAN", "TRANSACTION",
                                               "DISTRIBUTED"):
            p.i += 1
            until = ("END",)
        else:
            until = ()
        out = []
        while True:
            while p.is_(TokenType.SEMICOLON):
                p.i += 1
            if p.at_end() or p.w() in until:
                return out
            first = p.i
            p.statement()
            if p.i <= first:
                return []
            out.append((p.t[first].start, p.t[p.i - 1].end + 1))
    except Refused:
        return []


def _block_spans(body, dialect):
    """The statements of the outermost BEGIN ... END block of a PL/SQL (or
    PL/pgSQL) body, read by its tokens: [] where it holds none."""
    from sqlglot.dialects.dialect import Dialect
    from sqlglot.tokens import TokenType
    try:
        toks = Dialect.get_or_raise(dialect).tokenize(body)
    except Exception:  # noqa: BLE001 - not cut
        return []
    quoted = {TokenType.STRING, TokenType.IDENTIFIER}
    depth, out, start, skip = 0, [], None, False
    for k, t in enumerate(toks):
        word = t.text.upper() if t.token_type not in quoted else ""
        if skip:
            skip = False
            if word in ("IF", "LOOP", "CASE"):
                continue
        if depth == 0:
            if word == "BEGIN":
                depth = 1
                start = toks[k + 1].start if k + 1 < len(toks) else None
            continue
        if word in ("BEGIN", "CASE", "IF", "LOOP"):
            depth += 1
        elif word == "END":
            depth -= 1
            skip = True
            if depth == 0:
                return out
        elif depth == 1 and word == "EXCEPTION":
            return out
        elif depth == 1 and t.token_type == TokenType.SEMICOLON:
            if start is not None and start < t.start:
                out.append((start, t.start))
            start = toks[k + 1].start if k + 1 < len(toks) else None
    return []


def spans(routine, engine):
    """(start, end) of each statement of the routine's outermost block,
    in its body; [] where it cannot be cut."""
    body = routine.body or ""
    if engine == "mysql":
        return _mysql_spans(body)
    if engine == "mssql":
        return _tsql_spans(body)
    if engine == "postgres":
        from . import pg_mysql
        try:
            return pg_mysql.spans(body)
        except Refused:
            return []
    if engine == "oracle":
        return _block_spans(body, "oracle")
    return []


def _without(body, spans_, keep):
    """`body` with only the statements at the indexes in `keep`."""
    out, at = [], 0
    for i, (lo, hi) in enumerate(spans_):
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


#: the head of a CREATE, up to and without the name it makes
_HEAD = re.compile(r"(?is)^(\s*create\s+(?:or\s+(?:replace|alter)\s+)?"
                   r"(?:definer\s*=\s*\S+\s+)?(?:editionable\s+)?"
                   r"(?:function|procedure|proc)\s+)"
                   r"((?:(?:`[^`]+`|\"[^\"]+\"|\[[^\]]+\]|[\w$#]+)\s*\.\s*)*)"
                   r"(`[^`]+`|\"[^\"]+\"|\[[^\]]+\]|[\w$#]+)")


def renamed(routine, name, body, quote):
    """The routine's own CREATE with `body` in place of its body and
    `name` (quoted by `quote`) in place of its name; None where the body
    is not found in it."""
    made = routine.definition or ""
    old = routine.body or ""
    at = made.rfind(old) if old else -1
    if at < 0:
        return None
    made = made[:at] + body + made[at + len(old):]
    got, n = _HEAD.subn(lambda m: m.group(1) + m.group(2) + quote(name),
                        made, count=1)
    if n == 0:
        # PL/SQL keeps its header in its body: the name is there
        got, n = re.subn(r"(?is)^(\s*create\s+(?:or\s+replace\s+)?"
                         r"(?:editionable\s+)?(?:function|procedure)\s+)"
                         r"(\"[^\"]+\"|[\w$#]+)",
                         lambda m: m.group(1) + quote(name), made, count=1)
    return got if n else None


def smallest(proof, routine, args, convert):
    """(the source's lines that are enough for the two versions to differ
    on `args`, what differed), or None where the routine cannot be cut
    into statements. `convert` makes the target's version of a cut copy -
    the same candidate whose version differed."""
    if proof.a is None or routine.kind not in ("function", "procedure"):
        return None
    engine = proof.a.engine
    body = routine.body or ""
    cuts = spans(routine, engine)
    if len(cuts) < 2:
        return None
    name = f"migkit_reduce_{routine.name}"[:60]
    tables = list(proof.a.watched)

    def differs(keep):
        cut = _without(body, cuts, keep)
        made_src = renamed(routine, name, cut, proof.a.q)
        if made_src is None:
            return None
        src = Routine(kind=routine.kind, name=name, params=routine.params,
                      returns=routine.returns, body=cut,
                      definition=made_src, settings=routine.settings,
                      language=routine.language)
        try:
            made = convert(src)
            proof.a.drop(src)
            proof.a.create(_made_by(src, engine), routine.settings)
            proof.b.drop(Routine(kind=routine.kind, name=name))
            proof.b.clear(made.statements)
            proof.b.create(made.statements)
            t = proof.made(src)
        except Exception:  # noqa: BLE001 - that cut does not stand
            for side in (proof.a, proof.b):
                side.conn.rollback()
            return None
        if t is None:
            return None
        mode = harness.clock_mode(proof.a, proof.b, cut, t.body or "")
        x, y, clock = proof._both(
            proof.a, proof.b,
            lambda: proof._render(src, proof.a.call(src, list(args),
                                                    tables)),
            lambda: proof._render(src, proof.b.call(t, list(args),
                                                    list(proof.b.watched))),
            mode)
        return differences(x, y, clock) or None

    whole = set(range(len(cuts)))
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
    lines = [body[cuts[k][0]:cuts[k][1]].strip() for k in keep]
    return lines, found


def _made_by(src, engine):
    """The statements making a cut copy in the source's sandbox."""
    if engine == "postgres":
        from .prove import _pg_statements
        return _pg_statements(src.definition)
    return [src.definition]


def _drop(proof, routine, name):
    for side in (proof.a, proof.b):
        try:
            side.drop(Routine(kind=routine.kind, name=name))
        except Exception:  # noqa: BLE001 - nothing to drop
            side.conn.rollback()

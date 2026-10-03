"""A function of one expression held to its target's version for every
input the solver can reach, not only for the inputs tried (leapfrog
research P20, backlog CV.P20).

Both versions' bodies - the source's one expression and the target's
(`select <expression>` or `begin return <expression>; end`) - are read
into one SMT problem over the function's parameters, with the same
encoding the constraints' proof uses (`constraints._Encoder`: a value and
a NULL flag each, SQL's three values, CASE, COALESCE, IF, `+ - *`,
comparisons, text equality and LIKE, the declared ranges of numbers and
lengths of text), and the solver is asked for an input on which the two
answer apart - a different value, or NULL on one side only. Every input
it answers with is run on both sides like any other input, and only the
run decides the verdict (`prove`). Where the solver answers that no such
input exists, that is said beside the calls' evidence, with its bound:
within the parameters' declared ranges, and with no intermediate value
past an engine's integer range - an overflow one engine raises and
another does not is not in the model. A body using anything the model
does not hold (a division, a function it does not know, text ordered by
a collation, a conversion of type) is not asked at all.

Where this stops (as far as the research reaches): bodies of statements
- loops, assignments, queries - are not modelled; their bounded model
checking needs a symbolic executor per procedural language, and the
proof's own search, solver for untaken branches and coverage stand in
for it there.
"""
import re

from . import constraints, inputs
from .translate import Refused, parse

#: inputs asked of the solver per function, at most
MOST = 3
#: what an answer of "none" is bounded by, said with it
BOUND = ("the solver finds no input they answer apart on, within the"
         " parameters' declared ranges (an intermediate value past an"
         " engine's integer range is not in its model)")


def expression_of(routine):
    """The one expression a function's body is, or None."""
    one = routine.settings.get("expression")
    if one:
        return one
    body = (routine.body or "").strip().rstrip(";").strip()
    if not body:
        return None
    m = re.fullmatch(r"(?is)begin\s+return\s+(.*?)\s*;\s*end", body)
    if m and ";" not in m.group(1):
        return m.group(1)
    m = re.fullmatch(r"(?is)return\s+(.*)", body)
    if m and ";" not in m.group(1):
        return m.group(1)
    if (routine.language or "").lower() == "sql":
        m = re.fullmatch(r"(?is)select\s+(.*)", body)
        if m and not re.search(r"(?i)\b(from|where|group|order|limit|union)"
                               r"\b", m.group(1)) and ";" not in m.group(1):
            return m.group(1)
    return None


def _tree(text, dialect):
    tree = parse(f"SELECT {text}", dialect)
    if not tree.expressions or len(tree.expressions) != 1 or \
            tree.args.get("from_") or tree.args.get("from"):
        raise Refused("it is not one expression")
    got = tree.expressions[0]
    return got.this if got.key == "alias" else got


def question(pair, r, t):
    """(the solver's space, the condition that the two answer apart, the
    parameters it reads), or Outside/Refused where the bodies are beyond
    the model."""
    src_expr, dst_expr = expression_of(r), expression_of(t)
    if src_expr is None or dst_expr is None:
        raise Refused("a body is not one expression")
    if len(r.params) != len(t.params):
        raise Refused("the two take different arguments")
    from .. import canon
    engine = pair.src_engine.CANON_ENGINE
    cols = {}
    for p in r.params:
        cls = inputs.klass(engine, p.type)
        cols[p.name] = constraints.Column(
            cls or "text", True, canon.capacity(engine, p.type))
    space = constraints._Space(cols)
    z3 = space.z3
    names = {q.name: p.name for q, p in zip(t.params, r.params)}
    a = constraints._Encoder(space, None, pair.src_engine.SQL_DIALECT
                             ).value(_tree(src_expr,
                                           pair.src_engine.SQL_DIALECT))
    enc = constraints._Encoder(space, names, pair.dst_engine.SQL_DIALECT)
    b = enc.value(_tree(dst_expr, pair.dst_engine.SQL_DIALECT))
    read = set(space.vars)
    if a.term is None and b.term is None:
        return space, z3.BoolVal(False), read
    if a.term is None or b.term is None:
        known = b if a.term is None else a
        return space, z3.Not(known.null), read
    ta, tb = enc._alike([a.term, b.term])
    differ = z3.Or(a.null != b.null,
                   z3.And(z3.Not(a.null), z3.Not(b.null), ta != tb))
    return space, differ, read


def counterexamples(proof, r, t):
    """[argument tuple] the solver finds the two versions answering apart
    on; where it finds that there is none, `proof.bounded` holds what is
    said of it for the routine."""
    if r.kind != "function" or t is None:
        return []
    try:
        space, differ, read = question(proof.pair, r, t)
    except (constraints.Outside, Refused):
        return []
    got = space.solve([differ], read, most=MOST)
    if got is None:
        return []
    if not got:
        if not hasattr(proof, "bounded"):
            proof.bounded = {}
        proof.bounded[r.name] = BOUND
        return []
    out = []
    for row in got:
        by = {str(k).lower(): v for k, v in row.items()}
        out.append(tuple(by.get(p.name.lower()) if p.mode != "out" else None
                         for p in r.params))
    return out

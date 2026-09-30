"""Inputs for a branch the search never took, solved for (leapfrog
research P7).

A branch whose condition is a comparison of the parameters with each
other or with constants is a small equation: `IF p > 100 AND q = 'X'`
wants a `p` above 100 and a `q` of 'X'. The condition is read from the
body (the query translator's tree of it) and handed to an SMT solver for
parameter values that make it true - and, for a branch reached only when
every condition above it is false, values that make them all false.
Conditions that read a table, a variable the body assigns or a function
are not solved: the search and the fixture's values are what reach those.
"""
from sqlglot import exp

from .translate import Refused, parse


class Unsolvable(Exception):
    pass


def _encode(node, names, z3, vars_, nulls):
    """(z3 term, the parameters it reads) - or Unsolvable."""
    if isinstance(node, exp.Paren):
        return _encode(node.this, names, z3, vars_, nulls)
    if isinstance(node, exp.Column) and not node.table and \
            node.name.lower() in names:
        n = node.name.lower()
        return vars_[n], {n}
    if isinstance(node, exp.Literal):
        if node.is_string:
            return z3.StringVal(node.this), set()
        text = node.this
        return (z3.IntVal(int(text)) if text.lstrip("-").isdigit()
                else z3.RealVal(text)), set()
    if isinstance(node, exp.Boolean):
        return z3.BoolVal(bool(node.this)), set()
    if isinstance(node, exp.Neg):
        term, used = _encode(node.this, names, z3, vars_, nulls)
        return -term, used
    if isinstance(node, exp.Is) and isinstance(node.expression, exp.Null):
        if isinstance(node.this, exp.Column) and \
                node.this.name.lower() in names:
            return nulls[node.this.name.lower()], set()
        raise Unsolvable
    if isinstance(node, exp.Not):
        term, used = _encode(node.this, names, z3, vars_, nulls)
        return z3.Not(term), used
    binary = {exp.EQ: lambda a, b: a == b, exp.NEQ: lambda a, b: a != b,
              exp.GT: lambda a, b: a > b, exp.GTE: lambda a, b: a >= b,
              exp.LT: lambda a, b: a < b, exp.LTE: lambda a, b: a <= b,
              exp.Add: lambda a, b: a + b, exp.Sub: lambda a, b: a - b,
              exp.Mul: lambda a, b: a * b,
              exp.And: lambda a, b: z3.And(a, b),
              exp.Or: lambda a, b: z3.Or(a, b)}
    for kind, make in binary.items():
        if type(node) is kind:
            a, ua = _encode(node.this, names, z3, vars_, nulls)
            b, ub = _encode(node.expression, names, z3, vars_, nulls)
            try:
                return make(a, b), ua | ub
            except z3.Z3Exception:
                raise Unsolvable
    if isinstance(node, exp.Between):
        a, ua = _encode(node.this, names, z3, vars_, nulls)
        lo, ul = _encode(node.args["low"], names, z3, vars_, nulls)
        hi, uh = _encode(node.args["high"], names, z3, vars_, nulls)
        return z3.And(a >= lo, a <= hi), ua | ul | uh
    if isinstance(node, exp.In) and node.expressions:
        a, ua = _encode(node.this, names, z3, vars_, nulls)
        options = [_encode(e, names, z3, vars_, nulls)[0]
                   for e in node.expressions]
        return z3.Or(*[a == o for o in options]), ua
    raise Unsolvable


def solve(conditions, params, dialect, want_true=True):
    """Parameter values - {name: value} - making every one of
    `conditions` true (or, with `want_true` False, every one false), or
    None where the solver cannot say (or there is no such input)."""
    import z3
    from decimal import Decimal
    names = {p.name.lower(): p for p in params}
    vars_, nulls = {}, {}
    for n, p in names.items():
        cls = p.klass
        vars_[n] = (z3.Int(n) if cls == "integer" else
                    z3.Real(n) if cls in ("decimal", "float") else
                    z3.String(n) if cls == "text" else z3.Int(n))
        nulls[n] = z3.Bool(f"{n}__null")
    s = z3.Solver()
    s.set("timeout", 2000)
    for text in conditions:
        try:
            tree = parse(f"SELECT {text}", dialect).expressions[0]
            term, used = _encode(tree, names, z3, vars_, nulls)
        except (Refused, Unsolvable, IndexError, AttributeError):
            return None
        # a comparison with a NULL is not true: every parameter it reads
        # has to hold a value for it to be taken
        known = z3.And(*[z3.Not(nulls[u]) for u in used]) if used else \
            z3.BoolVal(True)
        s.add(z3.And(known, term) if want_true else
              z3.Or(z3.Not(known), z3.Not(term)))
    if s.check() != z3.sat:
        return None
    model = s.model()
    out = {}
    for n, p in names.items():
        if z3.is_true(model.eval(nulls[n], model_completion=True)):
            out[n] = None
            continue
        v = model.eval(vars_[n], model_completion=True)
        if p.klass == "integer":
            out[n] = v.as_long()
        elif p.klass in ("decimal", "float"):
            frac = v.as_fraction()
            got = Decimal(frac.numerator) / Decimal(frac.denominator)
            out[n] = float(got) if p.klass == "float" else got
        elif p.klass == "text":
            out[n] = v.as_string()
        else:
            out[n] = v.as_long()
    return out


def branches(routine, missed):
    """[(conditions that must be false, the condition that must be true or
    None)] for each MySQL branch a probe says was never reached: the THEN
    of an IF by its own condition with the ones before it false, an ELSE
    (or the ELSE an IF does not have) by all of them false."""
    from . import mysql_pg
    try:
        tree = mysql_pg.read(routine.body or "")
    except Refused:
        return []
    out = []
    for node in tree.walk():
        if node.kind != "if":
            continue
        conds = [c for c, _, _, _ in node.branches]
        for k, (cond, _, _, line) in enumerate(node.branches):
            if (line, "then") in missed:
                out.append((conds[:k], cond))
        if (node.line, "else") in missed or \
                (node.line, "else (none)") in missed:
            out.append((conds, None))
    return out


class _Param:
    def __init__(self, name, mode, klass):
        self.name, self.mode, self.klass = name, mode, klass


def inputs(routine, missed, klass_of, dialect="mysql"):
    """Argument tuples that should reach the branches in `missed`
    ({(line, what)} of the source's probes never reached)."""
    params = [_Param(p.name, p.mode, klass_of(p.type))
              for p in routine.params]
    ins = [p for p in params if p.mode != "out"]
    found = []
    for before, cond in branches(routine, set(missed)):
        if cond is None:
            got = solve(before, ins, dialect, want_true=False)
        else:
            got = solve([f"NOT ({c})" for c in before] + [cond], ins,
                        dialect, want_true=True)
        if got is None:
            continue
        args = tuple(got.get(p.name.lower()) if p.mode != "out" else None
                     for p in params)
        if args not in found:
            found.append(args)
    return found

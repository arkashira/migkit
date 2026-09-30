"""The schema's constraints held to the rows they accept and refuse, by
running them (leapfrog research P12, backlog CV.P12).

The schema check compares a translated table by its columns and their
classes (`check_schema`) and by the column rules it keeps
(`_rules_drift`). Neither says whether the target refuses what the source
refuses and takes what the source takes - and that is what an
application meets on its first write after cutover: a CHECK loosened in
translation, a unique key dropped, a foreign key that no longer cascades,
a column a size smaller. Here both are asked. From the source's and the
target's catalogues each table gets probe statements - a plain insert,
every NOT NULL column set to NULL, the values at and beside each CHECK's
constants and NULL (a CHECK passes on NULL), a duplicate of each unique
key and duplicates with NULL in its nullable columns, a child naming no
parent, a delete and a key change of a parent with children, a text at
its declared length and one past, a decimal with one digit of scale too
many and one past its precision, an integer one past its range, a value
no enum holds - each built from a fixture row both sandboxes hold and run
on the two (`bench`) in a transaction rolled back after it. The two
outcomes are compared by what the caller sees: accepted or refused, the
error's class, the rows written.

The static half is three-valued. Each CHECK's predicate is read with the
query translator in its own side's dialect and handed to an SMT solver as
SQL evaluates it - NULL per nullable column as a flag of its own, AND, OR
and NOT in Kleene's three values, a comparison with NULL UNKNOWN - and a
row passes a CHECK that is TRUE or UNKNOWN for it. Whether the source's
CHECKs and the target's pass the same rows is asked over the values the
two declarations both hold; a row they are solved to disagree on becomes
one more probe, and the execution decides the verdict - the solver only
finds rows. A predicate outside what is solved for (a division, a
function the solver does not hold, text ordered by a collation) is left
to the probes and said as such, never taken as equal.

Three things are left to other proofs, on purpose: which spellings of a
text a comparison takes as one (case, accents, trailing spaces) is the
collations proof's, so text constants are tried as written; what a
counter does with a NULL or an explicit value is the identity's, so no
NOT NULL or range probe is made of a column a counter fills; and how far
a sequence moved is not compared at all.

The sandbox is a copy, and a copy can lose what it copies (a foreign key
made again without its ON DELETE cascades nothing). Each side's
constraints are therefore read from its sandbox (what the probes meet)
and from its own server, read-only; a constraint the server has and the
sandbox does not is not probed, and is named - a probe of the copy would
say what the copy does.

The verdict is `diff` naming the first probe the two sides answered
differently, in the order the probes run (tables by name; per table the
plain insert, NOT NULL, CHECK, unique, foreign key, declared limits,
enum); `warn` where something could not be probed - named - or a CHECK
refused none of the probes on a side that has it; `ok` only when every
probe answered alike and nothing was left unprobed.
"""
import datetime as _dt
import itertools
import operator
import re
import weakref
from dataclasses import dataclass, field
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, localcontext
from fractions import Fraction

from sqlglot import exp

from .. import canon
from . import harness
from .translate import Refused, parse

#: milliseconds the solver may take over one question; a question it does
#: not answer in them is said, and left to the probes
SOLVE_MS = 2000
#: rows the two sides' CHECKs are solved to disagree on, at most, per
#: table: each is one more probe, and a few say what one says
DISAGREEMENTS = 3
#: fractional digits the solver gives a decimal whose declaration names
#: no scale: what it hands back is written into a statement as a literal
FREE_SCALE = 6
#: the longest text a declared-length probe writes: a longer declaration
#: is a limit no application meets by accident
LONGEST = 4000
#: the kinds of probe, in the order they run per table and are counted in
KINDS = (("plain", "plain inserts"), ("not null", "NOT NULL"),
         ("check", "CHECK"), ("unique", "unique"),
         ("foreign key", "foreign key"), ("limit", "declared limits"),
         ("enum", "enum"))

class Outside(Exception):
    """What the solver is not given, in the words the verdict says it in."""


class NotProbed(Exception):
    """Why no probe could be made of a table, in the words the caller
    says it in."""


# ---- the static half: CHECK predicates in SQL's three values ---------------


@dataclass
class Column:
    """A column as the solver holds it."""
    klass: str = "text"
    nullable: bool = True
    #: what its declaration lets it hold, as `canon.capacity` says it
    limit: tuple = None


class _V:
    """A value: `term`, meaningless where `null` holds; a NULL written as
    such has no term."""
    __slots__ = ("term", "null")

    def __init__(self, term, null):
        self.term, self.null = term, null


class _T:
    """A truth in SQL's three values: TRUE where `t` holds, FALSE where
    `f` does, UNKNOWN where neither."""
    __slots__ = ("t", "f")

    def __init__(self, t, f):
        self.t, self.f = t, f


_COMPARE = {exp.EQ: operator.eq, exp.NEQ: operator.ne, exp.GT: operator.gt,
            exp.GTE: operator.ge, exp.LT: operator.lt, exp.LTE: operator.le}
_ARITH = {exp.Add: operator.add, exp.Sub: operator.sub,
          exp.Mul: operator.mul}
_REAL_TYPES = {"DECIMAL", "DOUBLE", "FLOAT", "BIGDECIMAL", "UDECIMAL",
               "UDOUBLE"}
_INT_TYPES = {"INT", "BIGINT", "SMALLINT", "TINYINT", "MEDIUMINT", "UINT",
              "UBIGINT", "USMALLINT", "UTINYINT", "UMEDIUMINT"}
_TEXT_TYPES = {"TEXT", "VARCHAR", "CHAR", "NVARCHAR", "NCHAR", "BPCHAR",
               "NAME"}


def _short(text, most=60):
    text = " ".join(str(text).split())
    return text if len(text) <= most else text[:most - 3] + "..."


class _Space:
    """The columns as the solver holds them - a value and a NULL flag
    each - and what their declarations let them hold."""

    def __init__(self, columns):
        import z3
        self.z3 = z3
        self.cols, self.vars, self.nulls, self.domain = {}, {}, {}, []
        sorts = {"integer": z3.IntSort(), "decimal": z3.RealSort(),
                 "float": z3.RealSort(), "text": z3.StringSort(),
                 "boolean": z3.BoolSort()}
        for name, spec in dict(columns).items():
            col = spec if isinstance(spec, Column) else Column(str(spec))
            key = str(name).lower()
            self.cols[key] = (name, col)
            sort = sorts.get(col.klass)
            if sort is None:
                continue
            var = z3.Const(f"v:{key}", sort)
            self.vars[key] = var
            self.nulls[key] = z3.Bool(f"n:{key}") if col.nullable \
                else z3.BoolVal(False)
            self.domain += self._holds(var, col)

    def _holds(self, var, col):
        """What a declaration lets a column hold. Text is held to
        printable ASCII: a value the solver hands back is written into a
        statement, and a control character there proves nothing."""
        z3 = self.z3
        lim = tuple(col.limit or ())
        if col.klass == "integer" and lim[:1] == ("int",):
            return [var >= lim[1], var <= lim[2]]
        if col.klass in ("decimal", "float"):
            if lim[:1] == ("numeric",):
                p, s = lim[1], lim[2]
                top = 10 ** max(p - s, 0)
                return [z3.IsInt(var * 10 ** s), var < top, var > -top]
            return [z3.IsInt(var * 10 ** FREE_SCALE)]
        if col.klass == "text":
            out = [z3.InRe(var, z3.Star(z3.Range(" ", "~")))]
            if lim[:1] in (("chars",), ("bytes",)) and lim[1]:
                out.append(z3.Length(var) <= lim[1])
            return out
        return []

    def truth(self, text, dialect, names=None):
        """(the predicate `text` as a truth, the columns it reads), or
        Outside."""
        try:
            tree = parse(f"SELECT {text}", dialect).expressions[0]
        except (Refused, IndexError, AttributeError):
            raise Outside("it could not be read") from None
        enc = _Encoder(self, names, dialect)
        try:
            got = enc.truth(tree)
        except self.z3.Z3Exception:
            raise Outside("it mixes values of different kinds") from None
        return got, enc.read

    def accepts(self, truths):
        """A row passes every one of `truths` - TRUE or UNKNOWN for it."""
        z3 = self.z3
        wants = [z3.Not(t.f) for t in truths]
        return z3.And(*wants) if wants else z3.BoolVal(True)

    def _lit(self, key, value):
        z3 = self.z3
        var = self.vars[key]
        if isinstance(value, bool):
            value = int(value) if not z3.is_bool(var) else value
        if z3.is_bool(var):
            return z3.BoolVal(bool(value)) if isinstance(
                value, (bool, int)) else None
        if z3.is_string(var):
            return z3.StringVal(value) if isinstance(value, str) else None
        if not isinstance(value, (int, float, Decimal)):
            return None
        d = Decimal(str(value))
        if z3.is_int(var):
            return z3.IntVal(int(d)) if d == d.to_integral_value() else None
        frac = Fraction(d)
        return z3.Q(frac.numerator, frac.denominator)

    def _is(self, key, value):
        z3 = self.z3
        if value is None:
            return self.nulls[key]
        lit = self._lit(key, value)
        if lit is None:
            raise Outside(f"{self.cols[key][0]} holds a value the solver"
                          " does not")
        return z3.And(z3.Not(self.nulls[key]), self.vars[key] == lit)

    def _same(self, model, key):
        z3 = self.z3
        null = self.nulls[key]
        if z3.is_true(model.eval(null, model_completion=True)):
            return null
        return z3.And(z3.Not(null), self.vars[key] == model.eval(
            self.vars[key], model_completion=True))

    def _py(self, value, col):
        z3 = self.z3
        if z3.is_int_value(value):
            return value.as_long()
        if z3.is_rational_value(value):
            frac = value.as_fraction()
            with localcontext() as ctx:
                ctx.prec = 60
                got = Decimal(frac.numerator) / Decimal(frac.denominator)
            lim = tuple(col.limit or ())
            if lim[:1] == ("numeric",):
                got = got.quantize(Decimal(1).scaleb(-lim[2]))
            return float(got) if col.klass == "float" else got
        if z3.is_string_value(value):
            return value.as_string()
        if z3.is_true(value) or z3.is_false(value):
            return z3.is_true(value)
        raise Outside("the solver answered with a value that cannot be"
                      " written")

    def solve(self, wants, read, fixed=None, avoid=None, most=1):
        """[{column: value}] - over the columns `read` - of rows meeting
        every one of `wants`, at most `most`, NULL as None; None where the
        solver gave no answer in time."""
        z3 = self.z3
        s = z3.Solver()
        s.set("timeout", SOLVE_MS)
        s.add(*self.domain)
        s.add(*wants)
        for key, value in (fixed or {}).items():
            s.add(self._is(key, value))
        for key, values in (avoid or {}).items():
            for v in values:
                lit = self._lit(key, v) if v is not None else None
                if lit is not None:
                    s.add(z3.Or(self.nulls[key], self.vars[key] != lit))
        out = []
        while len(out) < most:
            answer = s.check()
            if answer == z3.unknown:
                return out or None
            if answer != z3.sat:
                break
            m = s.model()
            out.append({self.cols[k][0]: None if z3.is_true(m.eval(
                self.nulls[k], model_completion=True)) else self._py(
                    m.eval(self.vars[k], model_completion=True),
                    self.cols[k][1]) for k in sorted(read)})
            if not read:
                break
            s.add(z3.Not(z3.And(*[self._same(m, k) for k in sorted(read)])))
        return out

    def disagreements(self, src, dst, most=DISAGREEMENTS, avoid=None):
        """Rows passed by every CHECK of one side and refused by one of
        the other's: [{column: value}], [] where there is none, None where
        the solver gave no answer in time. `src` and `dst` are [(predicate,
        dialect, {its column: the solver's column})]."""
        accepted, read = [], set()
        for side, preds in (("source", src), ("target", dst)):
            truths = []
            for text, dialect, names in preds:
                try:
                    t, used = self.truth(text, dialect, names)
                except Outside as e:
                    raise Outside(f"the {side}'s CHECK ({_short(text)}): {e}"
                                  ) from None
                truths.append(t)
                read |= used
            accepted.append(self.accepts(truths))
        return self.solve([accepted[0] != accepted[1]], read, avoid=avoid,
                          most=most)


def _bare(node):
    while isinstance(node, exp.Paren):
        node = node.this
    return node


def _cast_kind(node):
    """The type a cast converts to, as the translator names it (DECIMAL,
    INT, TEXT, ...)."""
    to = node.args.get("to")
    got = getattr(to, "this", None)
    return str(getattr(got, "name", got or "")).upper()


def _first_line(e, most=80):
    return (str(e).strip().splitlines() or [""])[0][:most]


class _Encoder:
    """One predicate's tree as the solver's terms, in SQL's three values."""

    def __init__(self, space, names, dialect):
        self.s, self.z3 = space, space.z3
        self.names = {str(k).lower(): str(v).lower()
                      for k, v in (names or {}).items()}
        self.dialect = dialect
        self.read = set()
        self.T, self.F = self.z3.BoolVal(True), self.z3.BoolVal(False)

    def _said(self, node):
        if isinstance(node, exp.Anonymous):
            what = f"it calls {str(node.name).upper()}"
        elif isinstance(node, exp.Func):
            what = f"it calls {node.sql_name()}"
        else:
            what = f"it uses {_short(node.sql(dialect=self.dialect), 40)}"
        return what + ", which the solver does not hold"

    # ---- truths -------------------------------------------------------------

    def truth(self, node):
        z3 = self.z3
        if isinstance(node, exp.Paren):
            return self.truth(node.this)
        if isinstance(node, exp.And):
            a, b = self.truth(node.this), self.truth(node.expression)
            return _T(z3.And(a.t, b.t), z3.Or(a.f, b.f))
        if isinstance(node, exp.Or):
            a, b = self.truth(node.this), self.truth(node.expression)
            return _T(z3.Or(a.t, b.t), z3.And(a.f, b.f))
        if isinstance(node, exp.Not):
            a = self.truth(node.this)
            return _T(a.f, a.t)
        if isinstance(node, exp.Boolean):
            v = bool(node.this)
            return _T(z3.BoolVal(v), z3.BoolVal(not v))
        if isinstance(node, exp.Is) and isinstance(node.expression,
                                                   exp.Null):
            null = self.value(node.this).null
            if node.args.get("negate"):
                null = z3.Not(null)
            return _T(null, z3.Not(null))
        if type(node) in _COMPARE:
            return self.compare(type(node), node.this, node.expression)
        if isinstance(node, exp.In):
            if node.args.get("query") or node.args.get("unnest") or \
                    node.args.get("field"):
                raise Outside(self._said(node))
            return self.within(node.this, node.expressions)
        if isinstance(node, exp.Between):
            return self._and(
                self.compare(exp.GTE, node.this, node.args.get("low")),
                self.compare(exp.LTE, node.this, node.args.get("high")))
        if type(node) is exp.Like:
            return self.like(node)
        if isinstance(node, exp.Column):
            v = self.column(node)
            if not z3.is_bool(v.term):
                raise Outside(f"it takes {node.name} for a truth")
            known = z3.Not(v.null)
            return _T(z3.And(known, v.term), z3.And(known, z3.Not(v.term)))
        raise Outside(self._said(node))

    def _and(self, a, b):
        return _T(self.z3.And(a.t, b.t), self.z3.Or(a.f, b.f))

    def _or(self, a, b):
        return _T(self.z3.Or(a.t, b.t), self.z3.And(a.f, b.f))

    def within(self, this, items):
        if not items:
            raise Outside("it tests membership of an empty list")
        got = None
        for e in items:
            one = self.compare(exp.EQ, this, e)
            got = one if got is None else self._or(got, one)
        return got

    def _listed(self, node):
        """The elements of `ARRAY[...]`, through the casts PostgreSQL
        writes an IN list back with (`= ANY (ARRAY[...]::text[])`)."""
        while isinstance(node, (exp.Paren, exp.Cast)):
            node = node.this
        if isinstance(node, exp.Array):
            return list(node.expressions)
        raise Outside(self._said(node))

    def _kind(self, term):
        z3 = self.z3
        return "text" if z3.is_string(term) else "truth" \
            if z3.is_bool(term) else "number"

    def compare(self, kind, left, right):
        z3 = self.z3
        if kind is exp.EQ and isinstance(right, exp.Any):
            return self.within(left, self._listed(right.this))
        if kind is exp.NEQ and isinstance(right, exp.Anonymous) and \
                str(right.name).upper() == "ALL" and right.expressions:
            got = self.within(left, self._listed(right.expressions[0]))
            return _T(got.f, got.t)
        a, b = self.value(left), self.value(right)
        if a.term is None or b.term is None:
            return _T(self.F, self.F)
        ka, kb = self._kind(a.term), self._kind(b.term)
        if ka != kb:
            raise Outside("it compares values of different kinds, which"
                          " each engine converts its own way")
        if ka == "text" and kind not in (exp.EQ, exp.NEQ):
            raise Outside("it orders text, which each engine orders by its"
                          " own collation")
        cmp = _COMPARE[kind](a.term, b.term)
        known = z3.Not(z3.Or(a.null, b.null))
        return _T(z3.And(known, cmp), z3.And(known, z3.Not(cmp)))

    def like(self, node):
        z3 = self.z3
        v = self.value(node.this)
        pattern = self._constant_text(node.expression)
        if v.term is None:
            return _T(self.F, self.F)
        if not z3.is_string(v.term):
            raise Outside("it matches a pattern against a value that is not"
                          " text")
        if self.dialect == "tsql" and "[" in pattern:
            raise Outside("it matches a pattern with a set of characters,"
                          " which the solver does not hold")
        m = z3.InRe(v.term, self._pattern(pattern))
        known = z3.Not(v.null)
        return _T(z3.And(known, m), z3.And(known, z3.Not(m)))

    def _constant_text(self, node):
        while isinstance(node, (exp.Paren, exp.Cast, exp.Introducer)):
            node = node.expression if isinstance(node, exp.Introducer) \
                else node.this
        if isinstance(node, exp.Literal) and node.is_string:
            return str(node.this)
        raise Outside("it matches a pattern that is not a constant")

    def _pattern(self, text):
        """A LIKE pattern as a regular expression: `%` any run, `_` any
        one character, and - where the engine reads it so without an
        ESCAPE clause - a backslash taking the next character as it is."""
        z3 = self.z3
        re_sort = z3.ReSort(z3.StringSort())
        escapes = self.dialect in ("mysql", "postgres")
        parts, run, chars = [], [], iter(text)
        for ch in chars:
            if escapes and ch == "\\":
                run.append(next(chars, "\\"))
                continue
            if ch in "%_":
                if run:
                    parts.append(z3.Re(z3.StringVal("".join(run))))
                    run = []
                parts.append(z3.Full(re_sort) if ch == "%"
                             else z3.AllChar(re_sort))
                continue
            run.append(ch)
        if run:
            parts.append(z3.Re(z3.StringVal("".join(run))))
        if not parts:
            return z3.Re(z3.StringVal(""))
        return parts[0] if len(parts) == 1 else z3.Concat(*parts)

    # ---- values -------------------------------------------------------------

    def column(self, node):
        name = str(node.name).lower()
        key = self.names.get(name, name)
        if key not in self.s.cols:
            raise Outside(f"it reads {node.name}, which is not a column both"
                          " sides hold")
        if key not in self.s.vars:
            raise Outside(f"it reads {node.name}, a column of a kind the"
                          " solver does not hold")
        self.read.add(key)
        return _V(self.s.vars[key], self.s.nulls[key])

    def _literal(self, node):
        z3 = self.z3
        if node.is_string:
            return z3.StringVal(str(node.this))
        text = str(node.this)
        d = Decimal(text)
        if d == d.to_integral_value() and not re.search(r"[.eE]", text):
            return z3.IntVal(int(d))
        frac = Fraction(d)
        return z3.Q(frac.numerator, frac.denominator)

    def _number(self, v):
        if not self.z3.is_arith(v.term):
            raise Outside("it does arithmetic on a value that is not a"
                          " number")

    def _alike(self, terms):
        """Terms of one kind, whole numbers made real beside real ones."""
        z3 = self.z3
        kinds = {self._kind(t) for t in terms}
        if len(kinds) > 1:
            raise Outside("it mixes values of different kinds")
        if kinds == {"number"} and any(z3.is_real(t) for t in terms):
            return [z3.ToReal(t) if z3.is_int(t) else t for t in terms]
        return list(terms)

    def value(self, node):
        z3 = self.z3
        if node is None:
            raise Outside("it could not be read")
        if isinstance(node, exp.Paren):
            return self.value(node.this)
        if isinstance(node, exp.Column):
            return self.column(node)
        if isinstance(node, exp.Literal):
            return _V(self._literal(node), self.F)
        if isinstance(node, exp.Introducer):
            return self.value(node.expression)
        if isinstance(node, exp.Null):
            return _V(None, self.T)
        if isinstance(node, exp.Boolean):
            return _V(z3.BoolVal(bool(node.this)), self.F)
        if isinstance(node, exp.Neg):
            v = self.value(node.this)
            if v.term is None:
                return v
            self._number(v)
            return _V(-v.term, v.null)
        if type(node) is exp.Cast:
            return self.cast(node)
        if type(node) in _ARITH:
            a, b = self.value(node.this), self.value(node.expression)
            if a.term is None or b.term is None:
                return _V(None, self.T)
            self._number(a)
            self._number(b)
            return _V(_ARITH[type(node)](a.term, b.term),
                      z3.Or(a.null, b.null))
        if isinstance(node, (exp.Div, exp.IntDiv)):
            raise Outside("it divides, and the engines answer a zero divisor"
                          " and a quotient of whole numbers each their own"
                          " way")
        if isinstance(node, exp.Mod):
            raise Outside("it takes a remainder, whose sign and zero divisor"
                          " the engines answer each their own way")
        if isinstance(node, exp.Coalesce):
            return self._first_known([self.value(e) for e in
                                      [node.this] + list(node.expressions)])
        if isinstance(node, exp.Case):
            operand = node.args.get("this")
            branches = []
            for w in node.args.get("ifs") or []:
                cond = self.compare(exp.EQ, operand, w.this) \
                    if operand is not None else self.truth(w.this)
                branches.append((cond.t, self.value(w.args.get("true"))))
            default = node.args.get("default")
            return self._choose(branches, self.value(default)
                                if default is not None else
                                _V(None, self.T))
        if isinstance(node, exp.If):
            other = node.args.get("false")
            return self._choose(
                [(self.truth(node.this).t, self.value(node.args.get("true")))],
                self.value(other) if other is not None else _V(None, self.T))
        if isinstance(node, exp.Abs):
            v = self.value(node.this)
            if v.term is None:
                return v
            self._number(v)
            return _V(z3.If(v.term < 0, -v.term, v.term), v.null)
        if isinstance(node, exp.Length):
            if node.args.get("binary"):
                raise Outside("it counts bytes, which depend on the"
                              " character set")
            if self.dialect == "tsql":
                raise Outside("its length leaves trailing spaces out")
            v = self.value(node.this)
            if v.term is None:
                return v
            if not z3.is_string(v.term):
                raise Outside("it takes the length of a value that is not"
                              " text")
            return _V(z3.Length(v.term), v.null)
        raise Outside(self._said(node))

    def cast(self, node):
        z3 = self.z3
        kind = _cast_kind(node)
        inner = _bare(node.this)
        if isinstance(inner, exp.Literal) and inner.is_string and \
                kind in _REAL_TYPES | _INT_TYPES:
            # PostgreSQL writes a negative constant back as
            # ('-100'::integer): a number, spelt as a text
            try:
                d = Decimal(str(inner.this))
            except ArithmeticError:
                raise Outside(f"it converts the text {inner.this!r} to a"
                              " number") from None
            if kind in _INT_TYPES and d != d.to_integral_value():
                raise Outside(f"it converts the text {inner.this!r} to a"
                              " whole number")
            frac = Fraction(d)
            return _V(z3.IntVal(int(d)) if kind in _INT_TYPES else
                      z3.Q(frac.numerator, frac.denominator), self.F)
        v = self.value(node.this)
        if v.term is None:
            return v
        sized = bool(getattr(node.args.get("to"), "expressions", None))
        literal = isinstance(inner, exp.Literal)
        if kind in _REAL_TYPES and z3.is_arith(v.term) and (not sized
                                                            or literal):
            return _V(z3.ToReal(v.term) if z3.is_int(v.term) else v.term,
                      v.null)
        if kind in _INT_TYPES and z3.is_int(v.term):
            return v
        if kind in _TEXT_TYPES and z3.is_string(v.term) and (not sized
                                                             or literal):
            return v
        raise Outside(f"it converts a value to {kind.lower() or 'a type'},"
                      " which each engine does its own way")

    def _first_known(self, items):
        z3 = self.z3
        known = [v for v in items if v.term is not None]
        if not known:
            return _V(None, self.T)
        terms = self._alike([v.term for v in known])
        term, null = terms[-1], known[-1].null
        for v, t in zip(reversed(known[:-1]), reversed(terms[:-1])):
            term = z3.If(v.null, term, t)
            null = z3.And(v.null, null)
        return _V(term, null)

    def _choose(self, branches, default):
        z3 = self.z3
        vals = [v for _, v in branches] + [default]
        known = [v.term for v in vals if v.term is not None]
        if not known:
            return _V(None, self.T)
        alike = iter(self._alike(known))
        terms = [next(alike) if v.term is not None else None for v in vals]
        sort = next(t for t in terms if t is not None).sort()
        terms = [t if t is not None else z3.FreshConst(sort) for t in terms]
        term, null = terms[-1], vals[-1].null
        for (cond, v), t in zip(reversed(branches), reversed(terms[:-1])):
            term = z3.If(cond, t, term)
            null = z3.If(cond, v.null, null)
        return _V(term, null)


def equivalent(src_checks, dst_checks, columns, src_dialect="mysql",
               dst_dialect="postgres", dst_names=None):
    """Whether a row passes every one of `src_checks` exactly where it
    passes every one of `dst_checks` - each a CHECK's predicate in its
    side's SQL - over `columns` ({name: class} or {name: Column}; the
    target's column names mapped to these through `dst_names`), in SQL's
    three values: a row passes a CHECK that is TRUE or UNKNOWN for it.

    (True, None) where no row tells them apart; (False, {column: value})
    with one that does, NULL as None; (None, why) where a predicate is
    outside what the solver is given, or it gave no answer in time."""
    try:
        space = _Space(columns)
        got = space.disagreements(
            [(t, src_dialect, None) for t in src_checks],
            [(t, dst_dialect, dst_names) for t in dst_checks], most=1)
    except Outside as e:
        return None, str(e)
    if got is None:
        return None, (f"the solver gave no answer within"
                      f" {SOLVE_MS / 1000:g} seconds")
    return (True, None) if not got else (False, got[0])


# ---- what each side's catalogue says ----------------------------------------


@dataclass
class Check:
    name: str
    text: str                 #: the predicate, in its engine's SQL
    enforced: bool = True


@dataclass
class Unique:
    name: str
    columns: list
    primary: bool = False
    deferred: bool = False


@dataclass
class Foreign:
    name: str
    columns: list
    parent: str
    parent_columns: list
    on_delete: str = "no action"
    on_update: str = "no action"
    enforced: bool = True
    deferred: bool = False


@dataclass
class Rules:
    """One table's constraints as one server keeps them, in its own
    column names."""
    declared: dict = field(default_factory=dict)   #: {column: type}
    nullable: dict = field(default_factory=dict)   #: {column: takes NULL}
    checks: list = field(default_factory=list)
    uniques: list = field(default_factory=list)
    foreign: list = field(default_factory=list)
    members: dict = field(default_factory=dict)    #: {column: [values]}
    #: why the server keeps no CHECK at all, where it keeps none
    no_checks: str = ""
    #: why a write to the table cannot be taken back, where it cannot
    untaken: str = ""


def _action(text):
    got = " ".join(str(text or "no action").lower().replace("_", " ").split())
    return got if got in ("cascade", "set null", "set default", "restrict",
                          "no action") else "no action"


def _quoted_list(text):
    """['a', 'b'] from `'a','b'`, a doubled quote kept as one."""
    return [m.replace("''", "'") for m in re.findall(r"'((?:[^']|'')*)'",
                                                     str(text))]


def _mysql_drops_checks(version):
    """Whether this MySQL reads a CHECK and keeps nothing of it: every
    MySQL before 8.0.16; MariaDB keeps them from 10.2."""
    if "mariadb" in str(version).lower():
        return False
    nums = [int(x) for x in re.findall(r"\d+", str(version))[:3]]
    return tuple(nums + [0] * (3 - len(nums))) < (8, 0, 16)


def _mysql_rules(eng, side, db, table):
    schema = eng._d(side, db)
    args = (schema, str(table).split(".")[-1])
    out = Rules()
    for name, null, ctype in eng._q(
            side, "select column_name, is_nullable, column_type from"
                  " information_schema.columns where table_schema = %s and"
                  " table_name = %s order by ordinal_position", args):
        name, ctype = str(name), str(ctype)
        out.declared[name] = ctype
        out.nullable[name] = str(null).upper() == "YES"
        m = re.match(r"(?is)^\s*(enum|set)\s*\((.*)\)\s*$", ctype)
        if m:
            out.members[name] = _quoted_list(m.group(2))
    got = eng._q(side, "select engine from information_schema.tables where"
                       " table_schema = %s and table_name = %s", args)
    if got and str(got[0][0] or "").lower() not in ("innodb", "ndbcluster",
                                                    "ndb"):
        out.untaken = (f"its engine ({got[0][0]}) keeps no transaction, so a"
                       " probe written there could not be taken back")
    version = str(eng._q(side, "select version()")[0][0])
    try:
        rows = eng._q(side, "select tc.constraint_name, cc.check_clause,"
                            " tc.enforced from"
                            " information_schema.table_constraints tc join"
                            " information_schema.check_constraints cc on"
                            " cc.constraint_schema = tc.constraint_schema and"
                            " cc.constraint_name = tc.constraint_name where"
                            " tc.table_schema = %s and tc.table_name = %s and"
                            " tc.constraint_type = 'CHECK' order by"
                            " tc.constraint_name", args)
    except Exception:  # noqa: BLE001 - MariaDB's catalogue, or none
        try:
            rows = eng._q(side, "select constraint_name, check_clause, 'YES'"
                                " from information_schema.check_constraints"
                                " where constraint_schema = %s and table_name"
                                " = %s order by constraint_name", args)
        except Exception:  # noqa: BLE001 - a server that keeps none
            rows = ()
    if _mysql_drops_checks(version):
        out.no_checks = (f"MySQL {version.split('-')[0]} reads a CHECK and"
                         " keeps nothing of it")
    out.checks = [Check(str(n), str(c), str(e).upper() != "NO")
                  for n, c, e in rows]
    try:
        rows = eng._q(side, "select index_name, column_name from"
                            " information_schema.statistics where"
                            " table_schema = %s and table_name = %s and"
                            " non_unique = 0 order by index_name <> 'PRIMARY',"
                            " index_name, seq_in_index", args)
    except Exception:  # noqa: BLE001 - read again without the order
        rows = ()
    keys = {}
    for index, col in rows:
        u = keys.get(str(index))
        if u is None:
            u = keys[str(index)] = Unique(str(index), [],
                                          str(index) == "PRIMARY")
        if col is not None:
            u.columns.append(str(col))
    out.uniques = [u for u in keys.values() if u.columns]
    fks = {}
    for name, col, parent, pcol, on_delete, on_update in eng._q(
            side, "select k.constraint_name, k.column_name,"
                  " k.referenced_table_name, k.referenced_column_name,"
                  " r.delete_rule, r.update_rule from"
                  " information_schema.key_column_usage k join"
                  " information_schema.referential_constraints r on"
                  " r.constraint_schema = k.constraint_schema and"
                  " r.constraint_name = k.constraint_name and r.table_name ="
                  " k.table_name where k.table_schema = %s and k.table_name ="
                  " %s and k.referenced_table_name is not null order by"
                  " k.constraint_name, k.ordinal_position", args):
        f = fks.get(str(name))
        if f is None:
            f = fks[str(name)] = Foreign(str(name), [], str(parent), [],
                                         _action(on_delete),
                                         _action(on_update))
        f.columns.append(str(col))
        f.parent_columns.append(str(pcol))
    out.foreign = list(fks.values())
    return out


_PG_ACTIONS = {"a": "no action", "r": "restrict", "c": "cascade",
               "n": "set null", "d": "set default"}


def _pg_check(text):
    """The predicate of `pg_get_constraintdef`'s `CHECK (...)`."""
    text = str(text).strip()
    for _ in range(2):
        text = re.sub(r"(?i)\s+(no inherit|not valid)\s*$", "", text)
    m = re.match(r"(?is)^check\s*\((.*)\)$", text)
    return m.group(1) if m else text


def _pg_rules(eng, side, db, table):
    sch, tbl = eng._split(table)
    lit = eng._lit
    oid = (f"(select c.oid from pg_class c join pg_namespace n on n.oid ="
           f" c.relnamespace where n.nspname = {lit(sch)} and c.relname ="
           f" {lit(tbl)})")
    out = Rules()
    for name, declared, nullable, labels in eng._catalog(side, db, f"""
            select a.attname, format_type(a.atttypid, a.atttypmod),
                   not a.attnotnull,
                   (select array_agg(e.enumlabel::text
                                     order by e.enumsortorder)
                      from pg_enum e where e.enumtypid = a.atttypid)
              from pg_attribute a
             where a.attrelid = {oid} and a.attnum > 0
               and not a.attisdropped
             order by a.attnum"""):
        out.declared[str(name)] = str(declared)
        out.nullable[str(name)] = bool(nullable)
        if labels:
            out.members[str(name)] = [str(x) for x in labels]
    out.checks = [Check(str(n), _pg_check(d)) for n, d in eng._catalog(
        side, db, f"""
            select o.conname, pg_get_constraintdef(o.oid)
              from pg_constraint o
             where o.conrelid = {oid} and o.contype = 'c'
             order by o.conname""")]
    for name, primary, cols, deferred in eng._catalog(side, db, f"""
            select coalesce(o.conname, i.relname), ix.indisprimary,
                   array(select a.attname::text
                           from unnest(ix.indkey) with ordinality k(n, p)
                           join pg_attribute a on a.attrelid = ix.indrelid
                                              and a.attnum = k.n
                          where k.p <= ix.indnkeyatts
                          order by k.p),
                   coalesce(o.condeferred, false)
              from pg_index ix
              join pg_class i on i.oid = ix.indexrelid
              left join pg_constraint o on o.conindid = ix.indexrelid
                                       and o.conrelid = ix.indrelid
                                       and o.contype in ('p', 'u')
             where ix.indrelid = {oid} and ix.indisunique
             order by not ix.indisprimary, 1"""):
        if cols:
            out.uniques.append(Unique(str(name), [str(c) for c in cols],
                                      bool(primary), bool(deferred)))
    for name, cols, parent, pcols, on_delete, on_update, deferred in \
            eng._catalog(side, db, f"""
            select o.conname,
                   array(select a.attname::text
                           from unnest(o.conkey) with ordinality k(n, p)
                           join pg_attribute a on a.attrelid = o.conrelid
                                              and a.attnum = k.n
                          order by k.p),
                   pn.nspname || '.' || p.relname,
                   array(select a.attname::text
                           from unnest(o.confkey) with ordinality k(n, p)
                           join pg_attribute a on a.attrelid = o.confrelid
                                              and a.attnum = k.n
                          order by k.p),
                   o.confdeltype, o.confupdtype, o.condeferred
              from pg_constraint o
              join pg_class p on p.oid = o.confrelid
              join pg_namespace pn on pn.oid = p.relnamespace
             where o.conrelid = {oid} and o.contype = 'f'
             order by o.conname"""):
        out.foreign.append(Foreign(
            str(name), [str(c) for c in cols], str(parent),
            [str(c) for c in pcols], _PG_ACTIONS.get(str(on_delete),
                                                     "no action"),
            _PG_ACTIONS.get(str(on_update), "no action"),
            deferred=bool(deferred)))
    return out


def _mssql_rules(eng, side, db, table):
    from ..engines.dbapi import MARK
    quoted = (eng._qualified(side, db, table),)
    out = Rules()
    for name, nullable, declared in eng._rows(
            side, db, f"select c.name, c.is_nullable,"
                      f" {eng.DECLARED.format(c='c')} from sys.columns c"
                      f" where c.object_id = object_id({MARK}) order by"
                      " c.column_id", quoted):
        out.declared[str(name)] = str(declared)
        out.nullable[str(name)] = bool(nullable)
    out.checks = [Check(str(n), str(d), not bool(off)) for n, d, off in
                  eng._rows(side, db, "select name, definition, is_disabled"
                                      " from sys.check_constraints where"
                                      " parent_object_id ="
                                      f" object_id({MARK}) order by name",
                            quoted)]
    keys = {}
    for name, primary, col in eng._rows(
            side, db, "select i.name, i.is_primary_key, c.name from"
                      " sys.indexes i join sys.index_columns ic on"
                      " ic.object_id = i.object_id and ic.index_id ="
                      " i.index_id join sys.columns c on c.object_id ="
                      " ic.object_id and c.column_id = ic.column_id where"
                      f" i.object_id = object_id({MARK}) and i.is_unique = 1"
                      " and ic.is_included_column = 0 order by"
                      " i.is_primary_key desc, i.name, ic.key_ordinal",
            quoted):
        keys.setdefault(str(name), Unique(str(name), [], bool(primary))
                        ).columns.append(str(col))
    out.uniques = list(keys.values())
    fks = {}
    for name, col, parent, pcol, on_delete, on_update, off in eng._rows(
            side, db, "select f.name, cp.name, rs.name + '.' + ro.name,"
                      " cr.name, f.delete_referential_action_desc,"
                      " f.update_referential_action_desc, f.is_disabled"
                      " from sys.foreign_keys f join"
                      " sys.foreign_key_columns k on k.constraint_object_id"
                      " = f.object_id join sys.columns cp on cp.object_id ="
                      " k.parent_object_id and cp.column_id ="
                      " k.parent_column_id join sys.columns cr on"
                      " cr.object_id = k.referenced_object_id and"
                      " cr.column_id = k.referenced_column_id join"
                      " sys.objects ro on ro.object_id ="
                      " f.referenced_object_id join sys.schemas rs on"
                      " rs.schema_id = ro.schema_id where f.parent_object_id"
                      f" = object_id({MARK}) order by f.name,"
                      " k.constraint_column_id", quoted):
        f = fks.get(str(name))
        if f is None:
            f = fks[str(name)] = Foreign(str(name), [], str(parent), [],
                                         _action(on_delete),
                                         _action(on_update), not bool(off))
        f.columns.append(str(col))
        f.parent_columns.append(str(pcol))
    out.foreign = list(fks.values())
    return out


def _oracle_rules(eng, side, db, table):
    from ..engines.dbapi import MARK
    from ..engines.oracle import DECLARED
    owner = eng._owner(side, db)
    args = (owner, eng._q(str(table).split(".")[-1])[1:-1])
    said = eng._said
    out = Rules()
    for name, nullable, declared in eng._rows(
            side, db, f"select column_name, nullable, {DECLARED} from"
                      f" all_tab_columns where owner = {MARK} and table_name"
                      f" = {MARK} order by column_id", args):
        out.declared[said(name)] = str(declared)
        out.nullable[said(name)] = str(nullable).upper() == "Y"
    try:
        rows = eng._rows(side, db, "select constraint_name,"
                                   " search_condition_vc, status, generated"
                                   " from all_constraints where owner ="
                                   f" {MARK} and table_name = {MARK} and"
                                   " constraint_type = 'C' order by"
                                   " constraint_name", args)
    except Exception:  # noqa: BLE001 - a server before 12.2
        rows = eng._rows(side, db, "select constraint_name,"
                                   " search_condition, status, generated"
                                   " from all_constraints where owner ="
                                   f" {MARK} and table_name = {MARK} and"
                                   " constraint_type = 'C' order by"
                                   " constraint_name", args)
    for name, text, status, generated in rows:
        # the server's own CHECK for a NOT NULL column is the column's
        if str(generated).upper().startswith("GENERATED") and re.fullmatch(
                r'(?is)\s*"?[\w$#]+"?\s+is\s+not\s+null\s*', str(text)):
            continue
        out.checks.append(Check(str(name), str(text),
                                str(status).upper() == "ENABLED"))
    keys = {}
    for name, kind, col, deferred in eng._rows(
            side, db, "select c.constraint_name, c.constraint_type,"
                      " cc.column_name, c.deferred from all_constraints c"
                      " join all_cons_columns cc on cc.owner = c.owner and"
                      " cc.constraint_name = c.constraint_name where c.owner"
                      f" = {MARK} and c.table_name = {MARK} and"
                      " c.constraint_type in ('P', 'U') order by"
                      " c.constraint_type, c.constraint_name, cc.position",
            args):
        keys.setdefault(str(name), Unique(
            str(name), [], str(kind) == "P",
            str(deferred).upper() == "DEFERRED")).columns.append(said(col))
    for name, col in eng._rows(
            side, db, "select i.index_name, ic.column_name from all_indexes"
                      " i join all_ind_columns ic on ic.index_owner ="
                      " i.owner and ic.index_name = i.index_name where"
                      f" i.table_owner = {MARK} and i.table_name = {MARK}"
                      " and i.uniqueness = 'UNIQUE' and not exists (select 1"
                      " from all_constraints c where c.owner = i.table_owner"
                      " and c.table_name = i.table_name and c.index_name ="
                      " i.index_name) order by i.index_name,"
                      " ic.column_position", args):
        keys.setdefault(str(name), Unique(str(name), [])).columns.append(
            said(col))
    out.uniques = list(keys.values())
    fks = {}
    for name, col, parent, pcol, on_delete, status, deferred in eng._rows(
            side, db, "select c.constraint_name, cc.column_name,"
                      " p.table_name, pc.column_name, c.delete_rule,"
                      " c.status, c.deferred from all_constraints c join"
                      " all_cons_columns cc on cc.owner = c.owner and"
                      " cc.constraint_name = c.constraint_name join"
                      " all_constraints p on p.owner = c.r_owner and"
                      " p.constraint_name = c.r_constraint_name join"
                      " all_cons_columns pc on pc.owner = p.owner and"
                      " pc.constraint_name = p.constraint_name and"
                      " pc.position = cc.position where c.owner ="
                      f" {MARK} and c.table_name = {MARK} and"
                      " c.constraint_type = 'R' order by c.constraint_name,"
                      " cc.position", args):
        f = fks.get(str(name))
        if f is None:
            f = fks[str(name)] = Foreign(
                str(name), [], said(parent), [], _action(on_delete),
                "no action", str(status).upper() == "ENABLED",
                str(deferred).upper() == "DEFERRED")
        f.columns.append(said(col))
        f.parent_columns.append(said(pcol))
    out.foreign = list(fks.values())
    return out


_READERS = {"mysql": _mysql_rules, "postgres": _pg_rules,
            "mssql": _mssql_rules, "oracle": _oracle_rules}


def read_rules(eng, side, db, table):
    """One table's constraints as the server on `side` of `eng` keeps
    them - a catalogue read, nothing written."""
    reader = _READERS.get(eng.CANON_ENGINE)
    if reader is None:
        raise NotProbed(f"migkit cannot read {eng.CANON_ENGINE}'s"
                        " constraints yet")
    return reader(eng, side, db, table)


def _capacity(engine, declared):
    """What a declared type holds, as `canon.capacity` says it - and, for
    SQL Server's and Oracle's lengths and precisions it does not map, as
    the declaration's own numbers say."""
    if not declared:
        return None
    got = canon.capacity(engine, declared)
    if got is not None:
        return got
    m = re.match(r"(?i)^\s*([a-z][a-z0-9 ]*?)\s*(\(|$)", str(declared))
    name = m.group(1).lower() if m else ""
    nums = canon.params(declared)
    if name in ("varchar", "nvarchar", "char", "nchar", "varchar2",
                "nvarchar2", "character varying", "character") and nums:
        return ("chars", nums[0])
    if name in ("decimal", "numeric", "number") and nums:
        return ("numeric", nums[0], nums[1] if len(nums) > 1 else 0)
    return None


def _meet(x, y):
    """What both of two capacities hold."""
    if x is None or y is None:
        return x if y is None else y
    if x[0] != y[0]:
        return x
    if x[0] in ("chars", "bytes"):
        known = [v for v in (x[1], y[1]) if v is not None]
        return (x[0], min(known) if known else None)
    if x[0] == "int":
        return ("int", max(x[1], y[1]), min(x[2], y[2]))
    digits = min(x[1] - x[2], y[1] - y[2])
    scale = min(x[2], y[2])
    return ("numeric", digits + scale, scale)


# ---- one table, as both sides hold it ---------------------------------------


#: "the fixture row's own value, as each side holds it"
_OWN = object()


def _key(value):
    """A value as a unique key compares it: text without case and trailing
    spaces, which some collations drop."""
    if isinstance(value, str):
        return value.lower().rstrip()
    return value


def _word(k):
    """'a', 'b', ... 'z', 'aa', ...: the k-th short word."""
    out = ""
    k += 1
    while k:
        k, r = divmod(k - 1, 26)
        out = chr(ord("a") + r) + out
    return out


def _as(cls, value):
    """A value made here, in the form one side's column class takes."""
    if value is None or value is _OWN:
        return value
    if cls == "boolean" and not isinstance(value, bool) and isinstance(
            value, (int, Decimal)) and value in (0, 1):
        return bool(value)
    if cls == "integer" and isinstance(value, bool):
        return int(value)
    return value


def _said(value):
    """A value made here, as a probe's words show it."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, str):
        if len(value) > 24:
            return f"a text {len(value)} characters long"
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, _dt.datetime):
        return f"'{value.isoformat(sep=' ')}'"
    if isinstance(value, _dt.date):
        return f"'{value.isoformat()}'"
    return str(value)


def _unlike(pattern, fill):
    """A text a LIKE pattern takes: each `%` as `fill`, each `_` as x."""
    out, chars = [], iter(pattern)
    for ch in chars:
        if ch == "\\":
            out.append(next(chars, ""))
        elif ch == "%":
            out.append(fill)
        elif ch == "_":
            out.append("x")
        else:
            out.append(ch)
    return "".join(out)


def _near(cls, nums, strs, likes, scale):
    """The values at and beside a predicate's constants, in a column's
    class: a boundary is where two translations of one CHECK part."""
    out = []
    if cls == "integer":
        for n in nums:
            if n == n.to_integral_value():
                k = int(n)
                out += [k - 1, k, k + 1]
            else:
                out += [int(n.to_integral_value(ROUND_FLOOR)),
                        int(n.to_integral_value(ROUND_CEILING))]
    elif cls in ("decimal", "float"):
        step = Decimal(1).scaleb(-scale) if scale is not None \
            else Decimal("0.01")
        for n in nums:
            out += [n - step, n, n + step]
        if cls == "float":
            out = [float(v) for v in out]
    elif cls == "text":
        for s in strs:
            out += [s, s + "x"]
        for p in likes:
            out += [_unlike(p, ""), _unlike(p, "zz")]
    elif cls == "boolean":
        out += [True, False]
    elif cls in ("date", "timestamp"):
        for s in strs:
            try:
                d = _dt.datetime.fromisoformat(s)
            except ValueError:
                continue
            if cls == "date":
                d = d.date()
            out += [d - _dt.timedelta(days=1), d, d + _dt.timedelta(days=1)]
    seen, kept = set(), []
    for v in out:
        if repr(v) not in seen:
            seen.add(repr(v))
            kept.append(v)
    return kept


def _constants(tree):
    """(numbers, texts, LIKE patterns) a predicate's tree holds."""
    nums, strs, likes = [], [], []
    for lit in tree.find_all(exp.Literal):
        numeric = isinstance(lit.parent, exp.Cast) and \
            _cast_kind(lit.parent) in _REAL_TYPES | _INT_TYPES
        if lit.is_string and not numeric:
            if str(lit.this) not in strs:
                strs.append(str(lit.this))
            continue
        try:
            d = Decimal(str(lit.this))
        except ArithmeticError:
            continue
        if isinstance(lit.parent, exp.Neg):
            d = -d
        if d not in nums:
            nums.append(d)
    for node in tree.find_all(exp.Like):
        node = node.expression
        while isinstance(node, (exp.Paren, exp.Cast, exp.Introducer)):
            node = node.expression if isinstance(node, exp.Introducer) \
                else node.this
        if isinstance(node, exp.Literal) and node.is_string:
            likes.append(str(node.this))
    return nums, strs, likes


@dataclass
class Probe:
    """One statement run on both sides, and what it is in words."""
    kind: str
    what: str
    table: str
    a: tuple = None          #: (statement, params) on the source's sandbox
    b: tuple = None          #: on the target's
    watch: list = field(default_factory=list)
    #: the constraint it probes: probes of one are said once among the
    #: differences, and not run where the sandbox lost it
    about: tuple = ()
    #: a delete or an update: what it did to other rows is said
    writes: bool = False


class _Table:
    """One fixture table as both sides hold it: its columns aligned, each
    side's constraints at the source's column positions, the fixture row
    the probes are made from."""

    def __init__(self, bench, t):
        self.bench = bench
        self.t, self.dt = t, bench.tables[t]
        self.label = str(t).split(".")[-1]
        a, b = bench.a, bench.b
        self.wa, self.wb = a.watched[t], b.watched[t]
        self.names = [c for c, _ in self.wa.columns]
        self.there = [c for c, _ in self.wb.columns]
        self.classes = [c for _, c in self.wa.columns]
        self.dst_classes = [c for _, c in self.wb.columns]
        self.at_src = {n.lower(): i for i, n in enumerate(self.names)}
        self.at_dst = {n.lower(): i for i, n in enumerate(self.there)}
        self.generated = {i for i, n in enumerate(self.names)
                          if n in (self.wa.generated or [])} | \
            {i for i, n in enumerate(self.there)
             if n in (self.wb.generated or [])}
        self.counted = {i for i, n in enumerate(self.names)
                        if n == self.wa.counter} | \
            {i for i, n in enumerate(self.there) if n == self.wb.counter}
        pair, sb = bench.pair, bench.sandbox
        self.src_kind = pair.src_engine.CANON_ENGINE
        self.dst_kind = pair.dst_engine.CANON_ENGINE
        self.src_dialect = pair.src_engine.SQL_DIALECT or self.src_kind
        self.dst_dialect = pair.dst_engine.SQL_DIALECT or self.dst_kind
        self.src = read_rules(sb.src, "dst", bench.db, t)
        self.dst = read_rules(sb.dst, "dst", sb.tdb, self.dt)
        #: what the static half and the probes could not do, said beside
        #: the verdict: `short` keeps it from `ok`, `said` does not
        self.short, self.said = [], []
        #: {(side, the sandbox's name of a CHECK): the server's name}
        self.own_name = {}
        self.never, self.disagree = set(), []
        self._merge()
        rows = self.both_rows()
        self.row = rows[0] if rows else None
        if self.row is not None:
            self.ra = a.raw[t][self.row[0]]
            self.rb = b.raw[t][self.row[1]]
        self._space()

    # ---- the two sides' constraints, at the source's positions --------------

    def _at(self, side, name):
        return (self.at_src if side == "source" else self.at_dst).get(
            str(name).lower())

    def _merge(self):
        n = len(self.names)
        self.refuses_null = {}
        self.caps = [[None, None, None, None] for _ in range(n)]
        self.members = [[None, None] for _ in range(n)]
        self.checks = []
        self.uniques, self.fks = [], []
        by_cols, by_fk = {}, {}
        for k, (side, rules, kind) in enumerate((
                ("source", self.src, self.src_kind),
                ("target", self.dst, self.dst_kind))):
            for col, nullable in rules.nullable.items():
                i = self._at(side, col)
                if i is not None and not nullable:
                    self.refuses_null.setdefault(i, []).append(side)
            for col, declared in rules.declared.items():
                i = self._at(side, col)
                if i is not None:
                    self.caps[i][2 * k] = _capacity(kind, declared)
                    self.caps[i][2 * k + 1] = declared
            for col, values in rules.members.items():
                i = self._at(side, col)
                if i is not None:
                    self.members[i][k] = list(values)
            self.checks += [(side, c) for c in rules.checks]
            for u in rules.uniques:
                cols = [self._at(side, c) for c in u.columns]
                if None in cols:
                    self.short.append(
                        f"the {side}'s unique key {u.name} on {self.label}"
                        f" is over ({', '.join(u.columns)}), not all of"
                        " them columns both sides hold")
                    continue
                key = frozenset(cols)
                if key not in by_cols:
                    by_cols[key] = (cols, {})
                    self.uniques.append(by_cols[key])
                by_cols[key][1][side] = u
            for f in rules.foreign:
                cols = [self._at(side, c) for c in f.columns]
                if None in cols:
                    self.short.append(
                        f"the {side}'s foreign key {f.name} from"
                        f" {self.label} is over ({', '.join(f.columns)}),"
                        " not all of them columns both sides hold")
                    continue
                parent = self._parent(side, f.parent)
                key = (frozenset(cols), parent or f.parent.lower())
                if key not in by_fk:
                    by_fk[key] = (cols, parent, {})
                    self.fks.append(by_fk[key])
                by_fk[key][2][side] = f
        for cols, sides in self.uniques:
            self._deferral("unique key", cols, sides)
        for cols, _, sides in self.fks:
            self._deferral("foreign key", cols, sides)

    def _deferral(self, what, cols, sides):
        deferred = {s for s, c in sides.items() if c.deferred}
        if deferred and len(sides) == 2 and len(deferred) == 1:
            side = deferred.pop()
            self.short.append(
                f"the {what} of {self.label} over"
                f" ({', '.join(self.names[i] for i in cols)}) is checked at"
                f" commit on the {side} and at each statement on the other:"
                " the probes compare what one statement keeps, not a"
                " transaction that breaks it for a while")

    def _parent(self, side, parent):
        """The source's name of a foreign key's parent table, where the
        fixture holds it."""
        leaf = str(parent).split(".")[-1].lower()
        for s, d in self.bench.tables.items():
            name = s if side == "source" else d
            if str(name).split(".")[-1].lower() == leaf:
                return s
        return None

    def defers(self, side):
        rules = self.src if side == "source" else self.dst
        return any(c.deferred for c in rules.uniques + rules.foreign)

    # ---- the fixture's rows -------------------------------------------------

    def both_rows(self):
        """[(row on the source, the same row on the target)] by their
        indices in the fixture - a row both hold, rendered alike - the
        fullest first."""
        a, b = self.bench.a, self.bench.b
        base_a = a.base.get(self.t) or []
        base_b = b.base.get(self.t) or []
        where_b = {}
        for j, r in enumerate(base_b):
            where_b.setdefault(tuple(r), j)
        both = [(i, where_b[tuple(r)]) for i, r in enumerate(base_a)
                if tuple(r) in where_b]
        return sorted(both, key=lambda p: (
            sum(v is None for v in base_a[p[0]]), repr(base_a[p[0]])))

    def values(self, i):
        return [r[i] for r in self.bench.a.raw.get(self.t) or []]

    def cap(self, i):
        return _meet(self.caps[i][0], self.caps[i][2])

    def scale(self, i):
        got = self.cap(i)
        return got[2] if got and got[0] == "numeric" else None

    def fk_columns(self):
        return {i for cols, _, _ in self.fks for i in cols}

    def parent_values(self, i):
        """The values a parent row holds for the column a single-column
        foreign key from column `i` points at, or None where `i` points at
        nothing the fixture holds: a new value of a column that is both a
        unique key and a foreign key has to name a parent row, or every
        insert of it is refused for the parent it lacks."""
        for cols, parent, sides in self.fks:
            if cols != [i] or parent is None:
                continue
            try:
                pctx = _context(self.bench, parent)
            except Exception:  # noqa: BLE001 - made by its class instead
                return None
            for side, f in sides.items():
                k = pctx._at(side, f.parent_columns[0])
                if k is not None:
                    return [v for v in pctx.values(k) if v is not None]
        return None

    def new_value(self, i, n, avoid=(), parents=True):
        """A value for column `i` that no fixture row - nor any of `avoid`
        - holds, and that both sides' declarations take; with `parents`, a
        parent row's where `i` is a foreign key's. None where migkit cannot
        make one of its class."""
        cls = self.classes[i]
        taken = {_key(v) for v in list(self.values(i)) + list(avoid)
                 if v is not None}
        held = self.parent_values(i) if parents else None
        if held is not None:
            free = [v for v in held if _key(v) not in taken]
            return free[n % len(free)] if free else None
        cap = self.cap(i)
        known = [m for m in (self.members[i][0] or self.members[i][1] or [])]
        if known:
            free = [m for m in known if _key(m) not in taken]
            return free[n % len(free)] if free else None
        if cls in ("integer", "decimal", "float"):
            nums = [v for v in list(self.values(i)) + list(avoid)
                    if isinstance(v, (int, float, Decimal))
                    and not isinstance(v, bool)]
            lo = hi = None
            if cap and cap[0] == "int":
                lo, hi = cap[1], cap[2]
            elif cap and cap[0] == "numeric":
                hi = 10 ** (cap[1] - cap[2]) - 1
                lo = -hi
            start = int(max(nums)) + 1 + n if nums else 1 + n
            seq = itertools.chain(
                range(start, start + 64),
                range((hi if hi is not None else 0) - n,
                      (hi if hi is not None else 0) - n - 64, -1),
                range((lo if lo is not None else 0) + n,
                      (lo if lo is not None else 0) + n + 64))
            for v in seq:
                if (lo is None or v >= lo) and (hi is None or v <= hi) and \
                        _key(v) not in taken:
                    return v if cls == "integer" else Decimal(v) \
                        if cls == "decimal" else float(v)
            return None
        if cls == "text":
            most = cap[1] if cap and cap[0] in ("chars", "bytes") else None
            for k in range(4000):
                v = f"mk{_word(k + n)}" if most is None or most >= 6 \
                    else _word(k + n)
                if (most is None or len(v) <= most) and _key(v) not in taken:
                    return v
            return None
        if cls in ("date", "timestamp"):
            days = [v for v in self.values(i) if isinstance(v, _dt.date)]
            if not days:
                return _dt.date(2000, 1, 1) + _dt.timedelta(days=n) \
                    if cls == "date" else _dt.datetime(2000, 1, 1) + \
                    _dt.timedelta(days=n)
            return max(days) + _dt.timedelta(days=1 + n)
        if cls == "boolean":
            free = [v for v in (True, False) if _key(v) not in taken]
            return free[0] if free else None
        return None

    def fresh(self, n, fixed=()):
        """{column: value} giving every unique key of either side - but
        those the columns in `fixed` cover whole - a value no fixture row
        holds in it; a key a counter fills is left to the counter. None
        where one cannot be made."""
        out, fixed = {}, set(fixed)
        fks = self.fk_columns()
        for cols, _ in self.uniques:
            free = [i for i in cols if i not in fixed]
            if not free or any(i in out for i in free) or \
                    any(i in self.counted for i in free):
                continue
            for i in sorted(free, key=lambda i: (i in fks, i)):
                v = self.new_value(i, n)
                if v is not None:
                    out[i] = v
                    break
            else:
                return None
        return out

    # ---- the solver's view of the table -------------------------------------

    def _space(self):
        cols = {}
        for i, name in enumerate(self.names):
            cols[name] = Column(self.classes[i], i not in self.refuses_null,
                                self.cap(i))
        self.space = _Space(cols)
        self.to_src = {self.there[i].lower(): self.names[i]
                       for i in range(len(self.names))}
        #: [(side, check, truth, columns it reads)] of every enforced CHECK
        #: the solver holds; `outside` [(side, check, why)] of the rest
        self.truths, self.outside = [], []
        for side, c in self.checks:
            if not c.enforced:
                continue
            dialect = self.src_dialect if side == "source" \
                else self.dst_dialect
            try:
                t, read = self.space.truth(
                    c.text, dialect, None if side == "source"
                    else self.to_src)
            except Outside as e:
                self.outside.append((side, c, str(e)))
                continue
            self.truths.append((side, c, t, read))

    def avoid(self, read):
        """{column: the values it holds} for each single-column unique key
        among `read`: a row the solver hands back must not be refused as a
        duplicate before it reaches the CHECK it is for."""
        out = {}
        for cols, _ in self.uniques:
            if len(cols) == 1 and self.names[cols[0]].lower() in read:
                out[self.names[cols[0]].lower()] = self.values(cols[0])
        return out

    def fit(self, over, made):
        """`over` with the values migkit made for the columns in `made`
        changed, where an enforced CHECK on either side refuses them, for
        values every one takes - found by the solver; as it was where no
        change is needed or none is found."""
        if not self.truths or not made or self.row is None:
            return over
        read = set().union(*[r for _, _, _, r in self.truths])
        mine = {self.names[i].lower() for i in made} & read
        if not mine:
            return over
        fixed = {}
        for key in read:
            i = self.at_src[key]
            v = over[i] if i in over else _OWN
            fixed[key] = self.ra[i] if v is _OWN else v
        wants = [self.space.z3.Not(t.f) for _, _, t, _ in self.truths]
        try:
            if self.space.solve(wants, set(), fixed=fixed):
                return over
            got = self.space.solve(
                wants, mine, fixed={k: v for k, v in fixed.items()
                                    if k not in mine},
                avoid={k: v for k, v in self.avoid(mine).items()})
        except Outside:
            return over
        if not got:
            return over
        out = dict(over)
        for name, v in got[0].items():
            out[self.at_src[name.lower()]] = v
        return out

    # ---- statements ---------------------------------------------------------

    def insert(self, rows):
        """((source statement, params), (target statement, params))
        inserting `rows` - each {column: value} over the fixture row, a
        column not named taking that row's own value on each side, one a
        counter fills left to it."""
        a, b = self.bench.a, self.bench.b
        over = set().union(*[set(r) for r in rows])
        cols = [i for i in range(len(self.names)) if i not in self.generated
                and (i not in self.counted or i in over)]

        def values(raw, classes, r):
            out = []
            for i in cols:
                v = r[i] if i in r else _OWN
                out.append(raw[i] if v is _OWN else _as(classes[i], v))
            return out
        pa = [v for r in rows for v in values(self.ra, self.classes, r)]
        pb = [v for r in rows for v in values(self.rb, self.dst_classes, r)]
        return ((_insert_sql(a, self.wa.name, self.names, cols, len(rows)),
                 pa),
                (_insert_sql(b, self.wb.name, self.there, cols, len(rows)),
                 pb))

    def probe(self, kind, what, rows, about):
        a, b = self.insert(rows)
        return Probe(kind, what, self.t, a, b, [self.t], about)

    def row_said(self, over):
        return ", ".join(f"{name} = {_said(v)}" for name, v in over.items())


def _insert_sql(runner, table, names, cols, count):
    tbl = runner.table(table)
    listed = ", ".join(runner.q(names[i]) for i in cols)
    one = "(" + ", ".join(["%s"] * len(cols)) + ")"
    if count == 1:
        return f"insert into {tbl} ({listed}) values {one}"
    if runner.engine == "oracle":
        return "insert all " + " ".join(
            f"into {tbl} ({listed}) values {one}" for _ in range(count)) + \
            " select 1 from dual"
    return f"insert into {tbl} ({listed}) values " + ", ".join(
        [one] * count)


def _where(runner, names, idx, raw):
    return (" and ".join(f"{runner.q(names[k])} = %s" for k in idx),
            [raw[k] for k in idx])


# ---- the probes of one table ------------------------------------------------


def _probes(ctx, tables, family):
    """[Probe] of one table in the order they run, and [what could not be
    made, in words]."""
    T, out, short = ctx.label, [], []
    seen = set()
    ctx.said, ctx.never, ctx.disagree = [], set(), []

    def add(p, key=None):
        key = key or (p.kind, repr(p.a))
        if key not in seen:
            seen.add(key)
            out.append(p)

    base = ctx.fresh(0)
    if base is None:
        return [], [f"{T}: migkit could not make a value for its unique"
                    " columns that no row holds"]
    base = ctx.fit(base, set(base))
    add(ctx.probe("plain", f"a new row in {T} (an existing row's values,"
                           " its unique columns made new)", [base],
                  ("plain",)))
    for i in sorted(ctx.refuses_null):
        if i in ctx.generated or i in ctx.counted:
            continue
        add(ctx.probe("not null", f"an insert into {T} with {ctx.names[i]}"
                                  " = NULL", [{**base, i: None}],
                      ("not null", i)))
    _check_probes(ctx, base, add, short)
    _unique_probes(ctx, add, short)
    _foreign_probes(ctx, base, tables, family, add, short)
    _limit_probes(ctx, base, add)
    _enum_probes(ctx, base, add)
    return out, short


def _check_probes(ctx, base, add, short):
    T = ctx.label
    for side, c in ctx.checks:
        at = ctx.at_src if side == "source" else ctx.at_dst
        dialect = ctx.src_dialect if side == "source" else ctx.dst_dialect
        try:
            tree = parse(f"SELECT {c.text}", dialect).expressions[0]
        except (Refused, IndexError, AttributeError):
            short.append(f"the {side}'s CHECK {c.name} on {T}"
                         f" ({_short(c.text)}) could not be read, so no"
                         " value was made for it")
            continue
        nums, strs, likes = _constants(tree)
        cols = []
        for node in tree.find_all(exp.Column):
            i = at.get(str(node.name).lower())
            if i is not None and i not in cols:
                cols.append(i)
        for i in cols:
            for v in _near(ctx.classes[i], nums, strs, likes, ctx.scale(i)):
                add(ctx.probe("check", f"an insert into {T} with"
                                       f" {ctx.names[i]} = {_said(v)}",
                              [{**base, i: v}], ("check",)),
                    _over_key({i: v}))
            if i not in ctx.refuses_null:
                add(ctx.probe("check", f"an insert into {T} with"
                                       f" {ctx.names[i]} = NULL",
                              [{**base, i: None}], ("check",)),
                    _over_key({i: None}))
    # a row each CHECK refuses, found by the solver where no constant
    # reached it: a predicate over two columns has none to start from
    for side, c, truth, read in ctx.truths:
        try:
            rows = ctx.space.solve([truth.f], read, avoid=ctx.avoid(read))
        except Outside:
            rows = None
        if rows == []:
            ctx.never.add((side, c.name))
            continue
        if not rows:
            continue
        over = {ctx.at_src[k.lower()]: v for k, v in rows[0].items()}
        name = ctx.own_name.get((side, c.name), c.name)
        add(ctx.probe("check", f"an insert into {T} with"
                               f" {ctx.row_said(rows[0])} (a row the {side}'s"
                               f" CHECK {name} refuses)",
                      [{**base, **over}], ("check",)), _over_key(over))
    if ctx.outside:
        side, c, why = ctx.outside[0]
        ctx.said.append(f"the CHECKs of {T} were compared by their probes"
                        f" alone - the {side}'s"
                        f" {ctx.own_name.get((side, c.name), c.name)}"
                        f" ({_short(c.text)}): {why}")
        return
    if not ctx.truths:
        return
    src = [t for s, _, t, _ in ctx.truths if s == "source"]
    dst = [t for s, _, t, _ in ctx.truths if s == "target"]
    read = set().union(*[r for _, _, _, r in ctx.truths])
    try:
        rows = ctx.space.solve(
            [ctx.space.accepts(src) != ctx.space.accepts(dst)], read,
            avoid=ctx.avoid(read), most=DISAGREEMENTS)
    except Outside as e:
        ctx.said.append(f"the CHECKs of {T} were compared by their probes"
                        f" alone: {e}")
        return
    if rows is None:
        ctx.said.append(f"the CHECKs of {T} were compared by their probes"
                        f" alone: the solver gave no answer within"
                        f" {SOLVE_MS / 1000:g} seconds")
        return
    for row in rows:
        over = {ctx.at_src[k.lower()]: v for k, v in row.items()}
        p = ctx.probe("check", f"an insert into {T} with"
                               f" {ctx.row_said(row)} (a row the two sides'"
                               " CHECKs were solved to disagree on)",
                      [{**base, **over}], ("check",))
        ctx.disagree.append(p)
        add(p, _over_key(over))


def _over_key(over):
    """What a CHECK probe sets, as probes are told apart by: 0.10 and 0.1
    are one value."""
    return ("check",) + tuple(sorted(
        (i, repr(v.normalize() if isinstance(v, Decimal) else v))
        for i, v in over.items()))


def _unique_probes(ctx, add, short):
    T = ctx.label
    for cols, sides in ctx.uniques:
        label = ", ".join(ctx.names[i] for i in cols)
        about = ("unique", frozenset(cols))
        other = ctx.fresh(1, fixed=cols)
        if other is None:
            short.append(f"the unique key of {T} over ({label}) was not"
                         " probed: migkit could not make new values for the"
                         " other unique columns")
            continue
        other = ctx.fit(other, set(other))
        add(ctx.probe("unique", f"an insert into {T} repeating an existing"
                                f" row's {label}",
                      [{**other, **{i: _OWN for i in cols}}], about))
        for c in cols:
            if c in ctx.refuses_null or c in ctx.generated:
                continue
            r1, r2 = ctx.fresh(2, fixed=cols), ctx.fresh(3, fixed=cols)
            if r1 is None or r2 is None:
                continue
            shared = {i: _OWN for i in cols}
            shared[c] = None
            add(ctx.probe("unique", f"two rows inserted into {T} alike in"
                                    f" ({label}) with {ctx.names[c]} NULL",
                          [{**ctx.fit(r1, set(r1)), **shared},
                           {**ctx.fit(r2, set(r2)), **shared}], about))


def _foreign_probes(ctx, base, tables, family, add, short):
    T = ctx.label
    bench = ctx.bench
    for cols, parent, sides in ctx.fks:
        label = ", ".join(ctx.names[i] for i in cols)
        any_side = next(iter(sides.values()))
        P = str(parent or any_side.parent).split(".")[-1]
        pctx = tables.get(parent) if parent is not None else None
        pidx = None
        if pctx is not None:
            for side, f in sides.items():
                got = [pctx._at(side, c) for c in f.parent_columns]
                if None not in got:
                    pidx = got
                    break
        about = ("foreign key", frozenset(cols), parent)
        # a child naming no parent
        taken = pctx.values(pidx[0]) if pctx is not None and pidx else []
        missing = ctx.new_value(cols[0], 5, avoid=taken, parents=False)
        other = ctx.fresh(4, fixed=cols)
        if missing is None or other is None:
            short.append(f"the foreign key of {T} ({label}) to {P} was not"
                         " probed: migkit could not make a value no"
                         f" {P} row holds")
        else:
            over = {**ctx.fit(other, set(other)), cols[0]: missing,
                    **{i: _OWN for i in cols[1:]}}
            add(ctx.probe("foreign key", f"an insert into {T} whose"
                                         f" {ctx.names[cols[0]]} ="
                                         f" {_said(missing)} names no {P}"
                                         " row", [over], about))
        # a delete, and a change of key, of a parent with children
        why = None
        if pctx is None:
            why = f"{P} is not in the fixture"
        elif pidx is None:
            why = f"{P}'s columns it points at are not ones both sides hold"
        elif pctx.row is None:
            why = f"{P} has no row both sides hold"
        pair = None
        if why is None:
            children = {tuple(r[i] for i in cols)
                        for r in bench.a.raw.get(ctx.t) or []}
            children = {k for k in children if None not in k}
            for i, j in pctx.both_rows():
                if tuple(bench.a.raw[pctx.t][i][k] for k in pidx) in \
                        children:
                    pair = (bench.a.raw[pctx.t][i], bench.b.raw[pctx.t][j])
                    break
            if pair is None:
                why = f"no {P} row in the fixture has {T} rows pointing at it"
        if why is not None:
            short.append(f"what a delete of a {P} row does to the {T} rows"
                         f" pointing at it was not probed: {why}")
            continue
        a, b = bench.a, bench.b
        wa, wb = pctx.wa, pctx.wb
        watch = family(pctx.t)
        ca, pa = _where(a, pctx.names, pidx, pair[0])
        cb, pb = _where(b, pctx.there, pidx, pair[1])
        paid = ("parent", frozenset(cols), parent)
        add(Probe("foreign key", f"a delete of a {P} row that {T} rows point"
                                 " at", ctx.t,
                  (f"delete from {a.table(wa.name)} where {ca}", pa),
                  (f"delete from {b.table(wb.name)} where {cb}", pb),
                  watch, paid, True))
        acts = {f.on_update for f in sides.values()}
        if acts <= {"no action", "restrict"} and set(pidx) & pctx.counted:
            # both keep the key, and it is a counter's: what a changed
            # counter value does is the identity's to prove
            continue
        new = pctx.new_value(pidx[0], 6)
        if new is None:
            continue
        k = pidx[0]
        add(Probe("foreign key", f"a change of the {pctx.names[k]} of a {P}"
                                 f" row that {T} rows point at", ctx.t,
                  (f"update {a.table(wa.name)} set {a.q(pctx.names[k])} ="
                   f" %s where {ca}", [_as(pctx.classes[k], new)] + pa),
                  (f"update {b.table(wb.name)} set {b.q(pctx.there[k])} ="
                   f" %s where {cb}", [_as(pctx.dst_classes[k], new)] + pb),
                  watch, paid, True))


def _limit_probes(ctx, base, add):
    T = ctx.label
    for i, name in enumerate(ctx.names):
        if i in ctx.generated or i in ctx.counted or \
                ctx.members[i][0] or ctx.members[i][1]:
            continue
        both = (ctx.classes[i], ctx.dst_classes[i])
        for k, side in ((0, "source"), (1, "target")):
            cap, declared = ctx.caps[i][2 * k], ctx.caps[i][2 * k + 1]
            if cap is None:
                continue
            tries = []
            if cap[0] == "chars" and cap[1] and cap[1] <= LONGEST and \
                    both == ("text", "text"):
                n = cap[1]
                tries = [("m" * n, f"a text {n} characters long, as long as"
                                   f" the {side}'s {declared} holds"),
                         ("m" * (n + 1), f"a text {n + 1} characters long,"
                                         f" one past the {side}'s"
                                         f" {declared}"),
                         ("é" * n, f"a text of {n} two-byte characters, as"
                                   f" many as the {side}'s {declared}"
                                   " holds")]
            elif cap[0] == "int" and both == ("integer", "integer"):
                lo, hi = cap[1], cap[2]
                tries = [(hi, f"{hi}, the largest the {side}'s {declared}"
                              " holds"),
                         (hi + 1, f"{hi + 1}, one past the largest the"
                                  f" {side}'s {declared} holds"),
                         (lo, f"{lo}, the smallest the {side}'s {declared}"
                              " holds"),
                         (lo - 1, f"{lo - 1}, one past the smallest the"
                                  f" {side}'s {declared} holds")]
            elif cap[0] == "numeric" and both == ("decimal", "decimal"):
                p, s = cap[1], cap[2]
                top = Decimal(10) ** (p - s)
                step = Decimal(1).scaleb(-s)
                finer = Decimal(1) + Decimal(5).scaleb(-(s + 1))
                tries = [(finer, f"{finer}, one more digit of scale than the"
                                 f" {side}'s {declared} keeps"),
                         (top - step, f"{top - step}, the largest the"
                                      f" {side}'s {declared} holds"),
                         (top, f"{top}, one past the largest the {side}'s"
                               f" {declared} holds"),
                         (-top, f"{-top}, one past the smallest the"
                                f" {side}'s {declared} holds")]
            for v, how in tries:
                add(ctx.probe("limit", f"an insert into {T} with {name} ="
                                       f" {how}", [{**base, i: v}],
                              ("limit", i)), ("limit", i, repr(v)))


def _enum_probes(ctx, base, add):
    T = ctx.label
    for i, name in enumerate(ctx.names):
        ms, mt = ctx.members[i]
        if ms is None and mt is None:
            continue
        known = set(ms or []) | set(mt or [])
        outside = next(w for w in ("zz", "zzz", "zz-migkit", "zz-migkit-2")
                       if w not in known)
        tries = [(outside, f"{_said(outside)}, none of its values")]
        if mt is not None:
            tries += [(m, f"{_said(m)}, one of the source's values and not"
                          " the target's") for m in ms or [] if m not in mt]
        if ms is not None:
            tries += [(m, f"{_said(m)}, one of the target's values and not"
                          " the source's") for m in mt or [] if m not in ms]
        for v, how in tries:
            add(ctx.probe("enum", f"an insert into {T} with {name} = {how}",
                          [{**base, i: v}], ("enum", i)), ("enum", i, v))


# ---- the sandbox held to the servers it copies ------------------------------


def _squash(text):
    return " ".join(str(text).lower().replace("`", "").replace('"', "")
                    .split())


def _lost(ctx):
    """[(about, words)] of the constraints a side's own server keeps and
    its sandbox's copy of the table does not - their probes would say what
    the copy does - and [what could not be read]."""
    bench, pair = ctx.bench, ctx.bench.pair
    out, unread = [], []
    for side, eng, own, db_side, table, copy in (
            ("source", pair.src_engine, "src", bench.db, ctx.t, ctx.src),
            ("target", pair.dst_engine, "dst", bench.db, ctx.dt, ctx.dst)):
        try:
            server = read_rules(eng, own, db_side, table)
        except Exception as e:  # noqa: BLE001 - said, and kept from `ok`
            unread.append(f"the {side}'s own catalogue could not be read to"
                          f" hold its sandbox's copy of {ctx.label} to it:"
                          f" {_first_line(e)}")
            continue
        T = ctx.label
        kept = {c.lower() for c, n in copy.nullable.items() if not n}
        for col, nullable in server.nullable.items():
            i = ctx._at(side, col)
            if not nullable and col.lower() not in kept and i is not None:
                out.append((("not null", i), f"{T}.{col} is NOT NULL on the"
                                             f" {side} and not in its"
                                             " sandbox"))
        # a sandbox copy's CHECK may be named by its server afresh; it is
        # said by the name the operator gave it
        named = {(_squash(c.text), c.enforced): c.name
                 for c in server.checks}
        for c in copy.checks:
            own = named.get((_squash(c.text), c.enforced))
            if own:
                ctx.own_name[(side, c.name)] = own
        mine = {(_squash(c.text), c.enforced) for c in copy.checks}
        for c in server.checks:
            if (_squash(c.text), c.enforced) not in mine:
                out.append((("check",), f"the {side}'s CHECK {c.name} on {T}"
                                        " is not in its sandbox as the"
                                        " server keeps it"))
        mine = {tuple(x.lower() for x in u.columns) for u in copy.uniques}
        for u in server.uniques:
            if tuple(x.lower() for x in u.columns) not in mine:
                cols = [ctx._at(side, c) for c in u.columns]
                if None not in cols:
                    out.append((("unique", frozenset(cols)),
                                f"the {side}'s unique key {u.name} on {T} is"
                                " not in its sandbox"))
        copies = {(tuple(x.lower() for x in f.columns),
                   str(f.parent).split(".")[-1].lower()): f
                  for f in copy.foreign}
        for f in server.foreign:
            cols = [ctx._at(side, c) for c in f.columns]
            if None in cols:
                continue
            parent = ctx._parent(side, f.parent) or f.parent.lower()
            got = copies.get((tuple(x.lower() for x in f.columns),
                              str(f.parent).split(".")[-1].lower()))
            P = str(f.parent).split(".")[-1]
            if got is None or got.enforced != f.enforced:
                out.append((("foreign key", frozenset(cols), parent),
                            f"the {side}'s foreign key {f.name} from {T} to"
                            f" {P} is not in its sandbox as the server"
                            " keeps it"))
                continue
            gone = [f"ON {verb} {kept_.upper()}" for verb, kept_, copied in (
                ("DELETE", f.on_delete, got.on_delete),
                ("UPDATE", f.on_update, got.on_update)) if kept_ != copied]
            if gone:
                out.append((("parent", frozenset(cols), parent),
                            f"the {side}'s foreign key {f.name} from {T} to"
                            f" {P} is in its sandbox without its"
                            f" {' and '.join(gone)}, so what a change to a"
                            f" {P} row does to its {T} rows was not"
                            " probed"))
    return out, unread


def _skipped(p, lost):
    if p.about in lost:
        return lost[p.about]
    if p.about[:1] == ("parent",):
        return lost.get(("foreign key",) + p.about[1:])
    if p.about[:1] == ("check",) and ("check",) in lost:
        return lost[("check",)]
    return None


# ---- running a probe, and what it answered ----------------------------------


def _plain(engine, outcome):
    """An outcome as compared here: without the counters - how far a
    sequence moved is the counters' proof. (A MySQL refusal sent with a
    class that says only "an error" has its engine's class already, from
    the harness: `harness.MYSQL_STATES`.)"""
    return harness.Outcome(error=outcome.error, changes=outcome.changes)


def _immediate(runner, statement, defers):
    """A statement run with the side's deferred constraints checked at its
    end: a probe's transaction is one statement, and what a commit after it
    would refuse is what it refuses."""
    if not defers:
        return statement
    if runner.engine == "postgres":
        return "set constraints all immediate; " + statement
    if runner.engine == "oracle":
        return ("begin execute immediate 'set constraints all immediate'; "
                f"{statement}; end;")
    return statement


def _run(bench, p, defers):
    x = bench.a.run(_immediate(bench.a, p.a[0], defers[0]), p.a[1], p.watch)
    y = bench.b.run(_immediate(bench.b, p.b[0], defers[1]), p.b[1], p.watch)
    return _plain(bench.a.engine, x), _plain(bench.b.engine, y)


def _judge(x, y):
    """'alike', 'unclassed' (refused on both, one side's error with no class
    to compare) or 'differ'."""
    found = harness.differences(x, y)
    if not found:
        return "alike"
    if x.error is not None and y.error is not None and \
            all(f.startswith("error:") for f in found):
        ka, kb = harness.error_key(x.error), harness.error_key(y.error)
        if ka[0][:2] in harness.UNKNOWN_CLASSES or \
                kb[0][:2] in harness.UNKNOWN_CLASSES:
            return "unclassed"
    return "differ"


def _did(outcome):
    parts = []
    for t in sorted(outcome.changes):
        c = outcome.changes[t]
        leaf = str(t).split(".")[-1]
        for mark, verb, prep in (("+", "added", "to"), ("-", "removed", "of"),
                                 ("~", "changed", "of")):
            n = len(c.get(mark) or [])
            if n:
                parts.append(f"{verb} {n} row{'s' if n != 1 else ''} {prep}"
                             f" {leaf}")
    return ", ".join(parts) or "changed nothing"


def _shown(v):
    if v is None:
        return "NULL"
    v = str(v)
    return "'" + (v if len(v) <= 40 else v[:37] + "...") + "'"


def _apart(bench, x, y):
    """How two accepted outcomes differ, in words."""
    da, db_ = _did(x), _did(y)
    if da != db_:
        return f"the source {da} and the target {db_}"
    for t in sorted(set(x.changes) | set(y.changes)):
        cx, cy = x.changes.get(t) or {}, y.changes.get(t) or {}
        names = [c for c, _ in bench.a.watched[t].columns]
        for mark in ("+", "~"):
            for rx, ry in zip(cx.get(mark) or [], cy.get(mark) or []):
                if mark == "~":
                    rx, ry = rx[1], ry[1]
                for k, (u, v) in enumerate(zip(rx, ry)):
                    if u != v and k < len(names):
                        return (f"{str(t).split('.')[-1]}.{names[k]} was"
                                f" stored as {_shown(u)} on the source and"
                                f" {_shown(v)} on the target")
    found = harness.differences(x, y)
    return found[0] if found else "they wrote different rows"


def _sentence(bench, p, x, y):
    """A probe the two sides answered differently, as the verdict says
    it."""
    if x.error is not None and y.error is None:
        how = f"refused on the source ({x.error[0]}) and accepted on the" \
              " target" + (f", which {_did(y)}" if p.writes else "")
    elif x.error is None and y.error is not None:
        how = "accepted on the source" + (
            f", which {_did(x)}," if p.writes else "") + \
            f" and refused on the target ({y.error[0]})"
    elif x.error is not None:
        how = (f"refused on both sides, with {x.error[0]} on the source and"
               f" {y.error[0]} on the target")
    else:
        how = f"accepted on both sides, but {_apart(bench, x, y)}"
    return f"{p.what}: {how}"


def _unkept(ctx, side):
    """What a side does not enforce of what it declares, in words."""
    rules = ctx.src if side == "source" else ctx.dst
    said = [rules.no_checks] if rules.no_checks else []
    said += [f"its CHECK {c.name} is not enforced there"
             for c in rules.checks if not c.enforced]
    said += [f"its foreign key {f.name} is disabled there"
             for f in rules.foreign if not f.enforced]
    return "; ".join(said)


def _excuse(ctx, p, x, y):
    """Why the side that accepted did, where it declares and does not
    enforce what the other refused by."""
    if p.kind not in ("check", "foreign key"):
        return ""
    side = "source" if x.error is None and y.error is not None else \
        "target" if y.error is None and x.error is not None else None
    if side is None:
        return ""
    why = _unkept(ctx, side)
    return f" (on the {side}, {why})" if why else ""


# ---- the sessions the probes run in -----------------------------------------


_CACHE = weakref.WeakKeyDictionary()


def _cache(bench):
    got = _CACHE.get(bench)
    if got is None:
        got = _CACHE[bench] = {"tables": {}, "modes": {}}
    return got


class _Sessions:
    """Each MySQL sandbox session held to the sql_mode its own server
    writes under, for as long as the probes run, and set back after: a
    lax source cuts a text too long to fit and takes the row, a strict
    one refuses it, and the sandbox's own mode is a container's default,
    not the source's. The global mode is the one an application meets
    unless it sets its own."""

    def __init__(self, bench):
        self.bench = bench
        self.kept = []
        self.short = []

    def __enter__(self):
        bench, pair = self.bench, self.bench.pair
        modes = _cache(bench)["modes"]
        for runner, eng, side, who in ((bench.a, pair.src_engine, "src",
                                        "source"),
                                       (bench.b, pair.dst_engine, "dst",
                                        "target")):
            if runner.engine != "mysql":
                continue
            try:
                if side not in modes:
                    modes[side] = str(eng._q(side, "select"
                                                   " @@global.sql_mode")
                                      [0][0])
                with runner.conn.cursor() as cur:
                    cur.execute("select @@session.sql_mode")
                    was = cur.fetchone()[0]
                    cur.execute("set session sql_mode = %s", (modes[side],))
                runner.conn.commit()
                self.kept.append((runner, was))
            except Exception as e:  # noqa: BLE001 - said, kept from `ok`
                self.short.append(
                    f"the {who}'s own sql_mode could not be read, so the"
                    " probes ran under its sandbox's:"
                    f" {(str(e).strip().splitlines() or [''])[0][:80]}")
        return self

    def __exit__(self, *exc):
        for runner, was in self.kept:
            try:
                with runner.conn.cursor() as cur:
                    cur.execute("set session sql_mode = %s", (was,))
                runner.conn.commit()
            except Exception:  # noqa: BLE001 - a session closing anyway
                pass


def _context(bench, t):
    tables = _cache(bench)["tables"]
    if t not in tables:
        if t not in bench.tables:
            raise NotProbed(f"{t} is not a table both sandboxes hold")
        tables[t] = _Table(bench, t)
    return tables[t]


def _families(contexts):
    """family(t): `t` and every fixture table its rows' deletes may reach
    through the foreign keys of either side."""
    children = {}
    for t, ctx in contexts.items():
        for _, parent, _ in ctx.fks:
            if parent is not None and parent in contexts:
                children.setdefault(parent, set()).add(t)

    def family(t):
        out, todo = [t], [t]
        while todo:
            for c in sorted(children.get(todo.pop(), ())):
                if c not in out:
                    out.append(c)
                    todo.append(c)
        return out
    return family


# ---- entry points -----------------------------------------------------------


def probe_row(bench, table, values):
    """(the source's outcome, the target's, what it was) of one insert into
    `table` (the source's name) of a fixture row both sides hold, its
    unique columns made new and the columns in `values` ({source column:
    value}) set - run on both sandboxes, each rolled back. Outcomes as
    compared here (`harness.differences` takes them). NotProbed where no
    such row can be made."""
    try:
        ctx = _context(bench, table)
    except NotProbed:
        raise
    except Exception as e:  # noqa: BLE001 - said in the caller's words
        raise NotProbed(f"the constraints of {table} could not be read:"
                        f" {_first_line(e)}") from e
    if ctx.row is None:
        raise NotProbed(f"{ctx.label} has no row both sides hold in the"
                        " fixture")
    if ctx.src.untaken or ctx.dst.untaken:
        raise NotProbed(f"{ctx.label}: {ctx.src.untaken or ctx.dst.untaken}")
    over = {}
    for name, v in dict(values).items():
        i = ctx.at_src.get(str(name).lower())
        if i is None:
            raise NotProbed(f"{name} is not a column both sides of"
                            f" {ctx.label} hold")
        over[i] = v
    made = ctx.fresh(0, fixed=set(over))
    if made is None:
        raise NotProbed(f"migkit could not make a value for the unique"
                        f" columns of {ctx.label} that no row holds")
    made = ctx.fit(made, set(made) - set(over))
    what = f"an insert into {ctx.label} with " + ", ".join(
        f"{ctx.names[i]} = {_said(v)}" for i, v in over.items())
    p = ctx.probe("limit", what, [{**made, **over}], ("probe",))
    with _Sessions(bench):
        x, y = _run(bench, p, (ctx.defers("source"), ctx.defers("target")))
    return x, y, what


def prove(bench):
    """The `constraints` line of the schema check, for the database the
    bench holds: [Result] - one - or [] where the fixture has no table."""
    from ..engines.base import Result
    if not bench.tables:
        return []
    db = bench.db
    contexts, short = {}, []
    for t in sorted(bench.tables):
        try:
            contexts[t] = _context(bench, t)
        except Exception as e:  # noqa: BLE001 - said, kept from `ok`
            short.append(f"{str(t).split('.')[-1]}: its constraints could"
                         f" not be read: {_first_line(e)}")
    family = _families(contexts)
    defers = (any(c.defers("source") for c in contexts.values()),
              any(c.defers("target") for c in contexts.values()))
    counts, diffs, said = {}, [], []
    unclassed, probed_tables = 0, 0
    with _Sessions(bench) as sessions:
        short += sessions.short
        for t, ctx in contexts.items():
            T = ctx.label
            short += ctx.short
            if ctx.row is None:
                short.append(f"{T} has no row both sides hold in the"
                             " fixture, so none of its constraints was"
                             " probed")
                continue
            untaken = ctx.src.untaken or ctx.dst.untaken
            if untaken:
                short.append(f"{T} was not probed: {untaken}")
                continue
            got, unread = _lost(ctx)
            short += unread
            lost = {}
            for about, words in got:
                lost.setdefault(about, words)
                short.append(words)
            probes, why = _probes(ctx, contexts, family)
            short += why
            said += ctx.said
            refused = {"source": [], "target": []}
            ran, blocked = 0, not probes
            for p in probes:
                if _skipped(p, lost) is not None:
                    continue
                try:
                    x, y = _run(bench, p, defers)
                except Exception as e:  # noqa: BLE001 - said, not compared
                    for runner in (bench.a, bench.b):
                        try:
                            runner.conn.rollback()
                        except Exception:  # noqa: BLE001 - closed already
                            pass
                    short.append(f"{p.what}: the sandbox could not run it:"
                                 f" {_first_line(e)}")
                    if p.kind == "plain":
                        blocked = True
                        break
                    continue
                ran += 1
                counts[p.kind] = counts.get(p.kind, 0) + 1
                for side, o in (("source", x), ("target", y)):
                    if o.error is not None:
                        refused[side].append(str(o.error[1] or "").lower())
                verdict = _judge(x, y)
                if verdict == "differ":
                    diffs.append((p, _sentence(bench, p, x, y)
                                  + _excuse(ctx, p, x, y)))
                elif verdict == "unclassed":
                    unclassed += 1
                elif p in ctx.disagree:
                    said.append(f"the CHECKs of {T} were solved to disagree"
                                f" on {p.what.split(' with ', 1)[-1]}, and"
                                " the two sides answered it alike")
                if p.kind == "plain" and (x.error is not None
                                          or y.error is not None):
                    # a plain insert one side takes and the other refuses
                    # is a difference, said above; refused by both, it
                    # leaves no row to build the table's other probes on
                    if verdict != "differ":
                        short.append(_blocked(p, x, y, T))
                    blocked = True
                    break
            if ran:
                probed_tables += 1
            if not blocked:
                short += _unreached(ctx, refused, lost)
    total = sum(counts.values())
    tally = ", ".join(f"{counts[k]} {label}" for k, label in KINDS
                      if counts.get(k))
    if diffs:
        status = "diff"
        first_p, detail = diffs[0]
        if len(diffs) > 1:
            detail += (f"; {len(diffs) - 1} more of the {total} probes"
                       " answered differently")
            groups, seen = [], {(first_p.table, first_p.about)}
            for p, _ in diffs[1:]:
                if (p.table, p.about) not in seen:
                    seen.add((p.table, p.about))
                    groups.append(p.what)
            if groups:
                detail += ", among them: " + "; ".join(groups[:6])
                if len(groups) > 6:
                    detail += f"; and {len(groups) - 6} more"
        hint = ("make the target's constraints accept and refuse what the"
                " source's do - the probe named is the row to try - then"
                " check again")
    else:
        status = "warn" if short or unclassed or not total else "ok"
        detail = (f"{total} probes over {probed_tables} tables answered"
                  f" alike" + (f": {tally}" if tally else ""))
        hint = "" if status == "ok" else (
            "what is named was not probed: give the fixture the rows it"
            " needs, or hold it by hand, then check again")
    if unclassed:
        detail += (f"; {unclassed} probes were refused on both sides with an"
                   " error one side gives no class to compare by")
    if short:
        detail += "; not probed: " + "; ".join(short[:6])
        if len(short) > 6:
            detail += f"; and {len(short) - 6} more"
    if said:
        detail += "; " + "; ".join(said[:4])
    return [Result("schema", f"{db} constraints", status, detail, "", hint)]


def _blocked(p, x, y, T):
    where = "both sides" if x.error is not None and y.error is not None \
        else "the source" if x.error is not None else "the target"
    return (f"{p.what} was refused on {where} ({(x.error or y.error)[0]}),"
            f" and with no row both sides take nothing else of {T} was"
            " probed")


def _unreached(ctx, refused, lost):
    """[words] for each enforced CHECK that refused none of the probes on a
    side that has it, and that the solver did not show refuses nothing
    within its columns' declarations. A server's refusal names the CHECK
    that refused."""
    if ("check",) in lost:
        return []
    out = []
    for side, c in ctx.checks:
        if not c.enforced or (side, c.name) in ctx.never:
            continue
        if not any(c.name.lower() in m for m in refused[side]):
            out.append(f"the {side}'s CHECK"
                       f" {ctx.own_name.get((side, c.name), c.name)} on"
                       f" {ctx.label} ({_short(c.text)}) refused none of the"
                       " probes")
    return out

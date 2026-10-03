"""What a routine is called with in the proof (backlog R11).

Four sources, in the order they are tried, all passed as parameters (never
spliced into SQL, so both engines are handed the same value):

1. per class, the values where engines are known to part: NULL, zero, the
   type's edges, a decimal that rounds, text that differs only by case or a
   trailing space, a character wider than a byte, a leap day;
2. the constants in the routine's own body and their neighbours - `= 6`
   gives 5, 6 and 7, `'CLOSED'` gives `'closed'` and `'CLOSED '`: each
   comparison is tried below, on and above its boundary;
3. for a parameter the body compares with a column, that column's values
   in the fixture, and one it does not hold;
4. search: values drawn by class and kept when they reach code the calls
   before them did not (`prove` drives it, with coverage as the score).

Every input that ever told two versions of a routine apart is kept
(`Bank`) and tried first on every later version, so a candidate that brings
back an old difference is caught by the input that found it.
"""
import datetime as dt
import itertools
import json
import random
import re
from decimal import Decimal

#: per class, what every routine is tried with
POOLS = {
    "integer": [None, 0, 1, -1, -7, 42, 2147483647, -2147483648],
    "decimal": [None, Decimal("0"), Decimal("1.555"), Decimal("-2.25"),
                Decimal("0.005"), Decimal("12345.678")],
    "float": [None, 0.0, 1.5, -2.75, 1e-07],
    "text": [None, "", "a", "A", "a ", " a", "Ab c", "é", "O'Brien"],
    "boolean": [None, 0, 1],
    "date": [None, dt.date(2024, 2, 29), dt.date(1999, 12, 31),
             dt.date(1970, 1, 1)],
    "timestamp": [None, dt.datetime(2024, 2, 29, 23, 59, 59),
                  dt.datetime(2000, 1, 1),
                  dt.datetime(1999, 12, 31, 12, 30, 15, 123456)],
    "time": [None, dt.time(0, 0), dt.time(23, 59, 59)],
    "bytes": [None, b"", b"\x00\xff"],
    "json": [None, '{"a": 1}', "[]"],
}
#: calls per routine from the fixed sources, at most
CALLS = 200


def klass(engine, declared):
    from .. import canon
    return canon.type_class(engine, declared) if declared else None


def constants(text, dialect):
    """(numbers, strings) written in a body, as its own dialect reads
    them."""
    from sqlglot import tokens
    from sqlglot.dialects.dialect import Dialect
    nums, strs = set(), set()
    try:
        toks = Dialect.get_or_raise(dialect).tokenize(text or "")
    except Exception:  # noqa: BLE001 - nothing to learn from it
        return nums, strs
    for t in toks:
        if t.token_type == tokens.TokenType.NUMBER:
            try:
                nums.add(Decimal(t.text))
            except Exception:  # noqa: BLE001 - not a plain number
                pass
        elif t.token_type in (tokens.TokenType.STRING,
                              tokens.TokenType.NATIONAL_STRING):
            strs.add(t.text)
    return nums, strs


def from_constants(cls, nums, strs):
    """A class's values from a body's constants and their neighbours."""
    out = []
    if cls == "integer":
        for n in nums:
            if n == n.to_integral_value():
                out += [int(n) - 1, int(n), int(n) + 1]
    elif cls in ("decimal", "float"):
        for n in nums:
            step = Decimal(1).scaleb(n.as_tuple().exponent) \
                if n.as_tuple().exponent < 0 else Decimal(1)
            out += [n - step, n, n + step]
        if cls == "float":
            out = [float(v) for v in out]
    elif cls == "text":
        for s in strs:
            out += [s, s.lower(), s.upper(), s + " "]
    elif cls in ("date", "timestamp"):
        for s in strs:
            try:
                got = dt.datetime.fromisoformat(s)
            except ValueError:
                continue
            out += [got.date() if cls == "date" else got]
    return out


def compared_columns(text, names):
    """{parameter: {column}} for each parameter the body compares with a
    column: `col = p`, `p <= t.col`, `p in (select col ...)` is not."""
    out = {}
    ops = r"(?:=|<>|!=|<=|>=|<|>|\blike\b)"
    for p in names:
        pat = re.escape(p)
        for m in re.finditer(rf"(?i)([A-Za-z_][\w.`\"]*)\s*{ops}\s*"
                             rf"[@:]?{pat}\b", text or ""):
            out.setdefault(p, set()).add(m.group(1).strip('`"'))
        for m in re.finditer(rf"(?i)(?<![\w.]){pat}\s*{ops}\s*"
                             rf"([A-Za-z_][\w.`\"]*)", text or ""):
            out.setdefault(p, set()).add(m.group(1).strip('`"'))
    for p in list(out):
        out[p] = {c for c in out[p] if c.split(".")[-1].lower() not in
                  {n.lower() for n in names} and not c.lower().startswith(
                      ("new.", "old."))}
    return out


def missing_value(cls, seen):
    """A value of `cls` the column does not hold."""
    if cls == "integer":
        return max([v for v in seen if isinstance(v, int)] or [0]) + 1000
    if cls in ("decimal", "float"):
        top = max([Decimal(str(v)) for v in seen if v is not None] or
                  [Decimal(0)]) + 1000
        return float(top) if cls == "float" else top
    if cls == "text":
        return "zz-not-there"
    return None


def combine(pools, most=CALLS, seed=0):
    """Argument tuples over per-parameter pools: every combination where
    there are no more than `most`, else every value of every parameter at
    least once and the rest drawn with a fixed seed, so two runs try the
    same calls."""
    if not pools:
        return [()]
    total = 1
    for p in pools:
        total *= max(1, len(p))
    if total <= most:
        return [tuple(c) for c in itertools.product(*pools)]
    out, seen = [], set()
    widest = max(len(p) for p in pools)
    for i in range(widest):
        one = tuple(p[i % len(p)] for p in pools)
        if repr(one) not in seen:
            seen.add(repr(one))
            out.append(one)
    rng = random.Random(seed)
    while len(out) < most:
        one = tuple(rng.choice(p) for p in pools)
        if repr(one) not in seen:
            seen.add(repr(one))
            out.append(one)
    return out


def strategy(cls):
    """Hypothesis' strategy for a class - the search's draws."""
    from hypothesis import strategies as st
    none = st.none()
    if cls == "integer":
        return none | st.integers(-2 ** 31, 2 ** 31 - 1)
    if cls == "decimal":
        return none | st.decimals(min_value=-10 ** 6, max_value=10 ** 6,
                                  places=3, allow_nan=False,
                                  allow_infinity=False)
    if cls == "float":
        return none | st.floats(-1e9, 1e9, allow_nan=False,
                                allow_infinity=False)
    if cls == "text":
        return none | st.text(alphabet=st.sampled_from(
            list("aAbBzZ 0-_'éß")), max_size=12)
    if cls == "boolean":
        return none | st.sampled_from([0, 1])
    if cls == "date":
        return none | st.dates(dt.date(1900, 1, 1), dt.date(2100, 12, 31))
    if cls == "timestamp":
        return none | st.datetimes(dt.datetime(1900, 1, 1),
                                   dt.datetime(2100, 12, 31)).map(
            lambda d: d.replace(microsecond=0))
    return none


# ---- the counterexample bank ------------------------------------------------


def _dump(value):
    if isinstance(value, Decimal):
        return {"decimal": str(value)}
    if isinstance(value, dt.datetime):
        return {"timestamp": value.isoformat()}
    if isinstance(value, dt.date):
        return {"date": value.isoformat()}
    if isinstance(value, dt.time):
        return {"time": value.isoformat()}
    if isinstance(value, (bytes, bytearray)):
        return {"bytes": bytes(value).hex()}
    return value


def _load(value):
    if isinstance(value, dict) and len(value) == 1:
        (k, v), = value.items()
        return {"decimal": Decimal, "timestamp": dt.datetime.fromisoformat,
                "date": dt.date.fromisoformat, "time": dt.time.fromisoformat,
                "bytes": bytes.fromhex}.get(k, lambda x: value)(v)
    return value


class Bank:
    """Every input that ever told two versions of a routine apart, per
    routine and per construct, kept beside the hop's reports: the memory
    a search loop needs so that a later version cannot bring an old
    difference back unnoticed."""

    def __init__(self, path):
        self.path = path
        try:
            self.data = json.loads(open(path).read()) if path and \
                path.exists() else {}
        except (OSError, ValueError):
            self.data = {}

    def inputs(self, routine_key, constructs=(), shape=None):
        """The kept inputs for this routine, then those of routines with
        the same constructs and the same parameter classes."""
        out = []
        for args in self.data.get(f"routine:{routine_key}", []):
            out.append(tuple(_load(a) for a in args))
        for code in constructs:
            for kept in self.data.get(f"construct:{code}", []):
                if kept.get("shape") == shape:
                    out.append(tuple(_load(a) for a in kept["args"]))
        return out

    def keep(self, routine_key, args, constructs=(), shape=None):
        dumped = [_dump(a) for a in args]
        mine = self.data.setdefault(f"routine:{routine_key}", [])
        if dumped not in mine:
            mine.append(dumped)
        for code in constructs:
            kept = self.data.setdefault(f"construct:{code}", [])
            one = {"shape": shape, "args": dumped}
            if one not in kept:
                kept.append(one)

    def keep_sequence(self, calls):
        """A sequence of calls [(routine name, args)] that told the two
        sides apart (`sequences`)."""
        one = [[name, [_dump(a) for a in args]] for name, args in calls]
        kept = self.data.setdefault("sequences", [])
        if one not in kept:
            kept.append(one)

    def sequences(self):
        return [[(name, tuple(_load(a) for a in args)) for name, args in one]
                for one in self.data.get("sequences", [])]

    def save(self):
        if not self.path:
            return
        from .. import durable
        durable.write_atomic(self.path, json.dumps(self.data, indent=1,
                                                   sort_keys=True))
